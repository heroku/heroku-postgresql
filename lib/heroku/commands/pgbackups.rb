require "pty"

module Heroku::Command
  class Pgbackups < BaseWithApp
    include PgUtils

    Help.group("pgbackups") do |group|
      group.command "pgbackups:capture [<DB_ID>]",                "capture a backup from database ID (e.g. DATABASE_URL)"
      group.command "pgbackups:list",                             "list captured backups"
      group.command "pgbackups:info <BACKUP_ID>",                 "list details for backup"
      group.command "pgbackups:download <BACKUP_ID>",             "download a backup"
      group.command "pgbackups:destroy <BACKUP_ID>",              "destroy a backup"
      group.command "pgbackups:restore <BACKUP_ID> --db <DB_ID>", "restore the database ID (e.g. DATABASE_URL) from the specified backup"
      group.command "pgbackups:restore <url> --db <DB_ID>",       "restore the database ID (e.g. DATABASE_URL) from the backup stored at the specified URL"
    end

    def initialize(*args)
      super
      @config_vars = heroku.config_vars(app)
    end

    def pgbackup_client
      url = ENV["PGBACKUPS_URL"] || @config_vars["PGBACKUPS_URL"]
      abort("heroku-pgbackups addon is not installed.") unless url
      @pgbackup_client ||= PGBackups::Client.new(url)
    end

    def pg_config_var_names
      # all config vars that are a postgres:// URL
      pg_config_vars = @config_vars.reject { |k,v| not v =~ /^postgres:\/\// }
      pg_config_vars.keys.sort!
    end

    def resolve_db_id(name, opts={})
      if !name && opts[:default]
        name = opts[:default]
        info = "Backing up the default DB, #{name}."
        info += " (Options are: #{pg_config_var_names.join(', ')})" if pg_config_var_names.length > 2
        display info
      end

      # try to find addon config var name from all config vars
      # if name is 'DATABASE_URL', try to return the addon config var name for better accounting
      addon_config_vars = pg_config_var_names - ["DATABASE_URL"]
      addon_config_vars.each { |n|
        next unless @config_vars[n] == @config_vars[name]
        return n, @config_vars[n]
      }

      # database url isn't an alias for another var
      return name, @config_vars[name] if name == "DATABASE_URL"

      abort("DB #{name} not found in config. (Options are: #{pg_config_var_names.join(', ')})") if name
      abort("DB is required. (Options are: #{pg_config_var_names.join(', ')})") unless opts[:default]
    end

    def capture
      db_id     = args.shift
      backup_id = args.shift

      from_name, from_url = resolve_db_id(db_id, :default => "DATABASE_URL")

      to_name = "BACKUP"
      to_url = nil # server will assign
      to_url = "backup://#{backup_id}" if backup_id

      result = transfer!(from_url, from_name, to_url, to_name)

      to_uri = URI.parse result["to_url"]
      backup_id = File.basename(to_uri.path, '.*')

      abort("Error. Backup not created.") if result["error_at"]
      display("Backup id #{backup_id} created.")
    end

    def restore
      db_id = extract_option("--db")
      to_name, to_url = resolve_db_id(db_id)
      confirm_command
      backup_id = args.shift

      if backup_id =~ /^http(s?):\/\//
        from_url  = backup_id
        from_name = "EXTERNAL_BACKUP"
      else
        if backup_id
          backup = pgbackup_client.get_backup(backup_id)
          abort("Backup #{backup_id} already deleted.") if backup["destroyed_at"]
        else
          backup = pgbackup_client.get_latest_backup
        end

        from_url  = backup["to_url"]
        from_name = "BACKUP"
      end

      display ""
      display_info("App",       @app)
      display_info("Backup",    "Taken from #{backup['from_name']} at #{backup['created_at']}") if backup
      display_info("Database",  db_id)
      display_info("Size",      backup['size']) if backup

      result = transfer!(from_url, from_name, to_url, to_name)

      abort("Error. Restore not successful.") if result["error_at"]
      display("#{db_id} restored.")
    end

    def list
      backups = []
      pgbackup_client.get_transfers.each { |t|
        next unless t['to_name'] == 'BACKUP' && !t['error_at']
        backups << [backup_name(t['to_url']), t['created_at'], t['size'], t['from_name'], ]
      }

      abort("No backups. Capture one with `heroku pg:backup`.") if backups.empty?
      display Display.new.render([["ID", "Backup Time", "Size", "Database"]], backups)
    end

    def info
      if name = args.shift
        b = pgbackup_client.get_backup(name)
      else
        b = pgbackup_client.get_latest_backup
      end

      display "=== Backup #{backup_name(b['to_url'])}"
      display_info("Backup Time",   b["created_at"])
      display_info("Database",      b["from_name"])
      display_info("Size",          b["size"])
      display_info("URL",           b["public_url"])
    end

    def download
      abort("Please install either the 'curl' command line tools") if `which curl` == ""

      @ticks = 0

      backup_id = args.shift
      if backup_id
        backup = pgbackup_client.get_backup(backup_id)
      else
        backup = pgbackup_client.get_latest_backup
      end

      outfile = File.basename(backup["to_url"])
      abort("'#{outfile}' already exists") if File.exists? outfile

      PTY.spawn("curl -o #{outfile} \"#{backup["public_url"]}\"") do |reader, writer, pid|
        output  = ""
        line    = ""
        begin
          while reader.readpartial(4096, output)
            @ticks += 1
            output.each_char do |char|
              if ["\r", "\n", "\r\n"].include? char # newline?
                vals = line.scan(/[0-9.]+[BkMG]/)
                if vals && vals[1]
                  redisplay "Download ... #{vals[1]}B / #{backup['size']} #{spinner(@ticks)}"
                end
                line = ""
              else
                line += char
              end
            end
          end
        rescue Errno::EIO, EOFError => e
          redisplay "Download ... #{backup['size']} / #{backup['size']}, done\n"
        end
      end
    end

    def destroy
      name = args.shift
      abort("Backup name required") unless name
      backup = pgbackup_client.get_backup(name)
      abort("Backup #{name} already deleted.") if backup["destroyed_at"]
      abort("Delete cancelled.")    unless confirm(message="Backup #{name} will be permanently deleted Are you sure (y/N)?")

      result = pgbackup_client.delete_backup(name)
      abort("Backup #{name} deleted.") if result
      abort("Error deleting backup #{name}.")
    end

    protected

    def backup_name(to_url)
      # translate s3://bucket/email/foo/bar.dump => foo/bar
      parts = to_url.split('/')
      parts.slice(4..-1).join('/').gsub(/\.dump$/, '')
    end

    def transfer!(from_url, from_name, to_url, to_name, opts={})
      transfer = pgbackup_client.create_transfer(from_url, to_url, :from_name => from_name, :to_name => to_name)

      display "\n"

      if transfer["errors"]
        abort(transfer["errors"].values.flatten.join("\n") + "\n")
      end

      while true
        update_display(transfer, opts)
        break if transfer["finished_at"]

        sleep 1
        transfer = pgbackup_client.get_transfer(transfer["id"])
      end

      display "\n"

      return transfer
    end

    def update_display(transfer, opts={})
      @ticks            ||= 0
      @last_updated_at  ||= 0
      @last_logs        ||= []
      @last_progress    ||= ["", 0]

      @ticks += 1

      if !transfer["log"]
        @last_progress = ['pending', nil]
        redisplay "Pending ... #{spinner(@ticks)}"
      else
        logs        = transfer["log"].split("\n")
        new_logs    = logs - @last_logs
        @last_logs  = logs

        new_logs.each do |line|
          matches = line.scan /^([a-z_]+)_progress:\s+([^ ]+)/
          next if matches.empty?

          step, amount = matches[0]

          if ['done', 'error'].include? amount
            # step is done, explicitly print result and newline
            redisplay "#{@last_progress[0].capitalize} ... #{@last_progress[1]}, #{amount}\n"
          end

          # store progress, last one in the logs will get displayed
          @last_progress = [step, amount]
        end

        step, amount = @last_progress
        unless ['done', 'error'].include? amount
          redisplay "#{step.capitalize} ... #{amount} #{spinner(@ticks)}"
        end
      end
    end

    class Display
      attr_reader :columns, :rows

      def initialize(columns=nil, rows=nil, opts={})
        @columns = columns
        @rows = rows
        @opts = opts.update(:display_columns => @columns, :display_rows => @rows)
      end

      def render(*data)
        _data = data
        data = DataSource.new(data, @opts)

        # join in grid lines
        lines = []
        data.rows.each { |row|
          lines << row.join(@opts[:delimiter] || " | ")
        }

        # insert header grid line
        if _data.length > 1
          grid_row = data.rows.first.map { |datum| "-" * datum.length }
          grid_line = grid_row.join("-+-")
          lines.insert(1, grid_line)
          lines << "" # trailing newline
        end
        return lines.join("\n")
      end

      class DataSource
        attr_reader :rows, :columns

        def initialize(data, opts={})
          rows = []
          data.each { |d| rows += d }
          columns = rows.transpose

          max_widths = columns.map { |c|
            c.map { |datum| datum.length }.max
          }

          max_widths = [10, 10] if opts[:display_columns]

          @columns = []
          columns.each_with_index { |c,i|
            column = @columns[i] = []
            c.each { |d| column << d.ljust(max_widths[i]) }
          }
          @rows = @columns.transpose
        end
      end
    end
  end
end