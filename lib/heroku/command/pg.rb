module Heroku::Command
  class Pg < BaseWithApp
    Help.group("heroku-postgresql") do |group|
      group.command "pg:info",   "show database status"
      group.command "pg:wait",   "wait for database to come online"
      group.command "pg:attach", "use heroku-postgresql database as DATABASE_URL"
      group.command "pg:detach", "revert to using shared Postgres database"
      group.command "pg:psql",   "open a psql shell to the database"

      group.command "pg:backup",            "capture a pgdump backup"
      group.command "pg:backup_url <name>", "get download URL for a pgdump"
      group.command "pg:backups",           "list backups"
      group.command "pg:restore <name>",    "restore from a pgdump backup"
      group.command "pg:restore <url>",     "restore from a pgdump at the given url"
    end

    def initialize(*args)
      super
      @config_vars =  heroku.config_vars(app)
      @heroku_postgresql_url = ENV["HEROKU_POSTGRESQL_URL"] ||
                               @config_vars["HEROKU_POSTGRESQL_URL"]
      @database_url = @config_vars["DATABASE_URL"]
      if !@heroku_postgresql_url
        abort("The addon is not installed for the app #{app}")
      end
      uri = URI.parse(@heroku_postgresql_url.gsub("_", "-"))
      @database_user =     uri.user
      @database_password = uri.password
      @database_host =     uri.host
      @database_name =     uri.path[1..-1]
    end

    def info
      database = heroku_postgresql_client.get_database
      display("=== #{app} heroku-postgresql database")

      display_info("State",
        "#{database[:state]} for " +
        "#{delta_format(Time.parse(database[:state_updated_at]))}")

      if database[:num_bytes] && database[:num_tables]
        display_info("Data size",
          "#{size_format(database[:num_bytes])} in " +
          "#{database[:num_tables]} table#{database[:num_tables] == 1 ? "" : "s"}")
      end

      if @heroku_postgresql_url && !(@heroku_postgresql_url =~ /NOT.READY/)
        display_info("URL", @heroku_postgresql_url)
      end

      #if version = database[:postgresql_version]
        display_info("PG version", "8.4.4")
      #end

      display_info("Born", time_format(database[:created_at]))
    end

    def wait
      ticking do |ticks|
        database = heroku_postgresql_client.get_database
        state = database[:state]
        if state == "running"
          redisplay("The database is now ready", true)
          break
        elsif state == "destroyed"
          redisplay("The database has been destroyed", true)
          break
        elsif state == "failed"
          redisplay("The database encountered an error", true)
          break
        else
          redisplay("#{state.capitalize} database #{spinner(ticks)}", false)
        end
      end
    end

    def attach
      database = heroku_postgresql_client.get_database
      if @database_url == @heroku_postgresql_url
        display("The database is already attached to app #{app}")
      elsif database[:state] != "running"
        display("The database is not running")
      else
        display("Attatching database to app #{app} ... ", false)
        res = heroku.add_config_vars(app, {"DATABASE_URL" => @heroku_postgresql_url})
        display("done")
      end
    end

    def detach
      if @database_url.nil?
        display("A heroku-postgresql database is not attached to app #{app}")
      elsif @database_url != @heroku_postgresql_url
        display("Database attached to app #{app} is not a heroku-postgresql database")
      else
        display("Detatching database from app #{app} ... ", false)
        res = heroku.remove_config_var(app, "DATABASE_URL")
        display("done")
      end
    end

    def psql
      if !has_psql?
        display("You do not have the psql command line tool installed")
      else
        database = heroku_postgresql_client.get_database
        if database[:state] == "running"
          display("Connecting to database for app #{app} ...")
          heroku_postgresql_client.ingress
          ENV["PGPASSWORD"] = @database_password
          cmd = "psql -U #{@database_user} -h #{@database_host} #{@database_name}"
          system(cmd)
        else
          display("The database is not running")
        end
      end
    end

    def backup
      database = heroku_postgresql_client.get_database
      backup_name = timestamp_name
      display("Capturing backup #{backup_name} of #{size_format(database[:num_bytes])} database for app #{app}")
      backup = heroku_postgresql_client.create_backup(backup_name)
      backup_id = backup[:id]
      ticking do |ticks|
        backup = heroku_postgresql_client.get_backup(backup_name)
        display_progress(backup[:progress], ticks)
        if backup[:finished_at]
          display("Backup complete")
          break
        elsif backup[:error_at]
          display("\nAn error occured while capturing the backup\n" +
                    "Your database was not affected")
          break
        end
      end
    end

    def backup_url
      backup_name = (args.first && args.first.strip) ||
                    abort("No backup name supplied")
      backup = heroku_postgresql_client.get_backup(backup_name)
      if backup[:finished_at]
        display(backup[:dump_url])
      elsif backup[:error_at]
        display("This backup did not complete successfully")
      else
        display("This backup has not yet completed")
      end
    end

    def backups
      backups = heroku_postgresql_client.get_backups
      valid_backups = backups.select { |b| !b[:error_at] }
      if backups.empty?
        display("App #{app} has no database backups")
      else
        name_width = backups.map { |b| b[:name].length }.max
        backups.sort_by { |b| b[:started_at] }.each do |b|
          state =
            if b[:finished_at]
              size_format(b[:size_compressed])
            elsif prog = b[:progress]
              "#{prog.last.first.capitalize}ing"
            else
              "Pending"
            end
          display(format("%-#{name_width}s  %s", b[:name], state))
        end
      end
    end

    def restore
      dump_arg = (args.first && args.first.strip) ||
                  abort("No pgdump name or url supplied")
      if (dump_arg =~ /^http.*sql\.gz/)
        display("Restoring database for app #{app} from #{dump_arg}")
        restore_with(:dump_url => dump_arg)
      else
        display("Restoring database for app #{app} from backup #{dump_arg}")
        restore_with(:backup_name => dump_arg)
      end
    end

    protected

    def restore_with(restore_param)
      restore = heroku_postgresql_client.create_restore(restore_param)
      restore_id = restore[:id]
      ticking do |ticks|
        restore = heroku_postgresql_client.get_restore(restore_id)
        display_progress(restore[:progress], ticks)
        if restore[:finished_at]
          display("Restore complete")
          break
        elsif restore[:error_at]
          display("\nAn error occured while restoring the backup")
          break
        end
      end
    end

    def heroku_postgresql_client
      ::HerokuPostgresql::Client.new(
        @database_user, @database_password, @database_name)
    end

    def ticking
      ticks = 0
      loop do
        yield(ticks)
        ticks +=1
        sleep 1
      end
    end

    def spinner(ticks)
      %w(/ - \\ |)[ticks % 4]
    end

    def redisplay(line, line_break = false)
      display("\r\e[0K#{line}", line_break)
    end

    def display_info(label, info)
      display(format("%-12s %s", label, info))
    end

    def display_progress_part(part, ticks)
      task, amount = part
      if amount == "start"
        redisplay(format("%-10s ... %s", task.capitalize, spinner(ticks)))
        @last_amount = 0
      elsif amount.is_a?(Fixnum)
        redisplay(format("%-10s ... %s  %s", task.capitalize, size_format(amount), spinner(ticks)))
        @last_amount = amount
      elsif amount == "finish"
        redisplay(format("%-10s ... %s, done", task.capitalize, size_format(@last_amount)), true)
      end
    end

    def display_progress(progress, ticks)
      new_progress = ((progress || []) - (@seen_progress || []))
      if !new_progress.empty?
        new_progress.each { |p| display_progress_part(p, ticks) }
      elsif !progress.empty? && progress.last[0] != "finish"
        display_progress_part(progress.last, ticks)
      end
      @seen_progress = progress
    end

    def delta_format(start, finish = Time.now)
      secs = (finish.to_i - start.to_i).abs
      mins = (secs/60).round
      hours = (mins / 60).round
      days = (hours / 24).round
      weeks = (days / 7).round
      months = (weeks / 4.3).round
      years = (months / 12).round
      if years > 0
        "#{years} yr"
      elsif months > 0
        "#{months} mo"
      elsif weeks > 0
        "#{weeks} wk"
      elsif days > 0
        "#{days}d"
      elsif hours > 0
        "#{hours}h"
      elsif mins > 0
        "#{mins}m"
      else
        "#{secs}s"
      end
    end

    KB = 1024
    MB = 1024 * KB
    GB = 1024 * MB

    def size_format(bytes)
      return "#{bytes}B" if bytes < KB
      return "#{(bytes / KB)}KB" if bytes < MB
      return format("%.1fMB", (bytes.to_f / MB)) if bytes < GB
      return format("%.2fGB", (bytes.to_f / GB))
    end

    def time_format(time)
      time = Time.parse(time) if time.is_a?(String)
      time.strftime("%Y-%m-%d %H:%M %Z")
    end

    def timestamp_name
      Time.now.strftime("%Y-%m-%d-%H:%M:%S")
    end

    def has_psql?
      `which psql` != ""
    end
  end
end
