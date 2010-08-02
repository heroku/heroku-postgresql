module Heroku::Command
  class Pg < BaseWithApp
    Help.group("heroku-postgresql") do |group|
      group.command "pg:info",   "show database status"
      group.command "pg:wait",   "wait for database to come online"
      group.command "pg:attach", "use heroku-postgresql database as DATABASE_URL"
      group.command "pg:detach", "revert to using shared Postgres database"
      group.command "pg:psql",   "open a psql shell to the database"
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
      database = heroku_postgresql_client.get_database(@database_name)
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
      ticks = 0
      loop do
        database = heroku_postgresql_client.get_database(@database_name)
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
          redisplay("#{state} database #{spinner(ticks)}", false)
        end
        ticks += 1
        sleep 1
      end
    end

    def attach
      database = heroku_postgresql_client.get_database(@database_name)
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
        database = heroku_postgresql_client.get_database(@database_name)
        if database[:state] == "running"
          display("Connecting to database for app #{app} ...")
          heroku_postgresql_client.ingress(@database_name)
          ENV["PGPASSWORD"] = @database_password
          cmd = "psql -U #{@database_user} -h #{@database_host} #{@database_name}"
          system(cmd)
        else
          display("The database is not running")
        end
      end
    end

    protected

    def heroku_postgresql_client
      ::HerokuPostgresql::Client.new(@database_user, @database_password)
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
      return "#{(bytes / KB).round}K" if bytes < MB
      return "#{(bytes / MB).round}M" if bytes < GB
      return "#{(bytes / GB).round}G"
    end

    def time_format(time)
      time = Time.parse(time) if time.is_a?(String)
      time.strftime("%Y-%m-%d %H:%M %Z")
    end

    def has_psql?
      `which psql` != ""
    end
  end
end
