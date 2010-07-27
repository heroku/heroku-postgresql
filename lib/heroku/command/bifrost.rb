module Heroku::Command
  class Bifrost < BaseWithApp
    Help.group("Bifrost") do |group|
      group.command "bifrost:info",   "show database status"
      group.command "bifrost:wait",   "wait for database to come online"
      group.command "bifrost:attach", "use Bifrost database as DATABASE_URL"
      group.command "bifrost:detach", "revert to using shared Postgres database"
      group.command "bifrost:psql",   "open a psql shell to the database"
    end

    def initialize(*args)
      super
      @config_vars =  heroku.config_vars(app)
      @bifrost_url = ENV["BIFROST_URL"] || @config_vars["BIFROST_URL"]
      @database_url = @config_vars["DATABASE_URL"]
      if !@bifrost_url
        abort("Bifrost is not installed for the app #{app}")
      end
      uri = URI.parse(@bifrost_url)
      @database_user =     uri.user
      @database_password = uri.password
      @database_host =     uri.host
      @database_name =     uri.path[1..-1]
    end

    def info
      database = bifrost_client.get_database(@database_name)
      display("state: #{database[:state]}")
      display("url:   #{@bifrost_url}")
    end

    def wait
      ticks = 0
      loop do
        database = bifrost_client.get_database(@database_name)
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
      if @database_url == @bifrost_url
        display("Bifrost database is already attached to app #{app}")
      else
        display("Attatching Bifrost database to app #{app} ... ", false)
        res = heroku.add_config_vars(app, {"DATABASE_URL" => @bifrost_url})
        display("done")
      end
    end

    def detach
      if @database_url.nil?
        display("Bifrost database is not attached to app #{app}")
      elsif @database_url != @bifrost_url
        display("Database attached to app #{app} is not a Bifrost database")
      else
        display("Detatching Bifrost database from app #{app} ... ", false)
        res = heroku.remove_config_var(app, "DATABASE_URL")
        display("done")
      end
    end

    def psql
      ENV["PGPASSWORD"] = @database_password
      cmd = "psql -U #{@database_user} -h #{@database_host} #{@database_name}"
      display("Connecting to Bifrost database for app #{app} ...")
      system(cmd)
    end

    protected

    def bifrost_client
      ::Bifrost::Client.new(@database_user, @database_password)
    end

    def spinner(ticks)
      %w(/ - \\ |)[ticks % 4]
    end

    def redisplay(line, line_break = false)
      display("\r\e[0K#{line}", line_break)
    end
  end
end
