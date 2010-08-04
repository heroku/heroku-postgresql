module HerokuPostgresql
  class Client
    Version = 2

    def initialize(database_user, database_password, database_name)
      @heroku_postgresql_host = ENV["HEROKU_POSTGRESQL_HOST"] || "https://shogun.heroku.com"
      @database_user = database_user
      @database_password = database_password
      @database_name = database_name
      @heroku_postgresql_resource = RestClient::Resource.new(
        "#{@heroku_postgresql_host}/client/databases",
        :user => @database_user,
        :password => @database_password,
        :headers => {:heroku_client_version => Version})
    end

    def ingress
      http_put("#{@database_name}/ingress")
    end

    def get_database
      http_get(@database_name)
    end

    def create_backup(backup_name)
      http_post("#{@database_name}/backups", {:name => backup_name})
    end

    def get_backup_recent
      http_get("#{@database_name}/backups/recent")
    end

    def get_backup(backup_name)
      http_get("#{@database_name}/backups/#{backup_name}")
    end

    def get_backups
      http_get("#{@database_name}/backups")
    end

    def create_restore(restore_param)
      http_post("#{@database_name}/restores", restore_param)
    end

    def get_restore(restore_id)
      http_get("#{@database_name}/restores/#{restore_id}")
    end

    protected

    def sym_keys(c)
      if c.is_a?(Array)
        c.map { |e| sym_keys(e) }
      else
        c.inject({}) do |h, (k, v)|
          h[k.to_sym] = v; h
        end
      end
    end

    def checking_client_version
      begin
        yield
      rescue RestClient::BadRequest => e
        if JSON.parse(e.response.to_s)["error"] =~ /not using current client version/
          abort("A new version of the heroku-postgresql plugin is available\n" +
                "Upgrade with `heroku plugins install git://github.com/heroku/heroku-postgresql.git`")
        else
          raise e
        end
      end
    end

    def http_get(path)
      checking_client_version do
        sym_keys(JSON.parse(@heroku_postgresql_resource[path].get.to_s))
      end
    end

    def http_post(path, payload = {})
      checking_client_version do
        sym_keys(JSON.parse(@heroku_postgresql_resource[path].post(payload.to_json).to_s))
      end
    end

    def http_put(path, payload = {})
      checking_client_version do
        sym_keys(JSON.parse(@heroku_postgresql_resource[path].put(payload.to_json).to_s))
      end
    end
  end
end
