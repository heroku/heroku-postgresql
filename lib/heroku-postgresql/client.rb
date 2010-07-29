module HerokuPostgresql
  class Client
    Version = 2

    def initialize(database_user, database_password)
      @heroku_postgresql_host = ENV["HEROKU_POSTGRESQL_HOST"] || "https://shogun.heroku.com"
      @database_user = database_user
      @database_password = database_password
      @heroku_postgresql_resource = RestClient::Resource.new(
        "#{@heroku_postgresql_host}/client",
        :user => @database_user,
        :password => @database_password,
        :headers => {:heroku_client_version => Version})
    end

    def ingress(database_name)
      http_put("databases/#{database_name}/ingress")
    end

    def get_database(database_name)
      http_get("databases/#{database_name}")
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
                "Upgrade with `heroku plugins install ...`")
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
