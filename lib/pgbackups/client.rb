module PGBackups
  Version = 1

  class Client
    def initialize(uri)
      @uri = URI.parse(uri)
    end

    def authenticated_resource(path)
      host = "#{@uri.scheme}://#{@uri.host}"
      host += ":#{@uri.port}" if @uri.port
      RestClient::Resource.new("#{host}#{path}",
        :user     => @uri.user,
        :password => @uri.password,
        :headers  => {:heroku_client_version => Version}
      )
    end

    def create_transfer(from_url, to_url, opts={})
      resource = authenticated_resource("/client/transfers")
      params = {:from_url => from_url, :to_url => to_url}.merge opts
      JSON.parse resource.post(params).body
    end

    def get_transfers
      resource = authenticated_resource("/client/transfers")
      JSON.parse resource.get.body
    end

    def get_transfer(id)
      resource = authenticated_resource("/client/transfers/#{id}")
      JSON.parse resource.get.body
    end

    def get_backups(opts={})
      resource = authenticated_resource("/client/backups")
      params = !opts[:latest] ? {} : {:latest => true}
      JSON.parse resource.get(:params => params).body
    end

    def get_backup(name, opts={})
      name = URI.escape(name)
      resource = authenticated_resource("/client/backups/#{name}")
      JSON.parse resource.get.body
    end

    def delete_backup(name)
      name = URI.escape(name)
      begin
        resource = authenticated_resource("/client/backups/#{name}")
        resource.delete.body
        true
      rescue RestClient::ResourceNotFound => e
        false
      end
    end
  end
end