module HerokuPGBackup
  Version = 1

  class Client
    def initialize(uri)
      @uri = URI.parse(uri)
    end

    def authenticated_resource(path)
      host = "#{@uri.scheme}://#{@uri.host}"
      host += ":#{@uri.port}" if @uri.port
      RestClient::Resource.new("#{host}#{path}",
        :user => @uri.user,
        :password => @uri.password,
        :headers => {:heroku_client_version => Version}
      )
    end

    def create_transfer(from_url, to_url, opts={})
      resource = authenticated_resource("/client/transfers")
      params = {:from_url => from_url, :to_url => to_url}.merge opts
      JSON.parse resource.post params
    end

    def get_transfer(id)
      resource = authenticated_resource("/client/transfers/#{id}")
      JSON.parse resource.get
    end
  end
end