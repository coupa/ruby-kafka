
module Kafka
    module Sasl
        class OAUTHBEARER
            OAUTH2_IDENT = "OAUTHBEARER"
            attr_reader :client_id, :client_secret, :server_url, :token_url

            def initialize(logger:, client_id:, client_secret:, server_url:, token_url:)
                @logger = logger
                @client_id = client_id
                @client_secret = client_secret
                @server_url = server_url
                @token_url = token_url || '/oauth2/token'
            end

            def ident
                OAUTH2_IDENT
            end

            def configured?
                client_id && client_secret && server_url
            end

            def authenticate!(host, encoder, decoder)
                @logger.debug "Authenticating with OAuth2 method"
                require_libraries
                response = token_request
                access_token = parse_and_get_token(response)
                token = construct_oauth_token(access_token)
                encoder.write_bytes(token)
                begin
                    token_to_verify = decoder.bytes
                    unless token_to_verify
                        raise Kafka::Error, "OAuth2 authentication failed."
                    else
                        @logger.info "Authentication successful"
                    end
                rescue EOFError => e
                    raise Kafka::Error, "OAuth2 authentication failed."
                end
            end

            private

            def require_libraries
                begin
                    require 'uri'
                    require 'net/http'
                    require 'json'
                    require 'base64'
                rescue LoadError
                    raise Kafka::Error, "In order to use OAuth2 authentication you need to install the `uri`, 'net/http', 'json', 'base64' gems."
                end
            end
            
            def token_request
                @logger.info "Requesting an access token to OAuth2 Server '#{server_url}' at endpoint '#{token_url}'"
                url = URI(token_endpoint)
                http = Net::HTTP.new(url.host, url.port)
                request = Net::HTTP::Post.new(url)
                request["Content-Type"] = 'application/x-www-form-urlencoded'
                request["Authorization"] = basic_auth_header
                request.body = "grant_type=client_credentials"
                response = http.request(request)
                response
            end

            def token_endpoint
                "#{server_url}#{token_url.start_with?('/') ? token_url : "/#{token_url}"}"
            end

            def basic_auth_header
                'Basic ' + Base64.strict_encode64(client_id + ':' + client_secret)
            end

            def parse_and_get_token(response)
                if response.code != '200'
                    raise Kafka::Error, "OAuth2 authentication failed with = #{response.message}"
                else
                    response_body = JSON.parse(response.read_body)
                    access_token = response_body['access_token']
                    @logger.info "Received valid access token" if access_token
                    access_token
                end
            end

            def construct_oauth_token(access_token)
                "n,,#{[1].pack("C")}auth=Bearer #{access_token}#{[1].pack("C")}#{[1].pack("C")}".force_encoding("utf-8")
            end
        end
    end
end