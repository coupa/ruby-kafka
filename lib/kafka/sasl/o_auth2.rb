
module Kafka
    module Sasl
        class OAuth2
            OAUTH2_IDENT = "OAUTH2"

            def initialize(client_id:, client_secret:, server_url:, logger:)
                @client_id = client_id
                @client_secret = client_secret
                @server_url = server_url
                @logger = logger
            end

            def ident
                OAUTH2_IDENT
            end

            def configured?
                @client_id && @client_secret
            end

            def authenticate!(host, encoder, decoder)
                load_oauth2
                client = OAuth2::Client.new(@client_id, @client_secret, :site => "#{@server_url}")
                p "client - ", client.inspect
                #use client.password.get_token(username, password)
                token_obj = client.client_credentials.get_token
                p "token - ", token_obj.inspect
                access_token = token_obj.token
                p "before access token = ", access_token
                encoder.write_bytes(access_token)
                token_to_verify = decoder.bytes
                unless token_to_verify
                    raise Kafka::Error, "OAuth2 token generation failed."
                end
                p "after token - ", token_to_verify
            end

            def load_oauth2
                begin
                    require "oauth2"
                rescue LoadError
                    @logger.error "In order to use OAuth2 authentication you need to install the `oauth2` gem."
                    raise
                end
            end
        end
    end
end