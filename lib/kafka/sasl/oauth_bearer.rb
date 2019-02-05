require 'oauth2'

module Kafka
	module Sasl
		class OAuthBearer
			OAUTH_IDENT = "OAUTHBEARER"
			attr_reader :client_id, :client_secret, :server_url, :token_url

			def initialize(logger:, client_id:, client_secret:, server_url:, token_url:)
				@logger = logger
				@client_id = client_id
				@client_secret = client_secret
				@server_url = server_url
				@token_url = token_url || '/oauth2/token'
			end

			def ident
				OAUTH_IDENT
			end

			def configured?
				client_id && client_secret && server_url
			end

			def authenticate!(host, encoder, decoder)
				@logger.debug "Authenticating with OAuth method"
				access_token = get_access_token
				token = construct_oauth_token(access_token)
				encoder.write_bytes(token)
				begin
					token_to_verify = decoder.bytes
					unless token_to_verify
						raise Kafka::Error, "OAuth authentication failed with 'No response received on socket', this can happen when sending token on socket is failed"
					else
						@logger.info "Authentication successful"
					end
				rescue EOFError => e
					raise Kafka::Error, "OAuth authentication failed with - #{e.message}"
				end
			end

			private
				
			def get_access_token
				@logger.debug "Requesting an access token from OAuth Server '#{server_url}' at endpoint '#{token_url}'"
        client = OAuth2::Client.new(client_id, client_secret, :site => server_url, :token_url => token_url)
        response = client.client_credentials.get_token
        response.token
			end

      def construct_oauth_token(access_token)
        token_ext = [1].pack("C")
				"n,,#{token_ext}auth=Bearer #{access_token}#{token_ext}#{token_ext}"
			end
		end
	end
end
