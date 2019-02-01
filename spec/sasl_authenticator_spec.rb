# frozen_string_literal: true

require 'fake_server'
require 'net/http'

describe Kafka::SaslAuthenticator do
  let(:logger) { LOGGER }

  let(:host) { "127.0.0.1" }
  let(:server) { TCPServer.new(host, 0) }
  let(:port) { server.addr[1] }

  let(:connection) {
    Kafka::Connection.new(
      host: host,
      port: port,
      client_id: "test",
      logger: logger,
      instrumenter: Kafka::Instrumenter.new(client_id: "test"),
      connect_timeout: 0.1,
      socket_timeout: 0.1,
    )
  }

  let!(:fake_server) { FakeServer.start(server) }

  let(:sasl_authenticator) {
    Kafka::SaslAuthenticator.new(
      { logger: logger }.merge(auth_options)
    )
  }

  let(:auth_options) {
    {
      sasl_gssapi_principal: nil,
      sasl_gssapi_keytab: nil,
      sasl_plain_authzid: nil,
      sasl_plain_username: nil,
      sasl_plain_password: nil,
      sasl_scram_username: nil,
      sasl_scram_password: nil,
      sasl_scram_mechanism: nil,
      sasl_oauth_client_id: nil,
      sasl_oauth_client_secret: nil,
      sasl_oauth_server_url: nil,
      sasl_oauth_token_url: nil,
    }
  }

  context "when SASL has not been configured" do
    it "still works" do
      sasl_authenticator.authenticate!(connection)
    end
  end

  context "when SASL PLAIN has been configured" do
    before do
      auth_options.update(
        sasl_plain_authzid: "",
        sasl_plain_username: "spec_username",
        sasl_plain_password: "spec_password",
      )
    end

    it "authenticates" do
      sasl_authenticator.authenticate!(connection)
    end

    it "raises Kafka::Error when the username or password is incorrect" do
      auth_options[:sasl_plain_password] = "wrong"

      expect {
        sasl_authenticator.authenticate!(connection)
      }.to raise_error(Kafka::Error, /SASL PLAIN authentication failed/)
    end
  end

  context "when SASL SCRAM has been configured" do
    before do
      auth_options.update(
        sasl_scram_username: "spec_username",
        sasl_scram_password: "spec_password",
        sasl_scram_mechanism: "sha256"
      )
    end

    it "authenticates" do
      sasl_authenticator.authenticate!(connection)
    end

    it "raises Kafka::Error when the username or password is incorrect" do
      auth_options[:sasl_scram_password] = "wrong"

      expect {
        sasl_authenticator.authenticate!(connection)
      }.to raise_error(Kafka::FailedScramAuthentication)
    end
  end

  context "when SASL OAuth has been configured" do
    before do
      auth_options.update(
        sasl_oauth_client_id: "TestClientID",
        sasl_oauth_client_secret: "TestClientSecret",
        sasl_oauth_server_url: "https://api.example.com"
      )
    end

    before do
      http = Net::HTTP.new('api.example.com', 443)
      expect(Net::HTTP).to receive(:new).with('api.example.com', 443).and_return(http)
      allow(http).to \
        receive(:request).with(an_instance_of(Net::HTTP::Post))
          .and_return(Net::HTTPResponse)
    end

    it "authenticates" do
      expect(Net::HTTPResponse).to receive(:read_body).and_return(response_body)
      sasl_authenticator.authenticate!(connection)
    end

    def response_body
      '{"access_token":"yqCHrUUHZ4Mc5Yw3edBSI_3hWAUsD6FyLlxEPEZlkDE.-ri5cnCXCzCsr44bwH-XBPvxAwImIW2Wurqk1RB8Wlg","expires_in":3599,"scope":"","token_type":"bearer"}'
    end
  end
end
