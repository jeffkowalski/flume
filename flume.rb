#!/usr/bin/env ruby
# frozen_string_literal: true

require 'bundler/setup'
Bundler.require(:default)

class Flume < RecorderBotBase
  desc 'authenticate', 'authenticate with the service'
  def authenticate
    credentials = load_credentials

    response = with_rescue([RestClient::BadGateway, RestClient::GatewayTimeout, RestClient::Exceptions::OpenTimeout, SocketError], logger) do |_try|
      RestClient.post('https://api.flumetech.com/oauth/token',
                      grant_type: 'password',
                      client_id: credentials[:client_id],
                      client_secret: credentials[:client_secret],
                      username: credentials[:username],
                      password: credentials[:password])
    end
    token = JSON.parse(response)
    credentials[:access_token] = token['data'].first['access_token']
    credentials[:refresh_token] = token['data'].first['refresh_token']
    store_credentials credentials
  end

  desc 'show-devices', 'print details about devices associated with the user'
  def show_devices
    authenticate

    credentials = load_credentials
    response = RestClient.get "https://api.flumetech.com/users/#{credentials[:user]}/devices?user=false&location=false",
                              authorization: "Bearer #{credentials[:access_token]}",
                              content_type: 'application/json'
    pp JSON.parse response
  end

  method_option :offset, type: :numeric, default: 0, desc: 'offset to earlier hours', for: :record_status
  no_commands do
    def main
      authenticate

      credentials = load_credentials
      until_datetime = (Time.now - options[:offset] * 60 * 60 - 60).strftime '%F %T'
      since_datetime = (Time.now - (options[:offset] + 18) * 60 * 60 + 1).strftime '%F %T' # catch-up 18 hours

      begin
        meter = with_rescue([RestClient::BadGateway, RestClient::GatewayTimeout, RestClient::InternalServerError, RestClient::Exceptions::OpenTimeout, SocketError], logger) do |_try|
          response = RestClient::Request.execute(
            method: 'POST',
            url: "https://api.flumetech.com/users/#{credentials[:user]}/devices/#{credentials[:device]}/query",
            headers: { authorization: "Bearer #{credentials[:access_token]}",
                       content_type: 'application/json' },
            payload: %({ "queries": [{
                         "raw": false,
                         "request_id": "graph",
                         "group_multiplier": 1,
                         "bucket": "MIN",
                         "until_datetime": "#{until_datetime}",
                         "since_datetime": "#{since_datetime}" }] })
          )
          JSON.parse response
        end
        logger.info meter

        influxdb = new_influxdb_client unless options[:dry_run]
        data = meter['data'].first['graph'].map do |reading|
          { series: 'flow',
            values: { value: reading['value'].to_f },
            timestamp: Time.parse(reading['datetime']).to_i }
        end
        influxdb.write_points(data) unless options[:dry_run]
      rescue RestClient::BadRequest, RestClient::Unauthorized => e
        logger.error e.response.body
      end
    end
  end
end

Flume.start
