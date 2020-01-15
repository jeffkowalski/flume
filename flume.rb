#!/usr/bin/env ruby
# frozen_string_literal: true

require 'thor'
require 'fileutils'
require 'logger'
require 'yaml'
require 'rest-client'
require 'json'
require 'influxdb'
require 'addressable/uri'

LOGFILE = File.join(Dir.home, '.log', 'flume.log')
CREDENTIALS_PATH = File.join(Dir.home, '.credentials', 'flume.yaml')

module Kernel
  def with_rescue(exceptions, logger, retries: 5)
    try = 0
    begin
      yield try
    rescue *exceptions => e
      try += 1
      raise if try > retries

      logger.info "caught error #{e.class}, retrying (#{try}/#{retries})..."
      retry
    end
  end
end

class Flume < Thor
  no_commands do
    def redirect_output
      unless LOGFILE == 'STDOUT'
        logfile = File.expand_path(LOGFILE)
        FileUtils.mkdir_p(File.dirname(logfile), mode: 0o755)
        FileUtils.touch logfile
        File.chmod 0o644, logfile
        $stdout.reopen logfile, 'a'
      end
      $stderr.reopen $stdout
      $stdout.sync = $stderr.sync = true
    end

    def setup_logger
      redirect_output if options[:log]

      @logger = Logger.new STDOUT
      @logger.level = options[:verbose] ? Logger::DEBUG : Logger::INFO
      @logger.info 'starting'
    end
  end

  class_option :log,     type: :boolean, default: true, desc: "log output to #{LOGFILE}"
  class_option :verbose, type: :boolean, aliases: '-v', desc: 'increase verbosity'

  desc 'authenticate', 'authenticate with the service'
  def authenticate
    credentials = YAML.load_file CREDENTIALS_PATH

    response = RestClient.post('https://api.flumetech.com/oauth/token',
                               grant_type: 'password',
                               client_id: credentials[:client_id],
                               client_secret: credentials[:client_secret],
                               username: credentials[:username],
                               password: credentials[:password])
    token = JSON.parse(response)
    credentials[:access_token] = token['data'].first['access_token']
    credentials[:refresh_token] = token['data'].first['refresh_token']
    File.open(CREDENTIALS_PATH, 'w') { |file| file.write(credentials.to_yaml) }
  end

  desc 'show-devices', 'print details about devices associated with the user'
  def show_devices
    authenticate

    credentials = YAML.load_file CREDENTIALS_PATH
    response = RestClient.get "https://api.flumetech.com/users/#{credentials[:user]}/devices",
                              authorization: "Bearer #{credentials[:access_token]}",
                              content_type: 'application/json'
    pp JSON.parse response
  end

  desc 'record-status', 'record the current usage data to database'
  method_option :offset, type: :numeric, default: 0, desc: 'offset to earlier hours'
  method_option :dry_run, type: :boolean, aliases: '-n', desc: "don't log to database"
  def record_status
    setup_logger

    authenticate

    credentials = YAML.load_file CREDENTIALS_PATH
    until_datetime = (Time.now - options[:offset] * 60 * 60 - 60).strftime '%F %T'
    since_datetime = (Time.now - (options[:offset] + 1) * 60 * 60 + 1).strftime '%F %T'

    begin
      meter = with_rescue([RestClient::BadGateway, RestClient::GatewayTimeout, RestClient::Exceptions::OpenTimeout], @logger) do |_try|
        response = RestClient::Request.execute(
          method: 'POST',
          url: "https://api.flumetech.com/users/#{credentials[:user]}/devices/#{credentials[:device]}/query",
          headers: { authorization: "Bearer #{credentials[:access_token]}",
                     content_type: 'application/json' },
          payload: %({"queries": [{
                    "raw": false,
                    "request_id": "graph",
                    "group_multiplier": 1,
                    "bucket": "MIN",
                    "until_datetime": "#{until_datetime}",
                    "since_datetime": "#{since_datetime}"
                  }]
                })
        )
        JSON.parse response
      end
      @logger.info meter

      influxdb = options[:dry_run] ? nil : (InfluxDB::Client.new 'flume')
      meter['data'].first['graph'].each do |reading|
        timestamp = Time.parse(reading['datetime']).to_i
        data = {
          values: { value: reading['value'].to_f },
          timestamp: timestamp
        }
        influxdb.write_point('flow', data) unless options[:dry_run]
      end
    rescue RestClient::BadRequest => e
      @logger.error e.response.body
    rescue RestClient::Unauthorized => e
      @logger.error e.response.body
    rescue StandardError => e
      @logger.error e
    end
  end
end

Flume.start
