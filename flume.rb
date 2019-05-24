#!/usr/bin/env ruby

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

  desc 'authorize', '[re]authorize the application'
  def authorize
    credentials = YAML.load_file CREDENTIALS_PATH

    state = Time.now.to_i
    redirected_url = nil
    RestClient.post('https://flumetech.com/login',
                    username: credentials[:username],
                    password: credentials[:password],
                    client_id: 'customer-portal',
                    redirect_uri: 'https://portal.flumetech.com',
                    state: state) do |response, request, result, &block|
      if [301, 302, 307].include? response.code
        redirected_url = response.headers[:location]
      else
        response.return!(request, result, &block)
      end
    end
    uri = Addressable::URI.parse redirected_url
    code = uri.query_values['code']

    response = RestClient.post('https://api.flumetech.com/oauth/token',
                               client_id: 'customer-portal',
                               grant_type: 'authorization_code',
                               code: code,
                               redirect_uri: 'https://portal.flumetech.com')
    token = JSON.parse(response)
    credentials[:access_token] = token['data'].first['access_token']
    File.open(CREDENTIALS_PATH, 'w') { |file| file.write(credentials.to_yaml) }
  end

  desc 'record-status', 'record the current usage data to database'
  def record_status
    setup_logger

    credentials = YAML.load_file CREDENTIALS_PATH
    until_datetime = (Time.now - 60).strftime '%F %T'
    since_datetime = (Time.now - 60 * 60 + 1).strftime '%F %T'

    begin
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
      meter = JSON.parse response
      @logger.info meter

      influxdb = InfluxDB::Client.new 'flume'
      meter['data'].first['graph'].each do |reading|
        timestamp = Time.parse(reading['datetime']).to_i
        data = {
          values: { value: reading['value'].to_f },
          timestamp: timestamp
        }
        influxdb.write_point('flow', data)
      end
    rescue RestClient::BadRequest => e
      @logger.error e.response.body
    end
  end
end

Flume.start
