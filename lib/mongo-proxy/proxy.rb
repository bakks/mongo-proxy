require 'em-proxy'
require 'logger'
require 'json'
require 'pp'

class MongoProxy
  VERSION = 1.0

  def initialize(config = nil)
    @config = {
      :client_host => '127.0.0.1',
      :client_port => 27017,
      :server_host => '127.0.0.1',
      :server_port => 27018,
      :motd => nil,
      :readonly => true,
      :verbose => false,
      :logger => nil,
      :debug => false
    }

    (config || []).each do |k, v|
      if @config.include?(k)
        @config[k] = v
      else
        raise "Unrecognized configuration value: #{k}"
      end
    end

    unless @config[:logger]
      @config[:logger] = Logger.new(STDOUT)
      @config[:logger].level = (@config[:verbose] || @config[:debug] ? Logger::DEBUG : Logger::WARN)
      @config[:logger].formatter = proc do |severity, datetime, progname, msg|
        if msg.is_a?(Hash)
          "#{JSON::pretty_generate(msg)}\n\n"
        else
          "#{msg}\n\n"
        end
      end
    end

    @log = @config[:logger]
    @auth = AuthMongo.new(@config)

    EM.error_handler do |e|
      @log.error [e.inspect, e.backtrace.first]
      raise e
    end

    Proxy.start({
        :host => @config[:client_host],
        :port => @config[:client_port],
        :debug => false
      },
      &method(:callbacks))
  end

  def callbacks(conn)
    conn.server(:srv, {
      :host => @config[:server_host],
      :port => @config[:server_port]})

    conn.on_data do |data|
      raw_msg, msg = WireMongo.receive(data)
      
      @log.info 'from client'
      @log.info msg

      if raw_msg == nil
        @log.info "Client disconnected"
        return
      end

      # get auth response about client query
      authed = (@config[:readonly] == true ? @auth.wire_auth(conn, msg) : true)
      r = nil

      if authed == true # auth succeeded
        r = WireMongo::write(msg)

      elsif authed.is_a?(Hash) # auth had a direct response
        response = WireMongo::write(authed)
        conn.send_data(response)

      else # otherwise drop the message
        @log.info 'dropping message'

      end

      r
    end

    conn.on_response do |backend, resp|
      if @config[:verbose]
        _, msg = WireMongo::receive(resp)
        @log.info 'from server'
        @log.info msg
      end

      resp
    end

    conn.on_finish do |backend, name|
      @log.info "closing client connection #{name}"
    end
  end
end

