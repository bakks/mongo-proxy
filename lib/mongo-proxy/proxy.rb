require 'em-proxy'
require 'logger'

class MongoProxy
  def initialize(config = nil)
    @config = {
      :client_host => '127.0.0.1',
      :client_port => 27017,
      :server_host => '127.0.0.1',
      :server_port => 27018,
      :motd => nil,
      :readonly => true,
      :verbose => false,
      :logger => nil
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
      @config[:logger].level = (@config[:verbose] ? Logger::DEBUG : Logger::WARN)
    end

    @log = @config[:logger]
    @auth = AuthMongo.new(@config)

    EM.error_handler { |e| @log.error [e.inspect, e.backtrace.first] }

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
      @log.info msg.to_s

      if raw_msg == nil
        @log.info "Client disconnected"
        return
      end

      # get auth response about client query
      authed = (@config[:readonly] == true ? @auth.wire_auth(msg) : true)
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

    conn.on_finish do |backend, name|
      @log.info "closing client connection #{name}"
    end
  end
end

