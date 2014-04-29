require 'socket'
require 'em-proxy'

class MongoProxy
  def initialize(config = nil)
    @config = {
      :client_host => '127.0.0.1',
      :client_port => 27017,
      :server_host => '127.0.0.1',
      :server_port => 27018,
      :motd => nil,
      :readonly => true
    }

    (config || []).each do |k, v|
      if @config.include?(k)
        @config[k] = v
      else
        raise "Unrecognized configuration value: #{k}"
      end
    end

    @auth = AuthMongo.new(@config)

    EM.error_handler{|e| p [e.inspect, e.backtrace.first] }

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
      
      puts 'from client'
      pp msg

      if raw_msg == nil
        #@@log.info "Client disconnected"
        puts 'Client disconnected'
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
        #@@log.info 'dropping message'
        puts 'dropping message'

      end

      r
    end

    conn.on_finish do |backend, name|
      #p [:on_finish, name]
    end
  end
end

