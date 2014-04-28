require 'socket'
require 'em-proxy'

module ServMongo
  def self.callbacks(conn)
    host = 'localhost'
    port = 27018
    conn.server(:srv, {:host => host, :port => port})

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
      authed = @auth.wire_auth(msg)
      r = nil
      pp authed


      if authed == true # auth succeeded
        unless @mongosocket
          @mongosocket = TCPSocket.open(host, port)
        end

        r = WireMongo::write(msg)

      elsif authed.is_a?(Hash) # auth had a direct response
        response = WireMongo::write(authed)
        puts 'response'
        p response
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

  def self.run
    @auth = AuthMongo.new

    host = '127.0.0.1'
    port = 27017

    EM.error_handler{|e| p [e.inspect, e.backtrace.first] }

    Proxy.start({
        :host => host,
        :port => port,
        :debug => false
      },
      &method(:callbacks))
  end
end

