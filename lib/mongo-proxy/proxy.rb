require 'em-proxy'
require 'logger'
require 'json'
require 'pp'

# This class uses em-proxy to help listen to MongoDB traffic, with some
# parsing and filtering capabilities that allow you to enforce a read-only
# mode, or use your own arbitrary logic.
class MongoProxy
  VERSION = 1.0

  def initialize(config = nil)
    # default config values
    @config = {
      :client_host => '127.0.0.1',
      :client_port => 27017,
      :server_host => '127.0.0.1',
      :server_port => 27018,
      :motd => nil,
      :read_only => true,
      :verbose => false,
      :logger => nil,
      :debug => false
    }
    @front_callbacks = []
    @back_callbacks = []

    # apply argument config to the default values
    (config || []).each do |k, v|
      if @config.include?(k)
        @config[k] = v
      else
        raise "Unrecognized configuration value: #{k}"
      end
    end

    # debug implies verbose
    @config[:verbose] = true if @config[:debug]

    # Set up the logger for mongo proxy. Users can also pass their own
    # logger in with the :logger config value.
    unless @config[:logger]
      @config[:logger] = Logger.new(STDOUT)
      @config[:logger].level = (@config[:verbose] ? Logger::DEBUG : Logger::WARN)

      if @config[:debug]
        @config[:logger].formatter = proc do |_, _, _, msg|
          if msg.is_a?(Hash)
            "#{JSON::pretty_generate(msg)}\n\n"
          else
            "#{msg}\n\n"
          end
        end
      end
    end

    @log = @config[:logger]
    @auth = AuthMongo.new(@config)
  end

  def start
    # em proxy launches a thread, this is the error handler for it
    EM.error_handler do |e|
      @log.error [e.inspect, e.backtrace.first]
      raise e
    end

    # kick off em-proxy
    Proxy.start({
        :host => @config[:client_host],
        :port => @config[:client_port],
        :debug => false
      },
      &method(:callbacks))
  end

  def add_callback_to_front(&block)
    @front_callbacks.insert(0, block)
  end

  def add_callback_to_back(&block)
    @back_callbacks << block
  end

  private

  def callbacks(conn)
    conn.server(:srv, {
      :host => @config[:server_host],
      :port => @config[:server_port]})

    conn.on_data do |data|
      # parse the raw binary message
      raw_msg, msg = WireMongo.receive(data)
      
      @log.info 'from client'
      @log.info msg

      if raw_msg == nil
        @log.info "Client disconnected"
        return
      end

      @front_callbacks.each do |cb|
        msg = cb.call(conn, msg)
        break unless msg
      end
      next unless msg

      # get auth response about client query
      authed = (@config[:read_only] == true ? @auth.wire_auth(conn, msg) : true)
      r = nil

      if authed == true # auth succeeded
        @back_callbacks.each do |cb|
          msg = cb.call(conn, msg)
          break unless msg
        end
        next unless msg

        r = WireMongo::write(msg)

      elsif authed.is_a?(Hash) # auth had a direct response
        response = WireMongo::write(authed)
        conn.send_data(response)

      else # otherwise drop the message
        @log.info 'dropping message'

      end

      r
    end

    # messages back from the server
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

