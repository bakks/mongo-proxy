require 'mongo-proxy'

front = 0
back = 0
config = {
  :client_port => 29017,
  :server_port => 27017,
  :read_only => true,
  :debug => true
}

proxy = MongoProxy.new(config)

m.add_callback_to_front do |conn, msg|
  front += 1
  puts "received #{front} client messages so far"
  msg
end

m.add_callback_to_back do |conn, msg|
  back += 1
  puts "forwarded #{back} client messages so far"
  msg
end

m.start

