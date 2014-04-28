$LOAD_PATH.unshift(File.join(File.dirname(__FILE__), '..', 'lib'))
$LOAD_PATH.unshift(File.join(File.dirname(__FILE__), '..', 'lib', 'mongo-proxy'))
$LOAD_PATH.unshift(File.dirname(__FILE__))
require 'rubygems'
require 'rspec'
require 'mocha/api'
require 'pp'
require 'mongo'
require 'mongo-proxy'

RSpec.configure do |config|
  config.mock_with :mocha
  config.fail_fast = true
end

def asset filename
  IO.read('spec/asset/' + filename)
end

def mongo
  host = 'localhost'
  port = 27018
  $mongo ||= Mongo::Connection.new(host, port)
end

def mongotestdb
  $db ||= mongo['mongo_proxy_test']
end

def default_mongo_config
  add_default_mongo_data(mongo)
end

def add_default_mongo_data(mongo)
  mongo['pbbakkum']['test'].remove
  mongo['pbbakkum']['sample'].remove
  mongo['pbbakkum']['big'].remove

  for i in 0..9
    mongo['pbbakkum']['test'].insert({:x => i})
  end

  for i in 0..9
    mongo['pbbakkum']['sample'].insert({:_id => i, :x => i})
  end

  for i in 0...1200
    mongo['pbbakkum']['big'].insert({:_id => i, :x => i})
  end
end


begin
  mongotestdb
rescue Exception => e
  puts "::
    Could not connect to mongo testing instance at
    host: #{$config['mongo']['backend_host']}
    port: #{$config['mongo']['backend_port']}
    db name: #{$config['mongo']['default_db']}
    to start this instance you can run:
      make start_mongo"
  exit
end

