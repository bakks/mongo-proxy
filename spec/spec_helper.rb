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

TEST_DB = 'mongo_proxy_test'

def mongotestdb
  $db ||= mongo[TEST_DB]
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

