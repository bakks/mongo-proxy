require 'spec_helper'
require 'mongo'
require 'pty'

describe MongoProxy do
  def mongo_test_data
    mongotestdb['test'].remove
    mongotestdb['sample'].remove
    mongotestdb['big'].remove

    for i in 0..9
      mongotestdb['test'].insert({:x => i})
    end

    for i in 0..9
      mongotestdb['sample'].insert({:_id => i, :x => i})
    end

    for i in 0...1200
      mongotestdb['big'].insert({:_id => i, :x => i})
    end
  end

  before :all do
    mongo_test_data
    config = {
      :motd => "foo\nbar",
      :client_port => 27017,
      :server_port => 27018,
      :read_only => true
    }
    @gate_thread = Thread.new do
      m = MongoProxy.new(config)
      m.start
    end
    sleep 0.5
  end

  after :all do
    @gate_thread.kill
  end

  it 'should accept public connections' do
    mongo = Mongo::Connection.new
    names = mongo.database_names
    names.should include 'mongo_proxy_test'

    cnames = mongo[TEST_DB].collection_names
    cnames.should include 'test'
    cnames.should include 'sample'
    cnames.should include 'big'
  end

  it 'should accept connections from mongo driver' do
    mongo = Mongo::Connection.new

    cursor = mongo[TEST_DB]['test'].find
    mongo[TEST_DB]['test'].count.should == 10

    cursor = mongo[TEST_DB]['big'].find
    i = 0
    while cursor.next
      i += 1
    end
    i.should == 1200
  end

  it 'should handle kill cursor' do
    mongo = Mongo::Connection.new

    cursor = mongo[TEST_DB]['big'].find
    cursor.count.should == 1200
    cursor.next
    cursor.close
    mongo.close
  end

  it 'should handle reconnections' do
    for i in 0..10
      mongo = Mongo::Connection.new
      cursor = mongo[TEST_DB]['big'].find
      i = 0
      while cursor.next
        i += 1
      end
      i.should == 1200
      mongo.close
    end
  end

  it 'should block where' do
    mongo = Mongo::Connection.new
    expect {
      cursor = mongo[TEST_DB]['big'].find({'$where' => 'true'})
      cursor.next
    }.to raise_error
    mongo.close
  end

  it 'should block mapreduce' do
    mongo = Mongo::Connection.new
    expect do
      x = mongo[TEST_DB]['big'].mapreduce('function() { emit(this.x, 1) }', 'function(k, v) { return 1 }', {:out => {:inline => 1}, :raw => true})
    end.to raise_error
    mongo.close
  end

  it 'should handle getmore' do
    mongo = Mongo::Connection.new
    coll = mongo[TEST_DB]['big']

    cursor = coll.find({}, {:sort => ['x', :asc]})
    i = 0

    while doc = cursor.next
      doc['x'].should == i
      i += 1
    end

    i.should == 1200
  end

  it 'should send motd' do
    mongo = Mongo::Connection.new
    admin = mongo['admin']
    x = admin.command({'getLog' => 'startupWarnings'})
    x['ok'].should == 1.0
    x['totalLinesWritten'].should be > 1
    x['log'].size.should be > 1
  end

  it 'should accept connections from shell driver' do
    cmd = `echo "db.test.find()" | mongo localhost/mongo_proxy_test`
    cmd.split("\n")[2..-2].size.should be >= 10
  end

  it 'should block arbitrary commands' do
    mongo = Mongo::Connection.new
    expect { mongo[TEST_DB].command({'repairDatabase' => 1}) }.to raise_error
    expect { mongo[TEST_DB].command({'fsync' => 1}) }.to raise_error
    expect { mongo[TEST_DB].command({'enableSharding' => 1}) }.to raise_error
    expect { mongo[TEST_DB].command({'shutdown' => 1}) }.to raise_error
    expect { mongo[TEST_DB].command({'ping' => 1}) }.to raise_error
  end

  it 'should allow ismaster' do
    mongo = Mongo::Connection.new
    r = mongo['admin'].command({'ismaster' => 1})
    r['ismaster'].should == true
    r = mongo['admin'].command({'isMaster' => 1})
    r['ismaster'].should == true
  end

  it 'should block writes' do
    mongo = Mongo::Connection.new
    db = mongo[TEST_DB]
    coll = db['big']
    expect { coll.insert({:foo => 'bar'}) }.to raise_error
    expect { coll.remove }.to raise_error
    expect { coll.update({:x => 0}, {:x => 1}) }.to raise_error
    expect { coll.ensure_index({:x => 1}) }.to raise_error

    coll.count.should == 1200

    cursor = coll.find({}, {:sort => ['x', :asc]})
    i = 0

    while doc = cursor.next
      doc['x'].should == i
      i += 1
    end

    i.should == 1200

    coll.index_information.size.should == 1
  end

  it 'should allow writes when not in read_only mode' do
    config = {
      :read_only => false,
      :client_port => 29017,
      :server_port => 27018
    }
    gate_thread2 = Thread.new do
      begin
        MongoProxy.new(config).start
      rescue Exception => e
        p [e.inspect, e.backtrace.first]
      end
    end
    sleep 0.5

    mongo = Mongo::Connection.new('127.0.0.1', 29017)
    db = mongo[TEST_DB]
    coll = db['big']
    coll.size.should == 1200
    coll.insert({:foo => 'xxxx'})
    coll.size.should == 1201

    gate_thread2.kill
  end

  it 'should allow front middleware' do
    config = {
      :read_only => false,
      :client_port => 29018,
      :server_port => 27018
    }
    i = 0

    gate_thread2 = Thread.new do
      m = MongoProxy.new(config)

      # add middleware that returns null so it can't connect
      m.add_callback_to_front do |conn, msg|
        nil
      end

      m.add_callback_to_front do |conn, msg|
        i += 1
        msg
      end

      m.start
    end
    sleep 0.5

    expect { mongo = Mongo::Connection.new('localhost', 29018, :connect_timeout => 0.2) }.to raise_error
    i.should == 1

    gate_thread2.kill
  end

  it 'should allow back middleware' do
    config = {
      :read_only => false,
      :client_port => 29018,
      :server_port => 27018
    }
    i = 0

    gate_thread2 = Thread.new do
      m = MongoProxy.new(config)

      # add middleware that returns null so it can't connect
      m.add_callback_to_back do |conn, msg|
        nil
      end

      m.add_callback_to_back do |conn, msg|
        i += 1
        msg
      end

      m.start
    end
    sleep 0.5

    expect { mongo = Mongo::Connection.new('localhost', 29018, :connect_timeout => 0.2) }.to raise_error
    i.should == 0

    gate_thread2.kill
  end

  it 'should shape traffic' do
    config = {
      :read_only => false,
      :client_port => 29020,
      :server_port => 27018
    }

    gate_thread2 = Thread.new do
      m = MongoProxy.new(config)
      m.add_callback_to_back do |conn, msg|
        if msg[:header][:opCode] == :insert
          msg[:documents] = [{:foo => 1}]
        end
        msg
      end
      m.start
    end
    sleep 0.5

    mongo = Mongo::Connection.new('localhost', 29020, :connect_timeout => 0.2)
    coll = mongo[TEST_DB]['test']
    coll.remove
    coll.insert({:x => 1})
    coll.insert({:x => 1})
    coll.count({:foo => 1}).should == 2

    gate_thread2.kill
  end
end

