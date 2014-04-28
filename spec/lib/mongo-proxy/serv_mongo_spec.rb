require 'spec_helper'
require 'mongo'
require 'pty'
require 'json'

describe ServMongo do
  before :all do
    puts "before all"
    default_mongo_config
    puts "did default"
    @gate_thread = Thread.new { ServMongo::run }
    sleep 1
  end

  after :all do
    @gate_thread.kill
  end

  it 'should accept public connections' do
    mongo = Mongo::Connection.new
    names = mongo.database_names
    names.should include 'commonwealth_testing'
    names.should include 'pbbakkum'

    cnames = mongo['pbbakkum'].collection_names
    cnames.should include 'test'
    cnames.should include 'sample'
    cnames.should include 'big'
  end

  it 'should accept connections from mongo driver' do
    mongo = Mongo::Connection.new

    cursor = mongo['pbbakkum']['test'].find
    mongo['pbbakkum']['test'].count.should == 10

    cursor = mongo['pbbakkum']['big'].find
    i = 0
    while cursor.next
      i += 1
    end
    i.should == 1200
  end

  it 'should handle kill cursor' do
    mongo = Mongo::Connection.new

    cursor = mongo['pbbakkum']['big'].find
    cursor.count.should == 1200
    cursor.next
    cursor.close
    mongo.close
  end

  it 'should handle reconnections' do
    for i in 0..10
      mongo = Mongo::Connection.new
      cursor = mongo['pbbakkum']['big'].find
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
    cursor = mongo['pbbakkum']['big'].find({'$where' => 'true'})
    expect { cursor.next }.to raise_error
    mongo.close
  end

  it 'should block mapreduce' do
    mongo = Mongo::Connection.new
    expect do
      x = mongo['pbbakkum']['big'].mapreduce('function() { emit(this.x, 1) }', 'function(k, v) { return 1 }', {:out => {:inline => 1}, :raw => true})
    end.to raise_error
    mongo.close
  end

  it 'should handle getmore' do
    mongo = Mongo::Connection.new
    coll = mongo['pbbakkum']['big']

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
    cmd = `echo "db.test.find()" | mongo localhost/pbbakkum`
    cmd.split("\n")[2..-2].size.should be >= 10
  end

  it 'should block arbitrary commands' do
    mongo = Mongo::Connection.new
    expect { mongo['pbbakkum'].command({'repairDatabase' => 1}) }.to raise_error
    expect { mongo['pbbakkum'].command({'fsync' => 1}) }.to raise_error
    expect { mongo['pbbakkum'].command({'enableSharding' => 1}) }.to raise_error
    expect { mongo['pbbakkum'].command({'shutdown' => 1}) }.to raise_error
    expect { mongo['pbbakkum'].command({'ping' => 1}) }.to raise_error
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
    db = mongo['pbbakkum']
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
end

