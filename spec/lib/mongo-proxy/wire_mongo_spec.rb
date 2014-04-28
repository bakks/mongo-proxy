require 'spec_helper'

describe WireMongo do
  let (:reply_sample) {"W\x00\x00\x00\x0E\x00\x00\x00\x01\x00\x00\x00\x01\x00\x00\x00\b\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x003\x00\x00\x00\bismaster\x00\x01\x10maxBsonObjectSize\x00\x00\x00\x00\x01\x01ok\x00\x00\x00\x00\x00\x00\x00\xF0?\x00"}
  let (:reply_sample2) {"\"\u0002\u0000\u0000\xAE{\u0000\u0000\u000E\u0000\u0000\u0000\u0001\u0000\u0000\u0000\b\u0000\u0000\u00002\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\n\u0000\u0000\u00003\u0000\u0000\u0000\u0002_id\u0000%\u0000\u0000\u00003427b4fa-98d3-b465-9725-013b0627c6f3\u0000\u00003\u0000\u0000\u0000\u0002_id\u0000%\u0000\u0000\u0000dabe4ffc-98d3-b465-9725-013b0627c6f9\u0000\u00003\u0000\u0000\u0000\u0002_id\u0000%\u0000\u0000\u0000268a9cfc-98d3-b465-9725-013b0627c6fc\u0000\u00003\u0000\u0000\u0000\u0002_id\u0000%\u0000\u0000\u00007156e9fd-98d3-b465-9725-013b0627c6fd\u0000\u00003\u0000\u0000\u0000\u0002_id\u0000%\u0000\u0000\u0000cc1237fe-98d3-b465-9725-013b0627c700\u0000\u00003\u0000\u0000\u0000\u0002_id\u0000%\u0000\u0000\u000047709144-98d3-b465-9725-013b0627c70a\u0000\u00003\u0000\u0000\u0000\u0002_id\u0000%\u0000\u0000\u00002b3751f8-98d3-b465-9725-013b0627c713\u0000\u00003\u0000\u0000\u0000\u0002_id\u0000%\u0000\u0000\u00007603aef9-98d3-b465-9725-013b0627c716\u0000\u00003\u0000\u0000\u0000\u0002_id\u0000%\u0000\u0000\u0000b332d3fc-98d3-b465-9725-013b0627c719\u0000\u00003\u0000\u0000\u0000\u0002_id\u0000%\u0000\u0000\u0000a595bbfe-98d3-b465-9725-013b0627c71c\u0000\u0000"}

  let (:reply_sample_doc) {{
    "ismaster" => true,
    "maxBsonObjectSize" => 16777216,
    "ok" => 1.0
  }}
  
  let (:reply_sample_docs2) {[
    {"_id" => "3427b4fa-98d3-b465-9725-013b0627c6f3"},
    {"_id" => "dabe4ffc-98d3-b465-9725-013b0627c6f9"},
    {"_id" => "268a9cfc-98d3-b465-9725-013b0627c6fc"},
    {"_id" => "7156e9fd-98d3-b465-9725-013b0627c6fd"},
    {"_id" => "cc1237fe-98d3-b465-9725-013b0627c700"},
    {"_id" => "47709144-98d3-b465-9725-013b0627c70a"},
    {"_id" => "2b3751f8-98d3-b465-9725-013b0627c713"},
    {"_id" => "7603aef9-98d3-b465-9725-013b0627c716"},
    {"_id" => "b332d3fc-98d3-b465-9725-013b0627c719"},
    {"_id" => "a595bbfe-98d3-b465-9725-013b0627c71c"}
  ]}

  it 'should read OP_REPLY' do
    message, x = WireMongo.receive(reply_sample)
    message.should == reply_sample

    x.should == {
      :responseFlags => 8,
      :cursorID => 0,
      :startingFrom => 0,
      :numberReturned => 1,
      :documents => [reply_sample_doc],
      :header => {
        :messageLength => 87,
        :requestID => 14,
        :responseTo => 1,
        :opCode => :reply
      }
    }

    message, x = WireMongo.receive(reply_sample2)
    message.should == reply_sample2
    x.should == {
      :responseFlags => 8,
      :startingFrom => 0,
      :numberReturned => 10,
      :cursorID => 50,
      :documents => reply_sample_docs2,
      :header => {
        :messageLength => 546,
        :requestID => 31662,
        :responseTo => 14,
        :opCode => :reply
      }
    }
  end

  it 'should build OP_REPLY' do
    raw, x = WireMongo.receive(reply_sample)
    x[:header].delete(:messageLength)
    WireMongo.build_reply(reply_sample_doc, 14, 1, 8).should == x

    raw, x = WireMongo.receive(reply_sample2)
    x[:header].delete(:messageLength)
    WireMongo.build_reply(reply_sample_docs2, 31662, 14, 8, 50, 0).should == x
  end

  it 'should write OP_REPLY' do
    x = WireMongo.build_reply(reply_sample_doc, 14, 1, 8)
    WireMongo.write(x).should == reply_sample

    x = WireMongo.build_reply(reply_sample_docs2, 31662, 14, 8, 50, 0)
    WireMongo.write(x).should == reply_sample2
  end

  let (:update_sample) {"d\x00\x00\x00\x0E\x00\x00\x00\x00\x00\x00\x00\xD1\a\x00\x00\x00\x00\x00\x00test.test\x00\x03\x00\x00\x00\x1C\x00\x00\x00\x02testkey\x00\n\x00\x00\x00testvalue\x00\x00&\x00\x00\x00\x03$set\x00\e\x00\x00\x00\x02testkey\x00\t\x00\x00\x00otherval\x00\x00\x00"}

  it 'should read OP_UPDATE' do
    message, x = WireMongo.receive(update_sample)
    message.should == update_sample

    x.should == {
      :database => 'test',
      :collection => 'test',
      :flags => 3,
      :selector => {
        'testkey' => 'testvalue',
      },
      :update => {
        '$set' => {
          'testkey' => 'otherval'
        }
      },
      :header => {
        :opCode => :update,
        :requestID => 14,
        :messageLength => 100,
        :responseTo => 0
      }
    }
  end

  it 'should build OP_UPDATE' do
    raw, x = WireMongo.receive(update_sample)
    x[:header].delete(:messageLength)
    WireMongo.build_update(14, 'test', 'test', {'testkey' => 'testvalue'},
      {'$set' => {'testkey' => 'otherval'}}, [:upsert, :multi]).should == x
  end

  it 'should write OP_UPDATE' do
    x = WireMongo.build_update(14, 'test', 'test', {'testkey' => 'testvalue'},
      {'$set' => {'testkey' => 'otherval'}}, [:upsert, :multi])
    WireMongo.write(x).should == update_sample
  end

  let (:insert_sample) {"K\x00\x00\x00\a\x00\x00\x00\x00\x00\x00\x00\xD2\a\x00\x00\x00\x00\x00\x00test.test\x00-\x00\x00\x00\a_id\x00P\x8C\xA2Ax\xB1\x0E%\xD6\x00\x00\x05\x02testkey\x00\n\x00\x00\x00testvalue\x00\x00"}

  INSERT_DOC = {
      "_id" => BSON::ObjectId('508ca24178b10e25d6000005'),
      "testkey" => "testvalue"
    }

  it 'should read OP_INSERT' do
    message, x = WireMongo.receive(insert_sample)
    message.should == insert_sample

    x.should == {
      :flags => 0,
      :database => 'test',
      :collection => 'test',
      :documents => [INSERT_DOC],
      :header => {
        :messageLength => 75,
        :requestID => 7,
        :responseTo => 0,
        :opCode => :insert
      }
    }
  end

  it 'should build OP_INSERT' do
    raw, x = WireMongo.receive(insert_sample)
    x[:header].delete(:messageLength)
    WireMongo.build_insert(7, 'test', 'test', INSERT_DOC).should == x
  end

  it 'should write OP_INSERT' do
    x = WireMongo.build_insert(7, 'test', 'test', INSERT_DOC)
    WireMongo.write(x).should == insert_sample
  end

  let (:query_sample) { ":\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\xD4\a\x00\x00\x00\x00\x00\x00admin.$cmd\x00\x00\x00\x00\x00\xFF\xFF\xFF\xFF\x13\x00\x00\x00\x10ismaster\x00\x01\x00\x00\x00\x00" }

  it 'should read OP_QUERY' do
    message, x = WireMongo.receive(query_sample)
    message.should == query_sample

    x.should == {
      :flags => 0,
      :database => 'admin',
      :collection => '$cmd',
      :numberToSkip => 0,
      :numberToReturn => 4294967295,
      :query => {
        "ismaster" => 1
      },
      :returnFieldSelector => nil,
      :header => {
        :messageLength => 58,
        :requestID => 1,
        :responseTo => 0,
        :opCode => :query
      }
    }
  end

  it 'should build OP_QUERY' do
    raw, x = WireMongo.receive(query_sample)
    x[:header].delete(:messageLength)
    WireMongo.build_query(1, 'admin', '$cmd', {'ismaster' => 1}, nil, 4294967295).should == x
  end

  it 'should write OP_QUERY' do
    x = WireMongo.build_query(1, 'admin', '$cmd', {'ismaster' => 1}, nil, 4294967295)
    WireMongo.write(x).should == query_sample
  end

  let (:get_more_sample) {"0\x00\x00\x00\t\x00\x00\x00\xFF\xFF\xFF\xFF\xD5\a\x00\x00\x00\x00\x00\x00bakks.submarine\x00\x00\x00\x00\x00'\xD7\xC6\xD8!\x8CK\x13"}
  let (:get_more_sample2) {"*\u0000\u0000\u0000\xAC\t\u0000\u0000\u0000\u0000\u0000\u0000\xD5\a\u0000\u0000\u0000\u0000\u0000\u0000bakks.big\u0000\u0000\u0000\u0000\u0000먤\b\xEEe\x84\u0006"}

  it 'should read OP_GET_MORE' do
    message, x = WireMongo.receive(get_more_sample)
    message.should == get_more_sample

    x.should == {
      :database => 'bakks',
      :collection => 'submarine',
      :numberToReturn => 0,
      :cursorID => 1390358986972649255,
      :header => {
        :messageLength => 48,
        :requestID => 9,
        :responseTo => 4294967295,
        :opCode => :get_more
      }
    }

    message, x = WireMongo.receive(get_more_sample2)
    message.should == get_more_sample2

    x.should == {
      :database => 'bakks',
      :collection => 'big',
      :numberToReturn => 0,
      :cursorID => 469612334175004907,
      :header => {
        :messageLength => 42,
        :requestID => 2476,
        :responseTo => 0,
        :opCode => :get_more
      }
    }
  end

  it 'should build OP_GET_MORE' do
    raw, x = WireMongo.receive(get_more_sample)
    x[:header].delete(:messageLength)
    WireMongo.build_get_more(9, 4294967295, 'bakks', 'submarine', 1390358986972649255).should == x

    raw, x = WireMongo.receive(get_more_sample2)
    x[:header].delete(:messageLength)
    WireMongo.build_get_more(2476, 0, 'bakks', 'big', 469612334175004907).should == x
  end

  it 'should write OP_GET_MORE' do
    x = WireMongo.build_get_more(9, 4294967295, 'bakks', 'submarine', 1390358986972649255)
    WireMongo.write(x).should == get_more_sample

    x = WireMongo.build_get_more(2476, 0, 'bakks', 'big', 469612334175004907)
    WireMongo.write(x).should == get_more_sample2
  end

  let(:delete_sample) {"'\x00\x00\x00\x02\x00\x00\x00\x00\x00\x00\x00\xD6\a\x00\x00\x00\x00\x00\x00test.test\x00\x00\x00\x00\x00\x05\x00\x00\x00\x00"}

  it 'should read OP_DELETE' do
    message, x = WireMongo.receive(delete_sample)
    message.should == delete_sample

    x.should == {
      :database => 'test',
      :collection => 'test',
      :flags => 0,
      :selector => {},
      :header => {
        :messageLength => 39,
        :requestID => 2,
        :responseTo => 0,
        :opCode => :delete
      }
    }
  end

  it 'should build OP_DELETE' do
    raw, x = WireMongo.receive(delete_sample)
    x[:header].delete(:messageLength)
    WireMongo.build_delete(2, 'test', 'test', {}).should == x
  end

  it 'should write OP_DELETE' do
    x = WireMongo.build_delete(2, 'test', 'test', {})
    WireMongo.write(x).should == delete_sample
  end

  let (:kill_cursors_sample) {" \u0000\u0000\u0000\x92\t\u0000\u0000\u0000\u0000\u0000\u0000\xD7\a\u0000\u0000\u0000\u0000\u0000\u0000\u0001\u0000\u0000\u0000\xC9e\x9A^R\x8A\xF2\u0006"}

  it 'should read OP_KILL_CURSORS' do
    message, x = WireMongo.receive(kill_cursors_sample)
    message.should == kill_cursors_sample

    x.should == {
      :cursorIDs => [500614594970674633],
      :header => {
        :messageLength => 32,
        :requestID => 2450,
        :responseTo => 0,
        :opCode => :kill_cursors
      }
    }
  end

  it 'should build OP_KILL_CURSORS' do
    raw, x = WireMongo.receive(kill_cursors_sample)
    x[:header].delete(:messageLength)
    WireMongo.build_kill_cursors(2450, 0, [500614594970674633]).should == x
  end

  it 'should write OP_KILL_CURSORS' do
    x = WireMongo.build_kill_cursors(2450, 0, [500614594970674633])
    WireMongo.write(x).should == kill_cursors_sample
  end

  it 'should hash query' do
    doc1 = WireMongo::build_query(10, 'foo', 'bar', {:x => 100}, {:x => 1})
    hash1 = WireMongo::hash doc1
    hash1.should =~ /^[0-9a-fA-F]{40}$/

    doc1[:header][:requestID] = 20
    doc1[:header][:responseTo] = 50
    WireMongo::hash(doc1).should == hash1

    doc2 = WireMongo::build_query(10, 'foo', 'bar', {:x => 100}, {:x => 0})
    WireMongo::hash(doc2).should_not == hash1
  end
end
