require 'bson'

# This is a set of functions for dealing with the Mongo wire protocol. It has
# methods for moving between Ruby hashes and Mongo wire segments, including
# their embedded BSON. The MongoDB wire protocol is documented at:
#
module WireMongo

  HEADER_SIZE           = 16

  OP_REPLY              = :reply
  OP_MSG                = :msg
  OP_UPDATE             = :update
  OP_INSERT             = :insert
  OP_QUERY              = :query
  OP_GET_MORE           = :get_more
  OP_DELETE             = :delete
  OP_KILL_CURSORS       = :kill_cursors

  OPS = {
    1    => OP_REPLY,
    1000 => OP_MSG,
    2001 => OP_UPDATE,
    2002 => OP_INSERT,
    2004 => OP_QUERY,
    2005 => OP_GET_MORE,
    2006 => OP_DELETE,
    2007 => OP_KILL_CURSORS
  }
  OPS_INVERTED = OPS.invert

  FLAG_UPDATE_UPSERT        = 1
  FLAG_UPDATE_MULTIUPDATE   = (1 << 1)
  FLAG_DELETE_MULTI         = 1

  # Parse out an arbitrary binary mongo message, returning a hash
  # representation for easy manipulation.
  def self.receive socket
    if socket.is_a?(String)
      socket = StringIO.new(socket)
      socket.set_encoding('UTF-8', 'UTF-8')
    end

    chunk1, x = receive_header(socket)
    return nil, nil unless x && chunk1

    parsed = {}

    chunk2 = socket.read(x[:messageLength] - HEADER_SIZE)

    case x[:opCode]
    when OP_REPLY
      parsed = receive_reply(chunk2)
    when OP_UPDATE
      parsed = receive_update(chunk2)
    when OP_INSERT
      parsed = receive_insert(chunk2)
    when OP_QUERY
      parsed = receive_query(chunk2)
    when OP_DELETE
      parsed = receive_delete(chunk2)
    when OP_GET_MORE
      parsed = receive_get_more(chunk2)
    when OP_KILL_CURSORS
      parsed = receive_kill_cursors(chunk2)
    else
      puts "could not parse message type :#{x[:opCode]}:"
    end

    parsed[:header] = x
    full = chunk1 + chunk2
    full = full.force_encoding('UTF-8')
    return full, parsed

  rescue Exception => e
    @@log.warn "failed to read from socket #{socket.to_s}"
    return nil
  end

  # Write a hash document representation into its corresponding binary form.
  # This method can be used with documents in the format that receive returns,
  # making it easy to parse a message, change it, and re-encode it.
  def self.write doc
    body = nil

    case doc[:header][:opCode]
    when OP_REPLY
      body = write_reply(doc)
    when OP_UPDATE
      body = write_update(doc)
    when OP_INSERT
      body = write_insert(doc)
    when OP_QUERY
      body = write_query(doc)
    when OP_DELETE
      body = write_delete(doc)
    when OP_GET_MORE
      body = write_get_more(doc)
    when OP_KILL_CURSORS
      body = write_kill_cursors(doc)
    else
      puts "could not write message type :#{doc[:header][:opCode]}:"
      return nil
    end

    body = body.force_encoding('UTF-8')

    return write_header(doc[:header], body)
  end

  # Receive the Mongo Wire message header from a stream.
  #
  # int32 :messageLength - Length in bytes of subsequent message.
  # int32 :requestID - Identifier of this message.
  # int32 :responseTo - RequestID from the original request.
  # int32 :opCode - Message type.
  def self.receive_header(stream)
    chunk = stream.read(HEADER_SIZE)
    return nil unless chunk != nil && chunk.bytesize == HEADER_SIZE

    x = {}
    x[:messageLength], x[:requestID], x[:responseTo], x[:opCode] = chunk.unpack('VVVV')
    x[:opCode] = OPS[x[:opCode]]
    return chunk, x
  end

  def self.write_header doc, body
    raise 'no requestID' unless doc[:requestID]
    raise 'no opCode' unless doc[:opCode]
    response_to = (doc[:responseTo] or 0)
    length = body.bytesize + HEADER_SIZE

    header = [length, doc[:requestID], response_to, OPS_INVERTED[doc[:opCode]]].pack('VVVV')
    header = header.force_encoding('UTF-8')
    return header + body
  end

  def self.receive_bson(chunk, start, max = 10000)
    docs = []

    while start < chunk.bytesize and docs.size < max
      bsonLength = chunk[start..(start + 4)].unpack('V')[0]
      doc = nil

      begin
        doc = BSON.deserialize(chunk[start..(start + bsonLength - 1)])
      rescue Exception => e
        puts 'could not deserialize BSON:'
        pp chunk[start..(start + bsonLength)]
        return nil, nil
      end

      docs << doc
      start += bsonLength
    end

    return docs, start
  end

  def self.write_bson(docs)
    docs = [docs] if docs.is_a? Hash
    
    x = ''
    docs.each do |doc|
      x << BSON.serialize(doc).to_s
    end

    return x
  end

  def self.min(a, b)
    (a > b ? b : a)
  end

  def self.parse_full_collection(full_collection)
    x = full_collection.split('.')
    return x[0], x[1..-1].join('.')
  end

  def self.build_full_collection(database, collection)
    return "#{database}.#{collection}"
  end

  # OP_REPLY: 1
  # A reply to a client request.
  #
  # header :header - Message header.
  # int32 :responseFlags - A bit vector of response flags.
  # int64 :cursorID - ID of open cursor, if there is one. 0 otherwise.
  # int32 :startingFrom - Offset in cursor of this reply message.
  # int64 :numberReturned - Number of documents in the reply.
  def self.receive_reply(chunk)
    x = {}
    x[:responseFlags], x[:cursorID], x[:startingFrom], x[:numberReturned] = chunk.unpack('VQ<VV')
    x[:documents], _ = receive_bson(chunk, 20, x[:numberReturned])
    return x
  end

  def self.build_reply(documents, request_id, response_to,
      response_flags = 0, cursor_id = 0, starting_from = 0)
    documents = [documents] if documents.is_a?(Hash)

    return {
      :responseFlags => response_flags,
      :startingFrom => starting_from,
      :numberReturned => documents.size,
      :cursorID => cursor_id,
      :documents => documents,
      :header => {
        :requestID => request_id,
        :responseTo => response_to,
        :opCode => OP_REPLY
      }
    }
  end

  def self.write_reply(doc)
    raise 'no responseTo' unless doc[:header][:responseTo]
    raise 'no documents' unless doc[:documents]
    responseFlags = (doc[:responseFlags] || 0)
    cursorId = (doc[:cursorID] || 0)
    startingFrom = (doc[:startingFrom] || 0)
    numberReturned = doc[:numberReturned]

    msg = [responseFlags, cursorId, startingFrom, numberReturned].pack('VQ<VV')
    msg << write_bson(doc[:documents])

    return msg
  end

  # OP_UPDATE: 2001
  # A MongoDB update query message.
  #
  # header :header - Message header.
  # int32 - An empty value.
  # string :database.:collection - Database and collection name for update.
  # int32 :flags - Bit vector of update flags.
  # document :selector - BSON document representing update target.
  # document :update - BSON document representing the update to perform.
  def self.receive_update(chunk)
    x = {}
    _, full, x[:flags] = chunk.unpack('VZ*V')
    x[:database], x[:collection] = parse_full_collection(full)
# TODO break out flags
    docs, _ = receive_bson(chunk, full.bytesize + 9, 2)
    x[:selector] = docs[0]
    x[:update] = docs[1]

    return x
  end

  def self.build_update(request_id, database_name, collection_name, selector, update, flags = [])
    flag = 0
    flag = (flag | FLAG_UPDATE_UPSERT) if flags.include?(:upsert)
    flag = (flag | FLAG_UPDATE_MULTIUPDATE) if flags.include?(:multi)

    return {
      :header => {
        :opCode => OP_UPDATE,
        :requestID => request_id,
        :responseTo => 0
      },
      :database => database_name,
      :collection => collection_name,
      :selector => selector,
      :update => update,
      :flags => flag
    }
  end

  def self.write_update doc
    raise 'missing collection info' unless doc[:database] && doc[:collection]
    raise 'missing selector' unless doc[:selector]
    raise 'missing update' unless doc[:update]
    flags = (doc[:flags] or 0)

    msg = [0, build_full_collection(doc[:database], doc[:collection]), flags].pack('VZ*V')
    msg << write_bson(doc[:selector])
    msg << write_bson(doc[:update])

    return msg
  end

  # OP_INSERT: 2002
  #
  # header :header - Message header.
  # int32 :flags - Bit vector flags.
  # string :database.:collection - Database + collection name.
  # document[] :documents - An array of BSON documents.
  def self.receive_insert(chunk)
    x = {}
    x[:flags], full = chunk.unpack('VZ*')
    x[:database], x[:collection] = parse_full_collection(full)
    x[:documents], _ = receive_bson(chunk, full.bytesize + 5)

    return x
  end

  def self.build_insert(request_id, database_name, collection_name, documents, flags = 0)
    documents = [documents] if documents.is_a?(Hash)
    return {
      :flags => flags,
      :database => database_name,
      :collection => collection_name,
      :documents => documents,
      :header => {
        :requestID => request_id,
        :responseTo => 0,
        :opCode => OP_INSERT
      }
    }
  end

  def self.write_insert(doc)
    raise 'missing full collection' unless doc[:database] && doc[:collection]
    raise 'missing documents' unless doc[:documents]
    flags = (doc[:flags] or 0)
    docs = doc[:documents]
    docs = [docs] if docs.is_a? Hash

    msg = [flags, build_full_collection(doc[:database], doc[:collection])].pack('VZ*')
    msg << write_bson(docs)

    return msg
  end

  # OP_QUERY: 2004
  #
  # header :header - Message header.
  # int32 :flags - A bit vector of query flags.
  # string :database.:collection - Database + collection name.
  # int32 :numberToSkip - Offset for results.
  # int32 :numberToReturn - Limit for results.
  # document :query - BSON document of query.
  # document :returnFieldsSelector - Optional BSON document to select fields in response.
  def self.receive_query(chunk)
    x = {}
    x[:flags], full, x[:numberToSkip], x[:numberToReturn] = chunk.unpack('VZ*VV')
    start = 3 * 4 + full.bytesize + 1
    x[:database], x[:collection] = parse_full_collection(full)
    docs, start = receive_bson(chunk, start, 2)
    x[:query] = docs[0]
    x[:returnFieldSelector] = (docs.size > 1 ? docs[1] : nil)

    return x
  end

  def self.build_query(request_id, database_name, collection_name,
      query = {}, fields = nil, num_to_return = 4294967295, number_to_skip = 0, flags = 0)
    {
      :header => {
        :opCode => OP_QUERY,
        :requestID => request_id,
        :responseTo => 0
      },
      :database => database_name,
      :collection => collection_name,
      :query => query,
      :returnFieldSelector => fields,
      :numberToReturn => num_to_return,
      :flags => flags,
      :numberToSkip => number_to_skip
    }
  end

  def self.write_query(doc)
    raise 'missing full collection name' unless doc[:database] && doc[:collection]
    flags = (doc[:flags] or 0)
    numberToSkip = (doc[:numberToSkip] or 0)
    numberToReturn = (doc[:numberToReturn] or 4294967295)
    query = (doc[:query] or {})
    returnFieldSelector = (doc[:returnFieldSelector] or nil)

    msg = [flags, build_full_collection(doc[:database], doc[:collection]), numberToSkip, numberToReturn].pack('VZ*VV')
    msg << write_bson([query])
    msg << write_bson([returnFieldSelector]) if returnFieldSelector

    return msg
  end

  # OP_QUERY: 2005
  #
  # header :header - Message header.
  # int32 - Empty.
  # string :database.:collection - Database + collection name.
  # int32 :numberToReturn - Limit for next results reply.
  # int64 :cursorID - ID of cursor to consume more from.
  def self.receive_get_more(chunk)
    x = {}
    _, full, x[:numberToReturn], x[:cursorID] = chunk.unpack('VZ*VQ<')
    x[:database], x[:collection] = parse_full_collection(full)

    return x
  end

  def self.build_get_more(request_id, response_to, database_name, collection_name, cursor_id, number_to_return = 0)
    {
      :header => {
        :opCode => OP_GET_MORE,
        :requestID => request_id,
        :responseTo => response_to
      },
      :database => database_name,
      :collection => collection_name,
      :cursorID => cursor_id,
      :numberToReturn => number_to_return
    }
  end

  def self.write_get_more(doc)
    raise 'missing full collection name' unless doc[:database] && doc[:collection]
    raise 'missing cursorID' unless doc[:cursorID]

    numberToReturn = (doc[:numberToReturn] or 0)
    msg = [0, build_full_collection(doc[:database], doc[:collection]), numberToReturn, doc[:cursorID]].pack('VZ*VQ<')

    return msg
  end

  # OP_DELETE: 2006
  #
  # header :header - Message header.
  # int32 - Empty.
  # string :database.:collection - Database + collection name.
  # int32 :flags - Bit vector of delete-related flags.
  # document selector - Selector for deletion
  def self.receive_delete(chunk)
    x = {}
    _, full, x[:flags] = chunk.unpack('VZ*V')
    docs, start = receive_bson(chunk, full.bytesize + 9, 1)
    x[:database], x[:collection] = parse_full_collection(full)
    x[:selector], _ = docs[0]

    return x
  end

  def self.build_delete(request_id, database_name, collection_name, selector, opt = [])
    flags = 0
    flags = FLAGS_DELETE_MULTI if opt.include?(:multi)

    {
      :header => {
        :opCode => OP_DELETE,
        :requestID => request_id,
        :responseTo => 0
      },
      :database => database_name,
      :collection => collection_name,
      :selector => selector,
      :flags => flags
    }
  end

  def self.write_delete(doc)
    raise 'missing full collection name' unless doc[:database] && doc[:collection]
    raise 'missing selector' unless doc[:selector]
    flags = (doc[:flags] or 0)

    msg = [0, build_full_collection(doc[:database], doc[:collection]), flags].pack('VZ*V')
    msg << write_bson(doc[:selector])

    return msg
  end

  # OP_KILL_CURSORS: 2007
  # Message to explicitly delete a cursor. Only sent from the client in very
  # specific circumstances, cursors can also time out.
  #
  # header :header - Message header.
  # int32 - Empty.
  # int32 :numberOfCursorIDs - Number of cursors to kill.
  # int64[] :cursorIDs - Array of cursor ids to kill.
  def self.receive_kill_cursors(chunk)
    x = {}
    _, n = chunk.unpack('VV')
    x[:cursorIDs] = chunk[8..-1].unpack("Q<#{n}")
    return x
  end

  def self.build_kill_cursors(request_id, response_to, cursor_ids)
    return {
      :cursorIDs => cursor_ids,
      :header => {
        :opCode => OP_KILL_CURSORS,
        :requestID => request_id,
        :responseTo => response_to
      }
    }
  end

  def self.write_kill_cursors(doc)
    raise 'missing cursorIDs' unless doc[:cursorIDs]
    return ([0, doc[:cursorIDs].size] + doc[:cursorIDs]).pack("VVQ<*")
  end

  @@hash_getmore_history = {}

  def self.hash doc
    if doc[:header][:requestID]
      temp_req_id = doc[:header][:requestID]
      doc[:header][:requestID] = 1234
    end
    if doc[:header][:responseTo]
      temp_response_to = doc[:header][:responseTo]
      doc[:header][:responseTo] = 4321
    end

    if doc[:header][:opCode] == OP_GET_MORE
      key = build_full_collection(doc[:database], doc[:collection]) + doc[:cursorID].to_s
      @@hash_getmore_history[key] ||= 0
      doc[:header][:requestID] = @@hash_getmore_history[key]
      temp_cursor_id = doc[:cursorID]
      doc[:cursorID] = 0
      @@hash_getmore_history[key] += 1
    end

    x = Digest::SHA1.hexdigest(write doc)
    
    doc[:header][:requestID] = temp_req_id if temp_req_id
    doc[:header][:responseTo] = temp_response_to if temp_response_to
    doc[:cursorID] = temp_cursor_id if temp_cursor_id

    return x
  end
end

