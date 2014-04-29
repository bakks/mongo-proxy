require 'securerandom'

class AuthMongo
  def initialize(config = nil)
    @config = config
    @request_id = 20
  end

  def reply_ismaster
    return true, {
      'ismaster' => true,
      'maxBsonObjectSize' => 16777216,
      'ok' => 1.0
    }
  end

  def reply_motd
    motd = @config[:motd]
    motd = motd.split("\n")
    return true, {
      'totalLinesWritten' => motd.size,
      'log' => motd,
      'ok' => 1.0
    }
  end

  def reply_unauth(db, coll)
    puts "replying unauthed for collection #{db}.#{coll}"
    return false, {
      'assertion' => 'not authorized',
      'assertionCode' => 10057,
      'errmsg' => 'Writes and Javascript execution are disallowed in this Commonwealth interface.',
      'ok' => 0.0
    }
  end

  def reply_error(err)
    return false, {
      'errmsg' => err,
      'ok' => 0.0
    }
  end

  def reply_ok
    return true, {
      'ok' => 1.0
    }
  end

  def get_request_id
    x = @request_id
    @request_id += 1
    return x
  end

  def wire_auth(msg)
    return nil unless msg

    authed, response = auth(msg)

    if !authed
      @last_error = true
    else
      @last_error = false
    end

    if response
      return WireMongo::build_reply(response, get_request_id, msg[:header][:requestID])
    else
      return authed
    end
  end

  def auth(msg)
    op = msg[:header][:opCode]

    if op == WireMongo::OP_KILL_CURSORS
      return true, nil
    end

    db = msg[:database]
    coll = msg[:collection]
    query = (msg[:query] or {})

    case op
    when WireMongo::OP_QUERY, WireMongo::OP_GET_MORE
      return reply_unauth(db, coll) unless db and coll

      return reply_unauth(db, coll) if query['$where'] != nil
      
      # handle authentication process
      if coll == '$cmd'

        if query['count']
          # fields key can be nil but must exist
          unless query.size == 3 and query['query'] and query.has_key? 'fields'
            return reply_unauth(db, coll)
          end
          return true, nil

        elsif query['getlasterror'] == 1
          if @last_error
            return reply_unauth(db, coll)
            @last_error = false
          else
            return true, nil
          end

        # allow ismaster query, listDatabases query
        elsif db == 'admin'
          if (query['ismaster'] == 1 || query['isMaster'] == 1) && query.size == 1
            return reply_ismaster
          elsif query['listDatabases'] == 1 && query.size == 1
            return true, nil
          elsif query['getLog'] == 'startupWarnings'
            if @config[:motd]
              return reply_motd
            else
              return true
            end
          end

        end
        return reply_unauth(db, coll)
      end

      return true, nil if coll == 'system.namespaces' # list collections
      return reply_unauth(db, coll) if coll[0] == '$' #other command

      return true, nil

    when WireMongo::OP_UPDATE, WireMongo::OP_INSERT, WireMongo::OP_DELETE
      return reply_unauth(db, coll)

    else
      return reply_unauth(db, coll)
    end
  end
end

