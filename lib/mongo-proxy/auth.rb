require 'securerandom'

class AuthMongo
  @@admin_cmd_whitelist = [
    { 'ismaster' => 1 },
    { 'isMaster' => 1 },
    { 'listDatabases' => 1},
    {
      'replSetGetStatus' => 1,
      'forShell' => 1
    }
  ]

  def initialize(config = nil)
    @config = config
    @log = @config[:logger]
    @request_id = 20
    @last_error = {}
  end

  def wire_auth(conn, msg)
    return nil unless msg

    authed, response = auth(conn, msg)

    if !authed
      @last_error[conn] = true
    else
      @last_error[conn] = false
    end

    if response
      return WireMongo::build_reply(response, get_request_id, msg[:header][:requestID])
    else
      return authed
    end
  end

  private

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
    @log.info "replying unauthed for collection #{db}.#{coll}"
    return false, {
      'ok' => 0,
      'n' => 0,
        'code' => 2,
        'errmsg' => 'Writes and Javascript execution are disallowed in this interface.',
      'writeErrors' => {
        'index' => 0,
        'code' => 2,
        'errmsg' => 'Writes and Javascript execution are disallowed in this interface.'
      }
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


  def auth(conn, msg)
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
          if @last_error[conn]
            return reply_unauth(db, coll)
            @last_error[conn] = false
          else
            return true, nil
          end

        # allow ismaster query, listDatabases query
        elsif db == 'admin'
          if @@admin_cmd_whitelist.include?(query)
            return true, nil
          elsif query['getLog'] == 'startupWarnings'
            if @config[:motd]
              return reply_motd
            else
              return true
            end
          end

        end # if db == 'admin'
        return reply_unauth(db, coll)
      end # if coll == '$cmd'

      return true, nil if coll == 'system.namespaces' # list collections
      return reply_unauth(db, coll) if coll[0] == '$' #other command

      return true, nil

    when WireMongo::OP_UPDATE, WireMongo::OP_INSERT, WireMongo::OP_DELETE
      #return reply_unauth(db, coll)
      return false, nil

    else
      return reply_unauth(db, coll)

    end
  end
end

