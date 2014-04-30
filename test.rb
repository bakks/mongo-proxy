require 'mongo'

mongo = Mongo::Connection.new
mongo['pbbakkum']['test'].remove({'$or' => []})

