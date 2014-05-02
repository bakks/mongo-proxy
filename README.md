Mongo Proxy
===========

A gem for proxying MongoDB at the wire-protocol level. The proxy intercepts MongoDB communication, presents it in an easy-to-manipulate format, and forwards only desired messages. This approach allows you to filter MongoDB traffic, such as writes, or even inject new messages, such as a message-of-the-day that appears on new connections.

mongo-proxy includes a command-line interface and an API for running it in a Ruby application. You can write your own hooks to manipulate MongoDB wire traffic as it arrives from the client.

Installation
------------

`gem install mongo-proxy`

More information at the [gem page](http://rubygems.org/gems/mongo-proxy).

Command Line
------------

You can run mongo-proxy from the command line:

`mongo-proxy [options]`

Here is an example:

`mongo-proxy --client-port 29017 --read-only --motd 'Connected to proxy!'`

This will proxy local client connections to port 29017 to a running MongoDB server at port 27017. Only read queries will be allowed and the client will be told on startup that he is connecting to a proxy server. With the proxy running, a connection looks something like:

```
> mongo --port 29017
MongoDB shell version: 2.6.0
connecting to: 127.0.0.1:29017/test
Server has startup warnings:
Connected to proxy!
```

#### Command Line Options

The following flags can be used when connecting:
```
        --client-host, -h <s>:   Set the host to bind the proxy socket on
                                 (default: 127.0.0.1)
        --client-port, -p <i>:   Set the port to bind the proxy socket on
                                 (default: 27018)
        --server-host, -H <s>:   Set the backend hostname which we proxy
                                 (default: 127.0.0.1)
        --server-port, -P <i>:   Set the backend port which we proxy (default:
                                 27017)
              --read-only, -r:   Prevent any traffic that writes to the
                                 database
                   --motd, -m:   Set a message-of-the-day to display to clients
                                 when they connect
  --verbose, --no-verbose, -v:   Print out MongoDB wire traffic (default: true)
      --debug, --no-debug, -d:   Print log lines in a more human-readible
                                 format (default: true)
                --version, -e:   Print version and exit
                   --help, -l:   Show this message

```

API
---

You can also use the mongo-proxy functionality directly in Ruby. This approach allows you to add your own hooks to manipulate MongoDB traffic. Here is an example:

```ruby
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
```

This example opens up a read-only proxy at port 29017. There are two stacks of callbacks: the 'front' callbacks are executed before applying the read-only authorization, while the 'back' callbacks are called after. Thus, the back callbacks will only receive messages that passed authorization. Calling `add_callback_to_front` adds a hook to the front of the front stack, while calling `add_callback_to_back` adds a hook to the back of the back stack. Once you call `start` the proxy will run and show both the messages it receives (because of the debug flag) and the messages from the callback.

Callbacks are executed and passed the client connection on which a message was received and the message itself as a `Hash`. The callback can make changes to this message, if desired. The callback is expected to return a message to pass along through the stack of callbacks and eventually forwarded to the backend MongoDB server. If a callback returns `nil` then the message will dropped.

mongo-proxy is a thin layer on top of [em-proxy](https://github.com/igrigorik/em-proxy), and the connection object passed to your hook is the same as the em-proxy connection object. This means that you can call the `send_data` method on it to send a raw message to the client.

#### MongoProxy Options

You can use the following options (shown with their default values), when creating a `MongoProxy` object.

```ruby
@config = {
  :client_host => '127.0.0.1', # Set the host to bind the proxy socket on.
  :client_port => 29017,       # Set the port to bind the proxy socket on.
  :server_host => '127.0.0.1', # Set the backend host which we proxy.
  :server_port => 27017,       # Set the backend port which we proxy.
  :motd => nil,                # Set a message-of-the-day to display to clients when they connect. nil for none.
  :read_only => false,         # Prevent any traffic that writes to the database.                                 
  :verbose => false,           # Print out MongoDB wire traffic.
  :logger => nil,              # Use this object as the logger instead of creating one.
  :debug => false              # Print log lines in a more human-readible format.
} 
```

#### Message Format

Here is an example message passed to a hook:
```ruby
{
  :flags => 0,
  :database => 'test',
  :collection => 'testcoll',
  :numberToSkip => 0,
  :numberToReturn => 4294967295,
  :query => {
    "foo" => "bar"
  },
  :returnFieldSelector => nil,
  :header => {
    :messageLength => 58,
    :requestID => 1,
    :responseTo => 0,
    :opCode => :query
  }
}
```

This format comes from our [wire parsing code](lib/mongo-proxy/wire.rb), and will look similar, but differ slightly, for other operations such as inserts or deletes.

Testing
-------

Running the unit tests requires a MongoDB instance at port 27018 (nonstandard) and nothing at port 27017. The tests use rspec, so you can run with `bundle exec rspec`.

License
-------

[The MIT License](LICENSE.md) - Copyright 2014 Peter Bakkum
