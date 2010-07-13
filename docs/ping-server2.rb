#!/usr/bin/env ruby

require 'rubygems'
require 'marinda'

$ts = Marinda::Client.new(UNIXSocket.open("/tmp/localts.sock"))
$ts.hello

$ts.take_all ["PING-SERVER", "EUROPE"] do end  # clear stale tuples
$ts.write ["PING-SERVER", "EUROPE"]  # register server

loop do
  request = $ts.take_priv ["PING", nil]
  addr = request[1]
  rtt = 123.456
  $ts.reply ["RESULT", addr, rtt]
end
