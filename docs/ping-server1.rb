#!/usr/bin/env ruby

require 'rubygems'
require 'marinda'

$ts = Marinda::Client.new(UNIXSocket.open("/tmp/localts.sock"))
$ts.hello

loop do
  request = $ts.take ["PING", nil]
  addr = request[1]
  rtt = 123.456
  $ts.reply ["RESULT", addr, rtt]
end
