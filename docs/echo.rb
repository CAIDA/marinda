#!/usr/bin/env ruby

require 'rubygems'
require 'marinda'

$ts = Marinda::Client.new(UNIXSocket.open("/tmp/localts.sock"))
$ts.hello
$ts2 = $ts.duplicate

eloop = Marinda::ClientEventLoop.new
eloop.add_source $ts
eloop.add_source $ts2

$ts.consume_async(["ECHO", nil]) do |tuple|
  p tuple[1]
end

$ts2.take_async(["QUIT"]) do |tuple|
  puts "Exiting."
  eloop.suspend()
end

eloop.start()
