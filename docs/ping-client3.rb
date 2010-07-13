#!/usr/bin/env ruby

require 'rubygems'
require 'marinda'

$ts = Marinda::Client.new(UNIXSocket.open("/tmp/localts.sock"))
$ts.hello

$ts.read ["PING-SERVER", "EUROPE"]  # get ref to private region
eu_peer = $ts.remember_peer()

$ts.read ["PING-SERVER", "ASIA"]
asia_peer = $ts.remember_peer()

$ts.write_to eu_peer, ["PING", "192.168.0.5"]
$ts.write_to asia_peer, ["PING", "192.168.0.5"]

result = $ts.take_priv ["RESULT", nil, nil]
result = $ts.take_priv ["RESULT", nil, nil]
