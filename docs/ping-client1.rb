#!/usr/bin/env ruby

require 'rubygems'
require 'marinda'

$ts = Marinda::Client.new(UNIXSocket.open("/tmp/localts.sock"))
$ts.hello

$ts.write ["PING", "192.168.0.5"]
result = $ts.take_priv ["RESULT", nil, nil]
printf "RTT = %f\n", result[2]
