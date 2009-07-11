#!/usr/bin/env ruby

require 'rubygems'
require 'marinda'

$ts = Marinda::Client.new(UNIXSocket.open("/tmp/localts.sock"))
$ts.hello

def dump(ts)
  ts.read_all([]) do |tuple|
    p tuple
  end
  nil
end

def clear(ts)
  while ts.takep []; end
  nil
end
