#!/usr/bin/env ruby
require 'dfsreqs'

include Comm

class TestMsgSer

  def test_roundtrip
    m1 = Msg.new(MsgType[:reply], MsgStatus[:ok], 342)
    j1 = JSON.generate(m1)
    m2 = JSON.parse(j1)
    return ((m1.code == m2.code) and
      (m1.status == m2.status) and
      (m1.length == m2.length))
  end

  # Disappointingly the Test::Unit automated runner fails in *their* code.
  # Have to do this the old-fashioned way.
  def self.main
    t = TestMsgSer.new
    t.methods.select { |m| m.to_s.index('test_') == 0 }.each { |m|
      puts "Running test #{m}..."
      if t.method(m).call
        puts "PASSED"
      else
        puts "FAILED"
      end
    }
  end

end

TestMsgSer.main

