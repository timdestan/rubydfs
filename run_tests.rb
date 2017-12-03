#!/usr/bin/env ruby
# Only runnable from main directory.
# Not sure the 'right' way people would normally accomplish this.


Dir.entries('./test/').each { |entry|
  case entry
  when '.'
    next
  when '..'
    next
  else
    puts "Running test #{entry}"
    ans = `ruby ./test/#{entry}`
    puts "RESULT:"
    puts ans
  end
}
