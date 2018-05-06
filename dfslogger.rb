module Dfs
  # Module that can be included or mixed in to
  # provide logging capability.
  #--
  # Note:I could just use log4r, but that would probably be overkill.
  #
  # Later note : Had more problems than I would have liked with my
  # own buggy implementation. Note to self: Next time use log4r.
  module Logger
    
    LoggingLevel = {
      :off => 0,
      :debug => 1,
      :warning => 2,
      :error => 3
    }

    # Sets the logging level.
    def logging_level=(val)
      check_input(val)
      @level = LoggingLevel[val]
    end
    
    # Log a message at a given level.
    def output(level, msg)
      check_input(level)
      check_msg(msg)

      if LoggingLevel[level] >= @level
        case level
        when :debug; puts " .. Debug .. #{msg}\n"
        when :warning; puts " -- WARNING -- #{msg}\n"
        when :error; puts " ** ERROR ** #{msg}\n"
        end
        $stdout.flush
      end
    end
    
    # Log a message at debug level
    def debug(msg); output(:debug, msg); end

    # Log a message at error level
    def error(msg); output(:error, msg); end

    # Log a message at warning level
    def warning(msg); output(:warning, msg); end

    private
    def check_input(val)
      unless LoggingLevel.has_key? val
        raise ArgumentError.new("#{val} is not a valid logging level.")
      end
     end

    def check_msg(str)
    end
  end
end
