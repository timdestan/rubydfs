#!/usr/bin/env ruby
require 'dfsreqs'
require 'optparse'
require 'monitor'

include Dfs::Constants


module Dfs
  # A single extent with its associated hash recipe.
  class Extent
    attr_reader :contents, :hash
    include Digest
    
    # Initialize this extent
    #
    def initialize(contents)
      @contents = contents
      @hash = SHA2.new(SHA2_KEY_LENGTH) << contents
    end

    # JSON serialization

    # Serializes the object to JSON
    #
    def to_json(*a)
      {
        'json_class' => self.class.name,
        'data' => [ @contents ] # Hash can be recomputed
      }.to_json(*a)
    end

    # Create an instance of this class from a serialized JSON object.
    #
    def self.json_create(o)
      new(*o['data'])
    end
  end

  # Server that serves extents to (potentially multiple) DFS clients,
  # which request said extents via their
  # cryptographic hashes.
  #
  # Author :: Tim Destan (mailto:tim.destan@gmail.com)
  #
  class ExtentServer
    include Comm
    
    def initialize(expath)
      @extent_path = expath
      @extents = {}
      @lock = Monitor.new
      logging_level = :debug
    end
    
    # Pushes an extent into our hash table.
    def put_extent ext
      unless ext.is_a? Extent
        raise ArgumentError.new("Bad extent type: #{ext.class}")
      end
      @lock.synchronize {
        # Overwrites existing extents
        @extents[ext.hash.to_s] = ext        
      }  
    end
    
    alias :<< :put_extent
    
    def flush_extents
      @lock.synchronize {
        @extents.values.each do |extent|
          file_name = File.join(@extent_path, extent.hash.to_s)
          
          begin
            f = File.open(file_name, "w+")
            f.write(JSON.generate([extent]))
          rescue
            output(:warning,
                   "Error writing extent file #{file_name} to disk (#{$!})!")
          end
      end
      }
    end
    
    # Tells whether we have an extent with the given hash
    def has_extent? hash
      @lock.synchronize {    
        @extents.has_key? hash.to_s
      }
    end
    
    # Gets the extent with the given hash or returns nil if undefined
    def get_extent hash
      @lock.synchronize { @extents[hash] }
    end
    
    def self.main
      options = {
        :extent_path => DEFAULT_EXTENT_PATH,
        :port => DEFAULT_EXTENT_PORT,
        :logging_level => :debug
      }
      
      clopts = OptionParser.new do |opts|
        opts.banner = "Usage: $0 [options] [-h for details]"
        
        opts.on("-p", "--port", Numeric, "Port number to run on;\n\t" +
                " #{DEFAULT_EXTENT_PORT} by default.") do |v|
          options[:port] = v
        end
        opts.on("-e", "--extents-path", String,
                "Path to the extent storage location;\n\t" +
                " \"#{DEFAULT_EXTENT_PATH}\" by default.") do |v|
          options[:extent_path] = v
        end
        opts.on("-l", "--logging-level", String,
                "Logging level;\n\tValid levels are " +
                "off, debug, warning, and error.") do |v|
          options[:logging_level] = v
        end
        opts.on("-h", "--help", "Show this message") do
          puts opts
          exit
        end
      end
      
      begin
        clopts.parse!
      rescue
        puts $!
        exit
      end
      
      # Create the directory if it doesn't exist
      unless File.exists? options[:extent_path]
        Dir.mkdir options[:extent_path]
      end
      
      # If it does exist but isn't a directory, complain and exit
      unless File.directory? options[:extent_path]
        puts "#{options[:extent_path]} is not a directory!"
        exit
      end
      
      server = ExtentServer.new(options[:extent_path])
      begin
        server.logging_level = options[:logging_level].to_sym
      rescue
        puts "#{options[:logging_level]} is not a valid logging level!"
        exit
      end
      server.read_extents
      server.run(options[:port])
    end
    
    # Read in all the extents in the extent directory
    def read_extents
      # Read in any extent files from this location
      Dir.entries(@extent_path) do |file_name|
        begin
          # Read string in from file, use JSON to parse it to an extent,
          # then add it to our collection.
          contents = File.read(file_name)
          self << JSON.parse(contents)[0]
        rescue
          # This could cause serious problems, but we'll warn them and continue.
          output(:warning,
                 "Encountered an error while reading extent #{file_name}:\n" +
                 "\t(#{$!})!")
        end
      end
    end

    # Listen on the given port for requests and return
    # the requested extents.
    def run(port)
      output(:debug, "Listening for requests on port #{port}...")
      begin
        TCPServer.open(port) do |server|
          # Listen on this port forever until we are killed
          loop do
            Thread.start(server.accept) do |client|
              listen(client)
            end
          end
        end
      rescue
        output(:warning, "Extent server interrupted!")
      ensure
        output(:debug, "ExtentServer::run exiting.")
      end
    end
    
    # Listen to a client socket and process requests.
    def listen(client)
      debug( "Opened socket for client #{client.fileno}...")
      begin
        # We never need to contact a client, so we can just listen
        until client.closed?
          m, data = receive(client)
          
          case m.code
          when MsgType[:get_extents]
            debug("Got get_extents request")
            hashes = JSON.parse(data)
            extents = []
            hashes.each { |hash|
              ex = get_extent(hash)
              if ex == nil
                error("Cannot find requested extent with hash #{hash}.")
                reply(client, MsgStatus[:negative], nil)
                next
              else
                extents << ex.contents
              end
            }
            reply(client, MsgStatus[:affirmative], JSON.generate(extents))
          when MsgType[:put_extents]
            debug("Got put_extents request")
            rv = []
            contents = JSON.parse(data)
            contents.each { |c|
              ex = Extent.new(c)
              rv << ex.hash.to_s
              self << ex
            }

            reply(client, MsgStatus[:ok], JSON.generate(rv))
          when MsgType[:poll_extent]
            hash = JSON.parse(data)[0]
            ex = get_extent(hash)
            # Just use reply code to signal the result
            if ex == nil
              reply(client, MsgStatus[:negative], nil) 
            else
              reply(client, MsgStatus[:affirmative], nil)
            end
          else
            output(:error, "Invalid message type #{m.code}.")
          end
        end
      rescue
        output(:error, "#{$!}\n\tType: #{$!.class}" +
               "\n\tBacktrace: #{$!.backtrace}")
      ensure
        client.close unless client.closed?
        output(:debug, "Connection to client #{client} closed.")
        output(:debug, "Flushing extents to disk.")
        flush_extents
      end
    end
  end
end
# Run the main method
Dfs::ExtentServer.main
