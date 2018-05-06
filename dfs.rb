#!/usr/bin/env ruby

require 'dfsreqs'
require 'dfsfile'
require 'optparse'

include FuseFS
include Dfs::Constants

# This class is the Distributed File System client that
# responds to the FUSE API calls. It should run locally
# on the user's machine, but may communicate remotely with
# other servers (e.g. ExtentServer)
#
# Author :: Tim Destan (mailto:tim.destan@gmail.com)
#
class Dfs::Client
  include Comm 
  
  attr_accessor :extent_host, :extent_port, :extent_sock

  # Constructor - Initializes the client.
  #
  def initialize(path, ll)
    logging_level = ll    
    @path = path
    @root = Dfs::DfsDir.new
    @root.client = self
  end

  # Main method - Parses command line options and
  # creates and runs an instance of Client.
  #
  def self.main
    options = {
      :extent_host => DEFAULT_EXTENT_HOSTNAME,
      :extent_port => DEFAULT_EXTENT_PORT,
      :logging_level => :debug,
      :mount_point => nil
    }
    
    clopts = OptionParser.new do |opts|
      opts.banner = "Usage: #{$0} [options] /mount/point [-h for details]"
      
      opts.on("-e", "--extent-port", Numeric,
              "Port number extent server is running on;\n\t" +
              " #{DEFAULT_EXTENT_PORT} by default.") do |v|
        options[:extent_port] = v
      end
      opts.on("-E", "--extent-host", String,
              "Hostname that extent server is running on;\n\t" +
              " #{DEFAULT_EXTENT_PATH} by default.") do |v|
        options[:extent_host] = v
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
    
    if (ARGV.size != 1)
      puts clopts
      exit
    else
      options[:mount_point] = ARGV[0]
    end
    
    begin
      clopts.parse!
    rescue
      puts $!
      exit
    end

    # Create the directory if it doesn't exist
    unless File.exists? options[:mount_point]
      puts "#{options[:mount_point]} does not exist."
      exit
    end

    # If it does exist but isn't a directory, complain and exit
    unless File.directory? options[:mount_point]
      puts "#{options[:mount_point]} is not a directory!"
      exit
    end

    # Create a client
    client = new(options[:mount_point], options[:logging_level])

    begin
      client.logging_level = options[:logging_level].to_sym
    rescue
      puts "#{options[:logging_level]} is not a valid logging level!"
      exit
    end

    # Set the extent server's hostname and port.
    client.extent_host = options[:extent_host]
    client.extent_port = options[:extent_port]
    
    client.mount
  end

  # Mount the FUSE filesystem. This method will not return unless interrupted.
  #
  def mount
    begin
      debug("Contacting the extent server.")
      @extent_sock = TCPSocket.open(@extent_host, @extent_port)
      debug("Got a socket (File descriptor = #{@extent_sock.fileno})")
      
      debug("Setting FUSE root.")
      FuseFS.set_root(@root)
      debug("Mounting FUSE... #{@path}")
      FuseFS.mount_under(@path)
      
      FuseFS.run
    rescue
      error("Encountered an error, quitting ...\n#{$!}\n")
    ensure
      cleanup
    end
  end

  # Cleans up by unmounting and closing any open sockets
  def cleanup
    if @extent_sock
      debug( "Closing extent server socket #{@extent_sock.fileno}")
      @extent_sock.close
      @extent_sock = nil
    end
    debug( "Unmounting FUSE...")
    begin
      FuseFS.exit # Stop running
      FuseFS.unmount
    rescue
      # Sadly this fails whenever any terminal is in a virtual Fuse
      # directory controlled by us.
      error("Couldn't unmount FUSE. You may need to unmount manually.")
    end
  end
end

# Run the main method
Dfs::Client.main
