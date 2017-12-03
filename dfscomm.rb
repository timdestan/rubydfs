require 'rubygems'
require 'json'

# This communication module handles the socket communication responsibilities
# of the DFS. The intended use is to mix this module into a class before
# calling the methods.
#
# Author :: Tim Destan (mailto:tim.destan@gmail.com)
#
module Comm
  # Include logger for logging errors.
  include Dfs::Logger

  # Hash of the message types
  MsgType = {
    :reply => 1,
    
    :get_extents => 2,
    :put_extents => 3,
    :poll_extent => 4
  }
  
  # Status codes for a message
  #
  # * none is a placeholder that should not be used.
  # * ok signals a successful message.
  # * affirmative represents a "YES" response to a query (alias for ok)
  # * negative represents a "NO" response to a query
  # * error signals some sort of error
  MsgStatus = {
    :none => 0,
    :ok => 1,
    :affirmative => 1, # Alias for OK
    :negative => 2,
    :error => 3
  }

  # A Msg is the header for messages passed through sockets.
  # It tells both the type of the message and the expected
  # number of bytes that follows.
  class Msg
      
    attr_accessor :code, :length, :status
    
    # Constructor
    # * code is a message code, e.g., Type[:reply]
    # * length is a number of bytes
    def initialize(code, length, stat)
      @code = code
      @length = length
      @status = stat
    end
    
    # Serialization (JSON)

    # Serializes this Msg to a JSON string 
    def to_json(*a)
      {
        'json_class'   => self.class.name,
        'data'         => [ @code, @length, @status ]
      }.to_json(*a)
    end
    
    # Create an instance of this class from
    # the given JSON string.
    def self.json_create(o)
      new(*o['data'])
    end
    
  end
  
  # Receive a message from a socket
  def receive(sock)
    debug( "Blocking to receive message header (fd=#{sock.fileno}).")
    # Get the message header first
    ser_msg = sock.gets.chomp
    debug( " Received serialized message #{ser_msg}")
    msg = JSON.parse(ser_msg)
    # How many bytes will follow?
    bytes = msg.length
    debug( " Expecting #{bytes} bytes...")
    data = ""

    if bytes > 0
      begin
        data = sock.gets.chomp
      rescue
        error( "Failed to receive data on socket #{sock.fileno}:\n#{$!}\n")
        msg.status = MsgStatus[:error]
        return [msg, ""]
      end
    end
    debug("   Done receiving #{data.size} bytes")
    [msg, data]
  end

  # Send a message to a socket
  def send(sock, msgcode, msgstat, data)
    datasize = data.nil? ? 0 : data.size
    debug( "Sending #{datasize} bytes of data " +
           "with message code = #{msgcode} on socket #{sock.fileno}")
       
    m = Msg.new(msgcode, datasize, msgstat)
    
    begin
      m_prime = JSON.generate(m)
      debug( "Sending message header: #{m_prime}")
      sock.puts(m_prime)
    rescue
      error( "Failed to send message:\t#{$!}")
    end

    if datasize > 0
      begin
        debug(    "Sending message payload (#{datasize} bytes)")
        sock.puts(data)
        debug(    "Data sent")
      rescue
        error( "Failed to send message:\t#{$!}")
      end
    else
      debug(  "No more data to send.")
    end
  end

  # Send a message to a socket and return the resulting reply
  def send_and_reply(sock, msgcode, msgstat, data)
    send(sock, msgcode, msgstat, data)
    receive(sock)
  end

  # Sends a reply message
  def reply(sock, msgstat, data)
    send(sock, MsgType[:reply], msgstat, data)
  end
  
end
