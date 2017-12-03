# Require farm
require 'fusefs'
require 'rubygems'
require 'json'
require 'dfslogger'
require 'digest/sha2'
require 'socket'
require 'dfscomm'

Thread.abort_on_exception = true # Don't hide exceptions that occur in threads.

module Dfs
  # Various constants for the DFS
  #--
  # Might as well put them here I guess.
  module Constants
    
    SNAPSHOT_PREFIX = "SNAP_"

    DEFAULT_LOGSERVER_HOSTNAME = "localhost"
    DEFAULT_LOGSERVER_PORT = 8990
    
    DEFAULT_EXTENT_PATH = ".extents"
    
    DEFAULT_EXTENT_HOSTNAME = "localhost"
    DEFAULT_EXTENT_PORT = 8989

    # Default path to mount to if none specified.
    # Either /tmp/username or /tmp/rubydfs if we can't figure out username.
    DEFAULT_MOUNT_POINT = ENV['USER'] ? "/tmp/#{ENV['USER']}" : "/tmp/rubydfs"
    
    SHA2_KEY_LENGTH = 256

    EXTENT_SIZE = 4096
    
  end
end
