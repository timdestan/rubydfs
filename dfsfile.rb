#!/usr/bin/env ruby

require 'dfsreqs'
require 'monitor'

include Dfs::Constants

class String
  # Converts the string into an array of blocks,
  # each with size less than or equal to block_size  
  #
  def blockify(block_size=EXTENT_SIZE)
    rv = []
    i = 0
    while i < length
      t = self[i .. i + block_size - 1]
      rv << t
      i += t.length
    end
    #puts "Successfully blockified string into an array of #{rv.length} blocks"
    rv
  end
end

module Dfs

  # Module representing a node in the filesystem,
  # whether a node or a directory.
  module DfsNode
    attr_accessor :flags, :mtime, :ctime, :atime
    attr_accessor :client
  end

  # Class representing a file node
  #
  class DfsFile
    include DfsNode
    
    attr_reader :size
    
    # Possibly unnecessary
    @@extent_lock = Monitor.new
    
    def initialize()
      @is_open = false
      @contents = ""
      @hashes = []
      @size = 0
    end

    # Read file (uses extent server).
    #
    def read
      raise RuntimeError.new("NO CLIENT!") unless @client
      @client.debug("    DfsFile::read")
      if @hashes.nil? or @hashes.empty?
        @client.debug("No hashes, empty file.")
        @contents = ""
        @size = 0
      else
        msgdata = JSON.generate(@hashes)
        response, data = nil, nil
        @@extent_lock.synchronize {
          response, data =
            @client.send_and_reply(@client.extent_sock,
                                 Comm::MsgType[:get_extents],
                                 Comm::MsgStatus[:none],
                                 msgdata)
        }
        if response.status == Comm::MsgStatus[:ok]
          @contents = JSON.parse(data).join
          @size = strlen(@contents)
        else
          @client.output(:error, "Bad response from extent server:\t" +
                         "#{response.status}.")
          @contents = ""
          @size = 0
        end
      end
      @contents
    end

    # Writes file (uses extent server).
    #
    def write(str)
      raise RuntimeError.new("NO CLIENT!") unless @client
      @client.debug("    DfsFile::write")
      @contents = str
      @size = strlen(@contents)
      if @size == 0
        @hashes = []
      else
        msgdata = JSON.generate(@contents.blockify)
        response, data = nil, nil
        @@extent_lock.synchronize {
          response, data =
            @client.send_and_reply(@client.extent_sock,
                                 Comm::MsgType[:put_extents],
                                 Comm::MsgStatus[:none],
                                 msgdata)
        }
        if response.status == Comm::MsgStatus[:ok]
          @hashes = JSON.parse(data)
          @client.debug("Broke string into #{@hashes.size} " +
                        "extents, stored as hashes.")
        else
          @client.error( "Bad response from extent server:\t" +
                         "#{response.status}.")
          @hashes = []
        end
      end
    end
  end

  # Class representing a directory node. Extends FuseFS::MetaDir,
  # which is in the provided library and provides a basic implementation
  # for a simple in memory inode.
  #
  class DfsDir < FuseFS::MetaDir
    include DfsNode

    # Contents of directory.
    def contents(path)
      @client.debug(" CONTENTS #{path}")
      base, rest = split_path(path)
      case
      when base.nil?
        (@files.keys + @subdirs.keys).sort.uniq
      when ! @subdirs.has_key?(base)
        nil
      when rest.nil?
        @subdirs[base].contents('/')
      else
        @subdirs[base].contents(rest)
      end
    end
    
    # File types (identify files and directories)
    
    def directory?(path)
      debug(" DIRECTORY? #{path}")
      base, rest = split_path(path)
      case
      when base.nil?
        true
      when ! @subdirs.has_key?(base)
        false
      when rest.nil?
        true
      else
        @subdirs[base].directory?(rest)
      end
    end

    def file?(path)
      debug(" FILE? #{path}")
      base, rest = split_path(path)
      case
      when base.nil?
        false
      when rest.nil?
        @files.has_key?(base)
      when ! @subdirs.has_key?(base)
        false
      else
        @subdirs[base].file?(rest)
      end
    end
    
    def chmod(*args)
      debug(" CHMOD #{args}")
    end
    
    # Required on Mac to work correctly
    def size(path)
      debug(" SIZE #{path}")
      base, rest = split_path(path)
      case
      when base.nil?
        default
      when rest
        @subdirs[base].size(rest)
      when @files.has_key?(base)
        @files[base].size
      else
        @subdirs[base].size(rest)
      end
    end
    
    # Read a file from the given path.
    # - path - Path to the file to read
    def read_file(path)
      @client.debug(" READ FILE #{path}")
      base, rest = split_path(path)
      case
      when base.nil?
        nil
      when rest.nil?
        @files[base].read
      when ! @subdirs.has_key?(base)
        nil
      else
        @subdirs[base].read_file(rest)
      end
    end

    # Write to a file
    def can_write?(path)
      @client.debug(" CAN_WRITE? #{path}")
      return false unless Process.uid == FuseFS.reader_uid
      base, rest = split_path(path)
      case
      when base.nil?
        true
      when rest.nil?
        true
      when ! @subdirs.has_key?(base)
        false
      else
        @subdirs[base].can_write?(rest)
      end
    end

    # Write to a file at the given path.
    # - path - Path to the file
    # - file - contents to write
    def write_to(path,file)
      @client.debug(" WRITE FILE #{path}")
      base, rest = split_path(path)
      case
      when base.nil?
        false
      when rest.nil?
        if @files[base].nil?
          @files[base] = DfsFile.new 
          @files[base].client = @client
        end
        @files[base].write(file)
      when ! @subdirs.has_key?(base)
        false
      else
        @subdirs[base].write_to(rest,file)
      end
    end

    # Make a new directory
    def can_mkdir?(path)
      @client.debug( "CAN MKDIR #{path}")
      unless Process.uid == FuseFS.reader_uid
        return false
      end
      base, rest = split_path(path)
      case
      when base.nil?
        false
      when rest.nil?
        ! (@subdirs.has_key?(base) || @files.has_key?(base))
      when ! @subdirs.has_key?(base)
        false
      else
        @subdirs[base].can_mkdir?(rest)
      end
    end

    # Makes a directory. Overriding default
    # to set client info correctly and create
    # instances of our subclass instead of the
    # super class.
    def mkdir(path,dir=nil)
      @client.debug(" MKDIR #{path}")
      base, rest = split_path(path)
      case
      when base.nil?
        false
      when rest.nil?
        dir ||= DfsDir.new
        dir.client = @client
        @subdirs[base] = dir
        true
      when ! @subdirs.has_key?(base)
        false
      else
        @subdirs[base].mkdir(rest,dir)
      end
    end
  end
end
