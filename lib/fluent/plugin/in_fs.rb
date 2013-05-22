#
#  Copyright 2013 the original author or authors.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

require 'sys/filesystem'
include Sys

module Fluent
  
  class FileSystemInput < Input
    Plugin.register_input('fs',self)

    SIZE_MB = 1024 * 1024
    SIZE_GB = 1024 * 1024 * 1024
    
    config_param :tag, :string, :default => nil
    config_param :filesystems, :string
    config_param :run_interval, :time
    
    def initialize
      super
    end
    
    def configure(conf)
      super
      @filesystems = @filesystems.split(',')      
    end
    
    def start
      @finished = false
      @thread = Thread.new(&method(:run_periodic))
    end
    
    def shutdown
      if @run_interval
        @finished = true
        @thread.join
      else
        Process.kill(:TERM, @pid)
        if @thread.join(60)
          return
        end
        Process.kill(:KILL, @pid)
        @thread.join
      end
    end
    
    # 
    # Main loop 
    # 
    # Uses Filesystem.stat to retrieve information related to the configured
    # filesystems.
    #
    # JSON output:
    #
    #   { 
    #     "path"      : "/var",
    #     "size"      : 10,
    #     "size_unit" : "GB",
    #     "free"      : 9,
    #     "free_unit" : "GB",
    #     "used"      : 1,
    #     "used_unit" : "GB",
    #     "perc"      : 20,
    #   }
    #
    def run_periodic
      until @finished
        sleep @run_interval
        
        tag = @tag
      
        for i in 0..@filesystems.length-1
          fs = @filesystems[i]
          st = Filesystem.stat(fs)
          sz = convert_size(st.block_size,st.blocks)
          sf = convert_size(st.block_size,st.blocks_available)
          su = convert_size(st.block_size,st.blocks - st.blocks_available) 
          bp = (st.blocks - st.blocks_available) * 100 / st.blocks

          Engine.emit(tag, Engine.now.to_i, {
            "path" => fs,
            "size" => sz[0], "size_unit" => sz[1],
            "free" => sf[0], "free_unit" => sf[1],
            "used" => su[0], "used_unit" => su[1],
            "perc" => bp.to_i
          })
        end
        
      end
    end    
    
    #
    # Convert the block 
    # 
    # block_size : size of the block
    # blocks     : number of blocks
    #
    # returns a human readable size and the size unit (MB or GB)
    # 
    def convert_size(block_size,blocks)
      size = block_size * blocks
      if size > SIZE_GB
        return [(size / SIZE_GB).to_i,"GB"]
      else
        return [(size / SIZE_MB).to_i,"MB"]
      end
    end
    
    private :run_periodic
    private :convert_size

  end
  
end

