
require 'celluloid'
require 'celluloid/io'

module Node
  class Frontend
    include Celluloid::IO

    def initialize(host = "127.0.0.1", port = 3456)
      puts "*** Starting echo server on #{host}:#{port}"
      @cache   = Hash.new{|hash, key| hash[key] = ""}
      @server  = TCPServer.new(host, port)
      @workers = 20.times.map{ Worker.new }
      @worker_index = 0
      run!
    end

    def finalize
      @server.close if @server
    end

    def run
      loop { handle_connection! @server.accept }
    end

    def handle_connection(socket)
      _, port, host = socket.peeraddr
      puts "*** Received connection from #{host}:#{port}"
      loop do
        data = socket.readpartial(4096)
        parse_data(socket, data)
      end
    rescue EOFError
      puts "*** #{host}:#{port} disconnected"
    end

    def parse_data(socket, data)
      if index = data.index("\n")
        @cache[socket] << data[0..index-1]
        handle_line(socket, @cache[socket])
        @cache[socket] = ""
        parse_data(socket, data[(index+1)..-1])
      else
        @cache[socket] << data
      end
    end

    def handle_line(socket, line)
      puts "HL: #{line}"
      result = next_worker.send(line)
      socket.write(result)
    end

    def next_worker
      @workers[next_worker_index]
    end

    def next_worker_index
      @worker_index += 1
      @worker_index = 0 if @worker_index > @workers.size
      @worker_index
    end
  end

  class Worker
    include Celluloid

    def m1
      Kernel.sleep(1)
      return "from m1"
    end

    def m2
      Kernel.sleep(20)
      return "from m2"
    end
  end
end

supervisor = Node::Frontend.supervise
trap("INT") { supervisor.terminate; exit }
sleep
