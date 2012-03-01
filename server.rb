
require 'celluloid'
require 'celluloid/io'
require 'dcell'
require 'securerandom'
require 'active_support/core_ext/array'

ENV['DCELL_PORT'] ||= (ENV['PORT'].to_i + 1).to_s
DCell.start :addr => "tcp://127.0.0.1:#{ENV['DCELL_PORT']}", :id => "id#{ENV['DCELL_PORT']}"

module Node
  class Frontend
    include Celluloid::IO

    def initialize(host = "127.0.0.1", port = ENV['PORT'])
      puts "*** Starting echo server on #{host}:#{port}"
      @cache   = Hash.new{|hash, key| hash[key] = ""}
      @server  = TCPServer.new(host, port)
      @workers = 20.times.map{ Worker.new(current_actor) }
      @worker_index = 0
      @auth = {}
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
      name = @auth[socket]
      @auth.delete(socket)
      @auth.delete(name) if name
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
      result = next_worker.send(line, socket.object_id, @auth[socket])
      if Array === result && result.first == :authenticated
        authenticated(socket, result.second)
      elsif result.nil?
      else
        socket.write("> #{result.to_s}\n")
      end
    end

    def next_worker
      @workers[next_worker_index]
    end

    def next_worker_index
      @worker_index += 1
      @worker_index = 0 if @worker_index >= @workers.size
      @worker_index
    end

    def authenticated(socket, name)
      @auth[socket] = name
      @auth[name]   = socket
    end

    def message(from, to)
      if socket = @auth[to]
        socket.write("> message from #{from}\n")
      else
        puts "skipping: #{from}, #{to}"
      end
    end

    def inspector
      puts @auth.inspect
    end
  end

  class Worker
    include Celluloid

    attr_accessor :frontend

    def initialize(frontend)
      self.frontend = frontend
    end

    def m1(ident, name)
      Kernel.sleep(1)
      return "from m1"
    end

    def m2(ident, name)
      Kernel.sleep(20)
      return "from m2"
    end

    def auth(ident, name)
      name = SecureRandom.hex
      puts "authenticated #{name}"
      return [:authenticated, name]
    end

    def inspector(ident, name)
      DCell::Global[ENV['NAME']].inspector
    end

    # from *name*
    # to *method*
    def method_missing(method, *args)
      ident, name = *args
      DCell::Global[ENV['TARGET']].message(name, method.to_s)
      nil
    end
  end
end

supervisor = Node::Frontend.supervise
DCell::Global[ENV['NAME']] = supervisor.actor
trap("INT") { supervisor.terminate; exit }
sleep
