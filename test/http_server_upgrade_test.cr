require "./test_helper"
require "../src/server"
require "socket"

class HTTP::Server
  # serves a connection in the current fiber, so an exception reaches the
  # test instead of ending a spawned fiber
  def handle_upgrade_test_client(io : IO) : Nil
    handle_client(io)
  end
end

class HTTP::ServerUpgradeTest < Minitest::Test
  def test_upgrade_sends_one_switching_protocols_response
    output = serve do |context|
      switch_protocols(context.response)
      context.response.upgrade do |io|
        io << "upgraded"
        io.flush
      end
    end

    assert_equal 1, output.scan("101 Switching Protocols").size, output
    assert output.ends_with?("\r\n\r\nupgraded"), output.inspect
  end

  def test_upgrade_sends_nothing_after_the_block
    output = serve do |context|
      switch_protocols(context.response)
      context.response.upgrade { }
    end

    assert output.starts_with?("HTTP/1.1 101 Switching Protocols\r\n"), output.inspect
    assert output.ends_with?("\r\n\r\n"), output.inspect
    assert_equal 1, output.scan("\r\n\r\n").size, output.inspect
  end

  def test_upgrade_without_a_protocol_sends_the_status_and_headers_set
    output = serve do |context|
      context.response.headers["Content-Type"] = "text/event-stream"
      context.response.upgrade do |io|
        io << "data: 1\n\n"
        io.flush
      end
    end

    assert output.starts_with?("HTTP/1.1 200 OK\r\n"), output.inspect
    assert output.includes?("Content-Type: text/event-stream\r\n"), output.inspect
    refute output.downcase.includes?("upgrade"), output.inspect
    refute output.includes?("transfer-encoding"), output.inspect
    assert output.ends_with?("\r\n\r\ndata: 1\n\n"), output.inspect
  end

  def test_upgraded_connection_serves_no_further_request
    calls = 0
    server = HTTP::Server.new do |context|
      calls += 1
      context.response.upgrade { }
    end
    request = "GET / HTTP/1.1\r\nHost: example.com\r\n\r\n"
    input = IO::Memory.new(request * 2)
    server.handle_upgrade_test_client(IO::Stapled.new(input, IO::Memory.new))

    assert_equal 1, calls
    assert_equal request.bytesize, input.pos
  end

  # The handler takes over the connection: the response is exactly what it
  # set, the block gets the bytes the client sent after the request, and
  # runs on the connection's fiber until the connection ends, even once the
  # server is closing.
  def test_upgrade_hands_over_the_connection_as_set
    closed = Channel(Nil).new(1)
    server = HTTP::Server.new do |context|
      context.response.version = "HTTP/1.1"
      context.response.headers.clear
      context.response.status = :switching_protocols
      context.response.upgrade do |io|
        closed.receive
        io << "got:" << io.gets << "\n"
        io.flush
      end
    end
    listener = TCPServer.new("127.0.0.1", 0)
    server.bind(listener)
    spawn { server.listen }
    socket = TCPSocket.new("127.0.0.1", listener.local_address.port)
    socket.read_timeout = 5.seconds
    socket << "GET / HTTP/1.0\r\nHost: example.com\r\nConnection: keep-alive\r\n\r\npipelined\n"
    socket.flush

    assert_equal "HTTP/1.1 101 Switching Protocols\r\n\r\n", socket.read_string(36)
    server.close
    waited = Channel(Bool).new(1)
    spawn { waited.send(server.wait_closed) }
    closed.send(nil)
    assert_equal "got:pipelined", socket.gets
    socket.close
    assert waited.receive
  ensure
    socket.try(&.close)
  end

  private def switch_protocols(response) : Nil
    response.status = :switching_protocols
    response.headers["Connection"] = "Upgrade"
    response.headers["Upgrade"] = "echo"
  end

  private def serve(&block : HTTP::Server::Context ->) : String
    server = HTTP::Server.new(block)
    request = "GET / HTTP/1.1\r\nHost: example.com\r\nConnection: Upgrade\r\nUpgrade: echo\r\n\r\n"
    output = IO::Memory.new
    server.handle_upgrade_test_client(IO::Stapled.new(IO::Memory.new(request), output))
    output.to_s
  end
end
