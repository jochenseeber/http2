require "./test_helper"
require "../src/server"
require "http/client/response"

# `HTTP::Server#close` ends the connections it accepted. Every read on the
# client side times out after 5 seconds, so a connection the server doesn't
# end fails the test instead of hanging it.
class HTTP::ServerCloseTest < Minitest::Test
  REQUEST = HTTP::Headers{
    ":method"    => "GET",
    ":scheme"    => "http",
    ":authority" => "example.com",
    ":path"      => "/",
  }

  def test_close_ends_an_idle_http1_connection
    server, port = start_server { |context| context.response.print "ok" }
    socket = connect(port)
    socket << "GET / HTTP/1.1\r\nHost: example.com\r\n\r\n"
    socket.flush
    response = HTTP::Client::Response.from_io(socket)
    assert_equal "ok", response.body
    assert response.keep_alive?

    server.close
    assert_closed socket
  ensure
    socket.try(&.close)
  end

  def test_close_lets_the_http1_request_in_progress_finish
    started = Channel(Nil).new(1)
    release = Channel(Nil).new(1)
    server, port = start_server do |context|
      started.send(nil)
      release.receive?
      context.response.print "done"
    end
    socket = connect(port)
    socket << "GET / HTTP/1.1\r\nHost: example.com\r\n\r\n"
    socket.flush
    wait_for(started)

    server.close
    release.send(nil)
    response = HTTP::Client::Response.from_io(socket)
    assert_equal "done", response.body
    assert_closed socket
  ensure
    release.try(&.close)
    socket.try(&.close)
  end

  def test_close_ends_an_idle_http2_connection
    server, port = start_server { |context| context.response.print "ok" }
    socket, _ = connect_http2(port)
    # the server read our SETTINGS: the connection is established
    read_until(socket) { |frame| frame[0].settings? && frame[1].ack? }

    server.close
    _, _, _, payload = read_until(socket, &.[0].goaway?)
    assert_equal({0, HTTP2::Error::Code::NO_ERROR}, goaway(payload))
    assert_closed socket
  ensure
    socket.try(&.close)
  end

  def test_close_sends_goaway_and_lets_http2_streams_finish
    release = Channel(Nil).new(1)
    server, port = start_server do |context|
      context.response.print "hello "
      context.response.flush
      release.receive?
      context.response.print "world"
    end
    socket, client = connect_http2(port)
    open_stream(client)
    read_until(socket) { |frame| frame[0].data? && frame[2] == 1 }

    server.close
    _, _, _, payload = read_until(socket, &.[0].goaway?)
    assert_equal({1, HTTP2::Error::Code::NO_ERROR}, goaway(payload))

    # a stream opened after the GOAWAY is refused
    open_stream(client)
    _, _, _, payload = read_until(socket) { |frame| frame[0].rst_stream? && frame[2] == 3 }
    assert_equal HTTP2::Error::Code::REFUSED_STREAM.value, IO::ByteFormat::BigEndian.decode(UInt32, payload)

    # the stream in progress finishes, then the server closes the connection
    release.send(nil)
    body = IO::Memory.new
    read_until(socket) do |type, flags, stream_id, payload|
      body.write(payload) if type.data? && stream_id == 1
      type.data? && stream_id == 1 && flags.end_stream?
    end
    assert_equal "world", body.to_s
    assert_closed socket
  ensure
    release.try(&.close)
    socket.try(&.close)
  end

  def test_close_ends_connections_after_the_close_timeout
    release = Channel(Nil).new
    server, port = start_server do |context|
      context.response.print "hello "
      context.response.flush
      release.receive?
    rescue HTTP::Server::ClientError | HTTP2::Error | IO::Error
      # the connection was closed under the handler
    end
    server.close_timeout = 100.milliseconds

    http1 = connect(port)
    http1 << "GET / HTTP/1.1\r\nHost: example.com\r\n\r\n"
    http1.flush
    assert_equal "HTTP/1.1 200 OK", http1.gets

    http2, client = connect_http2(port)
    open_stream(client)
    read_until(http2) { |frame| frame[0].data? && frame[2] == 1 }

    elapsed = Time.measure { server.close }
    assert elapsed < 1.second

    # the handlers never finish: the server closes the connections anyway
    read_until(http2, &.[0].goaway?)
    assert_closed http2
    http1.gets_to_end
    assert_closed http1
  ensure
    release.try(&.close)
    http1.try(&.close)
    http2.try(&.close)
  end

  def test_close_sends_goaway_during_the_preface
    server, port = start_server { |context| context.response.print "ok" }
    socket = connect(port)
    client = HTTP2::Connection.new(socket, :client)
    client.write_client_preface
    socket.flush
    read_until(socket, &.[0].settings?)

    # the server waits for our SETTINGS
    server.close
    _, _, _, payload = read_until(socket, &.[0].goaway?)
    assert_equal({0, HTTP2::Error::Code::NO_ERROR}, goaway(payload))
    assert_closed socket
  ensure
    socket.try(&.close)
  end

  def test_close_returns_while_a_handler_waits_to_write
    written = Channel(Nil).new(2048)
    server, port = start_server do |context|
      chunk = Bytes.new(64 * 1024)
      2048.times do
        context.response.write(chunk)
        written.send(nil)
      end
    rescue HTTP::Server::ClientError | HTTP2::Error | IO::Error
      # the connection was closed under the handler
    end
    server.close_timeout = 500.milliseconds

    # the client lets the server send as much as it wants (the largest stream
    # and connection windows), but reads nothing
    socket = connect(port)
    client = HTTP2::Connection.new(socket, :client)
    client.write_client_preface
    settings = Bytes.new(6)
    IO::ByteFormat::BigEndian.encode(0x4_u16, settings[0, 2]) # INITIAL_WINDOW_SIZE
    IO::ByteFormat::BigEndian.encode((HTTP2::MAXIMUM_WINDOW_SIZE - 1).to_u32, settings[2, 4])
    write_frame(socket, HTTP2::Frame::Type::SETTINGS, HTTP2::Frame::Flags::None, 0, settings)
    increment = Bytes.new(4)
    IO::ByteFormat::BigEndian.encode((HTTP2::MAXIMUM_WINDOW_SIZE - 65_535).to_u32, increment)
    write_frame(socket, HTTP2::Frame::Type::WINDOW_UPDATE, HTTP2::Frame::Flags::None, 0, increment)
    open_stream(client)

    # the handler waits to write once it made no progress for 200 ms
    wait_for(written)
    loop do
      select
      when written.receive
      when timeout(200.milliseconds)
        break
      end
    end

    closed = Channel(Nil).new(1)
    spawn do
      server.close
      closed.send(nil)
    end
    select
    when closed.receive
    when timeout(1.second)
      flunk "HTTP::Server#close waits for the handler"
    end

    # once the close timeout is over, the connection is closed before the
    # handler could send everything (reading earlier would let it go on)
    sleep 1.second
    assert assert_closed(socket) < 128 * 1024 * 1024
  ensure
    socket.try(&.close)
  end

  def test_close_lets_the_h2c_upgrade_request_finish
    started = Channel(Nil).new(1)
    release = Channel(Nil).new(1)
    server, port = start_server do |context|
      started.send(nil)
      release.receive?
      context.response.print "upgraded"
    end
    socket = connect(port)
    socket << "GET / HTTP/1.1\r\nHost: example.com\r\nConnection: Upgrade, HTTP2-Settings\r\n"
    socket << "Upgrade: h2c\r\nHTTP2-Settings: AAMAAABk\r\n\r\n" # MAX_CONCURRENT_STREAMS=100
    socket.flush
    assert_equal "HTTP/1.1 101 Switching Protocols", socket.gets(chomp: true)
    until socket.gets(chomp: true).try(&.empty?)
    end
    client = HTTP2::Connection.new(socket, :client)
    client.write_client_preface
    client.write_settings
    wait_for(started)

    # the request sent over HTTP/1.1 is stream 1 (RFC 7540 section 3.2)
    server.close
    _, _, _, payload = read_until(socket, &.[0].goaway?)
    assert_equal({1, HTTP2::Error::Code::NO_ERROR}, goaway(payload))

    release.send(nil)
    body = IO::Memory.new
    read_until(socket) do |type, flags, stream_id, payload|
      body.write(payload) if type.data? && stream_id == 1
      type.data? && stream_id == 1 && flags.end_stream?
    end
    assert_equal "upgraded", body.to_s
    assert_closed socket
  ensure
    release.try(&.close)
    socket.try(&.close)
  end

  def test_close_lets_large_http2_responses_arrive_whole
    body = Bytes.new(300_000) { |i| (i % 251).to_u8 }

    20.times do
      server, port = start_server { |context| context.response.write(body) }
      socket, client = connect_http2(port)
      open_stream(client)

      # reads the response as a client does: gives the window back for each
      # DATA frame, so the server can send the rest
      received = IO::Memory.new
      read_until(socket) do |type, flags, stream_id, payload|
        if type.data? && stream_id == 1
          server.close if received.size == 0
          received.write(payload)
          unless payload.empty?
            window_update(socket, 0, payload.size)
            window_update(socket, 1, payload.size)
          end
        end
        type.data? && stream_id == 1 && flags.end_stream?
      end
      assert_equal body, received.to_slice
      assert_closed socket
    ensure
      socket.try(&.close)
    end
  end

  def test_wait_closed_returns_at_once_for_an_idle_server
    server, port = start_server { |context| context.response.print "ok" }
    socket = connect(port)
    socket << "GET / HTTP/1.1\r\nHost: example.com\r\n\r\n"
    socket.flush
    assert_equal "ok", HTTP::Client::Response.from_io(socket).body

    # the client closes once the server ended the connection
    server.close
    spawn do
      assert_closed socket
      socket.close
    end
    elapsed = Time.measure { assert server.wait_closed }
    assert elapsed < 500.milliseconds
  ensure
    socket.try(&.close)
  end

  def test_wait_closed_raises_unless_the_server_is_closed
    server, _ = start_server { |context| context.response.print "ok" }
    assert_raises(Exception) { server.wait_closed }
  ensure
    server.try(&.close)
  end

  def test_wait_closed_waits_for_the_http1_and_http2_handlers
    started = Channel(Nil).new(2)
    release = Channel(Nil).new(2)
    returned = Atomic(Int32).new(0)
    server, port = start_server do |context|
      started.send(nil)
      release.receive?
      context.response.print "done"
    ensure
      returned.add(1)
    end

    http1 = connect(port)
    http1 << "GET / HTTP/1.1\r\nHost: example.com\r\n\r\n"
    http1.flush
    http2, client = connect_http2(port)
    open_stream(client)
    wait_for(started)
    wait_for(started)

    server.close
    waited = Channel(Bool).new(1)
    spawn { waited.send(server.wait_closed) }

    # the clients read the responses then close, so the connections end
    spawn do
      HTTP::Client::Response.from_io(http1).body
      http1.close
    end
    spawn do
      read_until(http2, &.[0].goaway?)
      assert_closed(http2)
      http2.close
    end

    select
    when waited.receive
      flunk "HTTP::Server#wait_closed returned while the handlers run"
    when timeout(200.milliseconds)
    end

    release.send(nil)
    release.send(nil)
    select
    when result = waited.receive
      assert result
      assert_equal 2, returned.get
    when timeout(5.seconds)
      flunk "HTTP::Server#wait_closed didn't return"
    end
  ensure
    release.try(&.close)
    http1.try(&.close)
    http2.try(&.close)
  end

  def test_wait_closed_waits_for_an_http2_handler_after_its_connection_ended
    release = Channel(Nil).new
    returned = Channel(Nil).new(1)
    server, port = start_server do |context|
      context.response.print "hello "
      context.response.flush
      release.receive?
    rescue HTTP::Server::ClientError | HTTP2::Error | IO::Error
      # the connection was closed under the handler
    ensure
      returned.send(nil)
    end
    server.close_timeout = 100.milliseconds

    socket, client = connect_http2(port)
    open_stream(client)
    read_until(socket) { |frame| frame[0].data? && frame[2] == 1 }

    # the connection is closed after the close timeout, the handler goes on
    server.close
    read_until(socket, &.[0].goaway?)
    assert_closed socket
    waited = Channel(Bool).new(1)
    spawn { waited.send(server.wait_closed) }
    select
    when waited.receive
      flunk "HTTP::Server#wait_closed returned while the handler runs"
    when timeout(200.milliseconds)
    end

    release.close
    wait_for(returned)
    select
    when result = waited.receive
      assert result
    when timeout(5.seconds)
      flunk "HTTP::Server#wait_closed didn't return"
    end
  ensure
    socket.try(&.close)
  end

  def test_wait_closed_gives_up_after_the_close_timeout
    release = Channel(Nil).new
    server, port = start_server do |context|
      context.response.print "hello "
      context.response.flush
      release.receive?
    rescue HTTP::Server::ClientError | HTTP2::Error | IO::Error
      # the connection was closed under the handler
    end
    server.close_timeout = 100.milliseconds

    socket, client = connect_http2(port)
    open_stream(client)
    read_until(socket) { |frame| frame[0].data? && frame[2] == 1 }

    server.close
    elapsed = Time.measure { refute server.wait_closed }
    assert elapsed < 3.seconds
  ensure
    release.try(&.close)
    socket.try(&.close)
  end

  def test_close_without_connections_starts_no_timer
    server = HTTP::Server.new { |context| context.response.print "ok" }
    server.close
    refute close_timer_running?(server)
  end

  def test_close_timer_ends_with_the_last_connection
    server, port = start_server { |context| context.response.print "ok" }
    socket = connect(port)
    socket << "GET / HTTP/1.1\r\nHost: example.com\r\n\r\n"
    socket.flush
    assert_equal "ok", HTTP::Client::Response.from_io(socket).body

    server.close
    assert close_timer_running?(server)
    assert_closed socket
    socket.close
    assert server.wait_closed
    # the timer may run on another thread: it ends well before the close
    # timeout (10 seconds)
    100.times do
      break unless close_timer_running?(server)
      sleep 10.milliseconds
    end
    refute close_timer_running?(server), "the close timer must end before the close timeout"
  ensure
    socket.try(&.close)
  end

  private def close_timer_running?(server) : Bool
    name = "http2:server:close-timeout:#{server.object_id}"
    running = false
    Fiber.each { |fiber| running ||= fiber.name == name && !fiber.dead? }
    running
  end

  private def start_server(&handler : HTTP::Server::Context ->) : {HTTP::Server, Int32}
    server = HTTP::Server.new(&handler)
    listener = TCPServer.new("127.0.0.1", 0)
    server.bind(listener)
    spawn { server.listen }
    # a server closed before it listens can't listen anymore
    until server.listening?
      Fiber.yield
    end
    {server, listener.local_address.port}
  end

  private def connect(port) : TCPSocket
    socket = TCPSocket.new("127.0.0.1", port)
    socket.read_timeout = 5.seconds
    socket
  end

  # The client connection only sends: the test reads the server's frames
  # from the socket.
  private def connect_http2(port) : {TCPSocket, HTTP2::Connection}
    socket = connect(port)
    client = HTTP2::Connection.new(socket, :client)
    client.write_client_preface
    client.write_settings
    {socket, client}
  end

  private def open_stream(client) : HTTP2::Stream
    client.streams.create.tap(&.send_headers(REQUEST, HTTP2::Frame::Flags::END_STREAM))
  end

  private def window_update(socket, stream_id, increment) : Nil
    payload = Bytes.new(4)
    IO::ByteFormat::BigEndian.encode(increment.to_u32, payload)
    write_frame(socket, HTTP2::Frame::Type::WINDOW_UPDATE, HTTP2::Frame::Flags::None, stream_id, payload)
    socket.flush
  end

  private def wait_for(channel) : Nil
    select
    when channel.receive
    when timeout(5.seconds)
      flunk "timed out"
    end
  end

  # The server closed the connection: reading gets EOF (or a reset), not a
  # timeout. Returns the number of bytes read until then.
  private def assert_closed(socket) : Int64
    buffer = Bytes.new(64 * 1024)
    count = 0_i64
    loop do
      n = socket.read(buffer)
      break if n == 0
      count += n
    end
    count
  rescue ex : IO::TimeoutError
    flunk "the server didn't close the connection"
  rescue ex : IO::Error
    # connection reset
    count || 0_i64
  end

  private def read_frame(io) : {HTTP2::Frame::Type, HTTP2::Frame::Flags, Int32, Bytes}
    size_and_type = io.read_bytes(UInt32, IO::ByteFormat::BigEndian)
    flags = HTTP2::Frame::Flags.new(io.read_bytes(UInt8))
    stream_id = (io.read_bytes(UInt32, IO::ByteFormat::BigEndian) & 0x7fff_ffff).to_i32
    payload = Bytes.new(size_and_type >> 8)
    io.read_fully(payload)
    {HTTP2::Frame::Type.new((size_and_type & 0xff).to_u8), flags, stream_id, payload}
  end

  # Reads frames until the block returns true for one, and returns it.
  private def read_until(io, &)
    loop do
      frame = read_frame(io)
      return frame if yield frame
    end
  end

  private def goaway(payload : Bytes) : {Int32, HTTP2::Error::Code}
    last_stream_id = IO::ByteFormat::BigEndian.decode(UInt32, payload[0, 4]) & 0x7fff_ffff
    code = HTTP2::Error::Code.new(IO::ByteFormat::BigEndian.decode(UInt32, payload[4, 4]))
    {last_stream_id.to_i32, code}
  end
end
