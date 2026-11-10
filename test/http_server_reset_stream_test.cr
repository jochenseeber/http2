require "./test_helper"
require "../src/server"
require "http/client/response"

# A handler ends its response at once with `Response#reset_stream`. Every read
# on the client side times out after 5 seconds, so a connection the server
# doesn't end fails the test instead of hanging it.
class HTTP::ServerResetStreamTest < Minitest::Test
  def test_http2_stream_is_reset_and_the_others_go_on
    server, port = start_server do |context|
      if context.request.path == "/bad"
        context.response.print "discarded"
        context.response.reset_stream(:protocol_error)
      else
        context.response.print "ok"
      end
    end
    socket, client = connect_http2(port)
    open_stream(client, "/bad")
    open_stream(client, "/")

    body = IO::Memory.new
    reset = nil
    ended = false
    until reset && ended
      type, flags, stream_id, payload = read_frame(socket)
      case {type, stream_id}
      when {HTTP2::Frame::Type::RST_STREAM, 1}
        reset = HTTP2::Error::Code.new(IO::ByteFormat::BigEndian.decode(UInt32, payload))
      when {HTTP2::Frame::Type::DATA, 3}
        body.write(payload)
        ended = flags.end_stream?
      else
        refute stream_id == 1 && (type.headers? || type.data?), "#{type} sent on the reset stream"
      end
    end
    assert_equal HTTP2::Error::Code::PROTOCOL_ERROR, reset
    assert_equal "ok", body.to_s
  ensure
    socket.try(&.close)
    server.try(&.close)
  end

  def test_http2_stream_is_reset_after_the_headers
    server, port = start_server do |context|
      context.response.print "hello "
      context.response.flush
      context.response.print "discarded"
      context.response.reset_stream(:cancel)
    end
    socket, client = connect_http2(port)
    open_stream(client, "/")

    _, _, _, payload = read_until(socket) do |type, flags, stream_id, _|
      refute type.data? && flags.end_stream?, "the stream was ended"
      type.rst_stream? && stream_id == 1
    end
    assert_equal HTTP2::Error::Code::CANCEL.value, IO::ByteFormat::BigEndian.decode(UInt32, payload)
  ensure
    socket.try(&.close)
    server.try(&.close)
  end

  def test_http1_connection_is_closed_without_a_response
    server, port = start_server do |context|
      context.response.print "discarded"
      context.response.reset_stream(:protocol_error)
    end
    socket = connect(port)
    socket << "GET / HTTP/1.1\r\nHost: example.com\r\n\r\n"
    socket.flush
    assert_equal "", socket.gets_to_end
  ensure
    socket.try(&.close)
    server.try(&.close)
  end

  def test_http1_response_is_cut_off_and_the_connection_closed
    server, port = start_server do |context|
      context.response.print "hello"
      context.response.flush
      context.response.print "discarded"
      context.response.reset_stream(:protocol_error)
    end
    socket = connect(port)
    socket << "GET / HTTP/1.1\r\nHost: example.com\r\n\r\n"
    socket.flush
    received = socket.gets_to_end
    assert received.starts_with?("HTTP/1.1 200 OK\r\n")
    assert received.ends_with?("\r\n\r\n5\r\nhello\r\n"), received.inspect
  ensure
    socket.try(&.close)
    server.try(&.close)
  end

  def test_reset_stream_after_close_does_nothing
    server, port = start_server do |context|
      context.response.print "ok"
      context.response.close
      context.response.reset_stream(:protocol_error)
    end
    socket = connect(port)
    socket << "GET / HTTP/1.1\r\nHost: example.com\r\n\r\n"
    socket.flush
    response = HTTP::Client::Response.from_io(socket)
    assert_equal "ok", response.body

    # the connection is kept alive
    socket << "GET / HTTP/1.1\r\nHost: example.com\r\n\r\n"
    socket.flush
    assert_equal "ok", HTTP::Client::Response.from_io(socket).body
  ensure
    socket.try(&.close)
    server.try(&.close)
  end

  private def start_server(&handler : HTTP::Server::Context ->) : {HTTP::Server, Int32}
    server = HTTP::Server.new(&handler)
    listener = TCPServer.new("127.0.0.1", 0)
    server.bind(listener)
    spawn { server.listen }
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

  private def open_stream(client, path) : HTTP2::Stream
    headers = HTTP::Headers{
      ":method"    => "GET",
      ":scheme"    => "http",
      ":authority" => "example.com",
      ":path"      => path,
    }
    client.streams.create.tap(&.send_headers(headers, HTTP2::Frame::Flags::END_STREAM))
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
end
