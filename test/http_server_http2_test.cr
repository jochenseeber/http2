require "./test_helper"
require "../src/server"
require "compress/gzip"
require "compress/deflate"
require "http/client/response"

class HTTP::Server
  # dispatches an accepted connection as `#listen` does
  def dispatch_for_test(io : IO) : Nil
    dispatch(io)
  end
end

# HTTP/2 requests and responses through `HTTP::Server`. Every read on the
# client side times out after 5 seconds, so a response the server doesn't
# send fails the test instead of hanging it.
class HTTP::ServerHTTP2Test < Minitest::Test
  def test_connection_specific_fields_are_not_sent
    headers = response_headers do |response|
      response.headers["Connection"] = "keep-alive"
      response.headers["Keep-Alive"] = "timeout=5"
      response.headers["Transfer-Encoding"] = "chunked"
      response.headers["Upgrade"] = "websocket"
      response.headers["Proxy-Connection"] = "keep-alive"
      response.headers["TE"] = "gzip"
      response.headers["x-custom"] = "kept"
      response.print "ok"
    end

    {"connection", "keep-alive", "transfer-encoding", "upgrade", "proxy-connection", "te"}.each do |name|
      refute headers.has_key?(name), "#{name} sent"
    end
    assert_equal "kept", headers["x-custom"]
    assert_equal "200", headers[":status"]
  end

  def test_te_trailers_is_sent
    headers = response_headers { |response| response.headers["TE"] = "trailers" }
    assert_equal "trailers", headers["te"]
  end

  def test_gzip_request_body_doesnt_stall_the_other_streams
    compressed = IO::Memory.new
    Compress::Gzip::Writer.open(compressed) { |gzip| gzip << "hello gzip" }
    assert_decoded_body("gzip", compressed.to_slice, "hello gzip")
  end

  def test_deflate_request_body_doesnt_stall_the_other_streams
    compressed = IO::Memory.new
    Compress::Deflate::Writer.open(compressed) { |deflate| deflate << "hello deflate" }
    assert_decoded_body("deflate", compressed.to_slice, "hello deflate")
  end

  def test_gzip_request_body_over_http1
    compressed = IO::Memory.new
    Compress::Gzip::Writer.open(compressed) { |gzip| gzip << "hello http/1" }
    server = HTTP::Server.new { |context| context.response.print context.request.body.try(&.gets_to_end) }
    listener = TCPServer.new("127.0.0.1", 0)
    server.bind(listener)
    spawn { server.listen }
    socket = TCPSocket.new("127.0.0.1", listener.local_address.port)
    socket.read_timeout = 5.seconds
    socket << "POST / HTTP/1.1\r\nHost: example.com\r\nContent-Encoding: gzip\r\n"
    socket << "Content-Length: " << compressed.size << "\r\n\r\n"
    socket.write(compressed.to_slice)
    socket.flush
    assert_equal "hello http/1", HTTP::Client::Response.from_io(socket).body
  ensure
    socket.try(&.close)
    server.try(&.close)
  end

  PRIOR_KNOWLEDGE_REQUEST = HTTP::Headers{
    ":method"    => "GET",
    ":scheme"    => "http",
    ":authority" => "example.com",
    ":path"      => "/",
  }

  # A prior-knowledge connection is served however the client writes its
  # preface, SETTINGS and first request.
  def test_prior_knowledge_connections_are_served_whatever_the_writes
    server = HTTP::Server.new { |context| context.response.print "ok" }
    listener = TCPServer.new("127.0.0.1", 0)
    server.bind(listener)
    spawn { server.listen }
    port = listener.local_address.port

    {:one_write, :settings_apart, :preface_split, :settings_delayed}.each do |split|
      100.times do |i|
        socket = TCPSocket.new("127.0.0.1", port)
        socket.read_timeout = 5.seconds
        write_prior_knowledge_request(socket, split)
        assert_equal "ok", read_body(socket, 1), "#{split} ##{i}"
      ensure
        socket.try(&.close)
      end
    end
  ensure
    server.try(&.close)
  end

  # A connection accepted before `#close` is served, and `#wait_closed` waits
  # for it, as for a server that listens on a single connection.
  def test_connection_dispatched_before_close_is_served
    300.times do |i|
      server = HTTP::Server.new { |context| context.response.print "ok" }
      listener = TCPServer.new("127.0.0.1", 0)
      socket = TCPSocket.new("127.0.0.1", listener.local_address.port)
      socket.read_timeout = 5.seconds
      write_prior_knowledge_request(socket, :one_write)
      io = listener.accept

      server.dispatch_for_test(io)
      server.close
      assert_equal "ok", read_body(socket, 1), "##{i}"
      socket.close
      assert server.wait_closed
    ensure
      socket.try(&.close)
      listener.try(&.close)
    end
  end

  # A cleartext HTTP/1 connection accepted before `#close` gets its first
  # request served too.
  def test_http1_connection_dispatched_before_close_is_served
    server = HTTP::Server.new { |context| context.response.print "ok" }
    listener = TCPServer.new("127.0.0.1", 0)
    socket = TCPSocket.new("127.0.0.1", listener.local_address.port)
    socket.read_timeout = 5.seconds
    socket << "GET / HTTP/1.1\r\nHost: example.com\r\n\r\n"
    socket.flush
    io = listener.accept

    server.dispatch_for_test(io)
    server.close
    assert_equal "ok", HTTP::Client::Response.from_io(socket).body
  ensure
    socket.try(&.close)
    listener.try(&.close)
  end

  private def write_prior_knowledge_request(socket, split) : Nil
    request = IO::Memory.new
    client = HTTP2::Connection.new(IO::Stapled.new(IO::Memory.new, request), :client)
    client.write_settings
    settings = request.to_slice.dup
    request.clear
    client.streams.create.send_headers(PRIOR_KNOWLEDGE_REQUEST, HTTP2::Frame::Flags::END_STREAM)
    headers = request.to_slice.dup
    preface = HTTP2::CLIENT_PREFACE.to_slice

    case split
    when :one_write
      socket.write(preface + settings + headers)
    when :settings_apart
      socket.write(preface)
      socket.flush
      socket.write(settings + headers)
    when :preface_split
      socket.write(preface[0, 16]) # "PRI * HTTP/2.0\r\n"
      socket.flush
      socket.write(preface[16..] + settings + headers)
    when :settings_delayed
      socket.write(preface)
      socket.flush
      sleep 5.milliseconds
      socket.write(settings + headers)
    end
    socket.flush
  end

  # Opens stream 1 with a body encoded with *encoding*, without sending it
  # yet, then stream 3: stream 3 is answered first, then stream 1 once its
  # body arrives, decoded.
  private def assert_decoded_body(encoding, compressed : Bytes, expected : String) : Nil
    server = HTTP::Server.new do |context|
      context.response.print context.request.body.try(&.gets_to_end) || "no body"
    end
    listener = TCPServer.new("127.0.0.1", 0)
    server.bind(listener)
    spawn { server.listen }

    socket = TCPSocket.new("127.0.0.1", listener.local_address.port)
    socket.read_timeout = 5.seconds
    client = HTTP2::Connection.new(socket, :client)
    client.write_client_preface
    client.write_settings
    request = HTTP::Headers{
      ":method"    => "POST",
      ":scheme"    => "http",
      ":authority" => "example.com",
      ":path"      => "/",
    }
    first = client.streams.create
    first.send_headers(request.dup.tap(&.["content-encoding"] = encoding))
    client.streams.create.send_headers(request, HTTP2::Frame::Flags::END_STREAM)

    assert_equal "no body", read_body(socket, 3)
    first.send_data(compressed, HTTP2::Frame::Flags::END_STREAM)
    assert_equal expected, read_body(socket, 1)
  ensure
    socket.try(&.close)
    server.try(&.close)
  end

  # Reads frames until the response body on *stream_id* ended, and returns
  # it.
  private def read_body(socket, stream_id) : String
    body = IO::Memory.new
    loop do
      size_and_type = socket.read_bytes(UInt32, IO::ByteFormat::BigEndian)
      flags = HTTP2::Frame::Flags.new(socket.read_byte.not_nil!)
      id = socket.read_bytes(UInt32, IO::ByteFormat::BigEndian) & 0x7fff_ffff
      payload = Bytes.new(size_and_type >> 8)
      socket.read_fully(payload)
      next unless id == stream_id && HTTP2::Frame::Type.new((size_and_type & 0xff).to_u8).data?
      body.write(payload)
      return body.to_s if flags.end_stream?
    end
  end

  # Serves one request over HTTP/2 with *handler* and returns the headers of
  # the response.
  private def response_headers(&handler : HTTP::Server::Response ->) : HTTP::Headers
    server = HTTP::Server.new { |context| handler.call(context.response) }
    listener = TCPServer.new("127.0.0.1", 0)
    server.bind(listener)
    spawn { server.listen }

    socket = TCPSocket.new("127.0.0.1", listener.local_address.port)
    socket.read_timeout = 5.seconds
    client = HTTP2::Connection.new(socket, :client)
    client.write_client_preface
    client.write_settings
    client.streams.create.send_headers(HTTP::Headers{
      ":method"    => "GET",
      ":scheme"    => "http",
      ":authority" => "example.com",
      ":path"      => "/",
    }, HTTP2::Frame::Flags::END_STREAM)

    decoder = HTTP2::HPACK::Decoder.new
    loop do
      size_and_type = socket.read_bytes(UInt32, IO::ByteFormat::BigEndian)
      socket.read_byte # flags
      stream_id = socket.read_bytes(UInt32, IO::ByteFormat::BigEndian) & 0x7fff_ffff
      payload = Bytes.new(size_and_type >> 8)
      socket.read_fully(payload)
      if HTTP2::Frame::Type.new((size_and_type & 0xff).to_u8).headers? && stream_id == 1
        return decoder.decode(payload)
      end
    end
  ensure
    socket.try(&.close)
    server.try(&.close)
  end
end
