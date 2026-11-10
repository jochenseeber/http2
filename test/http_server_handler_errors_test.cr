require "./test_helper"
require "../src/server"

class HTTP::Server
  # exposes the private request dispatch to the tests
  def handle_request_for_test(context : Context) : Nil
    handle_request(context)
  end

  # serves a connection in the current fiber, so an exception reaches the
  # test instead of ending a spawned fiber
  def handle_client_for_test(io : IO) : Nil
    handle_client(io)
  end
end

# Records the server's log entries, so the tests can check them and they stay
# out of the test output. Tests may run in parallel: each one looks for the
# entry of its own exception.
class HTTP::ServerHandlerErrorsLog < Log::Backend
  INSTANCE = new

  def initialize
    super(:direct)
    @entries = [] of ::Log::Entry
    @mutex = Mutex.new
  end

  def write(entry : ::Log::Entry) : Nil
    @mutex.synchronize { @entries << entry }
  end

  def for(exception : Exception) : Array(::Log::Entry)
    @mutex.synchronize do
      @entries.select(&.exception.same?(exception))
    end
  end
end

HTTP::Server::Log.backend = HTTP::ServerHandlerErrorsLog::INSTANCE
HTTP::Server::Log.level = :debug

class HTTP::ServerHandlerErrorsTest < Minitest::Test
  def test_http1_handler_error_before_headers
    error = Exception.new("before headers")
    request = "GET / HTTP/1.1\r\nHost: example.com\r\n\r\n"
    input = IO::Memory.new(request + "GET /next HTTP/1.1\r\nHost: example.com\r\n\r\n")
    output = IO::Memory.new
    io = IO::Stapled.new(input, output)

    server = HTTP::Server.new { |_| raise error }
    server.handle_client_for_test(io)

    # the server closes the connection: the 500 must say so (HTTP/1.1
    # connections are persistent by default)
    assert_equal "HTTP/1.1 500 Internal Server Error\r\n" \
                 "Connection: close\r\n" \
                 "Content-Type: text/plain\r\n" \
                 "Content-Length: 26\r\n" \
                 "\r\n" \
                 "500 Internal Server Error\n", output.to_s
    assert_equal request.bytesize, input.pos
    assert io.closed?
    assert_logged :error, "Unhandled exception on HTTP::Handler", error
  end

  def test_http1_handler_error_after_headers
    error = Exception.new("after headers")
    request = "GET / HTTP/1.1\r\nHost: example.com\r\n\r\n"
    input = IO::Memory.new(request + "GET /next HTTP/1.1\r\nHost: example.com\r\n\r\n")
    output = IO::Memory.new
    io = IO::Stapled.new(input, output)

    server = HTTP::Server.new do |context|
      context.response.print "partial"
      context.response.flush
      raise error
    end
    server.handle_client_for_test(io)

    # the response is incomplete: the chunked body misses its last chunk
    # (RFC 9112, section 7.1) and the connection is closed; the headers were
    # sent before the handler failed, so they still say keep-alive
    assert_equal "HTTP/1.1 200 OK\r\n" \
                 "Connection: keep-alive\r\n" \
                 "Transfer-Encoding: chunked\r\n" \
                 "\r\n" \
                 "7\r\npartial\r\n", output.to_s
    assert_equal request.bytesize, input.pos
    assert io.closed?
    assert_logged :error, "Unhandled exception on HTTP::Handler", error
  end

  def test_http1_chunked_response_ends_with_last_chunk
    request = "GET / HTTP/1.1\r\nHost: example.com\r\n\r\n"
    output = IO::Memory.new

    server = HTTP::Server.new do |context|
      context.response.print "hello"
      context.response.flush
    end
    server.handle_client_for_test(IO::Stapled.new(IO::Memory.new(request), output))

    assert_equal "HTTP/1.1 200 OK\r\n" \
                 "Connection: keep-alive\r\n" \
                 "Transfer-Encoding: chunked\r\n" \
                 "\r\n" \
                 "5\r\nhello\r\n" \
                 "0\r\n\r\n", output.to_s
  end

  def test_http2_handler_error_before_headers
    error = Exception.new("before headers")
    context, output = new_http2_context
    HTTP::Server.new { |_| raise error }.handle_request_for_test(context)

    assert_equal 500, context.response.status_code
    frames = read_frames(output)
    types = frames.map(&.[0])
    assert_equal HTTP2::Frame::Type::HEADERS, types.first
    assert_equal HTTP2::Frame::Type::RST_STREAM, types.last
    data = frames.select(&.[0].data?)
    assert data.last[1].end_stream?
    assert_equal "500 Internal Server Error\n", data.join { |frame| String.new(frame[2]) }
    assert_logged :error, "Unhandled exception on HTTP::Handler", error
  end

  def test_http2_handler_error_after_headers_resets_the_stream
    error = Exception.new("after headers")
    context, output = new_http2_context
    server = HTTP::Server.new do |ctx|
      ctx.response.print "partial"
      ctx.response.flush
      raise error
    end
    server.handle_request_for_test(context)

    # the response is incomplete: no END_STREAM, only RST_STREAM(INTERNAL_ERROR)
    frames = read_frames(output)
    assert_equal [HTTP2::Frame::Type::HEADERS, HTTP2::Frame::Type::DATA, HTTP2::Frame::Type::RST_STREAM], frames.map(&.[0])
    refute frames.any?(&.[1].end_stream?)
    assert_equal HTTP2::Error::Code::INTERNAL_ERROR.value, IO::ByteFormat::BigEndian.decode(UInt32, frames[2][2])
    assert_logged :error, "Unhandled exception on HTTP::Handler", error
  end

  def test_client_error_is_logged_at_debug
    cause = IO::Error.new("connection reset")
    error = HTTP::Server::ClientError.new("Error while writing data to the client", cause)
    context, _ = new_http2_context
    HTTP::Server.new { |_| raise error }.handle_request_for_test(context)

    assert_logged :debug, "Error while writing data to the client", cause
    assert_empty HTTP::ServerHandlerErrorsLog::INSTANCE.for(error)
  end

  private def assert_logged(severity : ::Log::Severity, message : String, exception : Exception) : Nil
    entries = HTTP::ServerHandlerErrorsLog::INSTANCE.for(exception)
    assert_equal [{severity, message}], entries.map { |entry| {entry.severity, entry.message} }
  end

  # A server stream that received a GET request, and the IO its frames go to.
  private def new_http2_context : {HTTP::Server::Context, IO::Memory}
    headers = HTTP2::HPACK::Encoder.new.encode(HTTP::Headers{
      ":method"    => "GET",
      ":scheme"    => "http",
      ":authority" => "example.com",
      ":path"      => "/",
    })
    input = IO::Memory.new
    write_frame(input, HTTP2::Frame::Type::HEADERS, HTTP2::Frame::Flags::END_HEADERS | HTTP2::Frame::Flags::END_STREAM, 1, headers)
    input.rewind
    output = IO::Memory.new
    connection = HTTP2::Connection.new(IO::Stapled.new(input, output), :server)

    frame = connection.receive.not_nil!
    assert_equal HTTP2::Frame::Type::HEADERS, frame.type
    stream = frame.stream
    response = HTTP::Server::Response.new(stream)
    {HTTP::Server::Context.new(HTTP::Request.new("GET", "/"), response, stream), output}
  end

  private def read_frames(output : IO::Memory) : Array({HTTP2::Frame::Type, HTTP2::Frame::Flags, Bytes})
    output.rewind
    frames = [] of {HTTP2::Frame::Type, HTTP2::Frame::Flags, Bytes}
    until output.pos == output.size
      size_and_type = output.read_bytes(UInt32, IO::ByteFormat::BigEndian)
      flags = HTTP2::Frame::Flags.new(output.read_byte.not_nil!)
      output.read_bytes(UInt32, IO::ByteFormat::BigEndian) # stream id
      payload = Bytes.new(size_and_type >> 8)
      output.read_fully(payload)
      frames << {HTTP2::Frame::Type.new((size_and_type & 0xff).to_u8), flags, payload}
    end
    frames
  end
end
