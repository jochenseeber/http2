require "./test_helper"
require "../src/server"
require "compress/gzip"
require "http/client/response"
require "http/server/handlers/compress_handler"
require "http/server/handlers/error_handler"
require "log/memory_backend"

# The stdlib's handlers and response methods must see the state of a response
# whose headers this library sends.
class HTTP::Server::ResponseStdlibTest < Minitest::Test
  class Handler
    include HTTP::Handler

    def initialize(&@block : HTTP::Server::Context ->)
    end

    def call(context : HTTP::Server::Context) : Nil
      @block.call(context)
    end
  end

  def test_wrote_headers
    response, _ = new_http1_response
    refute response.wrote_headers?
    response.print "buffered"
    refute response.wrote_headers?
    response.flush
    assert response.wrote_headers?

    response = HTTP::Server::Response.new(receive_request(new_http2_connection))
    refute response.wrote_headers?
    response.print "buffered"
    refute response.wrote_headers?
    response.flush
    assert response.wrote_headers?
  end

  def test_status_message
    response, _ = new_http1_response
    assert_equal "OK", response.status_message

    response.status = :not_found
    assert_equal "Not Found", response.status_message

    response.status_message = "Nothing Here"
    assert_equal "Nothing Here", response.status_message

    response, _ = new_http1_response
    response.respond_with_status(:gone, "Gone Fishing")
    assert_equal "Gone Fishing", response.status_message
  end

  def test_error_handler_responds_with_500_before_headers
    response, output = new_http1_response
    log = ::Log::MemoryBackend.new
    call(response, HTTP::ErrorHandler.new(log: ::Log.new("test", log, :debug)), Handler.new do |_|
      raise "boom"
    end)
    response.close

    client_response = HTTP::Client::Response.from_io(output.rewind)
    assert_equal 500, client_response.status_code
    assert_equal "500 Internal Server Error\n", client_response.body
    assert_equal "Unhandled exception", log.entries.first.message
  end

  def test_error_handler_discards_unsent_body
    response, output = new_http1_response
    log = ::Log::MemoryBackend.new
    call(response, HTTP::ErrorHandler.new(log: ::Log.new("test", log, :debug)), Handler.new do |context|
      context.response.print "partial"
      raise "boom"
    end)
    response.close

    client_response = HTTP::Client::Response.from_io(output.rewind)
    assert_equal 500, client_response.status_code
    refute_includes client_response.body, "partial"
  end

  def test_error_handler_after_headers
    response, output = new_http1_response
    log = ::Log::MemoryBackend.new
    call(response, HTTP::ErrorHandler.new(log: ::Log.new("test", log, :debug)), Handler.new do |context|
      context.response.print "partial"
      context.response.flush
      raise "boom"
    end)
    response.close

    client_response = HTTP::Client::Response.from_io(output.rewind)
    assert_equal 200, client_response.status_code
    assert_equal "partial", client_response.body
    assert_equal "Unhandled exception", log.entries.first.message
  end

  def test_error_handler_after_headers_on_http2_stream
    response = HTTP::Server::Response.new(receive_request(new_http2_connection))
    log = ::Log::MemoryBackend.new
    call(response, HTTP::ErrorHandler.new(log: ::Log.new("test", log, :debug)), Handler.new do |context|
      context.response.print "partial"
      context.response.flush
      raise "boom"
    end)
    response.close

    assert_equal 200, response.status_code
    assert_equal "Unhandled exception", log.entries.first.message
  end

  def test_respond_with_status
    response, output = new_http1_response
    response.respond_with_status(:not_found)

    client_response = HTTP::Client::Response.from_io(output.rewind)
    assert_equal 404, client_response.status_code
    assert_equal "Not Found", client_response.status_message
    assert_equal "text/plain", client_response.content_type
    assert_equal "404 Not Found\n", client_response.body
  end

  def test_respond_with_status_and_message
    response, output = new_http1_response
    response.respond_with_status(:gone, "Gone Fishing")

    client_response = HTTP::Client::Response.from_io(output.rewind)
    assert_equal 410, client_response.status_code
    assert_equal "Gone Fishing", client_response.status_message
    assert_equal "text/plain", client_response.content_type
    assert_equal "410 Gone Fishing\n", client_response.body
  end

  def test_respond_with_status_on_http2_stream
    output = IO::Memory.new
    response = HTTP::Server::Response.new(receive_request(new_http2_connection(output)))
    response.respond_with_status(:not_found)

    headers, body = read_http2_response(output.rewind)
    assert_equal "404", headers[":status"]
    assert_equal "text/plain", headers["content-type"]
    assert_equal "404 Not Found\n", body
  end

  def test_respond_with_status_and_message_on_http2_stream
    output = IO::Memory.new
    response = HTTP::Server::Response.new(receive_request(new_http2_connection(output)))
    response.respond_with_status(:gone, "Gone Fishing")

    headers, body = read_http2_response(output.rewind)
    assert_equal "410", headers[":status"]
    assert_equal "text/plain", headers["content-type"]
    assert_equal "410 Gone Fishing\n", body
  end

  def test_verbose_error_handler_responds_with_500_before_headers
    response, output = new_http1_response
    call(response, new_verbose_error_handler, raise_before_headers)
    response.close

    client_response = HTTP::Client::Response.from_io(output.rewind)
    assert_equal 500, client_response.status_code
    assert_equal "text/plain", client_response.headers["content-type"]
    refute client_response.headers.has_key?("x-discarded")
    assert client_response.body.starts_with?("ERROR: boom (Exception)"), client_response.body
  end

  def test_verbose_error_handler_responds_with_500_before_headers_on_http2_stream
    output = IO::Memory.new
    response = HTTP::Server::Response.new(receive_request(new_http2_connection(output)))
    call(response, new_verbose_error_handler, raise_before_headers)
    response.close

    headers, body = read_http2_response(output)
    assert_equal "500", headers[":status"]
    assert_equal "text/plain", headers["content-type"]
    refute headers.has_key?("x-discarded")
    assert body.starts_with?("ERROR: boom (Exception)"), body
  end

  def test_reset
    response, output = new_http1_response
    response.status = :not_found
    response.status_message = "Nothing Here"
    response.headers["x-discarded"] = "yes"
    response.cookies << HTTP::Cookie.new("discarded", "yes")
    # writing to the output, unlike writing to the response, doesn't send the
    # headers: the bytes stay in the output's buffer
    original_output = response.output
    original_output.print "discarded"
    response.output = IO::Memory.new

    response.reset
    assert_equal HTTP::Status::OK, response.status
    assert_equal "OK", response.status_message
    refute response.headers.has_key?("x-discarded")
    assert_empty response.cookies
    assert_same original_output, response.output

    response.print "kept"
    response.close

    client_response = HTTP::Client::Response.from_io(output.rewind)
    assert_equal 200, client_response.status_code
    refute client_response.headers.has_key?("x-discarded")
    refute client_response.headers.has_key?("set-cookie")
    assert_equal "kept", client_response.body
  end

  # Over HTTP/1 the stdlib's processor resets the response for the next
  # request; a stream's response serves one request.
  def test_reset_after_headers_on_http2_stream
    response = HTTP::Server::Response.new(receive_request(new_http2_connection))
    response.flush
    ex = assert_raises(IO::Error) { response.reset }
    assert_equal "Headers already sent", ex.message
  end

  def test_compress_handler
    response, output = new_http1_response
    call_compress_handler(response)
    response.close

    client_response = HTTP::Client::Response.from_io(output.rewind, decompress: false)
    assert_equal "gzip", client_response.headers["Content-Encoding"]?
    assert_equal "hello", gunzip(client_response.body)
  end

  def test_compress_handler_on_http2_stream
    output = IO::Memory.new
    response = HTTP::Server::Response.new(receive_request(new_http2_connection(output)))
    call_compress_handler(response)
    response.close

    headers = HTTP::Headers.new
    body = IO::Memory.new
    read_frames(output.rewind) do |type, payload|
      case type
      when HTTP2::Frame::Type::HEADERS
        HTTP2::HPACK::Decoder.new.decode(payload, headers)
      when HTTP2::Frame::Type::DATA
        body.write(payload)
      end
    end
    assert_equal "gzip", headers["content-encoding"]?
    assert_equal "hello", gunzip(body.to_s)
  end

  def test_inspect
    response, _ = new_http1_response
    assert_includes response.inspect, "HTTP::Server::Response"
    assert_includes response.pretty_inspect, "HTTP::Server::Response"
    assert_includes response.output.inspect, "HTTP::Server::Response::Output"
    assert_includes response.output.pretty_inspect, "HTTP::Server::Response::Output"

    response = HTTP::Server::Response.new(receive_request(new_http2_connection))
    assert_includes response.inspect, "HTTP::Server::Response"
    assert_includes response.pretty_inspect, "HTTP::Server::Response"
    assert_includes response.output.inspect, "HTTP2::Output"
    assert_includes response.output.pretty_inspect, "HTTP2::Output"
  end

  private def new_http1_response : {HTTP::Server::Response, IO::Memory}
    output = IO::Memory.new
    {HTTP::Server::Response.new(output), output}
  end

  private def new_verbose_error_handler : HTTP::ErrorHandler
    HTTP::ErrorHandler.new(verbose: true, log: ::Log.new("test", ::Log::MemoryBackend.new, :debug))
  end

  # A handler that changes the response and raises before writing to it, that
  # is before the headers are sent.
  private def raise_before_headers : Handler
    Handler.new do |context|
      context.response.status = :not_found
      context.response.headers["x-discarded"] = "yes"
      raise Exception.new("boom")
    end
  end

  private def call(response, *handlers, request = HTTP::Request.new("GET", "/")) : Nil
    context = HTTP::Server::Context.new(request, response)
    HTTP::Server.build_middleware(handlers.to_a.map(&.as(HTTP::Handler))).call(context)
  end

  # Lets HTTP::CompressHandler compress what the handler writes, and checks
  # that writing doesn't send the headers, so the handler can still add
  # `Content-Encoding`.
  private def call_compress_handler(response) : Nil
    request = HTTP::Request.new("GET", "/", HTTP::Headers{"Accept-Encoding" => "gzip"})
    call(response, HTTP::CompressHandler.new, Handler.new do |context|
      context.response.print "hello"
      refute context.response.wrote_headers?
    end, request: request)
  end

  private def gunzip(body : String) : String
    Compress::Gzip::Reader.open(IO::Memory.new(body), &.gets_to_end)
  end

  # Yields the type and payload of each frame that was written to *io*.
  private def read_frames(io : IO::Memory, &) : Nil
    until io.pos == io.size
      header = io.read_bytes(UInt32, IO::ByteFormat::BigEndian)
      type = HTTP2::Frame::Type.new((header & 0xff).to_u8)
      io.skip(5) # flags and stream id
      payload = Bytes.new(header >> 8)
      io.read_fully(payload)
      yield type, payload
    end
  end

  # A server connection that will receive the HEADERS of a request on stream 1,
  # and writes its frames to *output*.
  private def new_http2_connection(output : IO = IO::Memory.new) : HTTP2::Connection
    headers = HTTP2::HPACK::Encoder.new.encode(HTTP::Headers{
      ":method"    => "GET",
      ":scheme"    => "http",
      ":authority" => "example.com",
      ":path"      => "/",
    })
    input = IO::Memory.new
    write_frame(input, HTTP2::Frame::Type::HEADERS, HTTP2::Frame::Flags::END_HEADERS | HTTP2::Frame::Flags::END_STREAM, 1, headers)
    input.rewind
    HTTP2::Connection.new(IO::Stapled.new(input, output), :server)
  end

  private def receive_request(connection) : HTTP2::Stream
    frame = connection.receive.not_nil!
    assert_equal HTTP2::Frame::Type::HEADERS, frame.type
    frame.stream
  end

  # Decodes the frames a server connection wrote for a single stream.
  private def read_http2_response(output : IO::Memory) : {HTTP::Headers, String}
    headers = HTTP::Headers.new
    body = IO::Memory.new
    decoder = HTTP2::HPACK::Decoder.new
    output.rewind

    until output.pos == output.size
      size_and_type = output.read_bytes(UInt32, IO::ByteFormat::BigEndian)
      output.skip(5) # flags and stream id
      payload = Bytes.new(size_and_type >> 8)
      output.read_fully(payload)

      case HTTP2::Frame::Type.new((size_and_type & 0xff).to_i)
      when .headers? then decoder.decode(payload, headers)
      when .data?    then body.write(payload)
      end
    end

    {headers, body.to_s}
  end
end
