require "./test_helper"
require "../src/server"
require "http/client/response"

class HTTP::Server
  # serves a connection in the current fiber, so an exception reaches the
  # test instead of ending a spawned fiber
  def handle_client_for_test(io : IO) : Nil
    handle_client(io)
  end
end

class HTTP::ServerHTTP1Test < Minitest::Test
  NEXT_REQUEST = "GET /next HTTP/1.1\r\nHost: example.com\r\n\r\n"

  def test_http1_1_request_with_connection_close
    request = "GET / HTTP/1.1\r\nHost: example.com\r\nConnection: close\r\n\r\n"
    output, input = serve(request + NEXT_REQUEST)

    response = HTTP::Client::Response.from_io(output)
    assert_equal "HTTP/1.1", response.version
    assert_equal 200, response.status_code
    assert_equal "ok", response.body
    refute response.headers.has_key?("connection")
    assert_equal request.bytesize, input.pos
  end

  def test_http1_0_request
    request = "GET / HTTP/1.0\r\nHost: example.com\r\n\r\n"
    output, input = serve(request + NEXT_REQUEST)

    response = HTTP::Client::Response.from_io(output)
    assert_equal "HTTP/1.0", response.version
    assert_equal 200, response.status_code
    assert_equal "ok", response.body
    refute response.headers.has_key?("connection")
    assert_equal request.bytesize, input.pos
  end

  def test_handler_closing_a_keep_alive_connection
    request = "GET / HTTP/1.1\r\nHost: example.com\r\n\r\n"
    output, input = serve(request + NEXT_REQUEST, ->(context : HTTP::Server::Context) {
      context.response.headers["Connection"] = "close"
    })

    response = HTTP::Client::Response.from_io(output)
    assert_equal 200, response.status_code
    assert_equal "close", response.headers["connection"]
    assert_equal request.bytesize, input.pos
  end

  def test_keep_alive_connection_reads_the_next_request
    request = "GET / HTTP/1.1\r\nHost: example.com\r\n\r\n"
    requests = [] of String
    output, input = serve(request + NEXT_REQUEST, ->(context : HTTP::Server::Context) {
      requests << "#{context.request.method} #{context.request.resource}"
    })

    response = HTTP::Client::Response.from_io(output)
    assert_equal 200, response.status_code
    assert response.keep_alive?

    response = HTTP::Client::Response.from_io(output)
    assert_equal 200, response.status_code
    assert_equal "ok", response.body
    assert_equal ["GET /", "GET /next"], requests
    assert_equal (request + NEXT_REQUEST).bytesize, input.pos
  end

  def test_keep_alive_connection_reads_a_request_line_with_a_colon
    request = "GET / HTTP/1.1\r\nHost: example.com\r\n\r\n"
    next_request = "DELETE /admin?a=b:c HTTP/1.1\r\nHost: example.com\r\n\r\n"
    requests = [] of String
    output, _ = serve(request + next_request, ->(context : HTTP::Server::Context) {
      requests << "#{context.request.method} #{context.request.resource}"
    })

    2.times { assert_equal 200, HTTP::Client::Response.from_io(output).status_code }
    assert_equal ["GET /", "DELETE /admin?a=b:c"], requests
  end

  def test_keep_alive_connection_rejects_a_next_request_of_another_version
    request = "GET / HTTP/1.1\r\nHost: example.com\r\n\r\n"
    output, _ = serve(request + "GET /next HTTP/2.0\r\nHost: example.com\r\n\r\n")

    assert_equal 200, HTTP::Client::Response.from_io(output).status_code
    assert_equal 400, HTTP::Client::Response.from_io(output).status_code
  end

  def test_repeated_header_keeps_every_value
    request = "GET / HTTP/1.1\r\nHost: example.com\r\nAccept: text/html\r\nAccept: application/json\r\n\r\n"
    accept = nil
    output, _ = serve(request, ->(context : HTTP::Server::Context) {
      accept = context.request.headers.get("Accept")
    })

    assert_equal 200, HTTP::Client::Response.from_io(output).status_code
    assert_equal ["text/html", "application/json"], accept
  end

  # 400, or the stdlib's 501 for an unsupported transfer coding; the
  # connection is closed.
  def test_malformed_requests_get_an_error_without_calling_the_handler
    [
      "GET\t/ HTTP/1.1\r\nHost: example.com\r\n\r\n",
      "GET / HTTP/1.1\r\nHost: a\r\nHost: b\r\n\r\n",
      "GET / HTTP/1.1\r\nHost: example.com\r\nBad Name: value\r\n\r\n",
      "GET / HTTP/1.1\r\nHost: example.com\r\nX-Value: a\u0001b\r\n\r\n",
      "GET / HTTP/1.1\r\nHost: example.com\r\n:path: /admin\r\n\r\n",
      "POST / HTTP/1.1\r\nHost: example.com\r\nContent-Length: 1\r\nContent-Length: 2\r\n\r\nab",
      "POST / HTTP/1.1\r\nHost: example.com\r\nContent-Length: x\r\n\r\n",
      "POST / HTTP/1.1\r\nHost: example.com\r\nContent-Length: 5\r\nContent-Length: 0\r\n\r\nhello",
      "POST / HTTP/1.1\r\nHost: example.com\r\nContent-Length: 5, 0\r\n\r\nhello",
      "POST / HTTP/1.1\r\nHost: example.com\r\nContent-Length: 5\r\nTransfer-Encoding: chunked\r\n\r\n0\r\n\r\n",
      "POST / HTTP/1.1\r\nHost: example.com\r\nTransfer-Encoding: chunked\r\nContent-Length: 5\r\n\r\n0\r\n\r\n",
      "POST / HTTP/1.1\r\nHost: example.com\r\nTransfer-Encoding: gzip\r\n\r\nhello",
      "POST / HTTP/1.1\r\nHost: example.com\r\nTransfer-Encoding: chunked, gzip\r\n\r\nhello",
    ].each do |request|
      called = false
      output, _ = serve(request, ->(context : HTTP::Server::Context) { called = true })

      response = HTTP::Client::Response.from_io(output)
      assert_includes [400, 501, 505], response.status_code, request.inspect
      response.body
      assert_equal output.size, output.pos, request.inspect
      refute called, request.inspect
    end
  end

  # The stdlib's parser refuses a list such as `5, 5`, which RFC 9112
  # section 6.3 lets it do.
  def test_identical_repeated_content_lengths_are_accepted
    [
      "POST / HTTP/1.1\r\nHost: example.com\r\nContent-Length: 5\r\nContent-Length: 5\r\n\r\nhello",
    ].each do |request|
      body = nil
      output, _ = serve(request, ->(context : HTTP::Server::Context) { body = context.request.body.try(&.gets_to_end); nil })
      assert_equal 200, HTTP::Client::Response.from_io(output).status_code, request.inspect
      assert_equal "hello", body
    end
  end

  def test_chunked_request_body
    body = nil
    request = "POST / HTTP/1.1\r\nHost: example.com\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nhello\r\n0\r\n\r\n"
    output, _ = serve(request, ->(context : HTTP::Server::Context) { body = context.request.body.try(&.gets_to_end); nil })
    assert_equal 200, HTTP::Client::Response.from_io(output).status_code
    assert_equal "hello", body
  end

  def test_malformed_next_request_line_gets_400
    request = "GET / HTTP/1.1\r\nHost: example.com\r\n\r\n"
    requests = 0
    output, _ = serve(request + "GET\t/next HTTP/1.1\r\nHost: example.com\r\n\r\n", ->(context : HTTP::Server::Context) {
      requests += 1
    })

    assert_equal 200, HTTP::Client::Response.from_io(output).status_code
    assert_equal 400, HTTP::Client::Response.from_io(output).status_code
    assert_equal 1, requests
  end

  def test_empty_input_gets_no_reply
    output, _ = serve("")
    assert_empty output.to_s
  end

  def test_bodiless_statuses_are_not_chunked_on_a_keep_alive_connection
    {204, 304}.each do |status|
      request = "GET / HTTP/1.1\r\nHost: example.com\r\n\r\n"
      output, input = serve_status(request + NEXT_REQUEST, status) { }

      assert_equal "HTTP/1.1 #{status} #{HTTP::Status.new(status).description}\r\nConnection: keep-alive\r\n\r\n",
        output.gets("\r\n\r\n")
      next_response = HTTP::Client::Response.from_io(output)
      assert_equal 200, next_response.status_code
      assert_equal "ok", next_response.body
      assert_equal (request + NEXT_REQUEST).bytesize, input.pos
    end
  end

  def test_no_content_drops_the_body_and_a_transfer_encoding
    request = "GET / HTTP/1.1\r\nHost: example.com\r\n\r\n"
    output, _ = serve_status(request + NEXT_REQUEST, 204) do |response|
      response.headers["Transfer-Encoding"] = "chunked"
      response.print "dropped"
      response.flush
    end

    assert_equal "HTTP/1.1 204 No Content\r\nConnection: keep-alive\r\n\r\n", output.gets("\r\n\r\n")
    assert_equal "ok", HTTP::Client::Response.from_io(output).body
  end

  def test_flush_before_the_body_sends_the_headers
    request = "GET / HTTP/1.1\r\nHost: example.com\r\n\r\n"
    sent, output = serve_flush(request + NEXT_REQUEST)

    assert_match /\AHTTP\/1\.1 200 OK\r\n/, sent
    assert_match /\r\nTransfer-Encoding: chunked\r\n/, sent
    assert sent.ends_with?("\r\n\r\n"), sent.inspect
    response = HTTP::Client::Response.from_io(output)
    assert_equal "chunked", response.headers["Transfer-Encoding"]
    assert_equal "data", response.body
    assert_equal "ok", HTTP::Client::Response.from_io(output).body
  end

  # The body that the flush started ends with the last chunk on close.
  def test_flush_without_a_body_ends_the_chunked_body
    request = "GET / HTTP/1.1\r\nHost: example.com\r\n\r\n"
    sent, output = serve_flush(request + NEXT_REQUEST, write: false)

    assert_match /\r\nTransfer-Encoding: chunked\r\n/, sent
    assert_equal sent + "0\r\n\r\n", output.gets("0\r\n\r\n")
    assert_equal "ok", HTTP::Client::Response.from_io(output).body
  end

  # Without a flush, the stdlib's close still sets the length of the body.
  def test_without_flush_an_empty_body_gets_a_content_length
    output, _ = serve_status("GET / HTTP/1.1\r\nHost: example.com\r\n\r\n", 200) { }
    response = HTTP::Client::Response.from_io(output)
    assert_equal "0", response.headers["Content-Length"]
    refute response.headers.has_key?("Transfer-Encoding")
  end

  def test_flush_before_the_body_keeps_a_content_length
    request = "GET / HTTP/1.1\r\nHost: example.com\r\n\r\n"
    sent, output = serve_flush(request + NEXT_REQUEST) do |response|
      response.content_length = 4
    end

    assert_match /Content-Length: 4\r\n/, sent
    refute_match /Transfer-Encoding/, sent
    assert_equal "data", HTTP::Client::Response.from_io(output).body
    assert_equal "ok", HTTP::Client::Response.from_io(output).body
  end

  # HTTP/1.0 has no chunked encoding: the body ends with the connection.
  def test_flush_before_the_body_over_http1_0_closes_the_connection
    request = "GET / HTTP/1.0\r\nHost: example.com\r\nConnection: keep-alive\r\n\r\n"
    sent, output = serve_flush(request + NEXT_REQUEST)

    assert_match /\AHTTP\/1\.0 200 OK\r\n/, sent
    assert_match /Connection: close\r\n/, sent
    refute_match /Transfer-Encoding/, sent
    assert_equal "data", HTTP::Client::Response.from_io(output).body
    assert_equal output.size, output.pos
  end

  # A HEAD response has no body to frame: the next response follows the
  # headers.
  def test_flush_of_a_head_response_starts_no_body
    request = "HEAD / HTTP/1.1\r\nHost: example.com\r\n\r\n"
    sent, output = serve_flush(request + NEXT_REQUEST, write: false)

    refute_match /Transfer-Encoding/, sent
    assert_equal sent, output.gets("\r\n\r\n")
    assert_equal "ok", HTTP::Client::Response.from_io(output).body
  end

  def test_without_h2c_an_upgrade_request_gets_an_http1_response
    request = "GET / HTTP/1.1\r\nHost: example.com\r\nConnection: Upgrade, HTTP2-Settings\r\n" \
              "Upgrade: h2c\r\nHTTP2-Settings: AAMAAABkAAQAAP__\r\n\r\n"
    output, input = serve_without_h2c(request + NEXT_REQUEST)

    response = HTTP::Client::Response.from_io(output)
    assert_equal "HTTP/1.1", response.version
    assert_equal 200, response.status_code
    assert_equal "GET /", response.body
    assert_equal "GET /next", HTTP::Client::Response.from_io(output).body
    assert_equal (request + NEXT_REQUEST).bytesize, input.pos
  end

  # A first line that is neither HTTP/1 nor the HTTP/2 preface is an invalid
  # preface for an HTTP/2 client: GOAWAY(PROTOCOL_ERROR), then the
  # connection is closed (RFC 9113 section 3.4).
  def test_first_line_neither_http1_nor_http2_gets_goaway
    ["INVALID CONNECTION PREFACE\r\n\r\n", "GET / HTTP/3.0\r\nHost: example.com\r\n\r\n"].each do |input|
      called = false
      output, _ = serve(input, ->(context : HTTP::Server::Context) { called = true })

      header = output.read_bytes(UInt32, IO::ByteFormat::BigEndian)
      assert_equal HTTP2::Frame::Type::GOAWAY.value, header & 0xff, input.inspect
      output.skip(5) # flags, stream id
      output.skip(4) # last stream id
      assert_equal HTTP2::Error::Code::PROTOCOL_ERROR.value, output.read_bytes(UInt32, IO::ByteFormat::BigEndian)
      refute called
    end
  end

  # Without h2c, the stdlib answers it as an HTTP/1 request.
  def test_without_h2c_an_invalid_first_line_gets_an_http1_response
    output, _ = serve_without_h2c("INVALID CONNECTION PREFACE\r\n\r\n")
    assert_equal "HTTP/1.1", HTTP::Client::Response.from_io(output).version
  end

  # The preface is a request of an unsupported version for the stdlib.
  def test_without_h2c_the_preface_gets_an_http1_response_then_closes
    output, _ = serve_without_h2c(HTTP2::CLIENT_PREFACE)

    response = HTTP::Client::Response.from_io(output)
    assert_equal "HTTP/1.1", response.version
    assert_includes [400, 505], response.status_code
    response.body
    # nothing else: no SETTINGS frame
    assert_equal output.size, output.pos
  end

  def test_request_processor_serves_http1_requests
    processor = HTTP::Server::RequestProcessor.new do |context|
      context.response.headers["X-Path"] = context.request.path
      context.response.print "ok"
    end
    request = "GET /first HTTP/1.1\r\nHost: example.com\r\n\r\n"
    output = IO::Memory.new
    processor.process(IO::Memory.new(request + NEXT_REQUEST), output)
    output.rewind

    first = HTTP::Client::Response.from_io(output)
    assert_equal 200, first.status_code
    assert_equal "/first", first.headers["X-Path"]
    assert_equal "keep-alive", first.headers["Connection"]
    assert_equal "ok", first.body
    second = HTTP::Client::Response.from_io(output)
    assert_equal "/next", second.headers["X-Path"]
    assert_equal "ok", second.body
  end

  # Serves *request* with `h2c` off; the handler answers with the request's
  # method and resource.
  private def serve_without_h2c(request : String) : {IO::Memory, IO::Memory}
    server = HTTP::Server.new do |context|
      context.response.print "#{context.request.method} #{context.request.resource}"
    end
    server.h2c = false
    input = IO::Memory.new(request)
    output = IO::Memory.new
    server.handle_client_for_test(IO::Stapled.new(input, output))
    {output.rewind, input}
  end

  # Serves *request*, answering the first request with *status* after the
  # block, and the next ones with 200 "ok".
  private def serve_status(request : String, status : Int32, &block : HTTP::Server::Response ->) : {IO::Memory, IO::Memory}
    first = true
    server = HTTP::Server.new do |context|
      if first
        first = false
        context.response.status_code = status
        block.call(context.response)
      else
        context.response.print "ok"
      end
    end
    input = IO::Memory.new(request)
    output = IO::Memory.new
    server.handle_client_for_test(IO::Stapled.new(input, output))
    {output.rewind, input}
  end

  # Serves *request*: the first response is flushed after the block, then
  # gets "data", unless *write* is false; the next ones get 200 "ok".
  # Returns what was sent when the flush returned, and all the output.
  private def serve_flush(request : String, write = true, &block : HTTP::Server::Response ->) : {String, IO::Memory}
    first = true
    sent = ""
    output = IO::Memory.new
    server = HTTP::Server.new do |context|
      if first
        first = false
        block.call(context.response)
        context.response.flush
        sent = output.to_s
        context.response.print "data" if write
      else
        context.response.print "ok"
      end
    end
    server.handle_client_for_test(IO::Stapled.new(IO::Memory.new(request), output))
    {sent, output.rewind}
  end

  private def serve_flush(request : String, write = true) : {String, IO::Memory}
    serve_flush(request, write) { }
  end

  private def serve(request : String, before : (HTTP::Server::Context ->)? = nil) : {IO::Memory, IO::Memory}
    server = HTTP::Server.new do |context|
      before.try &.call(context)
      context.response.print "ok"
    end
    input = IO::Memory.new(request)
    output = IO::Memory.new
    server.handle_client_for_test(IO::Stapled.new(input, output))
    {output.rewind, input}
  end
end
