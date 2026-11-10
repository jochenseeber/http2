require "../test_helper"
{% if flag?(:stdlib_server) %}
  require "http/server"
{% else %}
  require "../../src/server"
{% end %}
require "http/client/response"
require "compress/gzip"

# The HTTP/1 behaviour of `HTTP::Server`, run against the shard's server and
# against the standard library's (`crystal run -Dstdlib_server`, see
# `make contract`): requiring `http2/server` must not change it. The few
# deliberate differences are marked with `shard?`.
class HTTP::HTTP1ContractTest < Minitest::Test
  def test_request_line_too_long_gets_414
    status, _ = exchange("GET /#{"a" * 9000} HTTP/1.1\r\nHost: example.com\r\n\r\n")
    assert_equal 414, status
  end

  # 431, or 400 when the stdlib's parser gets the line in pieces, as it
  # sometimes does under load, with or without the shard.
  def test_headers_too_large_are_refused
    status, _ = exchange("GET / HTTP/1.1\r\nHost: example.com\r\nX-Big: #{"a" * 20_000}\r\n\r\n")
    assert_includes [431, 400], status
  end

  def test_malformed_requests_get_400
    [
      "GET\t/ HTTP/1.1\r\nHost: example.com\r\n\r\n",
      "GET / HTTP/1.1\r\nHost: example.com\r\nBad Name: value\r\n\r\n",
      "GET / HTTP/1.1\r\nHost: example.com\r\n:path: /admin\r\n\r\n",
    ].each do |request|
      next if stdlib_before_1_20? && !request.starts_with?("GET\t")
      status, _ = exchange(request)
      assert_equal 400, status, request.inspect
    end
  end

  # Differing content lengths: the stdlib takes the request; the shard
  # answers 400 (RFC 9112 section 6.3).
  def test_ambiguous_framing_gets_400
    requests = [] of String
    requests << "POST / HTTP/1.1\r\nHost: example.com\r\nContent-Length: 5\r\nTransfer-Encoding: chunked\r\n\r\n0\r\n\r\n" unless stdlib_before_1_20?
    requests << "POST / HTTP/1.1\r\nHost: example.com\r\nContent-Length: 5\r\nContent-Length: 0\r\n\r\nhello" if shard?
    requests.each do |request|
      status, _ = exchange(request)
      assert_equal 400, status, request.inspect
    end
  end

  def test_repeated_headers_keep_every_value
    values = nil
    exchange_with("GET / HTTP/1.1\r\nHost: example.com\r\nX-A: 1\r\nX-A: 2\r\n\r\n") do |context|
      values = context.request.headers.get("X-A")
    end
    assert_equal ["1", "2"], values
  end

  # The shard answers 400 (RFC 9112 section 3.2); the stdlib serves it, and
  # `Request#hostname` is nil.
  def test_repeated_host
    called = false
    status, _ = exchange_with("GET / HTTP/1.1\r\nHost: a.example\r\nHost: b.example\r\n\r\n") { called = true }
    if shard?
      assert_equal 400, status
      refute called
    else
      assert_equal 200, status
    end
  end

  def test_keep_alive_serves_the_next_request
    socket = connect { |context| context.response.print context.request.path }
    socket << "GET /first HTTP/1.1\r\nHost: example.com\r\n\r\n"
    socket << "GET /second HTTP/1.1\r\nHost: example.com\r\n\r\n"
    socket.flush
    assert_equal "/first", HTTP::Client::Response.from_io(socket).body
    assert_equal "/second", HTTP::Client::Response.from_io(socket).body
  ensure
    socket.try(&.close)
  end

  def test_handler_closing_the_connection
    socket = connect do |context|
      context.response.headers["Connection"] = "close"
      context.response.print "bye"
    end
    socket << "GET / HTTP/1.1\r\nHost: example.com\r\n\r\n"
    socket.flush
    assert_equal "bye", HTTP::Client::Response.from_io(socket).body
    assert_equal "", socket.gets_to_end
  ensure
    socket.try(&.close)
  end

  def test_unread_body_closes_the_connection
    socket = connect { |context| context.response.print "ok" }
    socket << "POST / HTTP/1.1\r\nHost: example.com\r\nContent-Length: 5\r\n\r\nhello"
    socket.flush
    assert_equal "ok", HTTP::Client::Response.from_io(socket).body
    assert_equal "", socket.gets_to_end
  ensure
    socket.try(&.close)
  end

  # The stdlib sends the body of a 204 (and chunks it); the shard drops it
  # (RFC 9110 sections 15.3.5 and 15.4.5).
  def test_bodiless_statuses_are_not_chunked
    skip "the stdlib sends the body" unless shard?
    {204, 304}.each do |code|
      _, response = exchange_with("GET / HTTP/1.1\r\nHost: example.com\r\nConnection: close\r\n\r\n") do |context|
        context.response.status_code = code
        context.response.print "dropped"
      end
      refute_match /chunked/i, response, code.to_s
      refute_match /dropped/, response, code.to_s
    end
  end

  # The shard sends the status and headers on a flush before the body, so a
  # client can wait on a stream; the stdlib sends them with the first byte
  # of the body.
  def test_flush_before_the_body_sends_the_headers
    skip "the stdlib sends them with the body" unless shard?
    body = Channel(Nil).new
    socket = connect do |context|
      context.response.flush
      body.receive
      context.response.print "data"
    end
    socket << "GET / HTTP/1.1\r\nHost: example.com\r\n\r\n"
    socket.flush
    assert_equal "HTTP/1.1 200 OK", socket.gets
    headers = [] of String
    until (line = socket.gets).nil? || line.empty?
      headers << line
    end
    assert_includes headers, "Transfer-Encoding: chunked"
    body.send(nil)
    assert_equal "4", socket.gets
    assert_equal "data", socket.gets
  ensure
    socket.try(&.close)
  end

  def test_handler_error_before_headers_gets_500
    status, _ = exchange_with("GET / HTTP/1.1\r\nHost: example.com\r\nConnection: close\r\n\r\n") { raise "boom" }
    assert_equal 500, status
  end

  # The shard cuts the response off, so the client can tell it's incomplete
  # (RFC 9112 section 7.1); the stdlib ends it as if it were complete.
  def test_handler_error_after_headers
    _, response = exchange_with("GET / HTTP/1.1\r\nHost: example.com\r\n\r\n") do |context|
      context.response.print "partial"
      context.response.flush
      raise "boom"
    end
    assert_match /partial/, response
    if shard?
      refute response.ends_with?("0\r\n\r\n"), response.inspect
    else
      assert response.ends_with?("0\r\n\r\n"), response.inspect
    end
  end

  def test_upgrade_hands_over_the_connection
    socket = connect do |context|
      context.response.status = :switching_protocols
      context.response.headers["Connection"] = "Upgrade"
      context.response.headers["Upgrade"] = "echo"
      context.response.upgrade do |io|
        io << "upgraded:" << io.gets << "\n"
        io.flush
      end
    end
    socket << "GET / HTTP/1.1\r\nHost: example.com\r\nConnection: Upgrade\r\nUpgrade: echo\r\n\r\n"
    socket.flush
    assert_equal "HTTP/1.1 101 Switching Protocols", socket.gets
    until socket.gets.try(&.empty?)
    end
    socket << "ping\n"
    socket.flush
    assert_equal "upgraded:ping", socket.gets
  ensure
    socket.try(&.close)
  end

  def test_request_processor_serves_http1
    processor = HTTP::Server::RequestProcessor.new { |context| context.response.print context.request.path }
    output = IO::Memory.new
    processor.process(IO::Memory.new("GET /a HTTP/1.1\r\nHost: example.com\r\n\r\nGET /b HTTP/1.1\r\nHost: example.com\r\n\r\n"), output)
    output.rewind
    assert_equal "/a", HTTP::Client::Response.from_io(output).body
    assert_equal "/b", HTTP::Client::Response.from_io(output).body
  end

  def test_requests_know_their_addresses
    remote = local = nil
    exchange_with("GET / HTTP/1.1\r\nHost: example.com\r\nConnection: close\r\n\r\n") do |context|
      remote = context.request.remote_address
      local = context.request.local_address
    end
    {% if compare_versions(Crystal::VERSION, "1.20.0") >= 0 %}
      assert_match /\A127\.0\.0\.1:\d+\z/, remote.to_s
      assert_match /\A127\.0\.0\.1:\d+\z/, local.to_s
    {% else %}
      # the stdlib sets them from Crystal 1.20
      if shard?
        assert_match /\A127\.0\.0\.1:\d+\z/, remote.to_s
        assert_match /\A127\.0\.0\.1:\d+\z/, local.to_s
      end
    {% end %}
  end

  def test_compress_handler
    socket = connect(HTTP::CompressHandler.new) { |context| context.response.print "a" * 1000 }
    socket << "GET / HTTP/1.1\r\nHost: example.com\r\nAccept-Encoding: gzip\r\n\r\n"
    socket.flush
    response = HTTP::Client::Response.from_io(socket, decompress: false)
    assert_equal "gzip", response.headers["Content-Encoding"]?
    assert_equal "a" * 1000, Compress::Gzip::Reader.open(IO::Memory.new(response.body), &.gets_to_end)
  ensure
    socket.try(&.close)
  end

  # The shard decodes a gzip request body, as over HTTP/2; the stdlib hands
  # it over encoded.
  def test_gzip_request_body
    compressed = IO::Memory.new
    Compress::Gzip::Writer.open(compressed) { |gzip| gzip << "hello" }
    body = nil
    exchange_with("POST / HTTP/1.1\r\nHost: example.com\r\nConnection: close\r\nContent-Encoding: gzip\r\nContent-Length: #{compressed.size}\r\n\r\n#{compressed}") do |context|
      body = context.request.body.try(&.gets_to_end)
    end
    if shard? || stdlib_before_1_20?
      assert_equal "hello", body
    else
      refute_equal "hello", body
    end
  end

  # The stdlib leaves an idle keep-alive connection open on close.
  def test_close_ends_an_idle_keep_alive_connection
    skip "the stdlib leaves it open" unless shard?
    server, port = start { |context| context.response.print "ok" }
    socket = TCPSocket.new("127.0.0.1", port)
    socket.read_timeout = 5.seconds
    socket << "GET / HTTP/1.1\r\nHost: example.com\r\n\r\n"
    socket.flush
    assert_equal "ok", HTTP::Client::Response.from_io(socket).body
    server.close
    assert_equal "", socket.gets_to_end
  ensure
    socket.try(&.close)
  end

  private def shard? : Bool
    {{ !flag?(:stdlib_server) }}
  end

  # The stdlib's server before Crystal 1.20 accepts invalid field names and
  # a `Content-Length` with a `Transfer-Encoding`, and decodes gzip request
  # bodies itself.
  private def stdlib_before_1_20? : Bool
    {{ flag?(:stdlib_server) && compare_versions(Crystal::VERSION, "1.20.0") < 0 }}
  end

  private def start(handler : HTTP::Handler? = nil, &block : HTTP::Server::Context ->) : {HTTP::Server, Int32}
    server = handler ? HTTP::Server.new([handler] of HTTP::Handler, &block) : HTTP::Server.new(&block)
    listener = TCPServer.new("127.0.0.1", 0)
    server.bind(listener)
    spawn { server.listen }
    until server.listening?
      Fiber.yield
    end
    {server, listener.local_address.port}
  end

  private def connect(handler : HTTP::Handler? = nil, &block : HTTP::Server::Context ->) : TCPSocket
    _, port = start(handler, &block)
    socket = TCPSocket.new("127.0.0.1", port)
    socket.read_timeout = 5.seconds
    socket
  end

  # Sends *request* on a fresh connection and returns the status of the
  # response, and everything read until the server closed or a read timed
  # out.
  private def exchange_with(request : String, &block : HTTP::Server::Context ->) : {Int32?, String}
    socket = connect(&block)
    socket.read_timeout = 1.second
    socket << request
    socket.flush
    received = IO::Memory.new
    buffer = Bytes.new(4096)
    begin
      while (n = socket.read(buffer)) > 0
        received.write(buffer[0, n])
      end
    rescue IO::TimeoutError | IO::Error
    end
    text = received.to_s
    {text.match(/\AHTTP\/1\.[01] (\d{3})/).try(&.[1].to_i), text}
  ensure
    socket.try(&.close)
  end

  private def exchange(request : String) : {Int32?, String}
    exchange_with(request) { |context| context.response.print "ok" }
  end
end
