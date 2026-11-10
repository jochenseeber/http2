require "./test_helper"
require "../src/server"
require "../src/client"
require "digest/sha256"
require "file_utils"

# `HTTP::Client` with HTTP/2 against the library's own `HTTP::Server` on the loopback
# interface.
{% unless flag?(:without_openssl) %}
  @[Link("ssl")]
  lib LibSSLShutdown
    RECEIVED_SHUTDOWN = 2

    fun ssl_get_shutdown = SSL_get_shutdown(handle : LibSSL::SSL) : LibC::Int
  end
{% end %}

class HTTP::Server
  # serves a connection in the current fiber
  def handle_client_for_test(io : IO) : Nil
    handle_client(io)
  end
end

{% unless flag?(:without_openssl) %}
  # A subclass of `OpenSSL::SSL::Socket` that does its own handshake, as
  # applications do: the shard's extension of the class must keep it
  # compiling, and must apply to it too.
  class HandshakingClient < OpenSSL::SSL::Socket
    def initialize(io, context : OpenSSL::SSL::Context::Client)
      super(io, context, true)
      {% if OpenSSL::SSL::Socket.has_method?(:ktls_safe_handshake) %}
        ret = ktls_safe_handshake { LibSSL.ssl_connect(@ssl) }
      {% else %}
        ret = LibSSL.ssl_connect(@ssl)
      {% end %}
      raise OpenSSL::SSL::Error.new(@ssl, ret, "SSL_connect") unless ret == 1
    end
  end
{% end %}

class HTTP2::ClientServerTest < Minitest::Test
  def test_echoes_a_large_body
    body = Random.new(1).random_bytes(1 << 20)
    with_server do |client|
      response = client.post("/echo", body: body)
      assert_equal 200, response.status_code
      assert_equal Digest::SHA256.hexdigest(body), Digest::SHA256.hexdigest(response.body)
    end
  end

  def test_echoes_a_streamed_body
    body = "a line\n" * 10_000
    with_server do |client|
      response = client.post("/echo", body: IO::Sized.new(IO::Memory.new(body), body.bytesize))
      assert_equal body, response.body
    end
  end

  def test_concurrent_requests_share_the_connection
    with_server do |client|
      responses = Channel(String | Exception).new(20)
      20.times do |i|
        # one client is used from one thread under -Dpreview_mt
        HTTP2.spawn do
          responses.send(client.get("/path/#{i}").body)
        rescue ex
          responses.send(ex)
        end
      end

      bodies = Array.new(20) { receive_within(responses) }
      bodies.each { |body| raise body if body.is_a?(Exception) }
      assert_equal (0...20).map { |i| "/path/#{i}" }.sort, bodies.map(&.as(String)).sort
    end
  end

  def test_downloads_with_a_large_window
    with_server do |client|
      client.http2_settings.initial_window_size = 1 << 20
      response = client.get("/download?size=#{4 << 20}")
      assert_equal 4 << 20, response.body.bytesize
      assert_equal "4194304", response.headers["content-length"]
    end
  end

  def test_cancels_a_download_and_keeps_the_connection
    with_server do |client|
      client.get("/download?size=#{32 << 20}") do |response|
        response.body_io.read_fully(Bytes.new(100_000))
      end
      assert_equal "/after", client.get("/after").body
    end
  end

  def test_handler_sends_goaway_and_streams_in_flight_complete
    with_server do |client|
      waiting = Channel(String | Exception).new(1)
      spawn do
        waiting.send(client.get("/wait").body)
      rescue ex
        waiting.send(ex)
      end
      receive_within(@started)

      response = client.get("/goaway")
      assert_equal "/goaway", response.body
      assert client.closed?, "the client must have received GOAWAY"

      # the stream opened before the GOAWAY completes
      @release.send(nil)
      body = receive_within(waiting)
      raise body if body.is_a?(Exception)
      assert_equal "/wait", body

      # the client connects again
      assert_equal "/after", client.get("/after").body
    end
  end

  def test_head_request
    with_server do |client|
      response = client.head("/download?size=1000")
      assert_equal 200, response.status_code
      assert_equal "1000", response.headers["content-length"]
      assert_equal "", response.body
    end
  end

  def test_tls_with_alpn
    with_server(tls: true) do |client|
      assert_equal "https", client.scheme
      response = client.get("/secure")
      assert_equal "HTTP/2", response.version
      assert_equal "/secure", response.body
      response = client.post("/echo", body: "x" * 100_000)
      assert_equal 100_000, response.body.bytesize
    end
  end

  def test_requests_know_their_addresses_over_h2c
    with_server do |client|
      assert_addresses client.get("/addresses")
    end
  end

  def test_requests_know_their_addresses_over_tls
    with_server(tls: true) do |client|
      response = client.get("/addresses")
      assert_equal "HTTP/2", response.version
      assert_addresses response
    end
  end

  def test_requests_know_their_addresses_over_http1
    with_server(tls: true, alpn: false) do |client|
      response = client.get("/addresses")
      assert_equal "HTTP/1.1", response.version
      assert_addresses response
    end
  end

  private def assert_addresses(response) : Nil
    remote, local = response.body.split(' ')
    assert_match /\A127\.0\.0\.1:\d+\z/, remote
    assert_match /\A127\.0\.0\.1:\d+\z/, local
    refute_equal remote, local
  end

  # See `HTTP2::ServerTest#test_serves_streams_in_flight_after_client_goaway`.
  def test_tls_server_closes_the_connection_once_the_last_stream_ends_after_client_goaway
    single_threaded do
      release = Channel(Nil).new
      server = HTTP::Server.new do |context|
        context.response.print "hello "
        context.response.flush
        release.receive
        context.response.print "world"
      end
      listener = TCPServer.new("127.0.0.1", 0)
      tls_listener = OpenSSL::SSL::Server.new(listener, server_tls_context.tap(&.alpn_protocol = "h2"))
      tls_listener.start_immediately = false
      server.bind(tls_listener)
      spawn { server.listen }

      tcp = TCPSocket.new("127.0.0.1", listener.local_address.port)
      tcp.read_timeout = 5.seconds
      socket = OpenSSL::SSL::Socket::Client.new(tcp, client_tls_context.tap(&.alpn_protocol = "h2"), sync_close: true)
      client = Connection.new(socket, :client)
      client.write_client_preface
      client.write_settings
      stream = client.streams.create
      stream.send_headers(HTTP::Headers{
        ":method"    => "GET",
        ":scheme"    => "https",
        ":authority" => "example.com",
        ":path"      => "/",
      }, Frame::Flags::END_STREAM)
      receive_frames_until(client) { |frame| frame.type.data? }

      # the client goes away; the PING ACK tells the server read the GOAWAY
      write_frame(socket, Frame::Type::GOAWAY, Frame::Flags::None, 0, Bytes.new(8))
      write_frame(socket, Frame::Type::PING, Frame::Flags::None, 0, Bytes.new(8))
      socket.flush
      receive_frames_until(client) { |frame| frame.type.ping? }

      release.send(nil)
      receive_frames_until(client) { |frame| frame.stream.id == 1 && frame.flags.end_stream? }
      assert_equal "hello world", stream.data.gets_to_end
      receive_frames_until(client) { |frame| frame.type.goaway? }
      assert_raises(IO::EOFError) { loop { client.receive } }
      # the TLS session ended cleanly: a client can tell the connection
      # wasn't truncated
      assert_equal LibSSLShutdown::RECEIVED_SHUTDOWN, LibSSLShutdown.ssl_get_shutdown(socket.@ssl) & LibSSLShutdown::RECEIVED_SHUTDOWN
    ensure
      release.try(&.close)
      socket.try(&.close)
      server.try(&.close)
      listener.try(&.close)
    end
  end

  private def receive_frames_until(connection, &)
    loop do
      frame = connection.receive
      break if frame && yield frame
    end
  end

  def test_tls_falls_back_to_http1_unless_the_server_negotiates_h2
    with_server(tls: true, alpn: false) do |client|
      response = client.get("/plain")
      assert_equal "HTTP/1.1", response.version
      assert_equal "/plain", response.body
    end
  end

  def test_falls_back_to_http1_when_the_server_requires_it
    single_threaded do
      listener = TCPServer.new("127.0.0.1", 0)
      context = server_tls_context
      context.alpn_protocols = ["h2", "http/1.1"]
      alpn = Channel(String?).new(2)
      spawn do
        # HTTP/2: resets the first request with HTTP_1_1_REQUIRED
        socket = OpenSSL::SSL::Socket::Server.new(listener.accept? || next, context, sync_close: true)
        alpn.send(socket.alpn_protocol)
        connection = HTTP2::Connection.new(socket, :server)
        connection.read_client_preface
        connection.write_settings
        until (frame = connection.receive) && frame.type.headers?
        end
        frame.stream.send_rst_stream(Error::Code::HTTP_1_1_REQUIRED)

        # HTTP/1: answers the retried request and the next one
        socket = OpenSSL::SSL::Socket::Server.new(listener.accept? || next, context, sync_close: true)
        alpn.send(socket.alpn_protocol)
        while (request = HTTP::Request.from_io(socket)).is_a?(HTTP::Request)
          socket << "HTTP/1.1 200 OK\r\nContent-Length: " << request.path.bytesize << "\r\n\r\n" << request.path
          socket.flush
        end
      rescue IO::Error | OpenSSL::SSL::Error
        # the client closed the connection
      end

      client = HTTP::Client.new("127.0.0.1", listener.local_address.port, tls: client_tls_context)
      response = client.get("/first")
      assert_equal "HTTP/1.1", response.version
      assert_equal "/first", response.body
      assert_equal "/second", client.get("/second").body
      assert_equal "h2", receive_within(alpn)
      assert_equal "http/1.1", receive_within(alpn)
    ensure
      client.try(&.close)
      listener.try(&.close)
    end
  end

  def test_tls_close_doesnt_wait_for_a_write_the_server_doesnt_read
    single_threaded do
      listener = TCPServer.new("127.0.0.1", 0)
      context = server_tls_context
      context.alpn_protocols = ["h2"]
      spawn do
        # grants windows larger than the socket buffers, then stops reading
        socket = OpenSSL::SSL::Socket::Server.new(listener.accept? || next, context, sync_close: true)
        connection = HTTP2::Connection.new(socket, :server)
        connection.local_settings.initial_window_size = 1 << 24
        connection.read_client_preface
        connection.write_settings
        until (frame = connection.receive) && frame.type.headers?
        end
      rescue IO::Error | OpenSSL::SSL::Error
        # the client closed the connection
      end

      client = HTTP::Client.new("127.0.0.1", listener.local_address.port, tls: client_tls_context)
      result = Channel(Exception?).new(1)
      spawn do
        client.post("/", body: Bytes.new(1 << 23))
        result.send(nil)
      rescue ex
        result.send(ex)
      end
      sleep 200.milliseconds # the body writer blocks in a write

      closed = Channel(Nil).new(1)
      spawn { client.close; closed.send(nil) }
      receive_within(closed, 2.seconds)
      error = receive_within(result, 1.second)
      assert error.is_a?(IO::Error), "Expected an IO::Error, got #{error.inspect}"
    ensure
      client.try(&.close)
      listener.try(&.close)
    end
  end

  def test_tls_fails_unless_the_server_negotiates_h2
    single_threaded do
      listener = TCPServer.new("127.0.0.1", 0)
      context = server_tls_context # no ALPN
      spawn do
        if socket = listener.accept?
          OpenSSL::SSL::Socket::Server.new(socket, context, sync_close: true).close rescue nil
        end
      end

      client = HTTP::Client.new("127.0.0.1", listener.local_address.port, tls: client_tls_context, protocol: "h2")
      error = assert_raises(Error) { client.get("/") }
      assert_equal Error::Code::HTTP_1_1_REQUIRED, error.code
    ensure
      listener.try(&.close)
    end
  end

  def test_connect_timeout_bounds_the_tls_handshake
    with_silent_server do |port, accepted|
      client = HTTP::Client.new("127.0.0.1", port, tls: client_tls_context)
      client.connect_timeout = 200.milliseconds
      result = request_in_fiber(client)

      socket = receive_within(accepted)
      error = receive_within(result, 5.seconds)
      assert_instance_of IO::TimeoutError, error
    ensure
      socket.try(&.close)
    end
  end

  def test_connect_timeout_is_one_deadline_for_the_tls_handshake
    single_threaded do
      listener = TCPServer.new("127.0.0.1", 0)
      accepted = Channel(TCPSocket).new(1)
      spawn do
        if socket = listener.accept?
          accepted.send(socket)
          # the start of a TLS record that never completes, one byte every
          # 50 ms: each read gets data well within the connect timeout
          socket.write(Bytes[0x16, 0x03, 0x03, 0x40, 0x00])
          socket.flush
          loop do
            sleep 50.milliseconds
            socket.write(Bytes[0])
            socket.flush
          end
        end
      rescue IO::Error
        # the client closed the connection
      end

      client = HTTP::Client.new("127.0.0.1", listener.local_address.port, tls: client_tls_context)
      client.connect_timeout = 300.milliseconds
      result = request_in_fiber(client)

      socket = receive_within(accepted)
      error = nil
      elapsed = Time.measure { error = receive_within(result, 3.seconds) }
      assert_instance_of IO::TimeoutError, error
      assert elapsed < 2.seconds, "the connect took #{elapsed}"
    ensure
      socket.try(&.close)
      listener.try(&.close)
    end
  end

  def test_connect_timeout_spares_the_established_connection
    with_server(tls: true) do |client|
      client.connect_timeout = 100.milliseconds
      assert_equal "/first", client.get("/first").body

      # past the deadline, the connection is still open
      sleep 300.milliseconds
      refute client.closed?
      assert_equal "/second", client.get("/second").body
    end
  end

  def test_close_interrupts_a_connect
    with_silent_server do |port, accepted|
      client = HTTP::Client.new("127.0.0.1", port, tls: client_tls_context)
      result = request_in_fiber(client)

      # the client waits for the TLS handshake
      socket = receive_within(accepted)
      closed = Channel(Bool).new(1)
      spawn { closed.send(client.closed?) }
      refute receive_within(closed, 1.second)

      done = Channel(Nil).new(1)
      spawn do
        client.close
        done.send(nil)
      end
      receive_within(done, 1.second)
      error = receive_within(result, 1.second)
      assert error.is_a?(IO::Error), "expected an IO::Error, got #{error.inspect}"
      assert_equal "Client closed while connecting", error.try(&.message)

      # the client closed its socket: the server reads the client hello, then
      # the end of the stream (a read timeout raises otherwise)
      socket.read_timeout = 1.second
      socket.skip_to_end
      assert_equal 0, socket.read(Bytes.new(1))
    ensure
      socket.try(&.close)
    end
  end

  def test_cancel_closes_a_stalled_http1_request
    with_http1_server { |client| assert_cancels_http1(client) }
  end

  def test_cancel_closes_a_stalled_http1_request_over_tls
    with_server(tls: true, alpn: false) { |client| assert_cancels_http1(client) }
  end

  def test_cancel_after_an_http1_request_ended_doesnt_close_the_next_one
    with_http1_server do |client|
      cancellation = HTTP::Client::Cancellation.new
      response = client.exec(HTTP::Request.new("GET", "/first"), cancellation: cancellation)
      assert_equal "/first", response.body

      result = Channel(String | Exception).new(1)
      spawn do
        result.send(client.get("/wait").body)
      rescue ex
        result.send(ex)
      end
      receive_within(@started)
      cancellation.cancel
      @release.send(nil)
      body = receive_within(result)
      raise body if body.is_a?(Exception)
      assert_equal "/wait", body
    end
  end

  # A stalled HTTP/1 request raises the cancellation's error once canceled,
  # isn't sent again, and the client connects again for the next request.
  private def assert_cancels_http1(client) : Nil
    cancellation = HTTP::Client::Cancellation.new
    result = Channel(Exception?).new(1)
    spawn do
      client.exec(HTTP::Request.new("GET", "/wait"), cancellation: cancellation)
      result.send(nil)
    rescue ex
      result.send(ex)
    end
    receive_within(@started)

    cancellation.cancel
    error = receive_within(result, 1.second)
    assert error.is_a?(HTTP2::Error), "expected the cancellation's error, got #{error.inspect}"
    assert_equal HTTP2::Error::Code::CANCEL, error.as(HTTP2::Error).code

    # not retried
    select
    when @started.receive
      flunk "the canceled request was sent again"
    when timeout(200.milliseconds)
    end

    @release.send(nil)
    response = client.get("/after")
    assert_equal "HTTP/1.1", response.version
    assert_equal "/after", response.body
  end

  private def with_http1_server(&block : HTTP::Client ->) : Nil
    server = HTTP::Server.new { |context| handle(context) }
    listener = TCPServer.new("127.0.0.1", 0)
    server.bind(listener)
    spawn { server.listen }
    client = HTTP::Client.new("127.0.0.1", listener.local_address.port, protocol: "h1")
    client.read_timeout = 5.seconds
    block.call(client)
  ensure
    client.try(&.close)
    server.try(&.close)
  end

  # Over an IO that isn't a socket, a default context's kernel TLS option
  # must not break the handshake (with OpenSSL 3 on Linux, the BIO would cast
  # the IO to a socket).
  def test_tls_over_an_io_that_isnt_a_socket
    single_threaded do
      server_io, client_io = IO::Stapled.pipe
      server = HTTP::Server.new { |context| context.response << "over a pipe" }
      tls = OpenSSL::SSL::Socket::Server.new(server_io, server_tls_context, sync_close: true, accept: false)
      spawn { server.handle_client_for_test(tls) }

      client = HandshakingClient.new(client_io, OpenSSL::SSL::Context::Client.new.tap(&.verify_mode = OpenSSL::SSL::VerifyMode::NONE))
      client << "GET / HTTP/1.1\r\nHost: example.com\r\nConnection: close\r\n\r\n"
      client.flush
      response = HTTP::Client::Response.from_io(client)
      assert_equal "over a pipe", response.body
    ensure
      client.try(&.close) rescue nil
      server.try(&.close)
    end
  end

  def test_close_during_a_tcp_connect_fails_the_request_at_once
    with_full_backlog do |port|
      started = Channel(Nil).new(1)
      client = SignalingClient.new("127.0.0.1", port, started)
      result = request_in_fiber(client)

      # the connect waits for the TCP connection: the backlog is full
      receive_within(started)
      done = Channel(Nil).new(1)
      spawn do
        client.close
        done.send(nil)
      end
      receive_within(done, 1.second)
      error = receive_within(result, 1.second)
      assert error.is_a?(IO::Error), "expected an IO::Error, got #{error.inspect}"
      assert_equal "Client closed while connecting", error.try(&.message)
    end
  end

  def test_connect_timeout_bounds_the_tcp_connect
    with_full_backlog do |port|
      client = HTTP::Client.new("127.0.0.1", port, protocol: "h2")
      client.connect_timeout = 300.milliseconds
      error = nil
      elapsed = Time.measure { error = receive_within(request_in_fiber(client), 3.seconds) }
      assert_instance_of IO::TimeoutError, error
      assert elapsed < 2.seconds, "the connect took #{elapsed}"
    end
  end

  # A listener whose backlog is full and that never accepts: a new TCP
  # connection waits, because the SYN is dropped (Linux). Skips where a full
  # backlog resets new connections instead (macOS). Closing the listener at
  # the end resets the connects still waiting.
  private def with_full_backlog(&)
    listener = TCPServer.new("127.0.0.1", 0, backlog: 1)
    port = listener.local_address.port
    fillers = [] of TCPSocket
    full = false
    64.times do
      fillers << TCPSocket.new("127.0.0.1", port, connect_timeout: 200.milliseconds)
    rescue IO::TimeoutError
      full = true
      break
    rescue Socket::ConnectError
      # reset: the backlog doesn't hold the connection
    end
    skip "a full backlog doesn't hold new TCP connections here" unless full

    yield port
  ensure
    fillers.try(&.each(&.close))
    listener.try(&.close)
  end

  # A client that signals when it starts to connect.
  private class SignalingClient < HTTP::Client
    def initialize(host : String, port : Int32, @started : Channel(Nil))
      super(host, port, protocol: "h2")
    end

    private def connect(connecting, *, http1_only)
      @started.send(nil)
      super
    end
  end

  # A server that accepts a TCP connection and never answers. The block, a
  # test of a TLS client, runs single-threaded (see `#single_threaded`).
  private def with_silent_server(&block : Int32, Channel(TCPSocket) ->) : Nil
    single_threaded do
      listener = TCPServer.new("127.0.0.1", 0)
      accepted = Channel(TCPSocket).new(1)
      spawn { listener.accept?.try { |socket| accepted.send(socket) } }
      block.call(listener.local_address.port, accepted)
    ensure
      listener.try(&.close)
    end
  end

  private def request_in_fiber(client) : Channel(Exception?)
    result = Channel(Exception?).new(1)
    spawn do
      client.get("/")
      result.send(nil)
    rescue ex
      result.send(ex)
    end
    result
  end

  # Runs the block with a client of a server on the loopback interface. A
  # TLS client and the server run single-threaded (see `#single_threaded`).
  private def with_server(tls = false, alpn = true, &block : HTTP::Client ->) : Nil
    if tls
      single_threaded { serve(tls, alpn, &block) }
    else
      serve(tls, alpn, &block)
    end
  end

  private def serve(tls, alpn, &block : HTTP::Client ->) : Nil
    server = HTTP::Server.new { |context| handle(context) }
    listener = TCPServer.new("127.0.0.1", 0)
    port = listener.local_address.port
    if tls
      context = server_tls_context
      context.alpn_protocol = "h2" if alpn
      tls_listener = OpenSSL::SSL::Server.new(listener, context)
      tls_listener.start_immediately = false
      server.bind(tls_listener)
      client = HTTP::Client.new("127.0.0.1", port, tls: client_tls_context)
    else
      server.bind(listener)
      client = HTTP::Client.new("127.0.0.1", port, protocol: "h2")
    end
    client.read_timeout = 5.seconds
    spawn { server.listen }

    block.call(client)
  ensure
    client.try(&.close)
    server.try(&.close)
    # the server didn't get the listener if the TLS setup failed
    listener.try(&.close)
  end

  # The "/wait" handler tells it started, then waits for the release.
  @started = Channel(Nil).new(1)
  @release = Channel(Nil).new(1)

  private def handle(context)
    request, response = context.request, context.response

    case request.path
    when "/wait"
      @started.send(nil)
      receive_within(@release)
      response << request.path
    when "/goaway"
      context.http2_connection.not_nil!.send_goaway
      response << request.path
    when "/addresses"
      response << request.remote_address << ' ' << request.local_address
    when "/echo"
      IO.copy(request.body.not_nil!, response) if request.body
    when "/download"
      size = request.query_params["size"].to_i
      response.content_length = size
      unless request.method == "HEAD"
        chunk = Bytes.new(16384, 0x61_u8)
        while size > 0
          count = Math.min(size, chunk.size)
          response.write(chunk[0, count])
          size -= count
        end
      end
    else
      response << request.path
    end
  rescue IO::Error | HTTP::Server::ClientError
    # the client canceled the stream
  end

  private def server_tls_context : OpenSSL::SSL::Context::Server
    certificate, key = self.class.certificate
    context = OpenSSL::SSL::Context::Server.new
    context.certificate_chain = certificate
    context.private_key = key
    context
  end

  private def client_tls_context : OpenSSL::SSL::Context::Client
    context = OpenSSL::SSL::Context::Client.new
    context.verify_mode = OpenSSL::SSL::VerifyMode::NONE
    context
  end

  @@certificate : {String, String}?
  @@certificate_mutex = Mutex.new

  # A self-signed certificate, generated once.
  protected def self.certificate : {String, String}
    @@certificate_mutex.synchronize do
      @@certificate ||= begin
        dir = File.join(Dir.tempdir, "http2-client-test-#{Process.pid}")
        Dir.mkdir_p(dir)
        at_exit { FileUtils.rm_rf(dir) }
        certificate, key = File.join(dir, "server.crt"), File.join(dir, "server.key")
        status = Process.run("openssl", {
          "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1",
          "-subj", "/CN=localhost", "-keyout", key, "-out", certificate,
        }, error: Process::Redirect::Close)
        raise "openssl req failed" unless status.success?
        {certificate, key}
      end
    end
  end

  private def receive_within(channel : Channel(T), timeout = 10.seconds) : T forall T
    select
    when value = channel.receive
      value
    when timeout(timeout)
      flunk "timed out after #{timeout}"
    end
  end
end
