require "./test_helper"
require "../src/server"
require "../src/client"
require "digest/sha256"
require "file_utils"

# `HTTP::Client` with HTTP/2 against the library's own `HTTP::Server` on the loopback
# interface.
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
        spawn do
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

      # the client closed its socket: the server reads the client hello, then
      # the end of the stream (a read timeout raises otherwise)
      socket.read_timeout = 1.second
      socket.skip_to_end
      assert_equal 0, socket.read(Bytes.new(1))
    ensure
      socket.try(&.close)
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

  private def handle(context)
    request, response = context.request, context.response

    case request.path
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
