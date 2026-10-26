require "http/client"
require "socket"
require "sync/mutex"
{% unless flag?(:without_openssl) %}
  require "openssl"
  require "./openssl_context"
{% end %}
require "./http_headers"
require "../client/connecting"
require "../client/connection"

# Transparent HTTP/2 support for `HTTP::Client`: concurrent requests share
# one HTTP/2 connection, each on its own stream.
#
# ```
# require "http2/client"
#
# client = HTTP::Client.new("example.com", tls: true) # ALPN h2, else HTTP/1
# response = client.get("/")
# response.version # => "HTTP/2"
# ```
#
# The protocol of a connection:
#
# - TLS: ALPN offers `h2` and `http/1.1`. The client speaks HTTP/2 if the
#   server selects `h2`, HTTP/1 on the same connection otherwise.
# - Cleartext: HTTP/1, unless `protocol: "h2"` says that the server speaks
#   HTTP/2 (prior knowledge, RFC 9113 section 3.3). The deprecated `h2c`
#   upgrade isn't supported.
# - An existing IO: HTTP/1, unless `protocol: "h2"`, or the IO is an
#   `OpenSSL::SSL::Socket::Client` whose handshake selected `h2`.
#
# `protocol: "h1"` keeps HTTP/1, and `protocol: "h2"` fails a TLS connect
# unless the server selects `h2`. A server that answers HTTP_1_1_REQUIRED
# makes the client use HTTP/1 for the host from then on.
#
# Under HTTP/1, the requests of concurrent fibers take turns on the
# connection. Under HTTP/2, they run concurrently, and the response body is
# an `HTTP2::ClientBody`.
#
# The client spawns the fibers of an HTTP/2 connection (see
# `HTTP2::ClientConnection`) in the execution context that makes the first
# request on it. Use a client from that execution context: in a
# multi-threaded context, the fibers may read from and write to the IO in
# parallel, which `OpenSSL::SSL::Socket` doesn't support. Under the legacy
# `-Dpreview_mt` scheduler, without execution contexts, a TLS client can't
# be confined to one thread.
class HTTP::Client
  # The headers specific to an HTTP/1 connection, that are malformed in
  # HTTP/2 (RFC 9113 section 8.2.2).
  private HTTP2_CONNECTION_HEADERS = {"connection", "keep-alive", "proxy-connection", "transfer-encoding", "upgrade"}

  private PROTOCOLS = {"h1", "h2"}

  # The protocol asked for: `"h1"`, `"h2"`, or `nil` to negotiate it.
  getter protocol : String? = nil

  # The local settings of new HTTP/2 connections. For example, a larger
  # initial window size than the default 65535 bytes avoids stalls on long
  # round trips:
  #
  # ```
  # client.http2_settings.initial_window_size = 1 << 20
  # ```
  getter http2_settings : HTTP2::Settings = HTTP2::DEFAULT_SETTINGS.dup.tap(&.enable_push = false)

  @scheme : String? = nil
  @http2 : HTTP2::ClientConnection? = nil
  @connecting : HTTP2::ClientConnecting? = nil

  # The server answered HTTP_1_1_REQUIRED: new connections use HTTP/1.
  @http1_required = false

  # Whether the IO of a client on an existing IO was checked for HTTP/2:
  # until then, `@io` isn't known to be an HTTP/1 connection.
  @io_checked = false

  # Guards the connection state above and `@io`.
  @state = Sync::Mutex.new

  # The HTTP/1 requests take turns.
  @http1 = Sync::Mutex.new

  # Creates a client for *host* and *port* that uses *protocol* (see
  # `HTTP::Client`).
  def initialize(host : String, port = nil, tls : TLSContext = nil, *, protocol : String?)
    initialize(host, port, tls)
    @protocol = HTTP::Client.check_protocol(protocol)
  end

  # Creates a client on *io* that uses *protocol*. *scheme* is the
  # `:scheme` of HTTP/2 requests: `https` for an `OpenSSL::SSL::Socket` and
  # `http` otherwise when it's `nil`, for example `https` for an encrypted
  # tunnel.
  def initialize(io : IO, host = "", port = 80, *, protocol : String?, scheme : String? = nil)
    initialize(io, host, port)
    @protocol = HTTP::Client.check_protocol(protocol)
    @scheme = scheme
  end

  # :ditto:
  def initialize(io : IO, host = "", port = 80, *, scheme : String)
    initialize(io, host, port, protocol: nil, scheme: scheme)
  end

  protected def self.check_protocol(protocol : String?) : String?
    unless protocol.nil? || PROTOCOLS.includes?(protocol)
      raise ArgumentError.new("Unsupported protocol #{protocol.inspect} (expected \"h1\" or \"h2\")")
    end
    protocol
  end

  # The `:scheme` of HTTP/2 requests.
  def scheme : String
    @scheme || (tls? || ssl_socket?(@io) ? "https" : "http")
  end

  # Returns true once the HTTP/2 connection is closed, or the server sent
  # GOAWAY. A client on a host and port connects again on the next request.
  def closed? : Bool
    @state.synchronize { @http2 }.try(&.closed?) || false
  end

  # Closes the connection. Under HTTP/2, sends GOAWAY(NO_ERROR) and fails
  # the requests in flight. A connect in progress fails the requests that
  # wait for it at once (see `#connect_timeout=`). A client on a host and
  # port connects again on the next request.
  #
  # Once the server sent GOAWAY, the client stops using the HTTP/2
  # connection but lets the requests in flight on it complete, and the
  # connection closes when they're done. `#close` doesn't close such a
  # draining connection once the client connected again.
  def close : Nil
    http2, connecting, io = @state.synchronize do
      {@http2, @connecting.tap { @connecting = nil }, @io.tap { @io = nil }}
    end
    connecting.try(&.cancel(IO::Error.new("Client closed while connecting")))
    http2.try(&.close)
    begin
      io.try(&.close)
    rescue IO::Error
      # already closed
    end
  end

  private def exec_internal(request)
    if connection = http2_connection
      exec_http2(connection, request) do |response|
        body = response.body_io?.try(&.gets_to_end)
        return HTTP::Client::Response.new(response.status, body, response.headers, nil, response.version)
      end
      # the request falls back to HTTP/1
    end
    @http1.synchronize { previous_def(request) }
  end

  private def exec_internal(request, &block : Response -> T) : T forall T
    if connection = http2_connection
      exec_http2(connection, request) { |response| return yield response }
      # the request falls back to HTTP/1
    end
    @http1.synchronize do
      previous_def(request) { |response| yield response }
    end
  end

  # The HTTP/1 connection, while an HTTP/1 request runs. Connects again
  # with HTTP/1, for example after stdlib's retry closed a stale connection.
  private def io
    if io = @state.synchronize { @io }
      return io
    end
    raise "This HTTP::Client cannot be reconnected" unless @reconnect

    io = connect(HTTP2::ClientConnecting.new, http1_only: true)
    @state.synchronize { @io = io }
  end

  # The HTTP/2 connection to use, or `nil` for HTTP/1. Connects, or waits
  # for a connect in progress, unless a connection is open.
  private def http2_connection : HTTP2::ClientConnection?
    connecting = @state.synchronize do
      if http2 = @http2
        return http2 unless http2.closed?
        # a client on an IO can't connect again
        raise IO::Error.new("Connection closed") unless @reconnect
      end
      if @io && (@reconnect || @io_checked)
        return
      end
      raise "This HTTP::Client cannot be reconnected" unless @reconnect || @io

      # a fiber of its own connects, outside the lock, so `#close` and
      # `#closed?` don't wait for it; `#close` or the connect timer can
      # fail the requests that wait meanwhile at once
      @connecting ||= HTTP2::ClientConnecting.new.tap do |pending|
        spawn(name: "http2:client:connect") { run_connect(pending) }
      end
    end
    connecting.wait.as?(HTTP2::ClientConnection)
  end

  private def run_connect(connecting : HTTP2::ClientConnecting) : Nil
    begin
      result = open_connection(connecting)
    rescue error
    end

    # #close and the connect timer cancel the connect under the same lock
    current = @state.synchronize do
      next false unless @connecting.same?(connecting)
      @connecting = nil
      case result
      when HTTP2::ClientConnection
        @http2 = result
        @io = nil # a client on an IO gave it to the connection
      when IO
        @io = result
        @io_checked = true
      end
      true
    end

    if current
      connecting.finish(error || result.not_nil!)
    else
      # canceled: the waiters failed already
      close_result(result)
    end
  end

  private def close_result(result) : Nil
    result.try(&.close)
  rescue IO::Error
    # already closed
  end

  private def open_connection(connecting : HTTP2::ClientConnecting) : HTTP2::ClientConnecting::Result
    unless @reconnect
      io = @state.synchronize { @io }
      raise IO::Error.new("Connection closed") unless io
      return http2_io?(io) ? new_http2_connection(io, sync_close: true) : io
    end

    io = connect(connecting, http1_only: false)
    return io unless http2_io?(io)

    begin
      new_http2_connection(io, sync_close: true)
    rescue ex
      io.close
      raise ex
    end
  end

  # Whether to speak HTTP/2 on *io*, a connected socket or the IO of a
  # client on an existing IO.
  private def http2_io?(io : IO) : Bool
    return false if @http1_required || @protocol == "h1"
    return true if @protocol == "h2"
    {% if flag?(:without_openssl) %}
      false
    {% else %}
      io.is_a?(OpenSSL::SSL::Socket::Client) && io.alpn_protocol == "h2"
    {% end %}
  end

  private def ssl_socket?(io : IO?) : Bool
    {% if flag?(:without_openssl) %}
      false
    {% else %}
      io.is_a?(OpenSSL::SSL::Socket)
    {% end %}
  end

  private def new_http2_connection(io : IO, sync_close : Bool) : HTTP2::ClientConnection
    @scheme ||= scheme
    HTTP2::ClientConnection.new(io, http2_settings,
      read_timeout: @read_timeout,
      write_timeout: @write_timeout,
      sync_close: sync_close)
  end

  # Opens the TCP connection and does the TLS handshake. The connect
  # timeout is one deadline for both: once the host is resolved, a timer
  # cancels the connect when it passes. Sets the socket's read timeout
  # only for HTTP/1: an HTTP/2 connection idles legitimately while no
  # stream is active, and its watchdog enforces the read timeout per
  # stream.
  private def connect(connecting : HTTP2::ClientConnecting, *, http1_only : Bool) : IO
    hostname = @host.starts_with?('[') && @host.ends_with?(']') ? @host[1..-2] : @host
    HTTP2::Log.trace { "connecting to #{hostname}:#{@port}" }

    deadline = nil
    socket = Socket::Addrinfo.tcp(hostname, @port, timeout: @dns_timeout) do |addrinfo|
      # the host is resolved: the connect timeout starts
      unless deadline
        if span = @connect_timeout
          deadline = now + span
          start_connect_timer(connecting, span)
        end
      end
      timeout = remaining(deadline)
      raise IO::Error.new("Connect canceled") if connecting.canceled?

      # the socket isn't closed while it connects: it breaks the event loop
      # of a multi-threaded execution context
      tcp = TCPSocket.new(addrinfo.family)
      if error = tcp.connect(addrinfo, timeout: timeout) { |error| error }
        tcp.close
        error
      else
        tcp
      end
    end
    raise Socket::ConnectError.new("Error connecting to '#{hostname}:#{@port}'") unless socket

    # connected: from now on, canceling the connect closes the socket
    connecting.socket = socket

    socket.tcp_nodelay = true
    socket.sync = false
    socket.write_timeout = @write_timeout

    {% unless flag?(:without_openssl) %}
      if context = @tls
        protocols = alpn_protocols(http1_only)
        context.alpn_protocols = protocols if protocols
        begin
          # the connect timer closes the socket if the handshake takes too
          # long; sync_close: closing the TLS socket while the reader fiber
          # reads it must close the TCP socket too
          tls = OpenSSL::SSL::Socket::Client.new(socket, context, sync_close: true, hostname: hostname.rchop('.'))
        rescue ex
          socket.close
          raise ex
        end
        if @protocol == "h2" && tls.alpn_protocol != "h2"
          tls.close
          raise HTTP2::Error.http_1_1_required("#{@host}:#{@port} didn't negotiate HTTP/2 (ALPN h2)")
        end
        tls.sync = false
        socket.read_timeout = @read_timeout unless http2_io?(tls)
        return tls
      end
    {% end %}

    socket.read_timeout = @read_timeout unless http2_io?(socket)
    socket
  end

  # The ALPN protocols to offer, if any: none for `protocol: "h1"`, where
  # the context is left as it is.
  private def alpn_protocols(http1_only : Bool) : Array(String)?
    return if @protocol == "h1"
    return ["h2"] if @protocol == "h2"
    http1_only || @http1_required ? ["http/1.1"] : ["h2", "http/1.1"]
  end

  # Cancels the connect once *span* passed, unless it ended first.
  private def start_connect_timer(connecting : HTTP2::ClientConnecting, span : Time::Span) : Nil
    spawn(name: "http2:client:connect-timer") do
      select
      when connecting.ended.receive?
        next
      when timeout(span)
      end

      @state.synchronize { @connecting = nil if @connecting.same?(connecting) }
      connecting.cancel(IO::TimeoutError.new("Connect timed out"))
    end
  end

  # The time left until *deadline*, for the TCP connect. Raises
  # `IO::TimeoutError` once it passed.
  private def remaining(deadline) : Time::Span?
    return unless deadline
    left = deadline - now
    raise IO::TimeoutError.new("Connect timed out") unless left.positive?
    left
  end

  private def now
    {% if Time.class.has_method?(:instant) %}
      Time.instant
    {% else %}
      Time.monotonic
    {% end %}
  end

  # Executes *request* on the HTTP/2 *connection* and yields the response
  # once its headers arrived; the block reads the body from
  # `HTTP::Client::Response#body_io` as the server sends it. Leaving the
  # block before the response ended cancels the stream.
  #
  # Returns without yielding when the request falls back to HTTP/1: the
  # server answered HTTP_1_1_REQUIRED, or a retry connected with HTTP/1.
  # The request is then as it was before: the HTTP/1 path sets its
  # defaults and runs the `before_request` callbacks again.
  private def exec_http2(connection : HTTP2::ClientConnection, request : HTTP::Request, &) : Nil
    original_headers = request.headers.dup
    original_body = request.body

    implicit_compression = implicit_compression?(request)
    set_defaults request
    run_before_request_callbacks(request)

    body = request_body(request)
    replay = body.as?(IO::Memory).try { |memory| memory.to_slice[memory.pos..] }
    headers = http2_headers(request, body)

    unless exchange = start(connection, headers, body, replay)
      request.headers = original_headers
      request.body = replay ? IO::Memory.new(replay) : original_body
      return
    end

    response = build_response(exchange, implicit_compression, head: request.method == "HEAD")
    begin
      yield response
    ensure
      response.body_io?.try(&.close)
      exchange.close_body
    end
  end

  # Opens the stream and waits for the response headers. A request the
  # server refused without processing it (REFUSED_STREAM or GOAWAY, RFC
  # 9113 section 8.7) is retried once on a new connection, if the client
  # can connect again and the request body can be sent again. Returns `nil`
  # when the request falls back to HTTP/1 (see `#exec_http2`).
  #
  # A retry sends a body in memory again from its own `IO::Memory` over the
  # same bytes: the body writer of the refused attempt may still read the
  # body once more before it notices that the stream is closed.
  private def start(connection, headers, body, replay) : HTTP2::ClientExchange?
    retried = false

    loop do
      exchange = nil
      begin
        exchange = connection.request(headers, body)
        exchange.wait_headers
        return exchange
      rescue ex : HTTP2::Error
        replayable = body.nil? || replay
        if ex.code.http_1_1_required?
          raise ex unless http1_required(connection) && replayable
          return
        end

        raise ex unless ex.code.refused_stream? && exchange.try(&.refused?) != false
        raise ex if retried || !@reconnect || !replayable

        HTTP2::Log.trace { "retrying refused request: #{ex.message}" }
        retried = true
        body = replay.try { |bytes| IO::Memory.new(bytes, writable: false) }
        return unless connection = http2_connection
      end
    end
  end

  # The server answered HTTP_1_1_REQUIRED (RFC 9113 section 8.2.2): new
  # connections use HTTP/1, and *connection* takes no new requests. Returns
  # false if the client can't fall back: it's on an existing IO, or asked
  # for HTTP/2.
  private def http1_required(connection) : Bool
    return false unless @reconnect && @protocol.nil?

    @state.synchronize do
      @http1_required = true
      @http2 = nil if @http2.same?(connection)
    end
    connection.retire
    true
  end

  # The request body, unless it's empty.
  private def request_body(request : HTTP::Request) : IO?
    case body = request.body
    when IO::Memory
      body unless body.pos == body.size
    else
      body
    end
  end

  # Maps the request to the HTTP/2 header list (RFC 9113 section 8.3.1).
  private def http2_headers(request : HTTP::Request, body : IO?) : HTTP::Headers
    headers = HTTP::Headers{
      ":method" => request.method,
      ":scheme" => scheme,
    }
    if authority = request.headers["Host"]?.presence
      headers[":authority"] = authority
    end
    headers[":path"] = request.resource

    request.headers.each do |name, values|
      name = name.downcase
      next if name == "host" || HTTP2_CONNECTION_HEADERS.includes?(name)

      if name == "te"
        # only "trailers" is allowed (RFC 9113 section 8.2.2)
        if values.any? { |value| value.split(',').any? { |coding| coding.strip.compare("trailers", case_insensitive: true) == 0 } }
          headers["te"] = "trailers"
        end
      else
        headers.add(name, values)
      end
    end

    if cookies = request.@cookies
      cookies.add_request_headers(headers)
    end

    if body.is_a?(IO::Memory) && !headers.has_key?("content-length")
      headers["content-length"] = (body.size - body.pos).to_s
    end

    headers
  end

  # A response to a HEAD request has no body, whatever its headers say
  # (RFC 9110 section 9.3.2).
  private def build_response(exchange : HTTP2::ClientExchange, decompress : Bool, head : Bool) : HTTP::Client::Response
    stream_headers = exchange.stream.headers

    unless status_code = stream_headers[":status"]?.try(&.to_i?)
      exchange.cancel
      raise HTTP2::Error.protocol_error("INVALID :status pseudo-header")
    end
    status = HTTP::Status.new(status_code)

    headers = HTTP::Headers.new
    stream_headers.each { |name, values| headers.add(name, values) unless name.starts_with?(':') }

    if HTTP::Client::Response.mandatory_body?(status) && !head
      encoding = headers["content-encoding"]? if decompress
      if encoding.in?("gzip", "x-gzip", "deflate")
        headers.delete("content-encoding")
        headers.delete("content-length")
      else
        encoding = nil
      end
      body = HTTP2::ClientBody.new(exchange, encoding)
    end

    HTTP::Client::Response.new(status, nil, headers, nil, "HTTP/2", body)
  end
end
