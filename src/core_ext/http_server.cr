require "base64"
require "compress/deflate"
require "compress/gzip"
require "http/server"
require "./http_headers"
require "./http_request"
require "./http_server_context"
require "./http_server_response"
require "./openssl_ssl_socket"
require "../connection"
require "../server/http1"
require "../server/open_connection"

class HTTP::Server
  {% unless flag?(:without_openssl) %}
    def bind_tls(host : String, port : Int32, context : OpenSSL::SSL::Context::Server, reuse_port : Bool = false) : Socket::IPAddress
      context.alpn_protocol = "h2"
      previous_def(host, port, context, reuse_port)
    end
  {% end %}

  # How long `#close` lets the requests in progress finish before it closes
  # their connections anyway.
  property close_timeout : Time::Span = 10.seconds

  # Whether the server speaks HTTP/2 over cleartext connections (h2c): to a
  # client that starts with the HTTP/2 preface (prior knowledge), or that
  # asks for `Upgrade: h2c`. When false, the server speaks HTTP/1.1 only on
  # cleartext connections, as the stdlib's does: it ignores `Upgrade: h2c`,
  # and answers the preface as a request of an unsupported version. HTTP/2
  # over TLS, negotiated with ALPN, is not affected.
  property h2c : Bool = true

  @open_connections = Set(OpenConnection).new
  @open_connections_mutex = Sync::Mutex.new

  # The connections and HTTP/2 stream handlers in progress, for
  # `#wait_closed`: `@ended` is closed once the server is closed and none is
  # left. Guarded by `@open_connections_mutex`.
  @running = 0
  @ended = Channel(Nil).new
  {% if Time.class.has_method?(:instant) %}
    @close_deadline : Time::Instant? = nil
  {% else %}
    @close_deadline : Time::Span? = nil
  {% end %}

  def initialize(@handler : HTTP::Handler | HTTP::Handler::HandlerProc)
    # its size limits apply to the HTTP/1 connections
    @processor = RequestProcessor.new(@handler)
  end

  # Closes the server: it stops accepting connections, closes the idle ones
  # and lets the others finish the requests in progress, then closes them.
  # HTTP/2 connections get a GOAWAY frame. A connection accepted but not
  # served yet gets its first request served, which its client may have sent
  # already. Connections still open after `#close_timeout` are closed anyway.
  #
  # Returns at once: the connections end in their own fibers. Over TLS, the
  # connections are shut down from other fibers than the ones reading from
  # them, which `OpenSSL::SSL::Socket` only supports while they all run on
  # one thread.
  def close : Nil
    previous_def

    connections = @open_connections_mutex.synchronize do
      # the connections aborted after the close timeout may linger
      @close_deadline ||= now + close_timeout + OpenConnection::LINGER
      @ended.close if @running == 0
      @open_connections.dup
    end

    # started first: ending a connection may wait (see `OpenConnection#close`);
    # ends as soon as every connection ended
    unless connections.empty?
      HTTP2.spawn(name: "http2:server:close-timeout:#{object_id}") do
        select
        when @ended.receive?
        when timeout(close_timeout)
          @open_connections_mutex.synchronize { @open_connections.dup }.each(&.abort)
        end
      end
    end

    connections.each { |connection| HTTP2.spawn { connection.close } }
  end

  # Waits, after `#close`, until every connection the server accepted has
  # ended, and every handler of their requests returned. Returns true once
  # they all did, or false if some are still running when `#close_timeout`
  # (plus a second for the connections closed at its end) passed since
  # `#close`: a handler that doesn't return when its connection is closed.
  #
  # ```
  # server.close
  # server.wait_closed
  # ```
  #
  # Raises if the server isn't closed.
  def wait_closed : Bool
    deadline = @open_connections_mutex.synchronize { @close_deadline }
    raise "Can't wait for a server that isn't closed" unless deadline

    select
    when @ended.receive?
      true
    when timeout({deadline - now, Time::Span.zero}.max)
      false
    end
  end

  private def now
    {% if Time.class.has_method?(:instant) %}
      Time.instant
    {% else %}
      Time.monotonic
    {% end %}
  end

  # A connection or an HTTP/2 stream handler starts, for `#wait_closed`.
  private def running : Nil
    @open_connections_mutex.synchronize { @running += 1 }
  end

  # A connection or an HTTP/2 stream handler ended, for `#wait_closed`.
  private def ended : Nil
    @open_connections_mutex.synchronize do
      @running -= 1
      @ended.close if @running == 0 && @close_deadline
    end
  end

  # Registers the connection before its fiber starts: `#close` and
  # `#wait_closed` then know of it, and it's served even if the server
  # closes before the fiber runs.
  protected def dispatch(io)
    client = open_connection(io)
    HTTP2.spawn { handle_client(io, client) }
  end

  private def open_connection(io : IO) : OpenConnection
    connection = OpenConnection.new(io)
    closed = @open_connections_mutex.synchronize do
      @open_connections << connection
      @running += 1
      closed?
    end
    connection.close if closed
    connection
  end

  {% begin %}
  private def handle_client(io : IO)
    handle_client(io, open_connection(io))
  end

  private def handle_client(io : IO, client : OpenConnection)
    if io.is_a?(IO::Buffered)
      io.sync = false
    end

    scheme = "http"

    {% unless flag?(:without_openssl) %}
      if io.is_a?(OpenSSL::SSL::Socket::Server)
        begin
          io.accept
        rescue ex
          Log.debug(exception: ex) { "Error during SSL handshake" }
          return
        end

        if io.alpn_protocol == "h2"
          handle_http2_connection(client, io, alpn: "h2")
          return
        end

        scheme = "https"
      end
    {% end %}

    prefix = Bytes.empty
    if scheme == "http" && h2c
      # the HTTP/2 preface starts with a request line of its own (RFC 9113
      # section 3.4)
      line = io.gets('\n', max_request_line_size + 1, chomp: false) || ""
      prefix = line.to_slice
      case first_line(line)
      in .http2?
        handle_http2_connection(client, io)
        return
      in .invalid?
        # neither HTTP/1 nor HTTP/2: an invalid preface for an HTTP/2 client
        HTTP2::Connection.new(io, :server).close(error: HTTP2::Error.protocol_error("PREFACE expected"))
        return
      in .http1?
      end
    end

    serve_http1(client, io, prefix, scheme)
  rescue ex : IO::Error | ClientError {% unless flag?(:without_openssl) %} | OpenSSL::SSL::Error {% end %}
    # silence: the client is gone
  ensure
    begin
      {% if flag?(:h2spec) %}
        # FIXME: works around a bug in h2spec where the GOAWAY frame may
        # sometimes not be read *before* it notices that the IO is closed.
        sleep(100.milliseconds)
      {% end %}
      client.close_io
    rescue ex : IO::Error {% unless flag?(:without_openssl) %} | OpenSSL::SSL::Error {% end %}
      # silence
    end
    @open_connections_mutex.synchronize { @open_connections.delete(client) }
    ended
  end
  {% end %}

  private PREFACE_LINE = "PRI * HTTP/2.0\r\n"

  private enum FirstLine
    HTTP1
    HTTP2
    Invalid
  end

  # What the first line of a cleartext connection starts: the HTTP/2
  # preface, an HTTP/1 request, or neither. A line the stdlib's processor
  # answers anyway (too long, cut by EOF, or blank) counts as HTTP/1.
  private def first_line(line : String) : FirstLine
    return FirstLine::HTTP2 if line == PREFACE_LINE
    return FirstLine::HTTP1 unless line.ends_with?('\n')
    line = line.rstrip
    return FirstLine::HTTP1 if line.empty? || line.ends_with?(" HTTP/1.1") || line.ends_with?(" HTTP/1.0")
    FirstLine::Invalid
  end

  # Serves the HTTP/1 requests of a connection with the stdlib's
  # `RequestProcessor`, behind `HTTP1Handler`. *prefix* holds the bytes
  # already read. Doesn't close the IO.
  private def serve_http1(client : OpenConnection, io : IO, prefix : Bytes, scheme : String) : Nil
    http1 = HTTP1IO.new(io, client, prefix)
    processor = RequestProcessor.new(HTTP1Handler.new(self, client, http1, scheme))
    processor.max_request_line_size = max_request_line_size
    processor.max_headers_size = max_headers_size
    processor.process(http1, http1)
  rescue ArgumentError
    # the stdlib's parser refuses the request before any response, for
    # example for content lengths that differ (RFC 9112 section 6.3)
    io << "HTTP/1.1 400 Bad Request\r\nConnection: close\r\nContent-Length: 0\r\n\r\n"
  end

  # :nodoc:
  #
  # Calls the server's handler, for `HTTP1Handler`.
  protected def call_handler(context : Context) : Nil
    @handler.call(context)
  end

  # :nodoc:
  #
  # Serves an HTTP/1 request that asked for `Upgrade: h2c` as stream 1 of
  # an HTTP/2 connection, for `HTTP1Handler`.
  protected def serve_h2c_upgrade(client : OpenConnection, io : IO, request : HTTP::Request, settings : Bytes) : Nil
    handle_http2_connection(client, io, request, settings, alpn: "h2c")
  end

  private def handle_http2_connection(client, io, request = nil, settings = nil, alpn = nil) : Nil
    connection = HTTP2::Connection.new(io, HTTP2::Connection::Type::SERVER)

    if settings
      # HTTP/1 => HTTP/2 upgrade: we got settings
      connection.remote_settings.parse(settings) do |setting, value|
        Log.trace { "  #{setting}=#{value}" }
      end
    end

    connection.read_client_preface(truncated: alpn.nil?)
    connection.write_settings

    # the connection may get GOAWAY from now on, once the HTTP/1 => HTTP/2
    # upgrade opened its stream
    client.http2 = connection unless request

    frame = connection.receive
    unless frame.try(&.type) == HTTP2::Frame::Type::SETTINGS
      raise HTTP2::Error.protocol_error("Expected SETTINGS frame")
    end

    if request
      # HTTP/1 => HTTP/2 upgrade: reply to HTTP/1 request on stream #1 of HTTP/2
      stream = connection.upgrade_stream
      client.http2 = connection
      context = context_for(stream, request)
      set_addresses(context.request, client)
      running
      spawn_stream(client, connection, context)
    end

    loop do
      unless frame = connection.receive
        next
      end

      case frame.type
      when HTTP2::Frame::Type::HEADERS
        # don't dispatch twice
        next if frame.stream.trailers?

        # the connection refuses the streams opened after our GOAWAY
        client.stream_opened
        context = context_for(frame.stream)
        set_addresses(context.request, client)
        running
        spawn_stream(client, connection, context)
      end

      # the client won't open new streams, or a handler sent GOAWAY: keep
      # serving the ones it opened, then close the connection (RFC 9113
      # section 6.8)
      break if http2_drained?(client, connection)
    end
  rescue ex : HTTP2::Error
    if connection
      connection.close(error: ex) unless connection.closed?
    end
  rescue ex : HTTP::Server::ClientError | IO::Error
    # silence
  ensure
    if connection
      connection.close unless connection.closed?
    end
  end

  # Serves the stream in a fiber of its own, with arguments of its own: a
  # block in the connection's loop would see the next stream's context.
  private def spawn_stream(client, connection : HTTP2::Connection, context : Context) : Nil
    HTTP2.spawn { handle_stream(client, connection, context) }
  end

  # Serves a request on a stream of an HTTP/2 connection. The stream that
  # ends last after the client sent GOAWAY closes the connection: the
  # connection's fiber, that reads from the client, would only notice at the
  # next frame, that may never come.
  private def handle_stream(client, connection : HTTP2::Connection, context : Context) : Nil
    handle_request(context)
  ensure
    ended

    # the server may be closing and waiting for the stream to finish
    client.finish_if_done

    if http2_drained?(client, connection)
      # sends GOAWAY, then ends the connection gracefully: the client reads
      # everything and closes, which ends the connection's fiber
      connection.close
      client.finish
    end
  end

  # The client sent GOAWAY, or a handler did (`HTTP2::Connection#send_goaway`),
  # and no stream it opened is left. While the server closes, its own GOAWAY
  # doesn't count: the server ends the connection (see `OpenConnection`).
  private def http2_drained?(client, connection : HTTP2::Connection) : Bool
    goaway = connection.streams.goaway? || (!client.closing? && connection.goaway_sent_id)
    !!goaway && connection.streams.active_count(:client) == 0
  end

  private def set_addresses(request : HTTP::Request, client : OpenConnection) : Nil
    request.remote_address = client.remote_address
    request.local_address = client.local_address
  end

  private def context_for(stream : HTTP2::Stream, request = nil) : Context
    unless request
      headers = stream.headers
      body = decode_body(stream.data, headers) if stream.state.open?
      request = HTTP::Request.new(headers, body, "HTTP/2.0")
    end
    response = HTTP::Server::Response.new(stream)
    HTTP::Server::Context.new(request, response, stream)
  end

  # Serves the request of an HTTP/2 stream.
  private def handle_request(context : Context) : Nil
    Log.with_context { @handler.call(context) }
  rescue ex : ClientError
    Log.debug(exception: ex.cause) { ex.message }
  rescue ex
    Log.error(exception: ex) { "Unhandled exception on HTTP::Handler" }
    abort_response(context)
  ensure
    begin
      context.response.close
    rescue ex : HTTP::Server::ClientError | HTTP2::Error | IO::Error
      # silence
    end
  end

  # Responds 500 if the handler failed before the headers were sent, as the
  # stdlib does. Otherwise the response is incomplete: the stream is reset
  # with INTERNAL_ERROR instead of being ended (RFC 9113, sections 5.4.2 and
  # 8.1).
  private def abort_response(context : Context) : Nil
    response = context.response
    return if response.closed?

    if !response.headers_written?
      response.respond_with_status(:internal_server_error)
    else
      response.reset_stream(HTTP2::Error::Code::INTERNAL_ERROR)
    end
  rescue ClientError | HTTP2::Error | IO::Error
    # the client is gone
  end

  # :nodoc:
  #
  # Decodes a gzip or deflate HTTP/1 request body on the first read, as
  # over HTTP/2, for `HTTP1Handler`.
  protected def decode_request_body(request : HTTP::Request) : Nil
    if body = request.body
      request.body = decode_body(body, request.headers)
    end
  end

  private def decode_body(body, headers)
    return unless body

    {% if flag?(:without_zlib) %}
      case headers["content-encoding"]?
      when "gzip", "deflate"
        raise "Can't decompress because `-D without_zlib` was passed at compile time"
      end
    {% else %}
      encoding = headers["content-encoding"]?
      if encoding && encoding.in?("gzip", "deflate")
        body = DecodedBody.new(body, encoding)
        headers.delete("content-encoding")
        headers.delete("content-length")
      end
    {% end %}

    check_content_type_charset(body, headers)

    body
  end

  private def check_content_type_charset(body, headers)
    content_type = headers["content-type"]?
    return unless content_type

    mime_type = MIME::MediaType.parse?(content_type)
    return unless mime_type

    charset = mime_type["charset"]?
    return unless charset

    body.set_encoding(charset, invalid: :skip)
  end


  {% unless flag?(:without_zlib) %}
    # :nodoc:
    #
    # A request body decoded on the first read, in the handler's fiber: a
    # decoder reads the encoding's header right away, and over HTTP/2 the
    # fiber that builds the request is the one that receives the DATA
    # frames (RFC 9113 section 5.2.2).
    class DecodedBody < IO
      @reader : IO?
      @closed = false

      def initialize(@body : IO, @content_encoding : String)
      end

      def read(slice : Bytes) : Int32
        reader.read(slice)
      end

      def write(slice : Bytes) : NoReturn
        raise IO::Error.new("Can't write to a request body")
      end

      def close : Nil
        return if closed?
        @closed = true
        (@reader || @body).close
      end

      def closed? : Bool
        @closed
      end

      private def reader : IO
        @reader ||=
          if @content_encoding == "gzip"
            Compress::Gzip::Reader.new(@body, sync_close: true)
          else
            Compress::Deflate::Reader.new(@body, sync_close: true)
          end
      end
    end
  {% end %}
end
