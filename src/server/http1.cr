require "base64"
require "http/server/handler"
require "./open_connection"

class HTTP::Server
  # :nodoc:
  #
  # The IO the stdlib's `RequestProcessor` serves an HTTP/1 connection on.
  # It returns the bytes the server read first, to tell an HTTP/2 preface
  # apart, then reads from the connection, and tells the connection when it
  # waits for the next request: once the server is closing, it doesn't wait,
  # so the processor ends the connection.
  class HTTP1IO < IO
    # the first request is served, a request is in progress, or it was
    # upgraded: reads aren't the wait for the next request
    @between_requests = false
    @upgraded = false

    def initialize(@io : IO, @client : OpenConnection, @prefix : Bytes)
    end

    def read(slice : Bytes) : Int32
      return 0 if slice.empty?
      unless @prefix.empty?
        count = Math.min(slice.size, @prefix.size)
        slice.copy_from(@prefix[0, count])
        @prefix += count
        return count
      end
      return 0 unless next_request?
      @io.read(slice)
    end

    def peek : Bytes?
      return @prefix unless @prefix.empty?
      return Bytes.empty unless next_request?
      @io.peek
    end

    def write(slice : Bytes) : Nil
      @io.write(slice)
    end

    def flush : Nil
      @io.flush
    end

    def close : Nil
      @io.close
    end

    def closed? : Bool
      @io.closed?
    end

    def remote_address : Socket::Address?
      @client.remote_address
    end

    def local_address : Socket::Address?
      @client.local_address
    end

    # The processor calls the server's handler: a request arrived.
    protected def request_started : Nil
      @between_requests = false
      @client.request_received
    end

    # The handler returned: the next read waits for the next request.
    protected def request_ended : Nil
      @between_requests = true
    end

    # The connection speaks another protocol now.
    protected def upgraded : Nil
      @upgraded = true
    end

    # False once the server is closing while the connection waits for its
    # next request: the read ends the connection.
    private def next_request? : Bool
      return true if @upgraded || !@between_requests
      @client.wait_for_request
    end
  end

  # :nodoc:
  #
  # The handler the server puts in front of its own on an HTTP/1
  # connection, that the stdlib's `RequestProcessor` serves:
  #
  # - sets the request's addresses (the stdlib only does from Crystal 1.20)
  #   and scheme;
  # - answers 400 to a repeated `Host` (RFC 9112 section 3.2), to ambiguous
  #   framing (RFC 9112 section 6.3) and to field names that aren't tokens,
  #   which the stdlib serves;
  # - decodes a gzip or deflate request body, as over HTTP/2;
  # - upgrades the connection to HTTP/2 on `Upgrade: h2c`;
  # - cuts a response off when the handler fails after the headers were
  #   sent, so the client can tell it's incomplete.
  class HTTP1Handler
    include HTTP::Handler

    def initialize(@server : HTTP::Server, @client : OpenConnection, @io : HTTP1IO, @scheme : String)
    end

    def call(context : HTTP::Server::Context) : Nil
      @io.request_started
      request, response = context.request, context.response
      request.remote_address ||= @client.remote_address
      request.local_address ||= @client.local_address
      request.scheme = @scheme
      response.head_request = request.method == "HEAD"

      if malformed?(request.headers)
        respond_and_close(response, :bad_request)
        return
      end

      if @scheme == "http" && @server.h2c && (settings = h2c_settings(request.headers))
        upgrade(context, settings)
        return
      end

      @server.decode_request_body(request)
      begin
        @server.call_handler(context)
      rescue ex : ClientError
        raise ex
      rescue ex
        if response.headers_written?
          # the response can't end as if it were complete
          response.reset_stream(HTTP2::Error::Code::INTERNAL_ERROR)
        elsif !response.closed?
          respond_and_close(response, :internal_server_error)
        end
        # the stdlib logs it and ends the connection
        raise ex
      end
    ensure
      # an upgraded connection speaks another protocol: its reads aren't the
      # wait for the next request
      if context.try(&.response.upgrade_handler)
        @io.upgraded
      else
        @io.request_ended
      end
    end

    # Responds with *status*, as `Response#respond_with_status` does, saying
    # that the server closes the connection after it.
    private def respond_and_close(response : HTTP::Server::Response, status : HTTP::Status) : Nil
      response.reset
      response.status = status
      response.headers["Connection"] = "close"
      response.content_type = "text/plain"
      response << status.code << ' ' << status.description << '\n'
      response.close
    end

    private def malformed?(headers : HTTP::Headers) : Bool
      # the stdlib's parser only refuses them from Crystal 1.20
      headers.each { |name, _| return true unless HTTP::Headers.token?(name) }
      return true if (headers.get?("Host").try(&.size) || 0) > 1
      return false unless lengths = headers.get?("Content-Length")
      return true if headers.has_key?("Transfer-Encoding")
      lengths = lengths.flat_map(&.split(',')).map(&.strip).uniq!
      lengths.size != 1 || !lengths.first.each_char.all?(&.ascii_number?) || lengths.first.to_u64?.nil?
    end

    private def h2c_settings(headers : HTTP::Headers) : String?
      return unless headers["Upgrade"]? == "h2c"
      return unless settings = headers.get?("HTTP2-Settings")
      settings.first if settings.size == 1
    end

    # Answers `101 Switching Protocols`, then serves the request as stream 1
    # of an HTTP/2 connection on the IO (RFC 7540 section 3.2).
    private def upgrade(context : HTTP::Server::Context, settings : String) : Nil
      request, response = context.request, context.response
      response.status = :switching_protocols
      response.headers["Connection"] = "Upgrade"
      response.headers["Upgrade"] = "h2c"
      response.upgrade do |io|
        @io.upgraded
        @server.serve_h2c_upgrade(@client, io, request, Base64.decode(settings))
      end
    end
  end
end
