require "http/server/response"

class HTTP::Server
  class Response < IO
    # :nodoc:
    #
    # A response on an HTTP/2 stream: the status and headers are the
    # stdlib's fields, `HTTP2::Output` sends them as a header block.
    def initialize(stream : HTTP2::Stream)
      @io = HTTP2::Output::CLOSED_IO
      @version = "HTTP/2.0"
      @headers = Headers.new
      @status = :ok
      @wrote_headers = false
      output = HTTP2::Output.new(stream)
      @output = output
      @original_output = output
      output.response = self
    end

    # Sends the status and headers, then yields the connection's IO to the
    # block once the handler returned, as the stdlib does. An HTTP/2 stream
    # can't be upgraded: raises `NotImplementedError`.
    def upgrade(&block : IO ->) : Nil
      if @original_output.is_a?(HTTP2::Output)
        raise NotImplementedError.new("Can't upgrade HTTP/2 stream")
      end
      previous_def(&block)
    end

    # Ends the response at once, without completing it: what the handler
    # wrote but wasn't sent yet is discarded. An HTTP/2 stream is reset with
    # *code* (RST_STREAM), the other streams on the connection go on. An
    # HTTP/1 response is cut off and the connection closed, since HTTP/1
    # has no stream to reset; *code* is ignored.
    #
    # Does nothing if the response is closed already.
    #
    # ```
    # context.response.reset_stream(:protocol_error)
    # ```
    def reset_stream(code : HTTP2::Error::Code) : Nil
      case output = @original_output
      when HTTP2::Output
        output.reset_stream(code)
      else
        return if output.closed?
        output.abort
        # the server doesn't read another request on the connection
        @headers["Connection"] = "close"
      end
    end

    # Discards what was set before the headers were sent. A response on an
    # HTTP/2 stream serves one request: raises once its headers were sent.
    def reset : Nil
      if @original_output.is_a?(HTTP2::Output) && @wrote_headers
        raise IO::Error.new("Headers already sent")
      end
      previous_def
    end

    # :nodoc:
    def headers_written? : Bool
      @wrote_headers
    end

    # :nodoc:
    #
    # The response answers a HEAD request: it has no body, a flush doesn't
    # start a chunked one.
    property? head_request = false

    protected def write_headers
      if (output = @original_output).is_a?(HTTP2::Output)
        output.send_headers
        @wrote_headers = true
      else
        # a 1xx or 204 has no transfer-encoding (RFC 9112 section 6.1)
        @headers.delete("Transfer-Encoding") if @status.informational? || @status.no_content?
        previous_def
      end
    end

    class Output < IO
      # :nodoc:
      #
      # Closes the output without ending the response, for a response cut
      # off: what is still buffered is discarded, and a chunked body doesn't
      # get its last chunk, so the client can tell that it's incomplete (RFC
      # 9112, section 7.1). The connection must be closed afterwards.
      def abort : Nil
        @closed = true
      end

      # A 1xx, 204 or 304 has no body (RFC 9110 sections 15.3.5 and
      # 15.4.5): what the handler writes is dropped, without chunked
      # framing, which a keep-alive client would read as the next response.
      private def unbuffered_write(slice : Bytes) : Nil
        status = response.status
        if status.informational? || status.no_content? || status.not_modified?
          ensure_headers_written
          return
        end
        previous_def
      end

      # A flush before the first byte of the body sends the status and the
      # headers, as over HTTP/2, so a handler can start a stream the client
      # waits on. The body that follows is chunked, unless it has a
      # `Content-Length`; over HTTP/1.0 it ends with the connection.
      private def unbuffered_flush : Nil
        unless response.headers_written?
          start_body unless bodiless?
          ensure_headers_written
        end
        previous_def
      end

      # Frames the body as the stdlib does on its first write.
      private def start_body : Nil
        headers = response.headers
        if headers["Transfer-Encoding"]? == "chunked"
          @chunked = true
        elsif headers.has_key?("Content-Length")
        elsif response.version == "HTTP/1.0"
          headers["Connection"] = "close"
        else
          headers["Transfer-Encoding"] = "chunked"
          @chunked = true
        end
      end

      private def bodiless? : Bool
        status = response.status
        response.head_request? || status.informational? || status.no_content? || status.not_modified?
      end
    end
  end
end

# :nodoc:
#
# The output of a response on an HTTP/2 stream: DATA frames, after a HEADERS
# frame built from the response's status and headers.
class HTTP2::Output < HTTP::Server::Response::Output
  # Stands for the stdlib's IO, that HTTP/2 doesn't write to: a real object,
  # so `#inspect` works, and closed, so a stdlib method that would still use
  # it raises.
  CLOSED_IO = IO::Memory.new.tap(&.close)

  def initialize(@stream : HTTP2::Stream)
    super(CLOSED_IO)
  end

  # Sends the status and the headers. HTTP/2 has no connection-specific
  # fields (RFC 9113 section 8.2.2): a handler may set them for HTTP/1.
  def send_headers : Nil
    headers = HTTP::Headers{":status" => response.status.code.to_s}
    response.headers.each do |name, values|
      name = name.downcase
      next if HTTP2::CONNECTION_SPECIFIC_FIELDS.includes?(name)
      next if name == "te" && values != ["trailers"]
      headers.add(name, values)
    end
    @stream.send_headers(headers)
  rescue ex : IO::Error | HTTP2::Error
    client_error("Error while writing headers to the client", ex)
  end

  def close : Nil
    return if closed?

    unless response.headers_written?
      status = response.status

      # determine if the `content-length` header should be added.
      # see https://tools.ietf.org/html/rfc7230#section-3.3.2.
      if !response.headers.has_key?("transfer-encoding") &&
          !response.headers.has_key?("content-length") &&
          !(status.not_modified? || status.no_content? || status.informational?)
        response.headers["content-length"] = @out_count.to_s
      end
    end

    # can't call super because it would call Output#close, we inline
    # IO::Buffered#close instead
    flush if @out_count > 0
    ensure_headers_written
  ensure
    # closed already, or by a failed flush: keep its error
    unbuffered_close unless closed?
  end

  # Discards what is still buffered and resets the stream, unless it's
  # closed already.
  def reset_stream(code : HTTP2::Error::Code) : Nil
    return if closed?
    @closed = true
    @stream.send_rst_stream(code) unless @stream.state.closed?
  rescue IO::Error | HTTP2::Error
    # the connection is gone
  end

  private def unbuffered_write(slice : Bytes) : Nil
    return if slice.empty?

    ensure_headers_written
    @stream.send_data(slice)
  rescue ex : IO::Error | HTTP2::Error
    client_error("Error while writing data to the client", ex)
  end

  # Sends the headers on the first flush that reaches the client, even when
  # nothing was written.
  private def unbuffered_flush : Nil
    ensure_headers_written
  end

  private def unbuffered_close : Nil
    @closed = true
    @stream.send_data("", flags: HTTP2::Frame::Flags::END_STREAM)
    @stream.send_rst_stream(HTTP2::Error::Code::NO_ERROR)
  rescue ex : IO::Error | HTTP2::Error
    client_error("Error while closing the stream to the client", ex)
  end

  # The stream is gone, reset by the client or closed with the connection:
  # there's nothing to end, the handler gets the same error as for HTTP/1.
  private def client_error(message, cause) : NoReturn
    @closed = true
    raise HTTP::Server::ClientError.new(message, cause)
  end
end
