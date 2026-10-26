{% unless flag?(:without_zlib) %}
  require "compress/gzip"
  require "compress/zlib"
{% end %}

module HTTP2
  # The body of a response, `HTTP::Client::Response#body_io`.
  #
  # Reads the DATA buffered by the stream; reading frees the stream's
  # flow-control window, so the server sends more as the body is read.
  # Returns EOF once the server ended the stream. Raises `Error` with the
  # error code if the server reset the stream, or the failure of the
  # exchange (connection closed, timeout).
  #
  # Closing the body before the server ended the stream cancels the stream
  # (RST_STREAM with CANCEL).
  class ClientBody < IO
    @content : IO?

    getter? closed = false

    # *content_encoding* is the content coding to decode (`gzip` or `deflate`),
    # if any.
    def initialize(@exchange : ClientExchange, @content_encoding : String? = nil)
    end

    def read(slice : Bytes) : Int32
      check_open
      begin
        @exchange.reading { content.read(slice) }
      rescue ex : IO::Error
        raise @exchange.error || ex
      end
    end

    def write(slice : Bytes) : Nil
      raise IO::Error.new("Can't write to a response body")
    end

    # The trailers that ended the response, if any. Available once the
    # body was read to the end.
    def trailers : HTTP::Headers?
      @exchange.stream.trailers?
    end

    def close : Nil
      return if closed?
      @closed = true
      @exchange.close_body
    end

    # The content reader is created on the first read: a decoder reads the
    # encoding's header right away.
    private def content : IO
      @content ||=
        {% if flag?(:without_zlib) %}
          @exchange.stream.data
        {% else %}
          case @content_encoding
          when "gzip", "x-gzip"
            Compress::Gzip::Reader.new(@exchange.stream.data)
          when "deflate"
            Compress::Zlib::Reader.new(@exchange.stream.data)
          else
            @exchange.stream.data
          end
        {% end %}
    end
  end
end
