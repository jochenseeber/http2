require "sync/mutex"
require "../connection"
require "../core_ext/openssl_ssl_socket"

class HTTP::Server
  # :nodoc:
  #
  # A connection the server accepted, so `HTTP::Server#close` can end it: at
  # once while it waits for a request, otherwise once the requests in
  # progress are done. A connection accepted before the server closed gets
  # its first request served: its client may have sent it already.
  #
  # The connection ends gracefully: the server shuts down its sending side
  # (FIN, after close_notify over TLS) and keeps reading until the client
  # closes too. Closing while the client still sends (for example
  # WINDOW_UPDATE frames while it reads the last response) would make the
  # kernel reset the connection, and the client would lose the data it
  # didn't read yet.
  class OpenConnection
    # How long the server waits for the client to close once it shut down its
    # sending side, before it closes anyway. Go's HTTP/2 server waits 1 second
    # for the client after its final GOAWAY (`goAwayTimeout`).
    LINGER = 1.second

    @mutex = Sync::Mutex.new
    @closing = false

    # the sending side was shut down
    @finished = false

    # the IO was closed, or shut down by `#abort`
    @io_closed = false

    # HTTP/1: waiting for the next request, not handling one
    @idle = false

    # No request was received yet, nor HTTP/2 started
    @fresh = true

    # The server closed before the connection got going: its first request
    # is served, for the client may have sent it already. An HTTP/2
    # connection gets GOAWAY once its first stream is open.
    @serve_first = false

    @http2 : HTTP2::Connection?
    @goaway_last_stream_id : Int32?

    # The addresses of the socket, under TLS too, for the requests'
    # `HTTP::Request#remote_address` and `#local_address`, if the IO is a
    # socket.
    getter remote_address : Socket::Address?
    getter local_address : Socket::Address?

    def initialize(@io : IO)
      io = raw_io
      begin
        @remote_address = io.remote_address if io.responds_to?(:remote_address)
        @local_address = io.local_address if io.responds_to?(:local_address)
      rescue Socket::Error
        # the socket is closed already
      end
    end

    def closing? : Bool
      @mutex.synchronize { @closing }
    end

    # HTTP/1: the connection waits for the next request. Returns false once
    # the server is closing: no further request may be served.
    def wait_for_request : Bool
      @mutex.synchronize do
        return false if @closing
        @idle = true
      end
    end

    # HTTP/1: a request arrived, the connection is busy until
    # `#wait_for_request`.
    def request_received : Nil
      @mutex.synchronize do
        @idle = false
        @fresh = false
      end
    end

    # HTTP/2: the server sent its SETTINGS, the connection may get GOAWAY.
    def http2=(connection : HTTP2::Connection) : Nil
      closing = @mutex.synchronize do
        @http2 = connection
        @closing && !@serve_first
      end
      go_away(connection) if closing
    end

    # HTTP/2: the client opened a stream. The first one is served even if
    # the server is closing: the connection gets its GOAWAY now, from a
    # fiber of its own (see `#close`).
    def stream_opened : Nil
      http2 = @mutex.synchronize do
        next unless @serve_first
        @serve_first = false
        @http2
      end
      HTTP2.spawn { go_away(http2) } if http2
    end

    # Ends the connection: at once if it's idle, otherwise once the requests
    # in progress are done. HTTP/2 sends GOAWAY first, which waits while
    # another frame is being written: call it from a fiber of its own.
    def close : Nil
      http2 = @mutex.synchronize do
        return if @closing
        @closing = true
        if @fresh && @http2.nil?
          @serve_first = true
          return
        end
        @http2
      end

      if http2
        go_away(http2)
      else
        finish_if_done
      end
    end

    private def go_away(http2 : HTTP2::Connection) : Nil
      last_stream_id = http2.send_goaway
      @mutex.synchronize { @goaway_last_stream_id = last_stream_id }
      finish_if_done
    rescue HTTP2::Error | IO::Error
      abort
    end

    # Ends the connection gracefully if it's closing and no request is in
    # progress.
    def finish_if_done : Nil
      finish if done?
    end

    private def done? : Bool
      @mutex.synchronize do
        next false unless @closing

        if http2 = @http2
          !@goaway_last_stream_id.nil? && http2.active_peer_streams == 0
        else
          @idle
        end
      end
    end

    {% begin %}
      # Shuts down the sending side, so the client reads everything then
      # closes the connection. Aborts the connection after `LINGER` if the
      # client doesn't.
      def finish : Nil
        return if @mutex.synchronize { @finished.tap { @finished = true } }

        if http2 = @mutex.synchronize { @http2 }
          http2.close_write { shutdown_write }
        else
          shutdown_write
        end

        HTTP2.spawn do
          sleep LINGER
          abort
        end
      rescue IO::Error {% unless flag?(:without_openssl) %} | OpenSSL::SSL::Error {% end %}
        abort
      end

      private def shutdown_write : Nil
        io = @io
        {% unless flag?(:without_openssl) %}
          if io.is_a?(OpenSSL::SSL::Socket)
            io.send_close_notify
            io = io.underlying_io
          end
        {% end %}
        io.flush
        @mutex.synchronize do
          next if @io_closed
          io.close_write if io.is_a?(Socket)
        end
      end

      # Ends the connection at once: shuts down the socket, which wakes the
      # fibers reading from it or waiting to write to it, without waiting
      # for them nor for the client. The connection's fiber closes the IO.
      def abort : Nil
        @mutex.synchronize do
          next if @io_closed

          io = raw_io
          if io.is_a?(Socket)
            io.close_read rescue nil
            io.close_write rescue nil
          else
            @io_closed = true
            io.close
          end
        end
      rescue IO::Error {% unless flag?(:without_openssl) %} | OpenSSL::SSL::Error {% end %}
        # silence
      end

      # Closes the IO once the connection ended. If the server is closing,
      # ends it gracefully first: reads until the client closes, bounded by
      # `#abort`.
      def close_io : Nil
        if closing?
          finish
          drain
        end
        return if @mutex.synchronize { @io_closed.tap { @io_closed = true } }
        @io.close
      rescue IO::Error {% unless flag?(:without_openssl) %} | OpenSSL::SSL::Error {% end %}
        # silence
      end

      private def drain : Nil
        io = raw_io
        buffer = Bytes.new(4096)
        loop do
          break if io.read(buffer) == 0
        end
      rescue IO::Error {% unless flag?(:without_openssl) %} | OpenSSL::SSL::Error {% end %}
        # the connection was reset or aborted
      end

      private def raw_io : IO
        {% if flag?(:without_openssl) %}
          @io
        {% else %}
          (io = @io).is_a?(OpenSSL::SSL::Socket) ? io.underlying_io : io
        {% end %}
      end
    {% end %}
  end
end
