require "sync/condition_variable"
require "sync/mutex"
require "./body"

module HTTP2
  # :nodoc:
  #
  # One request and its response on a stream of a `ClientConnection`.
  #
  # The reader fiber reports the response headers and failures; the
  # request fiber waits for the headers, then reads the body from the
  # stream (see `ClientBody`).
  class ClientExchange
    getter stream : Stream

    @headers_received = false
    @error : Exception? = nil

    # Read and write activity, in nanoseconds of `ClientConnection#now`, for the
    # watchdog: the number of fibers waiting to read (headers or body), the
    # last time the stream received a frame or a reader started waiting,
    # and the start of the current DATA send, if any.
    @readers = Atomic(Int32).new(0)
    @read_mark : Atomic(Int64)
    @write_mark = Atomic(Int64).new(-1)

    def initialize(@connection : ClientConnection, @stream : Stream)
      @mutex = Sync::Mutex.new
      @ready = Sync::ConditionVariable.new(@mutex)
      @read_mark = Atomic(Int64).new(connection.now)
    end

    # The failure of the exchange (the stream was reset, refused, canceled
    # or timed out, or the connection failed), if any.
    #
    # The stream knows that the peer reset it before the reader fiber
    # dispatched the RST_STREAM frame, and before a body reader woke up.
    def error : Exception?
      @mutex.synchronize do
        @error ||= received_reset_error
      end
    end

    # Returns true if the server didn't process the request, that may be
    # retried, for example on a new connection: the stream was reset with
    # REFUSED_STREAM or is above the last stream identifier of a GOAWAY
    # frame (RFC 9113 section 8.7).
    def refused? : Bool
      @mutex.synchronize do
        !@headers_received && (error = @error).is_a?(Error) && error.code.refused_stream?
      end
    end

    # Cancels the exchange: sends RST_STREAM(CANCEL) unless the stream is
    # closed, closes the response body (reads raise *error*, or an `Error`
    # with CANCEL), stops sending the request body and forgets the stream.
    def cancel(error : Exception? = nil) : Nil
      fail(error || Error.cancel("Stream #{stream.id} canceled"))

      unless stream.state.closed?
        begin
          stream.send_rst_stream(Error::Code::CANCEL)
        rescue IO::Error | Error
          # the connection failed: it closes the stream
        end
      end
      stream.abort
      @connection.release(self)
    end

    # Waits for the response headers. Raises the failure of the exchange
    # if it failed first.
    def wait_headers : Nil
      reading do
        @mutex.synchronize do
          until @headers_received || @error
            @ready.wait
          end
          if !@headers_received && (error = @error)
            raise error
          end
        end
      end
    end

    # The response body is closed: cancels the stream unless both sides
    # ended it.
    def close_body : Nil
      if stream.state.closed?
        @connection.release(self)
      else
        cancel
      end
    end

    protected def headers_received : Nil
      @mutex.synchronize do
        @headers_received = true
        @ready.broadcast
      end
    end

    protected def reset(code : Error::Code) : Nil
      fail(reset_error(code))
    end

    private def reset_error(code : Error::Code) : Error
      Error.new(code, 0, "Stream #{stream.id} reset by peer: #{code}")
    end

    private def received_reset_error : Error?
      stream.received_reset_code.try { |code| reset_error(code) }
    end

    # Fails the exchange, unless it failed already. A reset by the peer
    # that the stream received comes first, even if the reader fiber
    # didn't dispatch it yet when the connection fails.
    protected def fail(error : Exception) : Nil
      @mutex.synchronize do
        @error ||= received_reset_error || error
        @ready.broadcast
      end
    end

    # Marks the stream as active.
    protected def touch : Nil
      @read_mark.set(@connection.now)
    end

    protected def reading(&)
      touch
      @readers.add(1)
      begin
        yield
      ensure
        @readers.sub(1)
      end
    end

    protected def sending(&)
      @write_mark.set(@connection.now)
      begin
        yield
      ensure
        @write_mark.set(-1)
      end
    end

    # Called by the watchdog: cancels the exchange if a reader waits for
    # longer than *read_timeout* since the last frame on the stream, or if
    # sending DATA takes longer than *write_timeout* (waiting for the
    # flow-control window included).
    protected def check_timeouts(now : Int64, read_timeout : Time::Span?, write_timeout : Time::Span?) : Nil
      if read_timeout && @readers.get > 0 && now - @read_mark.get > read_timeout.total_nanoseconds
        cancel(IO::TimeoutError.new("Read timed out"))
      elsif write_timeout && (mark = @write_mark.get) >= 0 && now - mark > write_timeout.total_nanoseconds
        cancel(IO::TimeoutError.new("Write timed out"))
      end
    end
  end
end
