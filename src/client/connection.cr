require "sync/condition_variable"
require "sync/mutex"
require "../connection"
require "./exchange"

module HTTP2
  # :nodoc:
  #
  # An HTTP/2 connection of an `HTTP::Client`: wraps `HTTP2::Connection` and
  # runs its fibers.
  #
  # - The reader fiber is the only one to call `HTTP2::Connection#receive`.
  #   It dispatches the response HEADERS, RST_STREAM and GOAWAY frames to the
  #   exchanges, and fails every exchange when the connection fails.
  # - A body-writer fiber per request with a body sends the body while the
  #   response is received: the server may answer, or reset the stream,
  #   before the upload ended (RFC 9113 section 8.1).
  # - A watchdog fiber, if a read or write timeout is set, cancels the
  #   exchanges that wait too long.
  #
  # The fibers are spawned in the current execution context.
  class ClientConnection
    # Exchanges by stream identifier, along with the identifier of the last
    # stream we opened and claimed: the reader may get the response for a
    # stream before the fiber that opened it claimed it.
    @exchanges = {} of Int32 => ClientExchange
    @claimed_id = 0

    # Counts the closed streams, so a request waiting for a stream slot
    # doesn't miss one.
    @released = 0_u64

    @closed = false
    @goaway_id : Int32? = nil
    @goaway_code = Error::Code::NO_ERROR
    @stop_watch : Channel(Nil)? = nil

    {% if Time.class.has_method?(:instant) %}
      @started = Time.instant
    {% else %}
      @started = Time.monotonic
    {% end %}

    def initialize(@io : IO, settings : Settings, *, @read_timeout : Time::Span? = nil, @write_timeout : Time::Span? = nil, @sync_close : Bool = false)
      @connection = HTTP2::Connection.new(@io, :client)
      @connection.local_settings.parse(settings.to_payload) { }

      @mutex = Sync::Mutex.new
      @slots = Sync::ConditionVariable.new(@mutex)
      @open = Sync::Mutex.new

      @connection.write_client_preface
      @connection.write_settings

      spawn(name: "http2:client:reader") { read_loop }

      if @read_timeout || @write_timeout
        stop = @stop_watch = Channel(Nil).new
        spawn(name: "http2:client:watchdog") { watch(stop) }
      end
    end

    # Returns true once the connection is closed or the server sent GOAWAY:
    # it won't accept new requests.
    def closed? : Bool
      @mutex.synchronize { @closed || !@goaway_id.nil? }
    end

    # Stops accepting new requests, as if the server sent GOAWAY for the
    # streams opened so far: they may complete, then the connection closes.
    # A request opening a stream meanwhile is refused, and may be retried.
    def retire : Nil
      @mutex.synchronize do
        @goaway_id ||= @claimed_id
        @slots.broadcast
      end
      close_if_drained
    end

    # Opens a stream for the request *headers* and starts sending *body*,
    # if any, in a body-writer fiber.
    #
    # At the server's maximum of concurrent streams, waits until a stream
    # closes. Raises `Error` with REFUSED_STREAM after GOAWAY (the request
    # wasn't sent), `IO::Error` once the connection is closed.
    def request(headers : HTTP::Headers, body : IO?) : ClientExchange
      flags = body ? Frame::Flags::None : Frame::Flags::END_STREAM

      # streams are claimed in the order they're opened: the reader knows
      # that a stream above the last claimed one is being claimed
      exchange = @open.synchronize { claim(open_stream(headers, flags)) }
      release(exchange)

      if body
        spawn(name: "http2:client:body") { write_body(exchange, body) }
      end
      exchange
    end

    private def open_stream(headers, flags) : Stream
      loop do
        released = @mutex.synchronize do
          check_open
          @released
        end

        begin
          return @connection.open_stream(headers, flags)
        rescue ex : Error
          raise ex unless ex.code.refused_stream?

          # the server's maximum of concurrent streams is reached, or the
          # server sent GOAWAY (#check_open raises then)
          @mutex.synchronize do
            while @released == released && !@closed && @goaway_id.nil?
              @slots.wait
            end
          end
        rescue ex : IO::Error
          close(ex)
          raise ex
        end
      end
    end

    private def claim(stream : Stream) : ClientExchange
      @mutex.synchronize do
        @claimed_id = stream.id
        exchange = @exchanges[stream.id] ||= ClientExchange.new(self, stream)

        # the server didn't process a stream above the last stream id of
        # its GOAWAY, even if the connection got closed meanwhile: the
        # request may be retried
        if (last_stream_id = @goaway_id) && stream.id > last_stream_id
          exchange.fail(refused_error(stream, last_stream_id))
        elsif @closed
          @exchanges.delete(stream.id)
          exchange.fail(IO::Error.new("Connection closed"))
        end

        exchange
      end
    end

    private def check_open : Nil
      raise IO::Error.new("Connection closed") if @closed
      if last_stream_id = @goaway_id
        raise Error.new(refused_code, last_stream_id, "GOAWAY received")
      end
    end

    # The size of the chunks a body writer reads from the request body:
    # a DATA frame of the smallest maximum frame size, whatever the
    # server allows, so a request in flight holds two buffers of 16 KiB.
    private BODY_CHUNK_SIZE = MINIMUM_FRAME_SIZE

    private def write_body(exchange : ClientExchange, body : IO) : Nil
      stream = exchange.stream
      buffer, next_buffer = Bytes.new(BODY_CHUNK_SIZE), Bytes.new(BODY_CHUNK_SIZE)

      return unless count = read_body(exchange, body, buffer)

      loop do
        # reads ahead, so the last DATA frame ends the stream
        return unless following = read_body(exchange, body, next_buffer)
        flags = following == 0 ? Frame::Flags::END_STREAM : Frame::Flags::None
        exchange.sending { stream.send_data(buffer[0, count], flags) }
        break if following == 0

        buffer, next_buffer = next_buffer, buffer
        count = following
      end
    rescue ex : IO::Error
      # write errors are fatal to the connection
      close(ex)
    rescue ex : Error
      # the stream is closed: reset by the server (that may have answered
      # in full), canceled or timed out
      Log.trace { "stream #{exchange.stream.id}: stopped sending the body: #{ex.message}" }
    ensure
      release(exchange)
    end

    # Reads the request body, canceling the stream if the body raises.
    private def read_body(exchange, body, buffer) : Int32?
      body.read(buffer)
    rescue ex
      exchange.cancel(ex)
      nil
    end

    private def read_loop : Nil
      until @mutex.synchronize { @closed }
        if frame = @connection.receive
          dispatch(frame)
        else
          check_closed_streams
        end
      end
    rescue ex
      close(ex)
    end

    private def dispatch(frame : Frame) : Nil
      stream = frame.stream

      case frame.type
      when Frame::Type::GOAWAY
        goaway(frame.last_stream_id, frame.error)
        return
      when Frame::Type::SETTINGS
        # the server's maximum of concurrent streams may have grown
        @mutex.synchronize do
          @released += 1
          @slots.broadcast
        end
        return
      end
      return unless stream.id.odd?

      exchange = @mutex.synchronize do
        @exchanges[stream.id]? ||
          (@exchanges[stream.id] = ClientExchange.new(self, stream) if stream.id > @claimed_id)
      end
      return unless exchange

      exchange.touch

      case frame.type
      when Frame::Type::HEADERS
        exchange.headers_received
      when Frame::Type::RST_STREAM
        exchange.reset(frame.error)
      end

      release(exchange)
    end

    # The connection resets and closes a stream, for example on a malformed
    # response, without returning a frame: fails the exchanges whose stream
    # got closed.
    private def check_closed_streams : Nil
      closed = @mutex.synchronize do
        @exchanges.values.select { |exchange| exchange.stream.id <= @claimed_id && exchange.stream.state.closed? }
      end
      closed.each do |exchange|
        exchange.fail(Error.protocol_error("Stream #{exchange.stream.id} was reset: malformed response"))
        release(exchange)
      end
    end

    # The server won't process the streams above *last_stream_id* (they
    # may be retried) nor accept new streams; the other streams may
    # complete (RFC 9113 section 6.8). With HTTP_1_1_REQUIRED, the refused
    # streams fail with that code rather than REFUSED_STREAM: they must not
    # be retried over HTTP/2.
    private def goaway(last_stream_id : Int32, code : Error::Code) : Nil
      Log.trace { "GOAWAY last_stream_id=#{last_stream_id} code=#{code}" }

      refused = @mutex.synchronize do
        @goaway_id = Math.min(last_stream_id, @goaway_id || last_stream_id)
        @goaway_code = code if code.http_1_1_required?
        @slots.broadcast
        @exchanges.values.select { |exchange| exchange.stream.id > last_stream_id }
      end
      refused.each do |exchange|
        exchange.fail(refused_error(exchange.stream, last_stream_id))
        release(exchange)
      end

      close_if_drained
    end

    private def refused_error(stream, last_stream_id) : Error
      Error.new(refused_code, last_stream_id, "Stream #{stream.id} refused: GOAWAY with last_stream_id=#{last_stream_id}")
    end

    private def refused_code : Error::Code
      @goaway_code.http_1_1_required? ? @goaway_code : Error::Code::REFUSED_STREAM
    end

    # Forgets the exchange once its stream is closed. Once the server sent
    # GOAWAY and the last exchange is forgotten, closes the connection.
    protected def release(exchange : ClientExchange) : Nil
      stream = exchange.stream
      return unless stream.state.closed?

      @mutex.synchronize do
        return unless stream.id <= @claimed_id && @exchanges[stream.id]?.same?(exchange)
        @exchanges.delete(stream.id)
        @released += 1
        @slots.broadcast
      end
      close_if_drained
    end

    # Closes the connection once the server sent GOAWAY, no exchange is
    # left, and no request is opening a stream: a stream opened but not
    # claimed yet, at or below the last stream id, may still complete.
    private def close_if_drained : Nil
      drained = @mutex.synchronize { !@goaway_id.nil? && @exchanges.empty? }
      close if drained && @connection.streams.active_count(HTTP2::Connection::Type::CLIENT) == 0
    end

    private def watch(stop : Channel(Nil)) : Nil
      timeouts = [@read_timeout, @write_timeout].compact
      period = (timeouts.min / 4).clamp(1.millisecond, 1.second)

      loop do
        select
        when stop.receive?
          return
        when timeout(period)
        end

        now = self.now
        exchanges = @mutex.synchronize { @exchanges.values }
        exchanges.each(&.check_timeouts(now, @read_timeout, @write_timeout))
      end
    end

    # Monotonic time since the connection started, in nanoseconds.
    protected def now : Int64
      {% if Time.class.has_method?(:instant) %}
        (Time.instant - @started).total_nanoseconds.to_i64
      {% else %}
        (Time.monotonic - @started).total_nanoseconds.to_i64
      {% end %}
    end

    # Closes the connection: sends GOAWAY unless the IO failed, fails the
    # exchanges in flight with *error* (or `IO::Error`), and closes the IO
    # if the connection owns it, whatever happens before. The GOAWAY frame
    # has the error's code for an `Error`, NO_ERROR without an error, and
    # INTERNAL_ERROR for an unexpected error.
    def close(error : Exception? = nil) : Nil
      exchanges = @mutex.synchronize do
        return if @closed
        @closed = true
        @slots.broadcast
        @exchanges.values.tap { @exchanges.clear }
      end
      Log.trace { "closing connection: #{error.try(&.message) || "NO_ERROR"}" }

      begin
        failure = error || IO::Error.new("Connection closed")
        exchanges.each(&.fail(failure))

        goaway_error =
          case error
          when Error, Nil, IO::Error
            error.as?(Error)
          else
            Error.internal_error(error.message || error.class.name)
          end
        @connection.close(error: goaway_error, notify: !error.is_a?(IO::Error))
      ensure
        @stop_watch.try(&.close)
        close_io if @sync_close
      end
    end

    private def close_io : Nil
      @io.close
    rescue IO::Error
      # already closed or failed
    end
  end
end
