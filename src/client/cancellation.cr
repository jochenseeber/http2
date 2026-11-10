require "http/client"
require "sync/mutex"
require "../config"
require "../errors"

class HTTP::Client
  # Cancels the requests an `HTTP::Client` executes with it (see
  # `HTTP::Client#exec`), from any fiber.
  #
  # Under HTTP/2, canceling a request resets its stream (RST_STREAM with
  # CANCEL): the fiber waiting for the response headers, or reading the
  # response body, raises the cancellation's error, while the other
  # requests on the connection go on.
  #
  # ```
  # cancellation = HTTP::Client::Cancellation.new(timeout: 5.seconds)
  # client.exec(HTTP::Request.new("HEAD", "/"), cancellation: cancellation)
  # ```
  #
  # A request executed with a canceled cancellation raises at once. Under
  # HTTP/1, the request has the connection to itself: canceling it closes
  # the connection, and the next request connects again. A client on an
  # existing IO can't connect again.
  class Cancellation
    @error : Exception? = nil
    @callbacks = [] of Exception ->

    {% if Time.class.has_method?(:instant) %}
      @deadline : Time::Instant? = nil
    {% else %}
      @deadline : Time::Span? = nil
    {% end %}

    # The requests running with the cancellation, and the channel that
    # stops the timer fiber while it runs: the timer only runs while a
    # request does.
    @running = 0
    @stop_timer : Channel(Nil)? = nil

    @mutex = Sync::Mutex.new

    def initialize
    end

    # Creates a cancellation that cancels its requests with
    # `IO::TimeoutError` once *timeout* passed since its creation: a
    # deadline for the requests it's given to.
    def initialize(*, timeout : Time::Span)
      @deadline = now + timeout
    end

    # Cancels the requests, unless it was canceled already: they raise
    # *error*, or `HTTP2::Error` with CANCEL.
    def cancel(error : Exception? = nil) : Nil
      error ||= HTTP2::Error.cancel("Request canceled")
      callbacks = @mutex.synchronize do
        return if @error
        @error = error
        @callbacks.dup.tap { @callbacks.clear }
      end
      callbacks.each(&.call(error))
    end

    def canceled? : Bool
      !error.nil?
    end

    # The error the requests raise once canceled.
    def error : Exception?
      check_deadline
      @mutex.synchronize { @error }
    end

    # :nodoc:
    #
    # Raises the error if canceled.
    def check : Nil
      if error = self.error
        raise error
      end
    end

    # :nodoc:
    #
    # Calls *callback* with the error on cancel, or right away if canceled
    # already.
    def add(callback : Exception ->) : Nil
      check_deadline
      error = @mutex.synchronize do
        @callbacks << callback unless @error
        @error
      end
      callback.call(error) if error
    end

    # :nodoc:
    def remove(callback : Exception ->) : Nil
      @mutex.synchronize { @callbacks.delete(callback) }
    end

    # :nodoc:
    #
    # Runs a request: the timer runs while a request does, so no fiber
    # outlives the requests.
    def run(&)
      start_request
      begin
        yield
      ensure
        end_request
      end
    end

    # :nodoc:
    def timer_running? : Bool
      @mutex.synchronize { !@stop_timer.nil? }
    end

    private def start_request : Nil
      check_deadline
      timer = @mutex.synchronize do
        @running += 1
        deadline = @deadline
        next unless @running == 1 && deadline && @error.nil?
        {@stop_timer = Channel(Nil).new, deadline - now}
      end
      return unless timer
      stop, remaining = timer

      HTTP2.spawn(name: "http2:client:cancel-timer") do
        select
        when stop.receive?
        when timeout(remaining)
          cancel(timeout_error)
        end
      end
    end

    private def end_request : Nil
      stop = @mutex.synchronize do
        @running -= 1
        @stop_timer.tap { @stop_timer = nil } if @running == 0
      end
      stop.try(&.close)
    end

    # Cancels once the deadline passed, whether the timer runs or not.
    private def check_deadline : Nil
      if (deadline = @deadline) && now >= deadline
        cancel(timeout_error)
      end
    end

    private def timeout_error : IO::TimeoutError
      IO::TimeoutError.new("Request timed out")
    end

    private def now
      {% if Time.class.has_method?(:instant) %}
        Time.instant
      {% else %}
        Time.monotonic
      {% end %}
    end
  end
end
