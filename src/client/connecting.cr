require "sync/condition_variable"
require "sync/mutex"
require "./connection"

module HTTP2
  # :nodoc:
  #
  # A connect of an `HTTP::Client` in progress. The fiber that connects
  # reports the result to the requests that wait for it: an HTTP/2
  # connection, or the IO of an HTTP/1 connection. `HTTP::Client#close` and
  # the connect timer cancel it.
  class ClientConnecting
    alias Result = ClientConnection | IO

    @result : Result | Exception | Nil = nil
    @socket : IO? = nil
    @canceled = false

    # Closed once the connect ended or was canceled, so the connect timer
    # stops.
    getter ended = Channel(Nil).new

    def initialize
      @mutex = Sync::Mutex.new
      @done = Sync::ConditionVariable.new(@mutex)
    end

    def canceled? : Bool
      @mutex.synchronize { @canceled }
    end

    # The connected socket, that `#cancel` closes. Closes it and raises if
    # the connect was canceled already.
    def socket=(socket : IO) : Nil
      canceled = @mutex.synchronize do
        @socket = socket unless @canceled
        @canceled
      end
      return unless canceled

      socket.close
      raise IO::Error.new("Connect canceled")
    end

    # Cancels the connect, unless it ended: the waiters fail with *error*
    # right away. Closes the socket if it's connected; the fiber that
    # connects closes it otherwise, when the TCP connect returns.
    def cancel(error : Exception) : Nil
      socket = @mutex.synchronize do
        return if @result
        @canceled = true
        @socket
      end
      finish(error)
      begin
        socket.try(&.close)
      rescue IO::Error
        # already closed
      end
    end

    # Reports the result or the failure to the waiters, unless the connect
    # was canceled first.
    def finish(result : Result | Exception) : Nil
      @mutex.synchronize do
        return if @result
        @result = result
        @done.broadcast
      end
      @ended.close
    end

    # Waits for the connect. Raises its failure.
    def wait : Result
      result = @mutex.synchronize do
        until value = @result
          @done.wait
        end
        value
      end
      raise result if result.is_a?(Exception)
      result
    end
  end
end
