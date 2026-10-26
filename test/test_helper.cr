require "minitest/autorun"

module AsyncTest
  @exception : Exception?

  def before_setup
    super
    @done = @exception = nil
  end

  def after_teardown
    @done = @exception = nil
    super
  end

  def wait
    loop do
      Fiber.yield
      break if @done
    end

    if exception = @exception
      raise exception
    end
  end

  def async(&block)
    @done = false

    spawn do
      begin
        block.call
      rescue ex
        @exception = ex
      ensure
        @done = true
      end
    end
  end
end

module HTTP2
  module TestHelpers
    def new_connection(type : Connection::Type, *frames)
      io = IO::Memory.new
      frames.each { |frame| write_frame(io, *frame) }
      io.rewind
      Connection.new(io, type)
    end

    def write_frame(io : IO, type : Frame::Type, flags : Frame::Flags, stream_id : Int, payload : Bytes? = nil)
      size = payload.try(&.size.to_u32) || 0_u32
      io.write_bytes((size << 8) | type.to_u8, IO::ByteFormat::BigEndian)
      io.write_byte(flags.to_u8)
      io.write_bytes(stream_id.to_u32, IO::ByteFormat::BigEndian)
      io.write(payload) if payload
    end

    {% if Fiber.has_constant?(:ExecutionContext) %}
      # One context for every test that needs it: a context can't be
      # stopped.
      private SINGLE_THREADED_CONTEXT =
        {% if Fiber::ExecutionContext.has_constant?(:Concurrent) %}
          Fiber::ExecutionContext::Concurrent.new("test:single-threaded")
        {% else %}
          Fiber::ExecutionContext::SingleThreaded.new("test:single-threaded")
        {% end %}

      # Runs the block in a fiber of a single-threaded execution context and
      # waits for it, at most *timeout*; re-raises what the block raised.
      #
      # A TLS `HTTP2::Client` must be used from a single-threaded execution
      # context: its fibers read from and write to an `OpenSSL::SSL::Socket`,
      # which doesn't support that from parallel threads. Minitest runs the
      # tests in a multi-threaded context.
      def single_threaded(timeout = 60.seconds, &block : ->) : Nil
        done = Channel(Exception?).new(1)
        SINGLE_THREADED_CONTEXT.spawn(name: "test:single-threaded") do
          block.call
          done.send(nil)
        rescue ex
          done.send(ex)
        end

        select
        when error = done.receive
          raise error if error
        when timeout(timeout)
          raise "single-threaded block timed out after #{timeout}"
        end
      end
    {% elsif flag?(:preview_mt) %}
      # Skips the test: the legacy `-Dpreview_mt` scheduler, without
      # execution contexts, runs fibers on several threads and can't confine
      # them to one.
      def single_threaded(timeout = 60.seconds, &) : Nil
        skip "a TLS client can't be confined to one thread under -Dpreview_mt without execution contexts"
      end
    {% else %}
      # Runs the block: without execution contexts and without
      # `-Dpreview_mt`, fibers run on one thread.
      def single_threaded(timeout = 60.seconds, &) : Nil
        yield
      end
    {% end %}
  end
end

class Minitest::Test
  include AsyncTest
  include HTTP2::TestHelpers
end
