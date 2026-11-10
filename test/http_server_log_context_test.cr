require "./test_helper"
require "../src/server"

class HTTP::Server
  # exposes the private request dispatch to the tests
  def handle_request_for_test(context : Context) : Nil
    handle_request(context)
  end
end

class HTTP::ServerLogContextTest < Minitest::Test
  # The requests of an HTTP/1 connection are served one after the other by the
  # fiber of the connection: the log context a handler sets for a request must
  # not be seen by the next one.
  def test_handler_log_context_is_reset_after_each_request
    seen = [] of ::Log::Metadata::Value?
    server = HTTP::Server.new do |context|
      seen << ::Log.context.metadata[:path]?
      ::Log.context.set(path: context.request.path)
    end

    ::Log.with_context do
      server.handle_request_for_test(new_http1_context("/first"))
      server.handle_request_for_test(new_http1_context("/second"))
      assert_equal [nil, nil], seen
      assert_nil ::Log.context.metadata[:path]?
    end
  end

  private def new_http1_context(path : String) : HTTP::Server::Context
    HTTP::Server::Context.new(HTTP::Request.new("GET", path), HTTP::Server::Response.new(IO::Memory.new))
  end
end
