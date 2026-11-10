require "http/server/context"

class HTTP::Server
  class Context
    # :nodoc:
    def initialize(@request : Request, @response : Response, @stream : HTTP2::Stream? = nil)
    end

    def trailers : Headers?
      @stream.try(&.trailers?)
    end

    # The HTTP/2 connection of the request, or `nil` for an HTTP/1 request.
    # For example, a handler may shut the connection down gracefully:
    #
    # ```
    # context.http2_connection.try(&.send_goaway)
    # ```
    def http2_connection : HTTP2::Connection?
      @stream.try(&.connection)
    end
  end
end
