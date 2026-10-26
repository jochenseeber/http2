require "./test_helper"
require "../src/server"

class HTTP::Server
  # exposes the private request dispatch to the tests
  def handle_request_for_test(context : Context) : Nil
    handle_request(context)
  end
end

class HTTP::Server::ResponseTest < Minitest::Test
  def test_writing_to_a_stream_the_client_reset_raises_client_error
    connection = new_reset_connection
    response = HTTP::Server::Response.new(receive_request(connection))
    response.print "hello"
    response.flush
    receive_reset(connection)

    response.print "world"
    error = assert_raises(HTTP::Server::ClientError) { response.flush }
    assert_instance_of HTTP2::Error, error.cause
    assert response.closed?
    response.close
  end

  def test_sending_headers_to_a_stream_the_client_reset_raises_client_error
    connection = new_reset_connection
    response = HTTP::Server::Response.new(receive_request(connection))
    receive_reset(connection)

    error = assert_raises(HTTP::Server::ClientError) { response.print "hello" }
    assert_instance_of HTTP2::Error, error.cause
    assert response.closed?
    response.close
  end

  def test_closing_a_stream_the_client_reset_raises_client_error
    connection = new_reset_connection
    response = HTTP::Server::Response.new(receive_request(connection))
    response.print "hello"
    response.flush
    receive_reset(connection)

    # nothing left to write: closing ends the stream
    error = assert_raises(HTTP::Server::ClientError) { response.close }
    assert_instance_of HTTP2::Error, error.cause
    assert response.closed?
  end

  def test_closing_the_output_keeps_the_client_error_of_its_flush
    connection = new_reset_connection
    response = HTTP::Server::Response.new(receive_request(connection))
    response.print "hello"
    response.flush
    receive_reset(connection)

    # a wrapping writer (e.g. `HTTP::CompressHandler`'s) closes the output,
    # which flushes the data it buffered
    output = response.output
    output.print "world"
    error = assert_raises(HTTP::Server::ClientError) { output.close }
    assert_equal "Error while writing data to the client", error.message
    assert_instance_of HTTP2::Error, error.cause
    assert response.closed?
  end

  def test_server_closes_the_response_to_a_stream_the_client_reset
    connection = new_reset_connection
    stream = receive_request(connection)
    receive_reset(connection)
    response = HTTP::Server::Response.new(stream)
    context = HTTP::Server::Context.new(HTTP::Request.new("GET", "/"), response, stream)

    # the handler wrote nothing: closing the response sends the headers
    HTTP::Server.new { |_| }.handle_request_for_test(context)
    assert response.closed?
  end

  # A server connection that will receive the HEADERS of a request on stream
  # 1, then RST_STREAM(CANCEL) on that stream.
  private def new_reset_connection : HTTP2::Connection
    headers = HTTP2::HPACK::Encoder.new.encode(HTTP::Headers{
      ":method"    => "GET",
      ":scheme"    => "http",
      ":authority" => "example.com",
      ":path"      => "/",
    })
    input = IO::Memory.new
    write_frame(input, HTTP2::Frame::Type::HEADERS, HTTP2::Frame::Flags::END_HEADERS | HTTP2::Frame::Flags::END_STREAM, 1, headers)
    write_frame(input, HTTP2::Frame::Type::RST_STREAM, HTTP2::Frame::Flags::None, 1, Bytes[0, 0, 0, HTTP2::Error::Code::CANCEL.value.to_u8])
    input.rewind
    HTTP2::Connection.new(IO::Stapled.new(input, IO::Memory.new), :server)
  end

  private def receive_request(connection) : HTTP2::Stream
    frame = connection.receive.not_nil!
    assert_equal HTTP2::Frame::Type::HEADERS, frame.type
    frame.stream
  end

  private def receive_reset(connection) : Nil
    frame = connection.receive.not_nil!
    assert_equal HTTP2::Frame::Type::RST_STREAM, frame.type
    assert frame.stream.state.closed?
  end
end
