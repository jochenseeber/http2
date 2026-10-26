require "./test_helper"
require "../src/client"
require "compress/gzip"

class HTTP2::ClientTest < Minitest::Test
  def test_sends_request_headers
    with_client do |client, peer|
      headers = HTTP::Headers{
        "Host"              => "example.org:8443",
        "X-Custom"          => "value",
        "Connection"        => "keep-alive",
        "Keep-Alive"        => "timeout=5",
        "Proxy-Connection"  => "keep-alive",
        "Transfer-Encoding" => "chunked",
        "Upgrade"           => "h2c",
        "TE"                => "trailers",
      }
      response = Future.new { client.get("/path?q=1", headers) }
      stream = peer.accept_request

      assert_equal "GET", stream.headers[":method"]
      assert_equal "http", stream.headers[":scheme"]
      assert_equal "example.org:8443", stream.headers[":authority"]
      assert_equal "/path?q=1", stream.headers[":path"]
      assert_equal "value", stream.headers["x-custom"]
      assert_equal "trailers", stream.headers["te"]
      %w(host connection keep-alive proxy-connection transfer-encoding upgrade).each do |name|
        refute stream.headers.has_key?(name), "#{name} header was sent"
      end
      assert stream.state.half_closed_remote?, "request without body must end the stream"

      stream.send_headers(HTTP::Headers{":status" => "204"}, Frame::Flags::END_STREAM)
      assert_equal 204, response.get.status_code
    end
  end

  def test_authority_defaults_to_client_host_and_port
    with_client(host: "example.com", port: 8080) do |client, peer|
      response = Future.new { client.get("/") }
      stream = peer.accept_request
      assert_equal "example.com:8080", stream.headers[":authority"]
      stream.send_headers(HTTP::Headers{":status" => "204"}, Frame::Flags::END_STREAM)
      response.get
    end
  end

  def test_sends_te_only_as_trailers
    with_client do |client, peer|
      response = Future.new { client.get("/", HTTP::Headers{"TE" => "gzip"}) }
      stream = peer.accept_request
      refute stream.headers.has_key?("te")
      stream.send_headers(HTTP::Headers{":status" => "204"}, Frame::Flags::END_STREAM)
      response.get
    end
  end

  def test_returns_buffered_response
    with_client do |client, peer|
      response = Future.new { client.get("/") }
      stream = peer.accept_request
      stream.send_headers(HTTP::Headers{":status" => "201", "content-type" => "text/plain", "x-reply" => "yes"})
      stream.send_data("hello ")
      stream.send_data("world", Frame::Flags::END_STREAM)

      response = response.get
      assert_equal HTTP::Status::CREATED, response.status
      assert_equal "HTTP/2", response.version
      assert_equal "hello world", response.body
      assert_equal "text/plain", response.headers["content-type"]
      assert_equal "yes", response.headers["x-reply"]
      refute response.headers.has_key?(":status")
    end
  end

  def test_sends_body_of_known_size_with_content_length
    body = "x" * 40_000
    with_client do |client, peer|
      response = Future.new { client.post("/upload", body: body) }
      stream = peer.accept_request
      frames = peer.receive_until { |frame| frame.stream == stream && frame.flags.end_stream? }

      assert_equal body.bytesize.to_s, stream.headers["content-length"]
      sizes = frames.select { |frame| frame.type.data? && frame.stream == stream }.map(&.size)
      assert sizes.all? { |size| size <= DEFAULT_MAX_FRAME_SIZE }, "DATA frames larger than max_frame_size: #{sizes}"
      assert_equal body, stream.data.gets_to_end

      stream.send_headers(HTTP::Headers{":status" => "204"}, Frame::Flags::END_STREAM)
      assert_equal 204, response.get.status_code
    end
  end

  def test_sends_body_of_unknown_size_without_content_length
    body = "y" * 20_000
    with_client do |client, peer|
      response = Future.new { client.post("/upload", body: IO::Sized.new(IO::Memory.new(body), body.bytesize)) }
      stream = peer.accept_request
      peer.receive_until { |frame| frame.stream == stream && frame.flags.end_stream? }

      refute stream.headers.has_key?("content-length")
      assert_equal body, stream.data.gets_to_end

      stream.send_headers(HTTP::Headers{":status" => "204"}, Frame::Flags::END_STREAM)
      assert_equal 204, response.get.status_code
    end
  end

  def test_sends_empty_body_with_end_stream_on_headers
    with_client do |client, peer|
      response = Future.new { client.post("/upload", body: "") }
      stream = peer.accept_request
      assert peer.last_headers.flags.end_stream?, "HEADERS must end the stream"
      stream.send_headers(HTTP::Headers{":status" => "204"}, Frame::Flags::END_STREAM)
      response.get
    end
  end

  def test_streams_response_body
    with_client do |client, peer|
      first_line = Channel(String?).new(1)
      response = Future.new do
        client.get("/stream") do |response|
          first_line.send(response.body_io.gets)
          response.body_io.gets_to_end
        end
      end
      stream = peer.accept_request
      stream.send_headers(HTTP::Headers{":status" => "200"})
      stream.send_data("first\n")

      # the block reads the first line before the response ended
      assert_equal "first", receive_within(first_line)
      stream.send_data("second\n", Frame::Flags::END_STREAM)
      assert_equal "second\n", response.get
    end
  end

  def test_exposes_trailers_on_the_body
    with_client do |client, peer|
      response = Future.new do
        client.get("/") do |response|
          body = response.body_io
          {body.gets_to_end, body.as(ClientBody).trailers}
        end
      end
      stream = peer.accept_request
      stream.send_headers(HTTP::Headers{":status" => "200"})
      stream.send_data("content")
      stream.send_headers(HTTP::Headers{"x-checksum" => "abc"}, Frame::Flags::END_STREAM)

      content, trailers = response.get
      assert_equal "content", content
      assert_equal "abc", trailers.try(&.["x-checksum"])
    end
  end

  def test_skips_interim_responses
    with_client do |client, peer|
      response = Future.new { client.get("/") }
      stream = peer.accept_request
      stream.send_headers(HTTP::Headers{":status" => "103", "link" => "</style.css>; rel=preload"})
      stream.send_headers(HTTP::Headers{":status" => "200"})
      stream.send_data("ok", Frame::Flags::END_STREAM)

      response = response.get
      assert_equal 200, response.status_code
      assert_equal "ok", response.body
      refute response.headers.has_key?("link")
    end
  end

  def test_response_to_head_request_has_empty_body
    with_client do |client, peer|
      response = Future.new { client.head("/") }
      stream = peer.accept_request
      stream.send_headers(HTTP::Headers{":status" => "200", "content-length" => "1234"}, Frame::Flags::END_STREAM)

      response = response.get
      assert_equal "", response.body
      assert_equal "1234", response.headers["content-length"]
    end
  end

  def test_response_without_content_has_no_body
    with_client do |client, peer|
      response = Future.new { client.get("/") { |response| response.body_io? } }
      stream = peer.accept_request
      stream.send_headers(HTTP::Headers{":status" => "304"}, Frame::Flags::END_STREAM)
      assert_nil response.get
    end
  end

  def test_response_to_head_request_ignores_its_content_encoding
    with_client do |client, peer|
      response = Future.new { client.head("/") }
      stream = peer.accept_request
      stream.send_headers(HTTP::Headers{
        ":status"          => "200",
        "content-encoding" => "deflate",
        "content-length"   => "1234",
      }, Frame::Flags::END_STREAM)

      response = response.get
      assert_equal "", response.body
      assert_equal "deflate", response.headers["content-encoding"]
      assert_equal "1234", response.headers["content-length"]
    end
  end

  def test_close_doesnt_wait_for_a_data_frame_being_received
    with_client do |client, peer|
      response = Future.new { client.get("/") { |response| response.body_io.gets_to_end } }
      stream = peer.accept_request
      stream.send_headers(HTTP::Headers{":status" => "200"})
      send_half_a_data_frame(peer, stream)

      Future.new { client.close }.get(1.second)
      assert_kind_of IO::Error, response.error(1.second)
    end
  end

  def test_read_timeout_while_a_data_frame_is_being_received
    with_client do |client, peer|
      client.read_timeout = 50.milliseconds
      response = Future.new { client.get("/") { |response| response.body_io.gets_to_end } }
      stream = peer.accept_request
      stream.send_headers(HTTP::Headers{":status" => "200"})
      send_half_a_data_frame(peer, stream)

      assert_kind_of IO::TimeoutError, response.error(1.second)
    end
  end

  def test_close_closes_the_io_when_writing_goaway_fails
    with_client do |client, peer, io|
      client.read_timeout = 1.second # with a watchdog fiber
      complete_request(client, peer)

      # the IO fails with an error that isn't an IO::Error, as a TLS socket
      # raises OpenSSL::SSL::Error; a connection error (DATA on stream 0)
      # makes the client send GOAWAY
      io.write_error = NotAnIOError.new("bad record mac")
      peer.send_frame(Frame::Type::DATA, Frame::Flags::None, 0, "x".to_slice)

      assert wait_until { io.closed? }, "the client didn't close its IO"
    end
  end

  def test_unexpected_reader_error_sends_goaway_with_internal_error
    with_client do |client, peer, io|
      complete_request(client, peer)

      io.read_error = NotAnIOError.new("unexpected")
      peer.send_frame(Frame::Type::PING, Frame::Flags::None, 0, Bytes.new(8))

      frames = peer.receive_until(&.type.goaway?)
      assert_equal Error::Code::INTERNAL_ERROR, frames.last.error
    end
  end

  def test_goaway_while_opening_a_stream_above_last_stream_id_refuses_it
    with_client do |client, peer, io|
      complete_request(client, peer)
      io.hold_next_headers
      response = Future.new { client.get("/second") }
      # stream 3 is open; its HEADERS frame isn't written yet
      receive_within(io.held)

      # the client closes the connection, as no request is in flight
      peer.send_goaway(last_stream_id: 1)
      assert wait_until { client.closed? }
      sleep 50.milliseconds
      io.release_headers

      error = response.error.as(Error)
      assert_equal Error::Code::REFUSED_STREAM, error.code
    end
  end

  def test_goaway_while_opening_a_stream_at_last_stream_id_lets_it_complete
    with_client do |client, peer, io|
      complete_request(client, peer)
      io.hold_next_headers
      response = Future.new { client.get("/second") }
      # stream 3 is open; its HEADERS frame isn't written yet
      receive_within(io.held)

      peer.send_goaway(last_stream_id: 3)
      assert wait_until { client.closed? }
      sleep 50.milliseconds
      io.release_headers

      stream = peer.accept_request
      assert_equal 3, stream.id
      stream.send_headers(HTTP::Headers{":status" => "200"})
      stream.send_data("second", Frame::Flags::END_STREAM)
      assert_equal "second", response.get.body
    end
  end

  def test_upload_chunks_dont_grow_with_the_server_max_frame_size
    with_client do |client, peer|
      peer.connection.local_settings.max_frame_size = 1 << 20
      complete_request(client, peer)

      Future.new { client.post("/", body: "z" * 100_000) }
      peer.accept_request
      frames = peer.receive_until(&.type.data?)
      assert_equal 16384, frames.last.size
    end
  end

  def test_reset_before_response_raises
    with_client do |client, peer|
      response = Future.new { client.get("/") }
      stream = peer.accept_request
      stream.send_rst_stream(Error::Code::INTERNAL_ERROR)

      error = response.error.as(Error)
      assert_equal Error::Code::INTERNAL_ERROR, error.code
    end
  end

  def test_reset_during_response_body_raises_on_read
    with_client do |client, peer|
      got_headers = Channel(Nil).new(1)
      response = Future.new do
        client.get("/") do |response|
          got_headers.send(nil)
          response.body_io.gets_to_end
        end
      end
      stream = peer.accept_request
      stream.send_headers(HTTP::Headers{":status" => "200"})
      stream.send_data("partial")
      receive_within(got_headers)
      stream.send_rst_stream(Error::Code::INTERNAL_ERROR)

      error = response.error.as(Error)
      assert_equal Error::Code::INTERNAL_ERROR, error.code
    end
  end

  def test_reset_raises_its_code_even_if_the_reader_wakes_first
    gate = ResetGate.new
    client_io, server_io = IO::Stapled.pipe
    peer = Peer.new(server_io)
    client = GatedClient.new(client_io, gate, host: "example.com")

    got_headers = Channel(Nil).new(1)
    response = Future.new do
      client.get("/") do |response|
        got_headers.send(nil)
        response.body_io.gets_to_end
      end
    end
    stream = peer.accept_request
    stream.send_headers(HTTP::Headers{":status" => "200"})
    stream.send_data("partial")
    receive_within(got_headers)
    stream.send_rst_stream(Error::Code::INTERNAL_ERROR)

    # the connection closed the body when it received the RST_STREAM frame:
    # the body reader wakes while the client holds the frame, before it
    # dispatched it, and must still raise the code
    receive_within(gate.received)
    error = response.error
    gate.release.send(nil)

    assert_instance_of Error, error
    assert_equal Error::Code::INTERNAL_ERROR, error.as(Error).code
  ensure
    gate.try(&.release.close)
    client.try(&.close)
    server_io.try(&.close)
  end

  def test_reset_keeps_its_code_when_the_connection_closes_before_the_dispatch
    gate = ResetGate.new
    client_io, server_io = IO::Stapled.pipe
    peer = Peer.new(server_io)
    client = GatedClient.new(client_io, gate, host: "example.com")

    got_headers = Channel(Nil).new(1)
    read_body = Channel(Nil).new(1)
    response = Future.new do
      client.get("/") do |response|
        got_headers.send(nil)
        read_body.receive
        response.body_io.gets_to_end
      end
    end
    stream = peer.accept_request
    stream.send_headers(HTTP::Headers{":status" => "200"})
    receive_within(got_headers)
    stream.send_rst_stream(Error::Code::INTERNAL_ERROR)

    # the client received the RST_STREAM frame but didn't dispatch it yet
    # when the connection closes: the body still raises the code
    receive_within(gate.received)
    client.close
    read_body.send(nil)
    error = response.error
    gate.release.send(nil)

    assert_instance_of Error, error
    assert_equal Error::Code::INTERNAL_ERROR, error.as(Error).code
  ensure
    gate.try(&.release.close)
    client.try(&.close)
    server_io.try(&.close)
  end

  def test_leaving_the_block_early_cancels_the_stream
    with_client do |client, peer|
      response = Future.new { client.get("/") { |response| response.status_code } }
      stream = peer.accept_request
      stream.send_headers(HTTP::Headers{":status" => "200"})
      stream.send_data("more to come")

      assert_equal 200, response.get
      frames = peer.receive_until(&.type.rst_stream?)
      assert_equal({stream.id, Error::Code::CANCEL}, {frames.last.stream.id, frames.last.error})
    end
  end

  def test_closing_the_body_cancels_the_stream
    with_client do |client, peer|
      closed = Channel(Nil).new(1)
      release = Channel(Nil).new(1)
      response = Future.new do
        client.get("/") do |response|
          response.body_io.close
          closed.send(nil)
          receive_within(release)
          response.body_io.closed?
        end
      end
      stream = peer.accept_request
      stream.send_headers(HTTP::Headers{":status" => "200"})

      receive_within(closed)
      frames = peer.receive_until(&.type.rst_stream?)
      assert_equal({stream.id, Error::Code::CANCEL}, {frames.last.stream.id, frames.last.error})
      release.send(nil)
      assert response.get
    end
  end

  def test_canceling_a_stream_keeps_the_connection
    with_client do |client, peer|
      first = Future.new { client.get("/first") { } }
      stream = peer.accept_request
      stream.send_headers(HTTP::Headers{":status" => "200"})
      first.get
      peer.receive_until(&.type.rst_stream?)

      # DATA the server sent before it received the RST_STREAM
      peer.send_frame(Frame::Type::DATA, Frame::Flags::None, stream.id, "late".to_slice)

      second = Future.new { client.get("/second") }
      stream = peer.accept_request
      assert_equal "/second", stream.headers[":path"]
      stream.send_headers(HTTP::Headers{":status" => "200"})
      stream.send_data("ok", Frame::Flags::END_STREAM)
      assert_equal "ok", second.get.body
    end
  end

  def test_goaway_refuses_streams_above_last_stream_id
    with_client do |client, peer|
      first = Future.new { client.get("/first") }
      stream1 = peer.accept_request
      second = Future.new { client.get("/second") }
      stream3 = peer.accept_request
      assert_equal 3, stream3.id

      peer.send_goaway(last_stream_id: 1)
      error = second.error.as(Error)
      assert_equal Error::Code::REFUSED_STREAM, error.code
      assert client.closed?

      # the stream at last_stream_id completes
      stream1.send_headers(HTTP::Headers{":status" => "200"})
      stream1.send_data("first", Frame::Flags::END_STREAM)
      assert_equal "first", first.get.body

      # a client on an IO can't reconnect
      assert_raises(IO::Error) { client.get("/third") }
    end
  end

  def test_connection_failure_fails_requests_in_flight
    with_client do |client, peer|
      got_headers = Channel(Nil).new(1)
      streaming = Future.new do
        client.get("/streaming") do |response|
          got_headers.send(nil)
          response.body_io.gets_to_end
        end
      end
      stream = peer.accept_request
      stream.send_headers(HTTP::Headers{":status" => "200"})
      receive_within(got_headers)
      waiting = Future.new { client.get("/waiting") }
      peer.accept_request

      peer.io.close
      assert_kind_of IO::Error, waiting.error
      assert_kind_of IO::Error, streaming.error
      assert client.closed?
    end
  end

  def test_read_timeout_while_waiting_for_the_response
    with_client do |client, peer|
      client.read_timeout = 50.milliseconds
      response = Future.new { client.get("/") }
      stream = peer.accept_request

      assert_kind_of IO::TimeoutError, response.error
      frames = peer.receive_until(&.type.rst_stream?)
      assert_equal({stream.id, Error::Code::CANCEL}, {frames.last.stream.id, frames.last.error})
    end
  end

  def test_read_timeout_while_reading_the_body
    with_client do |client, peer|
      client.read_timeout = 50.milliseconds
      response = Future.new { client.get("/") { |response| response.body_io.gets_to_end } }
      stream = peer.accept_request
      stream.send_headers(HTTP::Headers{":status" => "200"})
      stream.send_data("partial")

      assert_kind_of IO::TimeoutError, response.error
    end
  end

  def test_read_timeout_ignores_a_body_nobody_reads
    with_client do |client, peer|
      client.read_timeout = 50.milliseconds
      got_headers = Channel(Nil).new(1)
      release = Channel(Nil).new(1)
      response = Future.new do
        client.get("/") do |response|
          got_headers.send(nil)
          receive_within(release)
          response.body_io.gets_to_end
        end
      end
      stream = peer.accept_request
      stream.send_headers(HTTP::Headers{":status" => "200"})
      receive_within(got_headers)

      # the server waits, as long as the client doesn't read: no timeout,
      # PING round trips show the client is idle long past the timeout
      elapsed = Time::Span.zero
      while elapsed < 200.milliseconds
        elapsed += Time.measure { peer.ping }
      end
      stream.send_data("done", Frame::Flags::END_STREAM)
      release.send(nil)
      assert_equal "done", response.get
    end
  end

  def test_write_timeout_while_waiting_for_the_window
    with_client do |client, peer|
      client.write_timeout = 50.milliseconds
      # larger than the stream window, that the server never updates
      response = Future.new { client.post("/", body: "z" * 100_000) }
      stream = peer.accept_request
      frames = peer.receive_until(&.type.rst_stream?)

      assert_equal({stream.id, Error::Code::CANCEL}, {frames.last.stream.id, frames.last.error})
      assert_kind_of IO::TimeoutError, response.error
    end
  end

  def test_waits_for_a_stream_slot_at_max_concurrent_streams
    with_client(max_concurrent_streams: 1) do |client, peer|
      first = Future.new { client.get("/first") }
      stream1 = peer.accept_request
      second = Future.new { client.get("/second") }

      # the second request can't open a stream until the first one closed: a
      # PING round trip shows the client sent nothing else meanwhile
      peer.ping
      stream1.send_headers(HTTP::Headers{":status" => "200"})
      stream1.send_data("first", Frame::Flags::END_STREAM)
      assert_equal "first", first.get.body

      stream3 = peer.accept_request
      assert_equal "/second", stream3.headers[":path"]
      stream3.send_headers(HTTP::Headers{":status" => "200"})
      stream3.send_data("second", Frame::Flags::END_STREAM)
      assert_equal "second", second.get.body
    end
  end

  def test_early_response_and_reset_stops_the_upload
    with_client do |client, peer|
      response = Future.new { client.post("/", body: EndlessIO.new) }
      stream = peer.accept_request
      stream.send_headers(HTTP::Headers{":status" => "413"}, Frame::Flags::END_STREAM)
      stream.send_rst_stream(Error::Code::NO_ERROR)
      # receives (and ignores) the DATA in flight, until the next request
      next_request = Future.new { peer.accept_request }

      assert_equal 413, response.get.status_code

      # the connection is still usable
      second = Future.new { client.get("/again") }
      stream = next_request.get
      assert_equal "/again", stream.headers[":path"]
      stream.send_headers(HTTP::Headers{":status" => "204"}, Frame::Flags::END_STREAM)
      assert_equal 204, second.get.status_code
    end
  end

  def test_requests_and_decompresses_gzip
    with_client do |client, peer|
      response = Future.new { client.get("/") }
      stream = peer.accept_request
      assert_equal "gzip, deflate", stream.headers["accept-encoding"]

      gzipped = IO::Memory.new
      Compress::Gzip::Writer.open(gzipped) { |gzip| gzip << "plain text" }
      stream.send_headers(HTTP::Headers{":status" => "200", "content-encoding" => "gzip", "content-length" => gzipped.size.to_s})
      stream.send_data(gzipped.to_slice, Frame::Flags::END_STREAM)

      response = response.get
      assert_equal "plain text", response.body
      refute response.headers.has_key?("content-encoding")
      refute response.headers.has_key?("content-length")
    end
  end

  def test_compress_false_leaves_the_content_encoding
    with_client do |client, peer|
      client.compress = false
      response = Future.new { client.get("/") }
      stream = peer.accept_request
      refute stream.headers.has_key?("accept-encoding")
      stream.send_headers(HTTP::Headers{":status" => "200", "content-encoding" => "gzip"})
      stream.send_data("raw", Frame::Flags::END_STREAM)
      assert_equal "raw", response.get.body
    end
  end

  def test_runs_before_request_callbacks
    with_client do |client, peer|
      client.before_request { |request| request.headers["authorization"] = "Bearer token" }
      response = Future.new { client.get("/") }
      stream = peer.accept_request
      assert_equal "Bearer token", stream.headers["authorization"]
      stream.send_headers(HTTP::Headers{":status" => "204"}, Frame::Flags::END_STREAM)
      response.get
    end
  end

  def test_close_sends_goaway_and_fails_requests_in_flight
    with_client do |client, peer|
      response = Future.new { client.get("/") }
      peer.accept_request

      client.close
      assert_kind_of IO::Error, response.error
      assert client.closed?

      frames = peer.receive_until(&.type.goaway?)
      assert_equal Error::Code::NO_ERROR, frames.last.error
      # the client closed the IO
      assert_raises(IO::Error) { loop { peer.connection.receive } }
    end
  end

  def test_connection_error_sends_goaway_and_fails_requests
    with_client do |client, peer|
      response = Future.new { client.get("/") }
      peer.accept_request
      # DATA on stream 0 is a connection error (RFC 9113 section 6.1)
      peer.send_frame(Frame::Type::DATA, Frame::Flags::None, 0, "x".to_slice)

      error = response.error.as(Error)
      assert_equal Error::Code::PROTOCOL_ERROR, error.code
      frames = peer.receive_until(&.type.goaway?)
      assert_equal Error::Code::PROTOCOL_ERROR, frames.last.error
    end
  end

  def test_malformed_response_fails_the_request
    with_client do |client, peer|
      response = Future.new { client.get("/") { |response| response.body_io.gets_to_end } }
      stream = peer.accept_request
      stream.send_headers(HTTP::Headers{":status" => "200", "content-length" => "10"})
      stream.send_data("short", Frame::Flags::END_STREAM)

      error = response.error.as(Error)
      assert_equal Error::Code::PROTOCOL_ERROR, error.code
    end
  end

  def test_interim_response_ending_the_stream_fails_the_request
    with_client do |client, peer|
      response = Future.new { client.get("/") }
      stream = peer.accept_request
      stream.send_headers(HTTP::Headers{":status" => "100"}, Frame::Flags::END_STREAM)

      error = response.error.as(Error)
      assert_equal Error::Code::PROTOCOL_ERROR, error.code
    end
  end

  def test_connects_again_after_goaway
    with_listener do |listener, client|
      first = Future.new { client.get("/first") }
      peer = Peer.new(listener.accept)
      stream = peer.accept_request
      assert_equal "127.0.0.1:#{listener.local_address.port}", stream.headers[":authority"]
      stream.send_headers(HTTP::Headers{":status" => "200"})
      stream.send_data("first", Frame::Flags::END_STREAM)
      assert_equal "first", first.get.body

      # no stream left: the client closes the connection
      peer.send_goaway(last_stream_id: 1)
      peer.receive_until(&.type.goaway?)
      assert client.closed?

      second = Future.new { client.get("/second") }
      peer = Peer.new(listener.accept)
      stream = peer.accept_request
      assert_equal 1, stream.id
      stream.send_headers(HTTP::Headers{":status" => "200"})
      stream.send_data("second", Frame::Flags::END_STREAM)
      assert_equal "second", second.get.body
      refute client.closed?
    end
  end

  def test_retries_refused_request_on_a_new_connection
    with_listener do |listener, client|
      response = Future.new { client.post("/", body: "replayable") }
      peer = Peer.new(listener.accept)
      peer.accept_request
      peer.send_goaway(last_stream_id: 0)

      peer = Peer.new(listener.accept)
      stream = peer.accept_request
      peer.receive_until { |frame| frame.stream == stream && frame.flags.end_stream? }
      assert_equal "replayable", stream.data.gets_to_end
      stream.send_headers(HTTP::Headers{":status" => "200"})
      stream.send_data("done", Frame::Flags::END_STREAM)
      assert_equal "done", response.get.body
    end
  end

  def test_retry_sends_the_whole_body_while_the_refused_body_writer_reads
    body = GatedBody.new("0123456789abcdef" * 2500)
    with_listener do |listener, client|
      response = Future.new { client.post("/", body: body) }
      peer = Peer.new(listener.accept)
      peer.accept_request
      # the first attempt's body writer sent a DATA frame and reads ahead
      receive_within(body.parked)
      peer.send_goaway(last_stream_id: 0)

      peer = Peer.new(listener.accept)
      stream = peer.accept_request
      until stream.state.half_closed_remote? || stream.state.closed?
        peer.connection.receive
      end
      assert_equal "0123456789abcdef" * 2500, stream.data.gets_to_end
      stream.send_headers(HTTP::Headers{":status" => "200"})
      stream.send_data("done", Frame::Flags::END_STREAM)
      assert_equal "done", response.get.body
    end
  ensure
    body.try(&.release)
  end

  def test_refused_request_with_a_streamed_body_raises
    with_listener do |listener, client|
      response = Future.new { client.post("/", body: EndlessIO.new) }
      peer = Peer.new(listener.accept)
      peer.accept_request
      peer.send_goaway(last_stream_id: 0)

      error = response.error.as(Error)
      assert_equal Error::Code::REFUSED_STREAM, error.code
    end
  end

  def test_scheme_can_be_set_for_a_client_on_an_io
    with_client(scheme: "https") do |client, peer|
      response = Future.new { client.get("/") }
      stream = peer.accept_request
      assert_equal "https", stream.headers[":scheme"]
      stream.send_headers(HTTP::Headers{":status" => "204"}, Frame::Flags::END_STREAM)
      response.get
    end
  end

  def test_rejects_an_unsupported_protocol
    assert_raises(ArgumentError) { HTTP::Client.new("example.com", protocol: "h2c") }
    assert_raises(ArgumentError) { HTTP::Client.new(IO::Memory.new, "example.com", protocol: "h2c") }
  end

  def test_around_exec_wraps_http2_requests
    client_io, server_io = IO::Stapled.pipe
    peer = Peer.new(server_io)
    client = AroundClient.new(client_io, "example.com", protocol: "h2")

    response = Future.new { client.get("/around") }
    stream = peer.accept_request
    assert_equal "yes", stream.headers["x-around"]
    stream.send_headers(HTTP::Headers{":status" => "204"}, Frame::Flags::END_STREAM)

    assert_equal 204, response.get.status_code
    assert_equal ["before /around", "after 204"], client.events
  ensure
    client.try(&.close)
    server_io.try(&.close)
  end

  def test_http_1_1_required_raises_on_a_client_on_an_io
    with_client do |client, peer|
      response = Future.new { client.get("/") }
      peer.accept_request.send_rst_stream(Error::Code::HTTP_1_1_REQUIRED)

      error = response.error.as(Error)
      assert_equal Error::Code::HTTP_1_1_REQUIRED, error.code
    end
  end

  def test_read_timeout_spares_an_idle_connection
    with_listener do |listener, client|
      client.read_timeout = 100.milliseconds
      peer = nil
      2.times do |i|
        response = Future.new { client.get("/") }
        peer ||= Peer.new(listener.accept)
        stream = peer.accept_request
        # the same connection: no socket read timeout closed it meanwhile
        assert_equal 2 * i + 1, stream.id
        stream.send_headers(HTTP::Headers{":status" => "204"}, Frame::Flags::END_STREAM)
        assert_equal 204, response.get.status_code
        sleep 300.milliseconds
      end
    end
  end

  def test_cleartext_uses_http1_without_prior_knowledge
    with_http1_server do |port, connections|
      client = HTTP::Client.new("127.0.0.1", port)
      response = client.get("/plain")
      assert_equal "HTTP/1.1", response.version
      assert_equal "/plain", response.body
      assert_equal 1, connections.call
    ensure
      client.try(&.close)
    end
  end

  def test_http1_requests_of_concurrent_fibers_take_turns
    with_http1_server do |port, connections|
      client = HTTP::Client.new("127.0.0.1", port)
      bodies = Channel(String | Exception).new(10)
      10.times do |i|
        spawn do
          client.get("/path/#{i}") do |response|
            # the response body is read while the next request waits
            Fiber.yield
            bodies.send(response.body_io.gets_to_end)
          end
        rescue ex
          bodies.send(ex)
        end
      end

      received = Array.new(10) { receive_within(bodies) }
      received.each { |body| raise body if body.is_a?(Exception) }
      assert_equal (0...10).map { |i| "/path/#{i}" }.sort, received.map(&.as(String)).sort
      assert_equal 1, connections.call
    ensure
      client.try(&.close)
    end
  end

  # The scripted server end of a client connection.
  private class Peer
    getter io : IO
    getter connection : HTTP2::Connection

    def initialize(@io : IO)
      @connection = HTTP2::Connection.new(io, :server)
      @accepted = false
      @pending = Deque(Frame).new
    end

    # Accepts the client connection on the first call (preface and SETTINGS
    # exchange), then receives frames until the client opens a stream.
    def accept_request : Stream
      accept unless @accepted
      @last_headers = frame = receive_until(&.type.headers?).last
      frame.stream
    end

    # The HEADERS frame that opened the last accepted request.
    getter! last_headers : Frame

    private def accept
      @accepted = true
      connection.read_client_preface
      connection.write_settings
      # the client's SETTINGS and its ACK of ours: the client applied our
      # settings; keeps the other frames for later
      acked = false
      until acked
        next unless frame = connection.receive
        if frame.type.settings?
          acked = frame.flags.end_stream?
        else
          @pending << frame
        end
      end
    end

    # Receives frames until the block returns true for one. Returns the
    # received frames.
    def receive_until(&) : Array(Frame)
      frames = [] of Frame
      loop do
        frame = @pending.shift? || connection.receive
        next unless frame
        frames << frame
        return frames if yield frame
      end
    end

    # A PING round trip: once the client acknowledged it, the client
    # processed every frame sent before, and the frames it sent before are
    # received.
    def ping : Array(Frame)
      send_frame(Frame::Type::PING, Frame::Flags::None, 0, Bytes.new(8))
      receive_until { |frame| frame.type.ping? && frame.flags.end_stream? }
    end

    def send_goaway(last_stream_id : Int32, code = Error::Code::NO_ERROR) : Nil
      payload = IO::Memory.new
      payload.write_bytes(last_stream_id.to_u32, IO::ByteFormat::BigEndian)
      payload.write_bytes(code.to_u32, IO::ByteFormat::BigEndian)
      send_frame(Frame::Type::GOAWAY, Frame::Flags::None, 0, payload.to_slice)
    end

    def send_frame(type : Frame::Type, flags : Frame::Flags, stream_id : Int32, payload : Bytes) : Nil
      io.write_bytes((payload.size.to_u32 << 8) | type.to_u8, IO::ByteFormat::BigEndian)
      io.write_byte(flags.to_u8)
      io.write_bytes(stream_id.to_u32, IO::ByteFormat::BigEndian)
      io.write(payload)
      io.flush
    end
  end

  # Holds the client's reader fiber once it received a RST_STREAM frame,
  # before it dispatches the frame, until the test releases it.
  private class ResetGate
    getter received = Channel(Nil).new(1)
    getter release = Channel(Nil).new(1)
  end

  private class GatedClient < HTTP::Client
    def initialize(io : IO, @gate : ResetGate, *, host : String)
      super(io, host, protocol: "h2")
    end

    private def new_http2_connection(io, sync_close)
      GatedConnection.new(io, http2_settings, @gate, sync_close: sync_close)
    end
  end

  private class GatedConnection < HTTP2::ClientConnection
    def initialize(io : IO, settings : Settings, @gate : ResetGate, *, sync_close : Bool)
      super(io, settings, sync_close: sync_close)
    end

    private def dispatch(frame : Frame) : Nil
      if frame.type.rst_stream?
        @gate.received.send(nil)
        @gate.release.receive?
      end
      super
    end
  end

  # Records what its `around_exec` sees, and adds a header to the request.
  private class AroundClient < HTTP::Client
    getter events = [] of String

    protected def around_exec(request, &)
      request.headers["X-Around"] = "yes"
      @events << "before #{request.path}"
      super do
        response = yield
        @events << "after #{response.status_code}" if response.is_a?(HTTP::Client::Response)
        response
      end
    end
  end

  # Runs a block in a fiber; the test waits for its result with a timeout.
  private class Future(T)
    def initialize(&block : -> T)
      @channel = Channel(T | Exception).new(1)
      spawn do
        result = begin
          block.call
        rescue ex
          ex
        end
        @channel.send(result)
      end
    end

    def get(timeout = 5.seconds) : T
      result = wait(timeout)
      raise result if result.is_a?(Exception)
      result
    end

    def error(timeout = 5.seconds) : Exception
      result = wait(timeout)
      raise "expected an exception, got #{result.inspect}" unless result.is_a?(Exception)
      result
    end

    private def wait(timeout)
      select
      when result = @channel.receive
        result
      when timeout(timeout)
        raise "timed out after #{timeout}"
      end
    end
  end

  # A request body in memory whose third read, by the body writer of the
  # first attempt once it sent its first DATA frame, waits for the next
  # read (by the writer of a retry) and goes first, or waits until the test
  # releases it.
  private class GatedBody < IO::Memory
    getter parked = Channel(Nil).new(1)
    @resume = Channel(Nil).new(1)
    @resumed = Channel(Nil).new(1)
    @reads = Atomic(Int32).new(0)

    def initialize(string : String)
      super(string.to_slice, writable: false)
    end

    def read(slice : Bytes) : Int32
      case @reads.add(1) + 1
      when 3
        @parked.send(nil)
        @resume.receive?
        super.tap { @resumed.send(nil) }
      when 4
        @resume.send(nil)
        @resumed.receive
        super
      else
        super
      end
    end

    def release : Nil
      @resume.close
    end
  end

  private class NotAnIOError < Exception
  end

  # The client's end of the connection: fails reads or writes with the
  # given errors, and can hold the next HEADERS frame before it's written.
  private class FaultyIO < IO
    property read_error : Exception? = nil
    property write_error : Exception? = nil
    getter? closed = false
    getter held = Channel(Nil).new(1)
    @release = Channel(Nil).new(1)
    @hold_headers = false

    def initialize(@io : IO)
    end

    def read(slice : Bytes) : Int32
      count = @io.read(slice)
      if error = @read_error
        raise error
      end
      count
    end

    def write(slice : Bytes) : Nil
      if error = @write_error
        raise error
      end
      # the first 4 bytes of a frame header end with the frame type
      if @hold_headers && slice.size == 4 && slice[3] == Frame::Type::HEADERS.to_u8
        @hold_headers = false
        @held.send(nil)
        @release.receive
      end
      @io.write(slice)
    end

    def flush : Nil
      @io.flush
    end

    def close : Nil
      @closed = true
      @io.close
    end

    def hold_next_headers : Nil
      @hold_headers = true
    end

    def release_headers : Nil
      @release.send(nil)
    end
  end

  # An upload that never ends.
  private class EndlessIO < IO
    def read(slice : Bytes) : Int32
      slice.fill(0x61_u8)
      slice.size
    end

    def write(slice : Bytes) : Nil
      raise IO::Error.new("read only")
    end
  end

  private def with_client(host = "example.com", port = nil, max_concurrent_streams = nil, scheme = nil, &)
    client_io, server_io = IO::Stapled.pipe
    peer = Peer.new(server_io)
    max_concurrent_streams.try { |max| peer.connection.local_settings.max_concurrent_streams = max }
    io = FaultyIO.new(client_io)
    client = HTTP::Client.new(io, host, port || 80, protocol: "h2", scheme: scheme)

    # a test waiting for a frame that never comes fails instead of hanging:
    # closing the pipes makes the blocked reads raise
    done = Channel(Nil).new
    spawn do
      select
      when done.receive?
      when timeout(10.seconds)
        server_io.close
        client_io.close
      end
    end

    yield client, peer, io
  ensure
    done.try(&.close)
    client.try(&.close)
    server_io.try(&.close)
  end

  # Completes a request: the client is connected and applied the server's
  # settings.
  private def complete_request(client, peer) : Nil
    response = Future.new { client.get("/") }
    peer.accept_request.send_headers(HTTP::Headers{":status" => "204"}, Frame::Flags::END_STREAM)
    response.get
  end

  # Sends the header of a DATA frame of 100 bytes on *stream*, and 50 bytes.
  private def send_half_a_data_frame(peer, stream) : Nil
    peer.io.write_bytes((100_u32 << 8) | Frame::Type::DATA.to_u8, IO::ByteFormat::BigEndian)
    peer.io.write_byte(0_u8)
    peer.io.write_bytes(stream.id.to_u32, IO::ByteFormat::BigEndian)
    peer.io.write(Bytes.new(50))
    peer.io.flush
    sleep 50.milliseconds # the client waits for the rest of the frame
  end

  # Waits until the block returns true, at most *timeout*. Returns false
  # if it timed out.
  private def wait_until(timeout = 1.second, &) : Bool
    elapsed = Time::Span.zero
    until yield
      return false if elapsed > timeout
      elapsed += Time.measure { sleep 10.milliseconds }
    end
    true
  end

  # A client on a host and port, and the listener it connects to.
  private def with_listener(&)
    listener = TCPServer.new("127.0.0.1", 0)
    client = HTTP::Client.new("127.0.0.1", listener.local_address.port, protocol: "h2")
    done = Channel(Nil).new
    spawn do
      select
      when done.receive?
      when timeout(10.seconds)
        listener.close
        client.close
      end
    end
    yield listener, client
  ensure
    done.try(&.close)
    client.try(&.close)
    listener.try(&.close)
  end

  # An HTTP/1 server with keep-alive that answers each request with its
  # path. Yields its port, and a proc that returns the number of connections
  # it accepted.
  private def with_http1_server(&)
    listener = TCPServer.new("127.0.0.1", 0)
    connections = Atomic(Int32).new(0)
    spawn do
      while socket = listener.accept?
        connections.add(1)
        spawn { serve_http1(socket) }
      end
    end
    yield listener.local_address.port, -> { connections.get }
  ensure
    listener.try(&.close)
  end

  private def serve_http1(socket) : Nil
    while (request = HTTP::Request.from_io(socket)).is_a?(HTTP::Request)
      body = request.path
      socket << "HTTP/1.1 200 OK\r\nContent-Length: " << body.bytesize << "\r\n\r\n" << body
      socket.flush
    end
  rescue IO::Error
    # the client closed the connection
  ensure
    socket.close
  end

  private def assert_kind_of(klass : T.class, value, file = __FILE__, line = __LINE__) forall T
    assert value.is_a?(T), "Expected #{value.inspect} to be a #{T}", file, line
  end

  private def receive_within(channel : Channel(T), timeout = 5.seconds) : T forall T
    select
    when value = channel.receive
      value
    when timeout(timeout)
      flunk "timed out after #{timeout}"
    end
  end
end
