require "../test_helper"
require "json"
require "../../src/client"

# `HTTP::Client` with HTTP/2 against other HTTP/2 servers: nghttpd and Go's
# net/http (`test/interop/server.go`), over h2c with prior knowledge and over
# TLS.
#
# Skipped unless `H2_INTEROP=1`. The servers' addresses default to:
#
# - `H2_NGHTTPD=127.0.0.1:8080`: `nghttpd --no-tls -d www 8080`
# - `H2_NGHTTPD_TLS=127.0.0.1:8443`: `nghttpd -d www 8443 ssl/server.key ssl/server.crt`
# - `H2_GO=127.0.0.1:9080` and `H2_GO_TLS=127.0.0.1:9443`: `go run . -h2c :9080 -tls :9443`
#
# The nghttpd document root has a `1k.bin` file of 1024 bytes and a `1m.bin`
# file of 1 MiB.
class HTTP2::InteropTest < Minitest::Test
  def setup
    skip "set H2_INTEROP=1 to run the interoperability tests" unless ENV["H2_INTEROP"]? == "1"
  end

  {% for tls in [false, true] %}
    {% suffix = tls ? "_tls" : "" %}

    def test_nghttpd_downloads{{suffix.id}}
      with_client("H2_NGHTTPD", {{tls}}) do |client|
        assert_equal 1024, client.get("/1k.bin").body.bytesize
        response = client.get("/1m.bin")
        assert_equal 200, response.status_code
        assert_equal "HTTP/2", response.version
        assert_equal 1 << 20, response.body.bytesize
      end
    end

    def test_nghttpd_concurrent_requests{{suffix.id}}
      with_client("H2_NGHTTPD", {{tls}}) do |client|
        sizes = concurrently(20) { client.get("/1m.bin").body.bytesize }
        assert_equal [1 << 20] * 20, sizes
      end
    end

    def test_nghttpd_upload{{suffix.id}}
      with_client("H2_NGHTTPD", {{tls}}) do |client|
        # nghttpd discards the body and serves the file
        response = client.post("/1k.bin", body: Bytes.new(4 << 20))
        assert_equal 200, response.status_code
        assert_equal 1024, response.body.bytesize
      end
    end

    def test_go_echo{{suffix.id}}
      with_client("H2_GO", {{tls}}) do |client|
        body = Random.new(42).random_bytes(4 << 20)
        assert_equal body, client.post("/echo", body: body).body.to_slice

        streamed = "a line\n" * 50_000
        assert_equal streamed, client.post("/echo", body: IO::Sized.new(IO::Memory.new(streamed), streamed.bytesize)).body
      end
    end

    def test_go_streams_the_response{{suffix.id}}
      with_client("H2_GO", {{tls}}) do |client|
        client.get("/stream") do |response|
          # the first line arrives long before the last one (2 s later)
          assert_equal "chunk 00", response.body_io.gets
          lines = 1
          while response.body_io.gets
            lines += 1
          end
          assert_equal 40, lines
        end
      end
    end

    def test_go_trailers{{suffix.id}}
      with_client("H2_GO", {{tls}}) do |client|
        client.get("/headers", HTTP::Headers{"x-custom" => "value"}) do |response|
          json = JSON.parse(response.body_io.gets_to_end)
          assert_equal "HTTP/2.0", json["proto"].as_s
          assert_equal "GET", json["method"].as_s
          assert_equal ["value"], json["headers"]["X-Custom"].as_a.map(&.as_s)
          assert_equal "done", response.body_io.as(ClientBody).trailers.try(&.["x-trailer"])
        end
      end
    end

    def test_go_large_download{{suffix.id}}
      with_client("H2_GO", {{tls}}) do |client|
        client.http2_settings.initial_window_size = 1 << 20
        client.get("/big") do |response|
          assert_equal (32 << 20).to_s, response.headers["content-length"]
          block = Bytes.new(64 << 10)
          expected = Bytes.new(64 << 10) { |i| (i % 251).to_u8 }
          512.times do
            response.body_io.read_fully(block)
            assert_equal expected, block
          end
          assert_equal 0, response.body_io.read(block)
        end
      end
    end

    def test_go_server_reset{{suffix.id}}
      with_client("H2_GO", {{tls}}) do |client|
        error = assert_raises(Error) { client.get("/abort") }
        assert_equal Error::Code::INTERNAL_ERROR, error.code
        # the connection is still usable
        assert_equal "ok", client.post("/echo", body: "ok").body
      end
    end

    def test_go_early_response{{suffix.id}}
      with_client("H2_GO", {{tls}}) do |client|
        response = client.post("/noread", body: Bytes.new(4 << 20))
        assert_equal 413, response.status_code
        assert_equal "ok", client.post("/echo", body: "ok").body
      end
    end

    def test_go_head{{suffix.id}}
      with_client("H2_GO", {{tls}}) do |client|
        response = client.head("/big?size=12345")
        assert_equal "12345", response.headers["content-length"]
        assert_equal "", response.body
      end
    end

    def test_go_cancel{{suffix.id}}
      with_client("H2_GO", {{tls}}) do |client|
        client.get("/big") do |response|
          response.body_io.read_fully(Bytes.new(1 << 20))
        end
        assert_equal "ok", client.post("/echo", body: "ok").body
      end
    end

    def test_go_concurrent_requests{{suffix.id}}
      with_client("H2_GO", {{tls}}) do |client|
        results = concurrently(20) do |i|
          if i.even?
            body = Random.new(i).random_bytes(256 << 10)
            client.post("/echo", body: body).body.to_slice == body
          else
            client.get("/big?size=#{1 << 20}").body.bytesize == 1 << 20
          end
        end
        assert_equal [true] * 20, results
      end
    end

    def test_go_sequential_requests{{suffix.id}}
      with_client("H2_GO", {{tls}}) do |client|
        1000.times { |i| assert_equal i.to_s, client.post("/echo", body: i.to_s).body }
      end
    end
  {% end %}

  # A TLS client runs in a single-threaded execution context, as
  # an HTTP/2 `HTTP::Client` requires (see `#single_threaded`).
  private def with_client(variable, tls, &block : HTTP::Client ->) : Nil
    if tls
      single_threaded { use_client(variable, tls, &block) }
    else
      use_client(variable, tls, &block)
    end
  end

  private def use_client(variable, tls, &block : HTTP::Client ->) : Nil
    variable += "_TLS" if tls
    host, _, port = (ENV[variable]? || default_address(variable)).rpartition(':')
    if tls
      context = OpenSSL::SSL::Context::Client.new
      context.verify_mode = OpenSSL::SSL::VerifyMode::NONE
      client = HTTP::Client.new(host, port.to_i, tls: context, protocol: "h2")
    else
      client = HTTP::Client.new(host, port.to_i, protocol: "h2")
    end
    client.read_timeout = 10.seconds
    block.call(client)
  ensure
    client.try(&.close)
  end

  private def default_address(variable)
    case variable
    when "H2_NGHTTPD"     then "127.0.0.1:8080"
    when "H2_NGHTTPD_TLS" then "127.0.0.1:8443"
    when "H2_GO"          then "127.0.0.1:9080"
    else                       "127.0.0.1:9443"
    end
  end

  # Runs the block *count* times concurrently; returns the results in order.
  private def concurrently(count, &block : Int32 -> T) : Array(T) forall T
    channel = Channel({Int32, T | Exception}).new(count)
    count.times do |i|
      spawn do
        result = begin
          block.call(i)
        rescue ex
          ex
        end
        channel.send({i, result})
      end
    end

    results = Array(T?).new(count, nil)
    count.times do
      select
      when value = channel.receive
        i, result = value
        raise result if result.is_a?(Exception)
        results[i] = result
      when timeout(60.seconds)
        flunk "timed out"
      end
    end
    results.map(&.as(T))
  end
end
