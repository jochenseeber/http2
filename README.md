# HTTP/2

Pure Crystal implementation of the HTTP/2 protocol.

## Status

- [x] HPACK (including DH compression)
- [x] HTTP/2 connection, streams, frames, ...
- [x] HTTP/2 flow control (in/out, whole-connection, per-stream)
- [x] HTTP/2 server connections
- [x] HTTP/1 to HTTP/2 server connection upgrades
- [x] Integrate into `HTTP::Server`
- [x] HTTP/2 client connections
- [x] Integrate into `HTTP::Client`
- [x] Passes h2spec 2.6.0 💚

To add HTTP/2 support to your HTTP servers written in Crystal, if they're built
on top of `HTTP::Server` or a framework using `HTTP::Server` internally, add the
`http2` shard to your `shard.yml`, then require one file:

```crystal
require "http2/server"
```

That's it. Now every `HTTP::Server` instance is HTTP/2 compliant! ✨

Well, that's enough for `curl` and other CLI tools, because the HTTP/2 RFC
allows unencrypted HTTP/2 connections, and has multiple ways to upgrade a
HTTP/1 connection into a HTTP/2 connection, but the big browsers (Firefox,
Chrome, Safari) all require a TLS connection... even on localhost. You must
prepare a TLS certificate, instantiate an `OpenSSL::SSL::Context::Server` and
call `HTTP::Server#bind_tls`.

```crystal
context = OpenSSL::SSL::Context::Server.new
context.certificate_chain = File.join(TLS_PATH, "crt.pem")
context.private_key = File.join(TLS_PATH, "key.pem")

server = HTTP::Server.new(handlers)
server.bind_tls(host, port, context)
server.listen
```

A server speaks HTTP/2 over cleartext connections too (h2c), to a client that
starts with the HTTP/2 preface or asks for `Upgrade: h2c`. To speak only
HTTP/1.1 on cleartext connections, turn it off; HTTP/2 over TLS is not affected:

```crystal
server.h2c = false
```

A handler reaches the HTTP/2 connection of its request, for example to shut it
down gracefully with GOAWAY: the client opens no new stream on it, the streams
in flight complete, then the connection closes.

```crystal
context.http2_connection.try(&.send_goaway)
```

A handler can also end its response at once without completing it. Under
HTTP/2 the stream is reset with RST_STREAM and the given error code, while the
other streams on the connection go on. HTTP/1 has no stream to reset, so the
response is cut off and the connection closed:

```crystal
context.response.reset_stream(:protocol_error)
```

`HTTP::Server#close` stops accepting connections and lets the requests in
progress finish, for up to `close_timeout`, but returns at once. To wait until
every connection has ended and every handler has returned, HTTP/2 stream
handlers included, call `#wait_closed`. It returns false if some are still
running once `close_timeout` (plus a second) has passed:

```crystal
server.close
server.wait_closed
```

### Differences from the stdlib's `HTTP::Server`

Requiring `http2/server` adds HTTP/2 to `HTTP::Server`. HTTP/1 connections are
still served by the stdlib's `HTTP::Server::RequestProcessor`, so handlers see
the stdlib's behaviour, `Response#upgrade` included (its block runs once the
handler returned; an HTTP/2 stream can't be upgraded: it raises
`NotImplementedError`). The server adds, over HTTP/1:

- A request with several `Host` header fields, or with content lengths that
  differ, or with a `Content-Length` and a `Transfer-Encoding`, is answered
  with `400 Bad Request` and the connection closed (RFC 9112 sections 3.2 and
  6.3).
- A 1xx, 204 or 304 response has no body, and no chunked framing.
- `Response#flush` before the first byte of the body sends the status and the
  headers, as over HTTP/2, so a client can wait on a stream. The body is then
  chunked, unless it has a `Content-Length`; over HTTP/1.0 the connection is
  closed after it.
- A gzip or deflate request body is decoded, as over HTTP/2.
- A handler that fails after the headers were sent cuts the response off,
  without its last chunk, so the client can tell it's incomplete (RFC 9112
  section 7.1); a 500 says `Connection: close`.
- `HTTP::Server#close` ends idle keep-alive connections at once, and
  `#wait_closed` waits for the others.
- The requests have their `remote_address` and `local_address` on Crystal
  versions whose stdlib doesn't set them yet.

The contract tests in `test/contract/` check this against the stdlib's server:
`make contract`.

A TLS connection over an IO that isn't a `::Socket`, for example from an
`OpenSSL::SSL::Server` over another kind of listener, doesn't use kernel TLS,
which the standard library supports over sockets only: the server and the
client turn the context's `ENABLE_KTLS` option off for it.

## Client

To add HTTP/2 support to `HTTP::Client`, require one file:

```crystal
require "http2/client"

client = HTTP::Client.new("example.com", tls: true) # ALPN h2, else HTTP/1
response = client.get("/")
response.version # => "HTTP/2"

client.post("/upload", body: File.open("data.bin")) do |response|
  response.body_io.each_line { |line| puts line }
end
```

Over TLS, the client negotiates HTTP/2 with ALPN and falls back to HTTP/1 when
the server doesn't select `h2`. Under HTTP/2, concurrent requests share the
connection, each on its own stream; under HTTP/1 they take turns.

A cleartext connection uses HTTP/1, unless the server is known to speak HTTP/2
(prior knowledge, RFC 9113 section 3.3):

```crystal
client = HTTP::Client.new("localhost", 8080, protocol: "h2")
client = HTTP::Client.new(io, "example.com", protocol: "h2", scheme: "https")
```

`protocol: "h1"` keeps HTTP/1. The deprecated `h2c` upgrade isn't supported.

A cancellation gives up on one request: under HTTP/2, its stream is reset and
the other requests go on. Under HTTP/1, the request has the connection to
itself, so the connection is closed; the next request connects again.

```crystal
cancellation = HTTP::Client::Cancellation.new(timeout: 5.seconds)
client.exec(HTTP::Request.new("HEAD", "/"), cancellation: cancellation)

cancellation = HTTP::Client::Cancellation.new
spawn { client.exec(HTTP::Request.new("GET", "/events"), cancellation: cancellation) { |response| ... } }
cancellation.cancel # the request raises HTTP2::Error (CANCEL)
```

## Concurrency

An HTTP/2 connection is shared by several fibers: the one that receives
frames, and the ones that send requests, responses and bodies. Who owns what:

- **The socket.** Only the fiber that receives frames reads it. Every frame
  is written under the connection's write mutex, so frames never interleave.
- **The HPACK contexts.** The decoder belongs to the fiber that receives
  frames. Header blocks are encoded under the write mutex, together with
  sending them, so the encoder's state and the order of the header blocks
  match (RFC 9113 section 4.3).
- **Stream state.** Any fiber may read it. Receiving, sending and aborting
  change it under a per-stream lock. Locks are always taken in this order:
  the write mutex, then the stream's lock, then the lock of the table of
  streams.
- **Bodies.** A stream's body is a buffer with its own lock: the fiber that
  receives frames writes to it, and the handler or the response's reader
  reads from it.
- **Settings.** `HTTP2::Connection#remote_settings` is changed by the fiber
  that receives frames only. Each value is read and written at once, so other
  fibers may read it. Change `#local_settings` before `#write_settings`, or
  call `#send_settings` after changing it.

The library is synchronized for multi-threaded execution contexts, but
`OpenSSL::SSL::Socket` isn't: its reads and writes must not run in parallel.
Keep TLS connections on one thread: run a TLS server in a single-threaded
execution context, and use a TLS client from the execution context that made
its first request, if that context is single-threaded (see `HTTP::Client`).
Under the legacy `-Dpreview_mt` scheduler, each thread has its own event loop,
and a socket can't move between them while a fiber waits on it. The library
spawns the fibers of a connection on the thread that starts them, so a server's
connection and its handlers share one thread. Use a client from one thread: the
fibers making requests with it must run on the same thread, for example spawned
with `same_thread: true`. The suite's TLS tests don't run under `-Dpreview_mt`.

## Tests

Run the test suite:

```sh
$ shards install
$ make test
```

The client interoperability tests in `test/interop/` need nghttpd and the Go
server in that directory; see `test/interop/client_test.cr`, and the
`interop` job in `.github/workflows/ci.yml`. The server is checked with curl,
nghttp and h2load by `test/interop/server_check.sh`, against `bin/server`
over cleartext and TLS (the `server_interop` job).

CI runs the suite on the minimum Crystal version `shard.yml` declares and on
the latest release, and once with `CRFLAGS=-Dexecution_context`.

### h2spec

Build and run the `bin/server` server, then execute
[h2spec](https://github.com/summerwind/h2spec/releases).

```sh
$ make bin/server CRFLAGS=-Dh2spec
```

Test against HTTP:
```
$ bin/server
$ ./h2spec -p 9292 -S
```

Test against HTTPS:
```sh
$ TLS=1 bin/server
$ ./h2spec -p 9292 -k -t -S
```

NOTE: h2spec 2.6.0 targets RFC 7540: it hasn't been updated since 2020, while
RFC 9113 revised HTTP/2 in 2022. The library's own tests cover what RFC 9113
changed or made stricter, for example field validation (section 8.2) and
unknown frame types (section 5.5), in `test/connection_test.cr` and
`test/framing_test.cr`.

## RFC

### HTTP/2

- [RFC 7540](https://datatracker.ietf.org/doc/html/rfc7540) HTTP/2 (obsolete)
- [RFC 7541](https://datatracker.ietf.org/doc/html/rfc7541) HPACK Header Compression for HTTP/2
- [RFC 9113](https://datatracker.ietf.org/doc/html/rfc9113) HTTP/2

### HTTP/1

- [RFC 1945](https://datatracker.ietf.org/doc/html/rfc1945) HTTP/1.0 (informational)
- [RFC 2616](https://datatracker.ietf.org/doc/html/rfc2616) HTTP/1.1 (obsolete)
- [RFC 7230](https://datatracker.ietf.org/doc/html/rfc7230) HTTP/1.1 Message Syntax and Routing (obsolete)
- [RFC 7231](https://datatracker.ietf.org/doc/html/rfc7231) HTTP/1.1 Semantics and Content (obsolete)
- [RFC 7232](https://datatracker.ietf.org/doc/html/rfc7232) HTTP/1.1 Conditional Requests (obsolete)
- [RFC 7233](https://datatracker.ietf.org/doc/html/rfc7233) HTTP/1.1 Range Requests (obsolete)
- [RFC 7234](https://datatracker.ietf.org/doc/html/rfc7234) HTTP/1.1 Caching (obsolete)
- [RFC 7235](https://datatracker.ietf.org/doc/html/rfc7235) HTTP/1.1 Authentification (obsolete)
- [RFC 9112](https://datatracker.ietf.org/doc/html/rfc9112) HTTP/1.1

### HTTP

- [RFC 9110](https://datatracker.ietf.org/doc/html/rfc9110) HTTP semantics
- [RFC 9111](https://datatracker.ietf.org/doc/html/rfc9111) HTTP Caching
