module HTTP2
  # :nodoc:
  #
  # Spawns a fiber of a connection. Under the legacy `-Dpreview_mt`
  # scheduler each thread has its own event loop, and a socket can't move
  # between them while a fiber waits on it: the fiber runs on the current
  # thread, so the fibers of a connection share one. Execution contexts
  # don't need it.
  macro spawn(*, name = nil, &block)
    {% if flag?(:preview_mt) && !flag?(:execution_context) %}
      ::spawn(name: {{name}}, same_thread: true) {{block}}
    {% else %}
      ::spawn(name: {{name}}) {{block}}
    {% end %}
  end

  CLIENT_PREFACE = "PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"

  MINIMUM_FRAME_SIZE =    16384 # 2 ** 14
  MAXIMUM_FRAME_SIZE = 16777215 # 2 ** 24 - 1

  MINIMUM_WINDOW_SIZE =          1
  MAXIMUM_WINDOW_SIZE = 2147483647 # 2 ** 31 - 1

  PING_FRAME_SIZE          = 8
  PRIORITY_FRAME_SIZE      = 5
  RST_STREAM_FRAME_SIZE    = 4
  WINDOW_UPDATE_FRAME_SIZE = 4

  DEFAULT_HEADER_TABLE_SIZE   = 4096
  DEFAULT_ENABLE_PUSH         = true
  DEFAULT_INITIAL_WINDOW_SIZE = 65535
  DEFAULT_MAX_FRAME_SIZE      = MINIMUM_FRAME_SIZE

  REQUEST_PSEUDO_HEADERS  = %w(:method :scheme :authority :path)
  RESPONSE_PSEUDO_HEADERS = %w(:status)

  # Connection-specific fields, that HTTP/2 doesn't use: a message that has
  # one is malformed, as is a `te` field other than `trailers` (RFC 9113
  # section 8.2.2).
  CONNECTION_SPECIFIC_FIELDS = {"connection", "keep-alive", "proxy-connection", "transfer-encoding", "upgrade"}
end
