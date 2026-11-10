{% unless flag?(:without_openssl) %}
  require "openssl"

  {% if LibSSL::LIBRESSL_VERSION == "0.0.0" && compare_versions(LibSSL::OPENSSL_VERSION, "3.0.0") >= 0 && LibSSL::Options.has_constant?(:ENABLE_KTLS) %}
    lib LibSSL
      fun ssl_clear_options = SSL_clear_options(ssl : SSL, op : ULong) : ULong
    end
  {% end %}

  abstract class OpenSSL::SSL::Socket < IO
    # :nodoc:
    #
    # The IO the TLS connection runs over, so the server or the client can
    # shut down or close the socket without going through TLS.
    def underlying_io : IO
      {% if compare_versions(Crystal::VERSION, "1.20.0") >= 0 %}
        bio.io
      {% else %}
        @bio.io
      {% end %}
    end

    # :nodoc:
    #
    # Sends close_notify (the first step of `SSL_shutdown`) without closing the
    # socket, so it can still be read until the peer closes.
    def send_close_notify : Nil
      return if closed?
      flush
      LibSSL.ssl_shutdown(@ssl)
    end
  end
{% end %}

# Kernel TLS only works over a `::Socket`: the standard library's BIO casts the
# IO to one once OpenSSL enables it, and a context enables it by default with
# OpenSSL 3. Over another IO, for example a listener that isn't a socket, the
# connection doesn't use it. Both handshakes, and those of subclasses, go
# through `ktls_safe_handshake`. Defined once the program is loaded: the
# standard library's socket may be loaded after this file.
macro finished
  {% unless flag?(:without_openssl) %}
    {% if LibSSL::LIBRESSL_VERSION == "0.0.0" && compare_versions(LibSSL::OPENSSL_VERSION, "3.0.0") >= 0 &&
            LibSSL::Options.has_constant?(:ENABLE_KTLS) && OpenSSL::SSL::Socket.has_method?(:ktls_safe_handshake) %}
      abstract class OpenSSL::SSL::Socket < IO
        private def ktls_safe_handshake(&)
          LibSSL.ssl_clear_options(@ssl, LibSSL::Options::ENABLE_KTLS) unless bio.io.is_a?(::Socket)
          previous_def { yield }
        end
      end
    {% end %}
  {% end %}
end
