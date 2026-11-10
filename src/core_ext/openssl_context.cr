require "openssl"

abstract class OpenSSL::SSL::Context
  # Specifies the ALPN protocols to negotiate with the remote endpoint, in
  # order of preference. A client offers them all, for example
  # `["h2", "http/1.1"]`; a server that supports none of them may abort the
  # handshake (RFC 7301 section 3.2).
  def alpn_protocols=(protocols : Enumerable(String))
    wire = IO::Memory.new
    protocols.each do |protocol|
      unless 0 < protocol.bytesize < 256
        raise ArgumentError.new("Invalid ALPN protocol: #{protocol.inspect}")
      end
      wire.write_byte(protocol.bytesize.to_u8)
      wire << protocol
    end
    self.alpn_protocol = wire.to_slice
  end
end
