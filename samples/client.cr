require "../src/client"

unless ENV["CI"]?
  HTTP2::Log.level = Log::Severity::Trace
end

host = ENV["HOST"]? || "localhost"
port = (ENV["PORT"]? || 9292).to_i

if ENV["TLS"]?
  context = OpenSSL::SSL::Context::Client.new
  context.verify_mode = OpenSSL::SSL::VerifyMode::NONE
  client = HTTP::Client.new(host, port, tls: context) # ALPN h2, else HTTP/1
else
  client = HTTP::Client.new(host, port, protocol: "h2") # prior knowledge
end

headers = HTTP::Headers{"user-agent" => "crystal h2/0.0.0"}

10.times do |i|
  client.get("/", headers) do |response|
    puts "REQ ##{i}: #{response.version} #{response.status_code} #{response.headers.inspect}"

    while line = response.body_io.gets
      puts "REQ ##{i}: #{line}"
    end
  end
end

response = client.put("/echo", HTTP::Headers{"content-type" => "text/plain"}, body: "hello")
puts "PUT /echo: #{response.status_code} #{response.body.inspect}"

client.close
