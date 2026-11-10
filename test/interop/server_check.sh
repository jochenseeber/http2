#!/bin/sh
# Checks `bin/server` (samples/server.cr) with third-party HTTP/2 clients:
# curl, nghttp and h2load, over cleartext (prior knowledge) or TLS (ALPN).
#
#   test/interop/server_check.sh http 9292
#   test/interop/server_check.sh https 9292
set -eu

scheme=${1:-http}
port=${2:-9292}
url="$scheme://127.0.0.1:$port"

if [ "$scheme" = https ]; then
  curl="curl -sS --http2 --insecure"
else
  curl="curl -sS --http2-prior-knowledge"
fi

fail() {
  echo "FAIL: $*" >&2
  exit 1
}

# curl: an echoed body, and a 404, both over HTTP/2
result=$(printf 'hello' | $curl -X PUT --data-binary @- -w ' %{http_version}' "$url/echo")
[ "$result" = "hello 2" ] || fail "curl echo: $result"

result=$($curl -o /dev/null -w '%{http_code} %{http_version}' "$url/missing")
[ "$result" = "404 2" ] || fail "curl 404: $result"

# nghttp: the response's status, and an uploaded body echoed back
nghttp -nv "$url/missing" | grep -q ':status: 404' || fail "nghttp 404"

head -c 100000 /dev/urandom > /tmp/server_check.bin
nghttp -H ':method: PUT' -d /tmp/server_check.bin "$url/echo" > /tmp/server_check.out
cmp -s /tmp/server_check.bin /tmp/server_check.out || fail "nghttp echo"

# h2load: many concurrent streams on a few connections
h2load -n 2000 -c 4 -m 50 "$url/missing" > /tmp/server_check.h2load || fail "h2load"
grep -q '2000 done' /tmp/server_check.h2load || fail "h2load: $(grep requests: /tmp/server_check.h2load)"
grep -q '0 errored' /tmp/server_check.h2load || fail "h2load: $(grep requests: /tmp/server_check.h2load)"

echo "ok: $url"
