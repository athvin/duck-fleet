#!/bin/sh
# Generate the test CA and the server certificate once per volume, publish the trust
# bundle the workers mount as their system CA bundle, then serve.
set -eu
tls=/tls
bundle=/trust
mkdir -p "$tls" "$bundle"
if [ ! -s "$tls/ca.crt" ]; then
  openssl req -x509 -newkey rsa:2048 -nodes -days 3650 -subj "/CN=DuckFleet local test CA" \
    -keyout "$tls/ca.key" -out "$tls/ca.crt" 2>/dev/null
  openssl req -newkey rsa:2048 -nodes -subj "/CN=object-store-tls" \
    -keyout "$tls/server.key" -out "$tls/server.csr" 2>/dev/null
  printf 'subjectAltName=DNS:object-store-tls,DNS:localhost\nextendedKeyUsage=serverAuth\n' >"$tls/ext.cnf"
  openssl x509 -req -in "$tls/server.csr" -CA "$tls/ca.crt" -CAkey "$tls/ca.key" -CAcreateserial \
    -days 3650 -extfile "$tls/ext.cnf" -out "$tls/server.crt" 2>/dev/null
  chmod 0644 "$tls/server.key"
fi
# The system roots plus the test CA, at the name httpfs looks for first
# (CA_BUNDLES in server/src/worker.rs). Public certificates only.
cat /etc/ssl/certs/ca-certificates.crt "$tls/ca.crt" >"$bundle/ca-certificates.crt.tmp"
mv "$bundle/ca-certificates.crt.tmp" "$bundle/ca-certificates.crt"
exec nginx -g 'daemon off;'
