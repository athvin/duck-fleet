# A TLS front for the local object store (#213). RustFS speaks plain HTTP in Compose
# and kind, so the workers' DuckDB httpfs TLS path, which reads the system CA bundle
# lazily after the engine file lockdown, was exercised nowhere before EKS (#206).
# This terminates TLS with a throwaway test CA and forwards to RustFS unchanged
# (the Host header is passed through, so SigV4 signatures still verify).
FROM nginx:1.27.3-alpine
RUN apk add --no-cache openssl
COPY docker/object-store-tls/nginx.conf /etc/nginx/nginx.conf
COPY docker/object-store-tls/entrypoint.sh /usr/local/bin/object-store-tls
ENTRYPOINT ["/bin/sh", "/usr/local/bin/object-store-tls"]
