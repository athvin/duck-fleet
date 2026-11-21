#!/bin/sh
set -eu
arch=$(dpkg --print-architecture)
case "$arch" in
  amd64) sha=e25502612fed533a243b61f23a2a574daad030e7e88906222a5f0ab30d06769e ;;
  arm64) sha=811e8b4d0226f5e531f583aadc798070ffb33b350e3823be563a2ebeb2c68593 ;;
  *) exit 1 ;;
esac
curl -fsSL "https://github.com/duckdb/duckdb/releases/download/v2.0.1/duckdb-shared-libs-linux-$arch.tar.gz" -o /tmp/duckdb.tar.gz
printf '%s  /tmp/duckdb.tar.gz\n' "$sha" | sha256sum -c -
tar -xzf /tmp/duckdb.tar.gz -C /usr/local/lib libduckdb.so duckdb.h duckdb_v2.h duckdb_extension.h duckdb_extension_v2.h duckdb_static_extension.h
rm /tmp/duckdb.tar.gz
ldconfig
