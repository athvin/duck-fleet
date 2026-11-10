#!/bin/sh
set -eu
case "$(dpkg --print-architecture)" in
  amd64) sha=838d98a85e697bab9935010c88a8c67d3312ccedcab4cb4a0ba01da65113bb70 ;;
  arm64) sha=7e154fd14d5b7afd7c9f36071000bf4badd3fa57a125cf7dc596f2c5ad82c1b2 ;;
  *) exit 1 ;;
esac
curl -fsSL "https://github.com/duckdb/duckdb/releases/download/v1.5.4/libduckdb-linux-$(dpkg --print-architecture).zip" -o /tmp/duckdb.zip
printf '%s  /tmp/duckdb.zip\n' "$sha" | sha256sum -c -
unzip -j /tmp/duckdb.zip libduckdb.so duckdb.h -d /usr/local/lib
rm /tmp/duckdb.zip
ldconfig
