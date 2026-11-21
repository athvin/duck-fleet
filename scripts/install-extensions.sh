#!/bin/sh
set -eu
arch=$(dpkg --print-architecture)
case "$arch" in
  amd64)
    aws=ccf5f0f1ee3d39e66b5008771c94350e6c4f31ee2627d48ee3ea25bfa22ea4e4
    httpfs=c88a9686336b93d2cde3ef574ad7101f1c7be5396a2af8a6eb53c9cad1db2dc2
    postgres_scanner=e8ebe5b4ea5c0cda59a58bf2cf7109d0cb1ead256b29df6e4832a79ae5456dbc
    ducklake=3c8d4b154527efd2ccf07e2f0294003cfcfd7087543ba667faafd9ae41c3f704
    icu=7c5c93cbe8375e2eb7edddbd707c62223c9b1b79bac3579b1d2dd52656462f76 ;;
  arm64)
    aws=e6b8a90f686cef4754a3defa17f4023bcc33d90873f08a5a3c83a868805dd945
    httpfs=8ec5abca653685519743ebb6bc480b693b14860c49bf1bca9603108ae837aadd
    postgres_scanner=b828af91e9e0e7b32b43a3bbc8e7f17abec015dac3061d82ed8465903e99ff93
    ducklake=ee394c449de66f72ab5b4133dda390f3e6d65511a4acff6d5ca1f8f797b143a2
    icu=2f34958f9fb16040c546ccce0c847a421b1bc62a27a8dcea385f3f026372f827 ;;
  *) exit 1 ;;
esac
destination="/opt/duckdb/extensions/v2.0.1/linux_$arch"
mkdir -p "$destination"
for extension in httpfs postgres_scanner ducklake aws icu; do
  case "$extension" in
    aws) checksum=$aws ;;
    httpfs) checksum=$httpfs ;;
    postgres_scanner) checksum=$postgres_scanner ;;
    ducklake) checksum=$ducklake ;;
    icu) checksum=$icu ;;
  esac
  curl -fsSL --retry 3 "https://extensions.duckdb.org/v2.0.1/linux_$arch/$extension.duckdb_extension.gz" -o /tmp/extension.gz
  printf '%s  /tmp/extension.gz\n' "$checksum" | sha256sum -c -
  gzip -dc /tmp/extension.gz > "$destination/$extension.duckdb_extension"
done
rm /tmp/extension.gz
