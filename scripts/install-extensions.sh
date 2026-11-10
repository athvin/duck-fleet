#!/bin/sh
set -eu
arch=$(dpkg --print-architecture)
case "$arch" in
  amd64)
    aws=1481becbc81c554a08aeb26178ea37809d559fbf1a9d896195b867cb754805ff
    httpfs=ce93281f076302cc9779de83bb7e4b615f9494966a38fbd295fb532fdb7104ad
    postgres_scanner=15738f9574210f5147e6203e7c51e13454bc948502ede0abcaa11e424e8ba11a
    ducklake=648101794f4adb49e72939ef37cc4a0237414eafde1f6db1ee7b180384d396bf ;;
  arm64)
    aws=fadc2c5b62962291388836031b5c5509b80cdd900fced763ca5763ef7683c8ab
    httpfs=6418402f13fb3d74da41de7aba5982fa2db7c64ec01989bd26e2f030610374f8
    postgres_scanner=74f7a03add89c5691081e0c1e369d5d951d1a707e92206ee8665667f067c9313
    ducklake=0b1eab5666b142c2abad87d9d261fb45952a403cc34b7cd2f69c1806ca4f0a52 ;;
  *) exit 1 ;;
esac
destination="/opt/duckdb/extensions/v1.5.4/linux_$arch"
mkdir -p "$destination"
for extension in httpfs postgres_scanner ducklake aws; do
  case "$extension" in
    aws) checksum=$aws ;;
    httpfs) checksum=$httpfs ;;
    postgres_scanner) checksum=$postgres_scanner ;;
    ducklake) checksum=$ducklake ;;
  esac
  curl -fsSL --retry 3 "https://extensions.duckdb.org/v1.5.4/linux_$arch/$extension.duckdb_extension.gz" -o /tmp/extension.gz
  printf '%s  /tmp/extension.gz\n' "$checksum" | sha256sum -c -
  gzip -dc /tmp/extension.gz > "$destination/$extension.duckdb_extension"
done
rm /tmp/extension.gz
