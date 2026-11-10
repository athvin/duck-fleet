#!/usr/bin/env bash
set -euo pipefail
archive=$(mktemp -d)
trap 'rm -rf "$archive"' EXIT
curl -fsSL https://get.helm.sh/helm-v3.19.0-linux-amd64.tar.gz -o "$archive/helm.tar.gz"
curl -fsSL https://get.helm.sh/helm-v3.19.0-linux-amd64.tar.gz.sha256sum -o "$archive/checksum"
# The official checksum includes the published filename.
expected=$(cut -d ' ' -f 1 "$archive/checksum")
printf '%s  %s\n' "$expected" "$archive/helm.tar.gz" | sha256sum -c -
sudo tar xzf "$archive/helm.tar.gz" --strip-components=1 -C /usr/local/bin linux-amd64/helm
python3 -m venv .venv
.venv/bin/pip install PyYAML==6.0.3
printf '%s/.venv/bin\n' "$PWD" >> "$GITHUB_PATH"
