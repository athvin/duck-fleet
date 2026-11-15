#!/usr/bin/env bash
# Installs kind for the credential-free Kubernetes harness. Checksum-verified
# against the published sum, matching how helm is installed for the deploy lane.
set -euo pipefail
version=v0.30.0
download=$(mktemp -d)
trap 'rm -rf "$download"' EXIT
base="https://github.com/kubernetes-sigs/kind/releases/download/$version"
curl -fsSL "$base/kind-linux-amd64" -o "$download/kind"
curl -fsSL "$base/kind-linux-amd64.sha256sum" -o "$download/checksum"
expected=$(cut -d ' ' -f 1 "$download/checksum")
printf '%s  %s\n' "$expected" "$download/kind" | sha256sum -c -
sudo install -m 0755 "$download/kind" /usr/local/bin/kind
kind --version
