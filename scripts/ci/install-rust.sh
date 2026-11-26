#!/usr/bin/env bash
set -euo pipefail
if ! command -v rustup > /dev/null; then
  curl --proto '=https' --tlsv1.2 -fsSL https://sh.rustup.rs -o /tmp/duckfleet-rustup.sh
  sh /tmp/duckfleet-rustup.sh -y --profile minimal --default-toolchain none
  rm /tmp/duckfleet-rustup.sh
fi
source "$HOME/.cargo/env"
rustup toolchain install 1.99.0 --profile minimal
rustup default 1.99.0
