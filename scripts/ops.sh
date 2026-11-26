#!/usr/bin/env bash
set -euo pipefail
ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$ROOT"
exec cargo run --quiet --locked --manifest-path server/Cargo.toml --package duckfleet-ops -- "$@"
