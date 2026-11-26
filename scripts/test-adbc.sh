#!/usr/bin/env bash
# Real-client suite for the DuckFleet ADBC driver against Compose, through
# adbc-driver-manager 1.13.0 with pyarrow 26.0.1 and pandas 3.1.1. The driver
# is built for Linux in the pinned Rust image and loaded in a pinned uv image
# on the Compose network; nothing here needs a host Python (compat/README.md).
set -euo pipefail
trap 'echo "ADBC test failed at line $LINENO" >&2' ERR

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
network=$(docker network ls --format '{{.Name}}' | grep -m1 'duckfleet-local_default')
out="$root/artifacts/adbc-linux"
mkdir -p "$out"
docker run --rm -v "$root/server:/src:ro" -v duckfleet-adbc-target:/target -v duckfleet-adbc-registry:/usr/local/cargo/registry \
  -v "$out:/out" -e CARGO_TARGET_DIR=/target -w /src rust:1.99.0-bookworm \
  sh -c 'cargo build --locked --release -p duckfleet-adbc && cp /target/release/libadbc_duckfleet.so /out/'
docker run --rm --network "$network" \
  -e ADBC_DRIVER=/driver/libadbc_duckfleet.so -e DUCKFLEET_URL=http://server:8080 \
  -e DUCKFLEET_TOKEN="${DUCKFLEET_TOKEN:-duckfleet-local-development-token}" \
  -e PLATFORM_ADMIN_DSN="host=postgres port=5432 dbname=platform user=duckfleet password=local-administrator-password" \
  -v "$out:/driver:ro" -v "$root/compat:/compat:ro" \
  ghcr.io/astral-sh/uv:0.9.13-python3.12-bookworm-slim \
  uv run --quiet --no-project --with adbc-driver-manager==1.13.0 --with pyarrow==26.0.1 --with pandas==3.1.1 \
    --with 'psycopg[binary]==3.3.9' /compat/adbc_suite.py
printf 'PASS: ADBC driver: typed parameters and results, transactions, cancellation, 40001, metadata, compute options, and explicit NOT_IMPLEMENTED\n'
