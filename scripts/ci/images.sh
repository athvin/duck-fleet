#!/usr/bin/env bash
# images.sh save|load FILE
# The Compose images every harness runner needs, built once per workflow run and
# moved between runners as one zstd archive instead of being recompiled on each.
set -euo pipefail
images=(
  duckfleet-worker:local
  duckfleet-dbt:local
  duckfleet-api:local
  duckfleet-local-bucket-init
  duckfleet-local-object-store-tls
)
case "${1:-}" in
  save)
    started=$SECONDS
    docker save "${images[@]}" | zstd -T0 -3 -q -o "$2" -f
    printf 'saved %s images (%s bytes) in %ss\n' "${#images[@]}" "$(stat -c %s "$2")" "$((SECONDS - started))"
    ;;
  load)
    started=$SECONDS
    zstd -dc "$2" | docker load -q
    printf 'loaded images in %ss\n' "$((SECONDS - started))"
    ;;
  *)
    echo "usage: $0 save|load FILE" >&2
    exit 2
    ;;
esac
