#!/usr/bin/env bash
# run-suites.sh SHARD TARGET... : runs each make target in order, stopping at the
# first failure, and appends one row per target to artifacts/ci-timings.csv
# (shard,target,seconds,status) so every run leaves comparable timing data.
set -uo pipefail
shard=$1
shift
mkdir -p artifacts
timings=artifacts/ci-timings.csv
[[ -f "$timings" ]] || echo 'shard,target,seconds,status' > "$timings"
for target in "$@"; do
  echo "::group::make $target"
  started=$SECONDS
  make "$target"
  status=$?
  seconds=$((SECONDS - started))
  echo "::endgroup::"
  echo "$shard,$target,$seconds,$status" >> "$timings"
  printf '%s: make %s took %ss (exit %s)\n' "$shard" "$target" "$seconds" "$status"
  if [[ "$status" -ne 0 ]]; then
    echo "::error::make $target failed in shard $shard"
    exit "$status"
  fi
done
