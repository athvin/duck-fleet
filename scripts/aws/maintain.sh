#!/usr/bin/env bash
# Bounds the development lake, off the deploy's critical path.
#
# The scale acceptance run (#60) deposits ~100 Parquet tables per deploy, and
# DuckLake snapshots and small files accumulate on top across runs. This runs the
# platform's own maintenance — compaction, then bounded expiry, then cleanup of
# the files that leaves unreferenced — as a best-effort step AFTER the smoke test
# has rendered its verdict and BEFORE compute is suspended. Nothing here may fail
# the deploy: the workflow marks the step continue-on-error, and this script
# exits 0 regardless, so a slow or interrupted sweep is reported and the next
# deploy continues where it left off. This is why it is not in test.sh — a
# destructive, file-deleting operation has no place in a smoke test (duck-fleet#80).
#
# Reaches the gateway over a port-forward, like the kind team-namespace probe: the
# tunnel runs through the API server, so it does not depend on the isolation
# NetworkPolicy, and no ingress needs to exist.
set -uo pipefail
source "$(dirname "$0")/common.sh"
cd "$ROOT"
assert_cluster

token=$("${KUBE[@]}" get secret duckfleet-dev -o jsonpath='{.data.DUCKFLEET_TOKEN}' | base64 -d)
"${KUBE[@]}" port-forward svc/server 18080:8080 > /dev/null 2>&1 &
forward=$!
trap 'kill "$forward" 2>/dev/null || true' EXIT
ready=false
for _ in $(seq 1 30); do
  if curl -sf -o /dev/null http://127.0.0.1:18080/healthz; then ready=true; break; fi
  sleep 1
done
if [[ "$ready" != true ]]; then
  echo 'maintain: the gateway did not answer; skipping, the next deploy retries' >&2
  exit 0
fi

# Separate statements, each under the per-statement cap, so one that runs long on
# a large lake is interrupted alone rather than taking the others with it. Compact
# first (merges small files, orphaning the originals), then expire snapshots past
# a week, then delete what no surviving snapshot references.
maintain() { # maintain <description> <json>
  local code
  code=$(curl -s -o /dev/null -w '%{http_code}' --max-time 90 \
    -X POST http://127.0.0.1:18080/v1/lake/maintenance \
    -H "Authorization: Bearer $token" -H 'Content-Type: application/json' \
    -d "$2")
  if [[ "$code" == 200 ]]; then
    echo "maintain: $1 ok"
  else
    echo "maintain: $1 did not complete (HTTP $code); the next deploy continues" >&2
  fi
}
maintain compaction '{"compact":true}'
maintain 'snapshot expiry' '{"expire_older_than_days":7}'
maintain 'file cleanup' '{"cleanup":true}'
echo 'dev lake maintenance attempted; the deploy is unaffected either way'
