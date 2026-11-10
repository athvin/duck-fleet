#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/common.sh"
cd "$ROOT"
assert_cluster
mkdir -p artifacts/aws
# Capture independently of what the server reports about itself.
"${KUBE[@]}" get pods -o json > artifacts/aws/pods.json
status=0
helm test duckfleet --kube-context "$KUBE_CONTEXT" --namespace "$NAMESPACE" --timeout 22m || status=$?
"${KUBE[@]}" logs job/duckfleet-harness -c dbt > artifacts/aws/harness.log || status=1
"${KUBE[@]}" get events --sort-by=.lastTimestamp > artifacts/aws/events.txt
[[ "$status" == 0 ]] || { cat artifacts/aws/harness.log; exit "$status"; }
"${KUBE[@]}" get pods -o json > artifacts/aws/pods-after.json
python3 scripts/aws/verify.py
