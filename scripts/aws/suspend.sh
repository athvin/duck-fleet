#!/usr/bin/env bash
# Retain catalog PVC, S3 objects and release configuration; stop dev compute.
set -euo pipefail
source "$(dirname "$0")/common.sh"
cd "$ROOT"
assert_cluster
# A canceled test must not keep its harness pod running after the servers stop.
"${KUBE[@]}" delete job duckfleet-harness --ignore-not-found --wait=true --timeout=2m
helm upgrade duckfleet infra/helm/duckfleet --kube-context "$KUBE_CONTEXT" \
  --namespace "$NAMESPACE" --reuse-values --set suspended=true --wait --timeout 10m
# Helm considers zero desired replicas ready before old pods necessarily exit.
"${KUBE[@]}" wait --for=delete pod -l 'app in (duckfleet-worker,duckfleet-gateway,duckfleet-postgres)' --timeout=5m
