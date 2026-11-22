#!/usr/bin/env bash
# One-time schema-3 upgrade for an existing bundled development database.
set -euo pipefail
source "$(dirname "$0")/common.sh"
cd "$ROOT"
assert_cluster
[[ -f artifacts/aws/values.json ]] || { echo 'Publish images first' >&2; exit 1; }

bash scripts/aws/suspend.sh
"${KUBE[@]}" get secret duckfleet-dev -o json \
  | python3 scripts/database_roles.py "$NAMESPACE" --upgrade \
  | "${KUBE[@]}" apply -f -

render=(helm template duckfleet infra/helm/duckfleet --kube-context "$KUBE_CONTEXT"
  --namespace "$NAMESPACE" -f artifacts/aws/values.json
  --set suspended=false --set legacyDatabaseUpgrade=true)
"${render[@]}" --show-only templates/postgres.yaml | "${KUBE[@]}" apply -f -
"${KUBE[@]}" rollout status statefulset/duckfleet-postgres --timeout=10m

"${KUBE[@]}" delete job duckfleet-database-provision --ignore-not-found --wait=true --timeout=2m
"${render[@]}" --show-only templates/postgres-provision.yaml | "${KUBE[@]}" apply -f -
if ! "${KUBE[@]}" wait --for=condition=complete job/duckfleet-database-provision --timeout=10m; then
  "${KUBE[@]}" get job duckfleet-database-provision \
    -o jsonpath='Database provisioning failed: {.status.failed} pod attempt(s); inspect the bounded Job without publishing its environment or logs.{ "\n" }'
  exit 1
fi
"${KUBE[@]}" delete job duckfleet-database-provision --wait=true --timeout=2m
printf '%s\n' 'Database roles provisioned; existing administrator and bootstrap credentials were preserved'
