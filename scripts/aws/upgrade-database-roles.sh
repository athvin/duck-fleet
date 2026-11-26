#!/usr/bin/env bash
# One-time schema-3 upgrade for an existing bundled development database.
set -euo pipefail
source "$(dirname "$0")/common.sh"
cd "$ROOT"
assert_cluster
[[ -f artifacts/aws/values.json ]] || { echo 'Publish images first' >&2; exit 1; }

bash scripts/aws/suspend.sh
"${KUBE[@]}" get secret duckfleet-dev -o json \
  | bash scripts/ops.sh database-roles "$NAMESPACE" --upgrade \
  | "${KUBE[@]}" apply -f -

render=(helm template duckfleet infra/helm/duckfleet --kube-context "$KUBE_CONTEXT"
  --namespace "$NAMESPACE" -f infra/helm/duckfleet/values-aws.yaml -f artifacts/aws/values.json
  --set suspended=false --set legacyDatabaseUpgrade=true)
"${render[@]}" --show-only templates/postgres.yaml | "${KUBE[@]}" apply -f -
"${KUBE[@]}" rollout status statefulset/duckfleet-postgres --timeout=10m

"${KUBE[@]}" delete job duckfleet-database-provision --ignore-not-found --wait=true --timeout=2m
"${render[@]}" --show-only templates/postgres-provision.yaml | "${KUBE[@]}" apply -f -
completed=false
for ((attempt=0; attempt<300; attempt++)); do
  if [[ $("${KUBE[@]}" get job duckfleet-database-provision \
    -o jsonpath='{.status.conditions[?(@.type=="Complete")].status}') == True ]]; then
    completed=true
    break
  fi
  if [[ $("${KUBE[@]}" get job duckfleet-database-provision \
    -o jsonpath='{.status.conditions[?(@.type=="Failed")].status}') == True ]]; then
    break
  fi
  sleep 2
done
if [[ $completed != true ]]; then
  reason=$("${KUBE[@]}" get pods -l job-name=duckfleet-database-provision \
    -o jsonpath='{.items[*].status.containerStatuses[*].state.terminated.reason}')
  diagnostic=$("${KUBE[@]}" get pods -l job-name=duckfleet-database-provision \
    -o jsonpath='{.items[*].status.containerStatuses[*].state.terminated.message}')
  printf 'Database provisioning failed or timed out: reason=%s diagnostic=%s\n' \
    "${reason:-unavailable}" "${diagnostic:-no PostgreSQL error diagnostic}"
  exit 1
fi
"${KUBE[@]}" delete job duckfleet-database-provision --wait=true --timeout=2m
printf '%s\n' 'Database roles provisioned; existing administrator and bootstrap credentials were preserved'
