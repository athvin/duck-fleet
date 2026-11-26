#!/usr/bin/env bash
# Provisions a team/environment namespace: its own ownership boundary, its own
# PostgreSQL with fresh credentials, its own bootstrap token, and its own lake
# prefix — a whole DuckLake, isolated from every other namespace by construction
# rather than by configuration discipline. Operator-run, because the boundary
# needs cluster admin, which the deploy identity deliberately is not.
#
#   DUCKFLEET_KUBE_CONTEXT=<ctx> scripts/provision-namespace.sh <namespace> [helm --set args...]
#
# Idempotent and safe to re-run: boundary and release applies converge, and the
# credential Secret is created only when absent, so a re-run never rotates
# passwords out from under a running fleet. DUCKFLEET_PROVISION_TRANSFORM pipes
# the rendered release through a filter before it is applied — the kind harness
# localizes with it; a real cluster leaves it unset.
#
# On AWS, the worker IAM policy for the new namespace's prefix is provisioned
# with whoever owns IAM, not here: that policy is the storage half of the
# isolation this script sets up.
set -euo pipefail
ns="${1:?usage: provision-namespace.sh <namespace> [helm --set args...]}"
shift
context="${DUCKFLEET_KUBE_CONTEXT:?set DUCKFLEET_KUBE_CONTEXT}"
transform="${DUCKFLEET_PROVISION_TRANSFORM:-cat}"
ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$ROOT"
kube=(kubectl --context "$context" -n "$ns")

# A rerun can be an upgrade. Check before applying a Job that changes schema.
bash scripts/ops.sh compute-lifecycle check kube --context "$context" --namespace "$ns"

bash scripts/ops.sh aws-render-boundary "$ns" | kubectl --context "$context" apply -f -
if ! "${kube[@]}" get secret duckfleet-dev > /dev/null 2>&1; then
  bash scripts/ops.sh database-roles "$ns" | "${kube[@]}" create -f -
fi
"${kube[@]}" get secret duckfleet-dev -o json | bash scripts/ops.sh database-roles "$ns" --check
migration_run=$(bash scripts/ops.sh random-id)
helm template duckfleet infra/helm/duckfleet --namespace "$ns" \
  --set secretName=duckfleet-dev \
  --set "lakePrefix=$ns/lake" --set "results.prefix=$ns/results" --set-string "platformMigration.runId=$migration_run" "$@" \
  | sh -c "$transform" \
  | "${kube[@]}" apply -f -
if ! bash scripts/ops.sh aws-wait-rollout --context "$context" --namespace "$ns" --prune-migrations; then
  "${kube[@]}" get pods -o wide || true
  "${kube[@]}" logs -l app=duckfleet-platform-migrate --all-containers --prefix --tail=200 || true
  exit 1
fi
echo "provisioned $ns: its own boundary, catalog, credentials, and lake prefix $ns/lake"
