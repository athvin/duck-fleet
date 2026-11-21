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
python3 scripts/compute_lifecycle.py check kube --context "$context" --namespace "$ns"

python3 scripts/aws/render_boundary.py "$ns" | kubectl --context "$context" apply -f -
if ! "${kube[@]}" get secret duckfleet-dev > /dev/null 2>&1; then
  python3 - <<'PY' | "${kube[@]}" create -f -
import json, secrets
password = secrets.token_hex(32)
dsn = f'host=postgres port=5432 user=duckfleet password={password}'
print(json.dumps({'apiVersion': 'v1', 'kind': 'Secret',
                  'metadata': {'name': 'duckfleet-dev'}, 'type': 'Opaque',
                  'stringData': {'POSTGRES_PASSWORD': password,
                                 'DUCKFLEET_TOKEN': secrets.token_hex(32),
                                 'CATALOG_DSN': dsn + ' dbname=lake',
                                 'PLATFORM_DSN': dsn + ' dbname=platform'}}))
PY
fi
migration_run=$(python3 -c 'import uuid; print(uuid.uuid4().hex)')
helm template duckfleet infra/helm/duckfleet --namespace "$ns" \
  --set "lakePrefix=$ns/lake" --set-string "platformMigration.runId=$migration_run" "$@" \
  | sh -c "$transform" \
  | "${kube[@]}" apply -f -
python3 scripts/aws/wait-rollout.py --context "$context" --namespace "$ns" --prune-migrations
echo "provisioned $ns: its own boundary, catalog, credentials, and lake prefix $ns/lake"
