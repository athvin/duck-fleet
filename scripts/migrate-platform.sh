#!/usr/bin/env bash
# Run a fresh migration even when the image and database Secret name are unchanged.
# A restore/retarget must use this, or a new Helm platformMigration.runId; an old
# completed Job only proves that the database it saw at that time was migrated.
set -euo pipefail
mode="${1:?usage: migrate-platform.sh <compose|kube> [migrate|platform-grants]}"
role="${2:-migrate}"
[[ "$role" == migrate || "$role" == platform-grants ]] || { echo 'Expected migrate or platform-grants' >&2; exit 64; }
ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$ROOT"
case "$mode" in
  compose)
    python3 scripts/compute_lifecycle.py check compose
    if [[ "$role" == migrate ]]; then
      docker compose run --rm --no-deps platform-migrate
    else
      docker compose run --rm --no-deps -e DUCKFLEET_ROLE=platform-grants platform-migrate
    fi
    ;;
  kube)
    context="${DUCKFLEET_KUBE_CONTEXT:?set DUCKFLEET_KUBE_CONTEXT}"
    namespace="${DUCKFLEET_NAMESPACE:?set DUCKFLEET_NAMESPACE}"
    kube=(kubectl --context "$context" -n "$namespace")
    python3 scripts/compute_lifecycle.py check kube --context "$context" --namespace "$namespace"
    template=$("${kube[@]}" get deployment server \
      -o jsonpath='{.metadata.annotations.duckfleet\.io/migration-job}')
    [[ -n "$template" ]] || { echo 'No migration Job template: apply the current chart first' >&2; exit 1; }
    work=$(mktemp -d)
    trap 'rm -rf "$work"' EXIT
    "${kube[@]}" get job "$template" -o json > "$work/template.json"
    name="duckfleet-platform-manual-$(python3 -c 'import uuid; print(uuid.uuid4().hex[:12])')"
    python3 - "$work/template.json" "$name" "$role" <<'PY' | "${kube[@]}" create -f -
import json, sys
source = json.load(open(sys.argv[1]))
if source.get('metadata', {}).get('annotations', {}).get('duckfleet.io/platform-schema') != '8':
    sys.exit('Current migration template predates schema 7; stop compute and apply the desired chart/image first')
spec = source['spec']
for container in spec['template']['spec']['containers']:
    for item in container.get('env', []):
        if item['name'] == 'DUCKFLEET_ROLE':
            item['value'] = sys.argv[3]
for field in ('selector', 'manualSelector'):
    spec.pop(field, None)
metadata = spec['template']['metadata']
metadata.pop('creationTimestamp', None)
for label in ('controller-uid', 'job-name', 'batch.kubernetes.io/controller-uid',
              'batch.kubernetes.io/job-name'):
    metadata.get('labels', {}).pop(label, None)
# Keep failure diagnostics for a day; this ad-hoc Job is not the desired release Job.
spec['ttlSecondsAfterFinished'] = 86400
print(json.dumps({'apiVersion': 'batch/v1', 'kind': 'Job',
                  'metadata': {'name': sys.argv[2], 'labels': {'app': 'duckfleet-platform-migrate'},
                               'annotations': {'duckfleet.io/platform-schema': '8'}},
                  'spec': spec}))
PY
    deadline=$(python3 - "$work/template.json" <<'PY'
import json,sys
print(json.load(open(sys.argv[1]))['spec']['activeDeadlineSeconds'] + 30)
PY
)
    python3 scripts/kind/await_job.py --context "$context" --namespace "$namespace" \
      --job "$name" --timeout "$deadline"
    ;;
  *) echo "unknown mode: $mode (compose or kube)" >&2; exit 64 ;;
esac
