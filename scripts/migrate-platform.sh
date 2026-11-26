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
    bash scripts/ops.sh compute-lifecycle check compose
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
    bash scripts/ops.sh compute-lifecycle check kube --context "$context" --namespace "$namespace"
    template=$("${kube[@]}" get deployment server \
      -o jsonpath='{.metadata.annotations.duckfleet\.io/migration-job}')
    [[ -n "$template" ]] || { echo 'No migration Job template: apply the current chart first' >&2; exit 1; }
    work=$(mktemp -d)
    trap 'rm -rf "$work"' EXIT
    "${kube[@]}" get job "$template" -o json > "$work/template.json"
    name="duckfleet-platform-manual-$(bash scripts/ops.sh random-id --short)"
    bash scripts/ops.sh migration-clone "$work/template.json" "$name" "$role" | "${kube[@]}" create -f -
    deadline=$(bash scripts/ops.sh migration-deadline "$work/template.json")
    bash scripts/ops.sh kind-await-job --context "$context" --namespace "$namespace" \
      --job "$name" --timeout "$deadline"
    ;;
  *) echo "unknown mode: $mode (compose or kube)" >&2; exit 64 ;;
esac
