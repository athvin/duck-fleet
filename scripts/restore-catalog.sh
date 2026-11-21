#!/usr/bin/env bash
# Restores both databases from a backup-catalog.sh directory into the freshly
# initialised PostgreSQL the init script leaves behind: an empty `lake` and
# `platform`. Compute is stopped before restoration and remains stopped on failure.
# The explicit migration runs against the restored schema before compute may resume.
# Start the desired workers/gateway after this command succeeds.
#
# Destructive by nature: everything the catalog learned after the backup was
# taken is gone afterwards, which is what restoring to a point in time means.
# It refuses to run unless that is said out loud:
#
#   DUCKFLEET_CONFIRM_RESTORE=yes scripts/restore-catalog.sh <compose|kube> <dir>
set -euo pipefail
mode="${1:?usage: restore-catalog.sh <compose|kube> <backup-dir>}"
src="${2:?usage: restore-catalog.sh <compose|kube> <backup-dir>}"
if [ "${DUCKFLEET_CONFIRM_RESTORE:-}" != yes ]; then
  echo "set DUCKFLEET_CONFIRM_RESTORE=yes to restore: everything committed after" \
    "the backup in $src will be discarded" >&2
  exit 64
fi
test -s "$src/lake.dump" && test -s "$src/platform.dump"
ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
case "$mode" in
  compose)
    bash "$ROOT/scripts/stop-compute.sh" compose
    ;;
  kube)
    kube=(kubectl --context "${DUCKFLEET_KUBE_CONTEXT:?set DUCKFLEET_KUBE_CONTEXT}"
          --namespace "${DUCKFLEET_NAMESPACE:?set DUCKFLEET_NAMESPACE}")
    # Resolve the migration template before any mutation; old charts must be
    # upgraded before they can restore without worker-owned startup migrations.
    template=$("${kube[@]}" get deployment server \
      -o jsonpath='{.metadata.annotations.duckfleet\.io/migration-job}')
    [[ -n "$template" ]] || { echo 'Apply the current chart before restoring' >&2; exit 1; }
    version=$("${kube[@]}" get job "$template" \
      -o jsonpath='{.metadata.annotations.duckfleet\.io/platform-schema}')
    [[ "$version" == 4 ]] || { echo 'Stop compute and apply the schema 4 chart/image before restoring' >&2; exit 1; }
    bash "$ROOT/scripts/stop-compute.sh" kube
    ;;
  *) echo "unknown mode: $mode (compose or kube)" >&2; exit 64 ;;
esac

restore() { # restore <database>
  # --role restores objects under their dedicated database owner. Old backup ACLs
  # are discarded; the matching migration reinstalls the reviewed runtime grants.
  local owner
  case "$mode" in
    compose) owner=$(docker compose exec -T postgres psql -U duckfleet -d "$1" -tAc 'SELECT pg_get_userbyid(datdba) FROM pg_database WHERE datname=current_database()') ;;
    kube) owner=$("${kube[@]}" exec statefulset/duckfleet-postgres -- psql -U duckfleet -d "$1" -tAc 'SELECT pg_get_userbyid(datdba) FROM pg_database WHERE datname=current_database()') ;;
  esac
  [[ "$owner" != duckfleet && "$owner" =~ ^[a-z_][a-z0-9_]*$ ]] || { echo 'Provision separate database owners before restoring' >&2; return 1; }
  case "$mode" in
    compose)
      docker compose exec -T postgres pg_restore --exit-on-error -U duckfleet --role="$owner" --no-owner --no-privileges -d "$1" ;;
    kube)
      kubectl --context "${DUCKFLEET_KUBE_CONTEXT:?set DUCKFLEET_KUBE_CONTEXT}" \
        --namespace "${DUCKFLEET_NAMESPACE:?set DUCKFLEET_NAMESPACE}" \
        exec -i statefulset/duckfleet-postgres -- pg_restore --exit-on-error -U duckfleet --role="$owner" --no-owner --no-privileges -d "$1" ;;
    *)
      echo "unknown mode: $mode (compose or kube)" >&2; exit 64 ;;
  esac < "$src/$1.dump"
}

restore lake
restore platform
bash "$ROOT/scripts/migrate-platform.sh" "$mode"
bash "$ROOT/scripts/migrate-platform.sh" "$mode" platform-grants
echo 'Platform schema migrated successfully; compute remains stopped and may now be restarted.'
echo "restored lake and platform from $src, taken $(cat "$src/taken-at.txt" 2>/dev/null || echo 'at an unrecorded time')"
