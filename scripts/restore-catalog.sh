#!/usr/bin/env bash
# Restores both databases from a backup-catalog.sh directory into the freshly
# initialised PostgreSQL the init script leaves behind: an empty `lake` and
# `platform`. Workers and the gateway must be stopped first — they hold
# connections — and started after; their startup migrations are idempotent
# against the restored schema.
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

restore() { # restore <database>
  case "$mode" in
    compose)
      docker compose exec -T postgres pg_restore -U duckfleet --no-owner -d "$1" ;;
    kube)
      kubectl --context "${DUCKFLEET_KUBE_CONTEXT:?set DUCKFLEET_KUBE_CONTEXT}" \
        --namespace "${DUCKFLEET_NAMESPACE:?set DUCKFLEET_NAMESPACE}" \
        exec -i statefulset/duckfleet-postgres -- pg_restore -U duckfleet --no-owner -d "$1" ;;
    *)
      echo "unknown mode: $mode (compose or kube)" >&2; exit 64 ;;
  esac < "$src/$1.dump"
}

restore lake
restore platform
echo "restored lake and platform from $src, taken $(cat "$src/taken-at.txt" 2>/dev/null || echo 'at an unrecorded time')"
