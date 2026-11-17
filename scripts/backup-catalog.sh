#!/usr/bin/env bash
# Dumps both DuckFleet databases — `lake` (the DuckLake catalog) and `platform`
# (query history, usage ledger, budget) — as PostgreSQL custom-format archives.
#
# The catalog IS the lake: every Parquet object in the bucket is unreachable without
# the catalog rows that name it. S3 holds the data durably; this holds the map.
# Scheduling and retention are the operator's policy — run it from cron or by hand,
# and the age of the newest directory you keep is the recovery point you can claim.
# The two databases are dumped one after the other, each internally consistent; the
# instants differ by the dump gap, so history may lead or trail the catalog by it.
#
#   scripts/backup-catalog.sh compose <output-dir>
#   scripts/backup-catalog.sh kube <output-dir>   # needs DUCKFLEET_KUBE_CONTEXT
#                                                 # and DUCKFLEET_NAMESPACE
set -euo pipefail
mode="${1:?usage: backup-catalog.sh <compose|kube> <output-dir>}"
out="${2:?usage: backup-catalog.sh <compose|kube> <output-dir>}"
mkdir -p "$out"

dump() { # dump <database>
  case "$mode" in
    compose)
      docker compose exec -T postgres pg_dump -U duckfleet -Fc "$1" ;;
    kube)
      kubectl --context "${DUCKFLEET_KUBE_CONTEXT:?set DUCKFLEET_KUBE_CONTEXT}" \
        --namespace "${DUCKFLEET_NAMESPACE:?set DUCKFLEET_NAMESPACE}" \
        exec statefulset/duckfleet-postgres -- pg_dump -U duckfleet -Fc "$1" ;;
    *)
      echo "unknown mode: $mode (compose or kube)" >&2; exit 64 ;;
  esac > "$out/$1.dump"
  test -s "$out/$1.dump"
}

dump lake
dump platform
date -u +%Y-%m-%dT%H:%M:%SZ > "$out/taken-at.txt"
echo "backed up lake and platform to $out at $(cat "$out/taken-at.txt")"
