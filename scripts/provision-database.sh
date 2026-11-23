#!/usr/bin/env bash
# Explicit PostgreSQL administrator operation, independent of Helm or compute.
# Stop old compute before adopting an existing dedicated platform/lake database.
set -euo pipefail
ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
: "${DUCKFLEET_DATABASE_ADMIN_DSN:?set administrator DSN; it is never mounted in compute or migration}"
for part in MIGRATOR GATEWAY WORKER MAINTAINER CATALOG CATALOG_READER; do
  role="DUCKFLEET_DB_${part}_ROLE"
  password="DUCKFLEET_DB_${part}_PASSWORD"
  [[ -n "${!role:-}" && -n "${!password:-}" ]] || { echo "Set $role and $password" >&2; exit 64; }
done
export DUCKFLEET_PLATFORM_DATABASE="${DUCKFLEET_PLATFORM_DATABASE:-platform}"
export DUCKFLEET_LAKE_DATABASE="${DUCKFLEET_LAKE_DATABASE:-lake}"
export DUCKFLEET_ADOPT_DATABASE_OWNER="${DUCKFLEET_ADOPT_DATABASE_OWNER:-no}"
# libpq reads the connection string from the environment; no password in argv.
export PGDATABASE="$DUCKFLEET_DATABASE_ADMIN_DSN"
exec psql -X -q --no-password --file "$ROOT/infra/helm/duckfleet/files/provision-database.sql"
