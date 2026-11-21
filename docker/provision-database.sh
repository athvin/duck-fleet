#!/usr/bin/env bash
# Shared entrypoint for fresh bundled PostgreSQL and the Compose provision task.
set -euo pipefail
export DUCKFLEET_PLATFORM_DATABASE="${DUCKFLEET_PLATFORM_DATABASE:-platform}"
export DUCKFLEET_LAKE_DATABASE="${DUCKFLEET_LAKE_DATABASE:-lake}"
export DUCKFLEET_ADOPT_DATABASE_OWNER="${DUCKFLEET_ADOPT_DATABASE_OWNER:-no}"
exec psql -X -q --no-password --username "${POSTGRES_USER:-duckfleet}" --dbname postgres \
  --file /etc/duckfleet/provision-database.sql
