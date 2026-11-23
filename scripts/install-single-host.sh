#!/usr/bin/env bash
set -euo pipefail
ROOT=$(cd "$(dirname "$0")/.." && pwd)
ENV_DIR=${DUCKFLEET_CONFIG_HOME:-"$ROOT/.duckfleet"}
ENV_FILE="$ENV_DIR/single-host.env"
command -v docker >/dev/null
command -v openssl >/dev/null
docker compose version >/dev/null
docker info >/dev/null
mkdir -p "$ENV_DIR"
chmod 700 "$ENV_DIR"
umask 077
if [[ ! -f "$ENV_FILE" ]]; then
  for variable in \
    DUCKFLEET_POSTGRES_ADMIN_PASSWORD \
    DUCKFLEET_DB_MIGRATOR_PASSWORD \
    DUCKFLEET_DB_GATEWAY_PASSWORD \
    DUCKFLEET_DB_WORKER_PASSWORD \
    DUCKFLEET_DB_MAINTAINER_PASSWORD \
    DUCKFLEET_DB_CATALOG_PASSWORD \
    DUCKFLEET_DB_CATALOG_READER_PASSWORD \
    DUCKFLEET_OBJECT_SECRET_KEY \
    DUCKFLEET_READER_OBJECT_SECRET_KEY \
    DUCKFLEET_INTERNAL_TOKEN \
    DUCKFLEET_TOKEN; do
    printf '%s=%s\n' "$variable" "$(openssl rand -hex 32)" >> "$ENV_FILE"
  done
  printf 'DUCKFLEET_OBJECT_ACCESS_KEY=%s\n' "$(openssl rand -hex 16)" >> "$ENV_FILE"
  printf 'DUCKFLEET_PORT=%s\n' "${DUCKFLEET_PORT:-18080}" >> "$ENV_FILE"
fi
chmod 600 "$ENV_FILE"
set -a
source "$ENV_FILE"
set +a
for variable in \
  DUCKFLEET_POSTGRES_ADMIN_PASSWORD \
  DUCKFLEET_DB_MIGRATOR_PASSWORD \
  DUCKFLEET_DB_GATEWAY_PASSWORD \
  DUCKFLEET_DB_WORKER_PASSWORD \
  DUCKFLEET_DB_MAINTAINER_PASSWORD \
  DUCKFLEET_DB_CATALOG_PASSWORD \
  DUCKFLEET_OBJECT_ACCESS_KEY \
  DUCKFLEET_OBJECT_SECRET_KEY \
  DUCKFLEET_INTERNAL_TOKEN \
  DUCKFLEET_TOKEN; do
  [[ ${!variable:-} =~ ^[0-9a-f]{32,64}$ ]] || {
    echo "$variable in $ENV_FILE must be a generated hexadecimal secret" >&2
    exit 1
  }
done
docker compose --project-directory "$ROOT" --project-name "${DUCKFLEET_PROJECT:-duckfleet-local}" \
  --env-file "$ENV_FILE" up -d --wait server
printf 'DuckFleet is ready at http://127.0.0.1:%s; credentials are in %s\n' \
  "${DUCKFLEET_PORT:-18080}" "$ENV_FILE"
