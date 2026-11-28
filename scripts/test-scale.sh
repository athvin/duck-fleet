#!/usr/bin/env bash
# The 110-model dbt scale build on its own Compose stack. scripts/test-local.sh runs
# the same stage inline; CI runs it here instead, beside the rest of the harness,
# because it is the longest single stage and depends on nothing the others leave.
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p artifacts
trap 'result=$?; if [ "$result" -ne 0 ]; then docker compose --profile "*" logs --no-color > artifacts/compose-scale-failure.log 2>&1; fi; exit "$result"' EXIT
if [[ "${DUCKFLEET_PREBUILT_IMAGES:-}" != 1 ]]; then
  docker compose --profile test build dbt worker-small
fi
docker compose up -d --wait server
docker compose run --rm dbt duckfleet-server --scale
