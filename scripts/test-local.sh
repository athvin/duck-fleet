#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p artifacts
# Only this Compose project is touched. Volumes deliberately survive the test.
trap 'result=$?; if [ "$result" -ne 0 ]; then docker compose logs --no-color > artifacts/compose-failure.log 2>&1; fi; exit "$result"' EXIT
docker compose --profile test build
docker compose up -d --wait server
docker compose run --rm dbt
docker compose restart server
docker compose up -d --wait server
docker compose run --rm dbt python /app/tests/harness.py --after-restart
docker compose run --rm bucket-init --verify
