#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p artifacts
# Only this Compose project is touched. Volumes deliberately survive the test.
trap 'result=$?; if [ "$result" -ne 0 ]; then docker compose logs --no-color > artifacts/compose-failure.log 2>&1; fi; exit "$result"' EXIT
docker compose --profile test build
docker compose up -d --wait server
small_id=$(docker compose ps -q worker-small)
medium_id=$(docker compose ps -q worker-medium)
test -n "$small_id" && test -n "$medium_id" && test "$small_id" != "$medium_id"
test "$(docker inspect --format '{{.HostConfig.NanoCpus}} {{.HostConfig.Memory}}' "$small_id")" = '1000000000 1073741824'
test "$(docker inspect --format '{{.HostConfig.NanoCpus}} {{.HostConfig.Memory}}' "$medium_id")" = '2000000000 2147483648'
docker inspect --format '{"container":{{json .Name}},"nano_cpus":{{.HostConfig.NanoCpus}},"memory_bytes":{{.HostConfig.Memory}}}' "$small_id" "$medium_id" > artifacts/docker-worker-limits.jsonl
docker compose run --rm dbt
docker compose stop worker-medium
docker compose run --rm --no-deps dbt python /app/tests/harness.py --unavailable-medium
docker compose up -d --wait worker-medium
docker compose restart worker-small worker-medium server
docker compose up -d --wait server
docker compose run --rm dbt python /app/tests/harness.py --after-restart
docker compose run --rm bucket-init --verify
