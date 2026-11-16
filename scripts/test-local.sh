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
# DuckDB quotes the failing statement in its error and the process prints that error
# as it exits, so an unscrubbed ATTACH writes the catalog password to the container
# log on every boot failure. Force the failure and read the log back.
if docker compose run --rm --no-deps -T \
  -e DUCKFLEET_CATALOG_WAIT_SECONDS=0 \
  -e CATALOG_DSN='host=catalog-does-not-resolve port=5432 dbname=lake user=duckfleet password=local-catalog-password' \
  worker-small > artifacts/worker-boot-failure.log 2>&1; then
  echo 'worker booted despite an unresolvable catalog host' >&2
  exit 1
fi
# Configuration that disagrees with the container must stop the worker. Serving a
# size nobody configured would look like a correct placement in history and bill as
# the size the caller asked for, so it is refused rather than reported.
if docker compose run --rm --no-deps -T -e DUCKFLEET_WORKER_CPU=4 \
  worker-small > artifacts/worker-misconfigured.log 2>&1; then
  echo 'worker started with container limits that do not match its configuration' >&2
  exit 1
fi
grep -q 'do not match the configured' artifacts/worker-misconfigured.log
grep -q 'cpu: 1' artifacts/worker-misconfigured.log
grep -q 'cpu: 4' artifacts/worker-misconfigured.log
grep -q 'attaching the DuckLake catalog' artifacts/worker-boot-failure.log
grep -q 'still unreachable' artifacts/worker-boot-failure.log
grep -q 'catalog-does-not-resolve' artifacts/worker-boot-failure.log
! grep -q 'local-catalog-password' artifacts/worker-boot-failure.log
# A worker reached directly must refuse isolation as well. A worker *is* the shared
# compute the guarantee excludes, and workers are addressable in their own right, so
# refusing only at the gateway would leave the promise resting on nobody calling one.
# The persistent request alongside it proves the 501 is a refusal and not a worker
# that rejects everything.
session() { # session <worker> <json>
  docker compose exec -T "$1" sh -c 'curl -sS -o /dev/null -w "%{http_code}" \
    -X POST http://localhost:8080/v1/sessions \
    -H "Authorization: Bearer $DUCKFLEET_TOKEN" -H "Content-Type: application/json" \
    -d '"'$2'"
}
test "$(session worker-small '{"compute":{"compute":"isolated","cpu":1,"memory":"1Gi"}}')" = 501
test "$(session worker-small '{"compute":{"compute":"persistent","cpu":1,"memory":"1Gi"}}')" = 200
echo 'PASS: a worker addressed directly refuses isolation it cannot provide'
docker compose run --rm dbt
# Overlapping DuckLake transactions need both workers healthy, so this runs before
# any worker is stopped below.
docker compose run --rm dbt python /app/tests/harness.py --concurrency
docker compose run --rm dbt python /app/tests/harness.py --metering
# The usage ledger must reject edits in the database, not merely in application code.
for statement in 'UPDATE usage_events SET amount_micro_usd = 0' 'DELETE FROM usage_events'; do
  { docker compose exec -T postgres psql -U duckfleet -d platform -c "$statement" 2>&1 || true; } \
    | grep -q 'append-only'
done
docker compose stop worker-medium
docker compose run --rm --no-deps dbt python /app/tests/harness.py --unavailable-medium
docker compose up -d --wait worker-medium
docker compose stop worker-small worker-medium
docker compose run --rm --no-deps dbt python /app/tests/harness.py --history-without-workers
docker compose up -d --wait worker-small worker-medium
docker compose restart worker-small worker-medium server
docker compose up -d --wait server
docker compose run --rm dbt python /app/tests/harness.py --after-restart
docker compose run --rm bucket-init --verify

# A pod can start before DNS and PostgreSQL are ready. The worker has to wait rather
# than exit: a restart backoff outlives the deploy wait, which is what turned this
# race into a failed release rather than a slow one.
docker compose stop worker-small postgres
docker compose up -d --no-deps worker-small
waiting_id=$(docker compose ps -q worker-small)
# Serving but not ready, and specifically not restarting while it waits.
test "$(docker compose exec -T worker-small curl -sS -o /dev/null -w '%{http_code}' http://localhost:8080/healthz)" = 503
test "$(docker inspect -f '{{.State.Health.Status}}' "$waiting_id")" != healthy
test "$(docker inspect -f '{{.RestartCount}}' "$waiting_id")" = 0
docker compose up -d --wait postgres
docker compose up -d --wait --no-deps worker-small
# The same container became ready on its own: neither restarted nor replaced.
test "$(docker compose ps -q worker-small)" = "$waiting_id"
test "$(docker inspect -f '{{.RestartCount}}' "$waiting_id")" = 0
test "$(docker compose exec -T worker-small curl -sS -o /dev/null -w '%{http_code}' http://localhost:8080/healthz)" = 200
docker inspect -f '{"container":{{json .Name}},"restarts":{{.RestartCount}},"health":{{json .State.Health.Status}}}' "$waiting_id" > artifacts/worker-late-catalog.jsonl
docker compose up -d --wait server
echo 'PASS: worker waited for a late catalog and became ready without restarting'
