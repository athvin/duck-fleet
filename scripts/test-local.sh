#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p artifacts
# Only this Compose project is touched. Volumes deliberately survive the test.
trap 'result=$?; if [ "$result" -ne 0 ]; then docker compose --profile "*" logs --no-color > artifacts/compose-failure.log 2>&1; fi; exit "$result"' EXIT
python3 scripts/compute_lifecycle.py check compose
python3 scripts/test-database-roles.py
docker compose --profile test build
docker compose up -d --wait server

# One persistent worker tier now (sizing derives an isolated pod, which needs
# Kubernetes). Confirm its container limits match its configuration.
small_id=$(docker compose ps -q worker-small)
test -n "$small_id"
test "$(docker inspect --format '{{.HostConfig.NanoCpus}} {{.HostConfig.Memory}}' "$small_id")" = '1000000000 1073741824'
docker inspect --format '{"container":{{json .Name}},"nano_cpus":{{.HostConfig.NanoCpus}},"memory_bytes":{{.HostConfig.Memory}}}' "$small_id" > artifacts/docker-worker-limits.jsonl

# Catalog role validation probes PostgreSQL before DuckDB ATTACH. Force that
# earlier startup failure and verify its bounded, host-only diagnostic never
# writes the configured DSN or password to the container log. The redactor's
# unit tests separately cover ATTACH errors that quote the connection string.
if docker compose run --rm --no-deps -T \
  -e DUCKFLEET_CATALOG_WAIT_SECONDS=0 \
  -e CATALOG_DSN='host=catalog-does-not-resolve port=5432 dbname=lake user=duckfleet password=local-catalog-password' \
  worker-small > artifacts/worker-boot-failure.log 2>&1; then
  echo 'worker booted despite an unresolvable catalog host' >&2
  exit 1
fi
# Configuration that disagrees with the container must stop the worker.
if docker compose run --rm --no-deps -T -e DUCKFLEET_WORKER_CPU=4 \
  worker-small > artifacts/worker-misconfigured.log 2>&1; then
  echo 'worker started with container limits that do not match its configuration' >&2
  exit 1
fi
grep -q 'do not match the configured' artifacts/worker-misconfigured.log
grep -q 'cpu=1' artifacts/worker-misconfigured.log
grep -q 'cpu=4' artifacts/worker-misconfigured.log
grep -q 'catalog role validation' artifacts/worker-boot-failure.log
grep -q 'still unreachable after 0s' artifacts/worker-boot-failure.log
grep -q 'catalog-does-not-resolve' artifacts/worker-boot-failure.log
! grep -q 'local-catalog-password' artifacts/worker-boot-failure.log
! grep -q 'host=catalog-does-not-resolve port=5432' artifacts/worker-boot-failure.log

# A deleted pod's replacement can overlap its draining predecessor even under
# Deployment Recreate. Startup recovery must wait for the stable identity owner.
if docker compose run --rm --no-deps -T -e DUCKFLEET_CATALOG_WAIT_SECONDS=1 \
  -e DUCKFLEET_PLATFORM_WAIT_SECONDS=1 \
  worker-small > artifacts/worker-overlap-refusal.log 2>&1; then
  echo 'a second live process acquired the same worker recovery identity' >&2
  exit 1
fi
grep -q 'still owned by another process' artifacts/worker-overlap-refusal.log
echo 'PASS: overlapping worker startup refused recovery of a live predecessor'

# A worker only accepts internal service authority. Public bootstrap recovery
# belongs to the gateway and must never become a direct SQL-worker login.
status=$(docker compose exec -T -e PUBLIC_TEST_TOKEN="${DUCKFLEET_TOKEN:-duckfleet-local-development-token}" worker-small sh -c '
  curl -sS -o /dev/null -w "%{http_code}" -X POST http://localhost:8080/v1/sessions \
    -H "Authorization: Bearer $PUBLIC_TEST_TOKEN" -H "Content-Type: application/json" -d "{}"')
test "$status" = 401
echo 'PASS: public bootstrap cannot authenticate directly to a worker'

# dbt must be an option, not a dependency. The `api` image has no dbt binary and
# reaches the gateway only through the Rust client (the harness --api-workflow stage).
docker compose run --rm api
docker compose run --rm dbt duckfleet-server --rbac-storage
docker compose run --rm dbt duckfleet-server --rbac
docker compose run --rm dbt duckfleet-server --system-storage
docker compose run --rm dbt duckfleet-server --system
docker compose run --rm dbt duckfleet-server --jobs-storage
docker compose run --rm dbt duckfleet-server --jobs
# Fail only artifact publication after successful SQL; catalog/lake IO remains
# healthy. Restore the ordinary worker configuration before subsequent stages.
cat > artifacts/jobs-store-failure.override.yaml <<'EOF'
services:
  worker-small:
    environment:
      DUCKFLEET_RESULT_ENDPOINT: http://127.0.0.1:9
EOF
docker compose -f compose.yaml -f artifacts/jobs-store-failure.override.yaml up -d --no-deps --wait worker-small
docker compose run --rm --no-deps dbt duckfleet-server --jobs-store-failure
docker compose up -d --no-deps --wait worker-small
# A detached submitter reconnects by durable ID while the gateway is replaced.
# Only this project is restarted; the SQL worker keeps its accepted execution.
rm -f artifacts/jobs-long.started
jobs_long_container=$(docker compose run -d --no-deps dbt duckfleet-server --jobs-long)
jobs_long_ready=false
for attempt in $(seq 1 120); do
  if [ -f artifacts/jobs-long.started ]; then jobs_long_ready=true; break; fi
  if [ "$(docker inspect -f '{{.State.Running}}' "$jobs_long_container")" != true ]; then break; fi
  sleep 1
done
if [ "$jobs_long_ready" != true ]; then
  docker logs "$jobs_long_container" > artifacts/jobs-long.log 2>&1
  echo 'long job never reached execution' >&2
  exit 1
fi
docker compose restart --no-deps server
jobs_long_status=$(docker wait "$jobs_long_container")
docker logs "$jobs_long_container" > artifacts/jobs-long.log 2>&1
docker rm "$jobs_long_container" > /dev/null
cat artifacts/jobs-long.log
test "$jobs_long_status" = 0
docker compose run --rm dbt duckfleet-server --auth-continuity
docker compose run --rm dbt duckfleet-server --request-identity
docker compose run --rm dbt duckfleet-server --platform-maintenance
docker compose run --rm dbt duckfleet-server --queue-storage
docker compose run --rm dbt duckfleet-server --ownership-loss
# Ownership loss is terminal for that worker process. Start a fresh owner before
# the ordinary query/dbt workflow; the gateway's metadata reads stayed available.
docker compose up -d --wait worker-small server
# The dbt lifecycle: the `dbt` image runs the Rust integration harness
# (DUCKFLEET_ROLE=harness), which drives the forked dbt binary. Default = main
# lifecycle; then the stages that need both the gateway and a fresh fleet.
docker compose run --rm dbt
docker compose run --rm dbt duckfleet-server --scale
docker compose run --rm dbt duckfleet-server --concurrency
docker compose run --rm dbt duckfleet-server --metering

# The Postgres wire front: psql speaks to the warehouse with a platform credential.
PGTOKEN="${DUCKFLEET_TOKEN:-duckfleet-local-development-token}"
pgw() { docker compose exec -T -e PGPASSWORD="$PGTOKEN" postgres \
  psql -h server -p 5432 -U operator -d lake -v ON_ERROR_STOP=1 -qtAX -c "$1"; }
test "$(pgw 'select 1 + 1')" = 2
test "$(pgw "select 42::bigint, 'text value', true, 1.5::double")" = '42|text value|t|1.5'
test "$(pgw "select date '2026-01-02', timestamp '2026-01-02 03:04:05'")" = '2026-01-02|2026-01-02 03:04:05.000000'
test "$(pgw "select 12.34::decimal(6,2)")" = '12.34'
if pgw 'select * from no_such_table_for_pgwire' 2> artifacts/pgwire-error.txt; then
  echo 'a missing table did not fail over the wire' >&2; exit 1
fi
grep -q 'sql_error' artifacts/pgwire-error.txt
if printf 'select $1::int \\bind 7 \\g\n' | docker compose exec -T -e PGPASSWORD="$PGTOKEN" postgres \
  psql -h server -p 5432 -U operator -d lake -v ON_ERROR_STOP=1 -qtAX 2> artifacts/pgwire-bind.txt; then
  echo 'a bind parameter was not refused' >&2; exit 1
fi
grep -q 'not supported' artifacts/pgwire-bind.txt
if docker compose exec -T -e PGPASSWORD=not-a-credential postgres \
  psql -h server -p 5432 -U operator -d lake -c 'select 1' > /dev/null 2>&1; then
  echo 'the wire front accepted a bad credential' >&2; exit 1
fi
echo 'PASS: psql spoke to the warehouse through the wire front — typed results, classified errors, clean refusals'

# Every execution worker is stopped; metadata SQL and existing HTTP diagnostics
# must still work. --no-deps prevents the harness from restarting compute.
docker compose stop worker-small
docker compose run --rm --no-deps dbt duckfleet-server --system-without-workers
docker compose up -d --wait worker-small server

# The usage ledger must reject edits in the database, not merely in application code.
for statement in 'UPDATE usage_events SET amount_micro_usd = 0' 'DELETE FROM usage_events'; do
  { docker compose exec -T postgres psql -U duckfleet -d platform -c "$statement" 2>&1 || true; } \
    | grep -q 'append-only'
done
echo 'PASS: the usage ledger refuses edits in the database'

# Two persistent siblings exercise worker-reported placement across protocols.
# --no-deps keeps later harness invocations from bringing the deliberately stopped
# sibling back, or recreating the gateway with its default single-worker list.
docker compose --profile scale-out up -d --wait worker-small-b
DUCKFLEET_WORKER_URLS=http://worker-small:8080,http://worker-small-b:8080 \
  docker compose up -d --no-deps --wait server
docker compose run --rm --no-deps dbt duckfleet-server --spread
docker compose run --rm --no-deps dbt duckfleet-server --request-identity-concurrent
docker compose run --rm --no-deps dbt duckfleet-server --worker-status-pgwire
docker compose stop worker-small
docker compose run --rm --no-deps dbt duckfleet-server --worker-status-partial-outage
docker compose run --rm --no-deps dbt duckfleet-server --spread-failover
# Restore the normal topology for inspection and the next local run.
docker compose up -d --wait worker-small server
docker compose --profile scale-out stop worker-small-b

# Named persistent allocations share the same database but resolve independently
# of isolated resource requests. A private config copy lets the harness reload the
# default while a transaction/temp table remains pinned, without editing examples.
mkdir -p artifacts/pools-config
cp examples/pools/compose.json artifacts/pools-config/compose.json
export DUCKFLEET_POOLS_DIRECTORY=./artifacts/pools-config
docker compose --profile pools up -d --wait server-pools
interactive_id=$(docker compose --profile pools ps -q worker-pool-interactive-0)
test "$(docker inspect --format '{{.HostConfig.NanoCpus}} {{.HostConfig.Memory}}' "$interactive_id")" = '2000000000 3221225472'
docker compose run --rm --no-deps \
  -e DUCKFLEET_URL=http://server-pools:8080 \
  -e DUCKFLEET_POOL_TEST_FILE=/artifacts/pools-config/compose.json \
  dbt duckfleet-server --pools
# Includes the single production-length disconnect test; Kind exercises a shorter
# isolated/cap/recovery subset so CI does not pay for the same long wait twice.
docker compose run --rm --no-deps \
  -e DUCKFLEET_URL=http://server-pools:8080 \
  -e DUCKFLEET_POOL_TEST_FILE=/artifacts/pools-config/compose.json \
  dbt duckfleet-server --queue
docker compose --profile pools stop server-pools worker-pool-interactive-0
unset DUCKFLEET_POOLS_DIRECTORY

# ---------------------------------------------------------------------------
# PENDING MIGRATION (tracked for a dedicated follow-up — these stages are deeply
# coupled to the pre-rework 2-worker fleet and the Python client, and need
# re-expressing against the single persistent tier + the Rust harness/cli:
#   - worker late-catalog wait, worker ledger-reconnect
#   - SIGTERM drain of the worker and of the gateway (--drain-inflight /
#     --gateway-drain-inflight harness stages + docker kill)
#   - maintenance (--maintenance)
#   - the backup/restore drill (scripts/backup-catalog.sh / restore-catalog.sh,
#     with the post-backup write + restore verification now via `duckfleet-server cli`)
# The isolated-pod path (a sized model -> dedicated pod) is Kubernetes-only and is
# exercised by scripts/test-k8s.sh, not here.
# ---------------------------------------------------------------------------
echo 'PASS: compose core path (worker limits, secret scrub, direct-worker isolation refusal, dbt-free api proof, dbt lifecycle, concurrency, metering, pgwire, ledger immutability, busy-worker status, mixed-protocol routing, partial outage, persistent pools and reload)'
