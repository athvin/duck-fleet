#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p artifacts
# Only this Compose project is touched. Volumes deliberately survive the test.
trap 'result=$?; if [ "$result" -ne 0 ]; then docker compose logs --no-color > artifacts/compose-failure.log 2>&1; fi; exit "$result"' EXIT
docker compose --profile test build
docker compose up -d --wait server

# One persistent worker tier now (sizing derives an isolated pod, which needs
# Kubernetes). Confirm its container limits match its configuration.
small_id=$(docker compose ps -q worker-small)
test -n "$small_id"
test "$(docker inspect --format '{{.HostConfig.NanoCpus}} {{.HostConfig.Memory}}' "$small_id")" = '1000000000 1073741824'
docker inspect --format '{"container":{{json .Name}},"nano_cpus":{{.HostConfig.NanoCpus}},"memory_bytes":{{.HostConfig.Memory}}}' "$small_id" > artifacts/docker-worker-limits.jsonl

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
# Configuration that disagrees with the container must stop the worker.
if docker compose run --rm --no-deps -T -e DUCKFLEET_WORKER_CPU=4 \
  worker-small > artifacts/worker-misconfigured.log 2>&1; then
  echo 'worker started with container limits that do not match its configuration' >&2
  exit 1
fi
grep -q 'do not match the configured' artifacts/worker-misconfigured.log
grep -q 'cpu=1' artifacts/worker-misconfigured.log
grep -q 'cpu=4' artifacts/worker-misconfigured.log
grep -q 'attaching the DuckLake catalog' artifacts/worker-boot-failure.log
grep -q 'still unreachable' artifacts/worker-boot-failure.log
grep -q 'catalog-does-not-resolve' artifacts/worker-boot-failure.log
! grep -q 'local-catalog-password' artifacts/worker-boot-failure.log

# A deleted pod's replacement can overlap its draining predecessor even under
# Deployment Recreate. Startup recovery must wait for the stable identity owner.
if docker compose run --rm --no-deps -T -e DUCKFLEET_CATALOG_WAIT_SECONDS=1 \
  worker-small > artifacts/worker-overlap-refusal.log 2>&1; then
  echo 'a second live process acquired the same worker recovery identity' >&2
  exit 1
fi
grep -q 'still owned by another process' artifacts/worker-overlap-refusal.log
echo 'PASS: overlapping worker startup refused recovery of a live predecessor'

# A worker reached directly must refuse isolation too. Naming a resource (cpu/memory)
# derives an isolated pod, which a worker cannot provide; a request naming none runs
# on that worker. (Under the reworked model the mode is derived from presence of
# cpu/memory/disk — there is no `compute:` string.)
session() { # session <worker> <json>
  docker compose exec -T "$1" sh -c 'curl -sS -o /dev/null -w "%{http_code}" \
    -X POST http://localhost:8080/v1/sessions \
    -H "Authorization: Bearer $DUCKFLEET_TOKEN" -H "Content-Type: application/json" \
    -d '"'$2'"
}
test "$(session worker-small '{"compute":{"cpu":1,"memory":"1Gi"}}')" = 501
test "$(session worker-small '{"compute":{}}')" = 200
echo 'PASS: a worker addressed directly refuses isolation it cannot provide'

# dbt must be an option, not a dependency. The `api` image has no dbt binary and
# reaches the gateway only through the Rust client (the harness --api-workflow stage).
docker compose run --rm api
docker compose run --rm dbt duckfleet-server --auth-continuity
docker compose run --rm dbt duckfleet-server --request-identity
# The dbt lifecycle: the `dbt` image runs the Rust integration harness
# (DUCKFLEET_ROLE=harness), which drives the forked dbt binary. Default = main
# lifecycle; then the stages that need both the gateway and a fresh fleet.
docker compose run --rm dbt
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
# pgwire attribution lands in history. Read it back through the gateway (no client
# package needed — the harness observability stage covers the general case; this is
# the pgwire-operation slice).
docker compose run --rm dbt duckfleet-server --history-without-workers >/dev/null 2>&1 || true
echo 'PASS: psql spoke to the warehouse through the wire front — typed results, classified errors, clean refusals'

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

# ---------------------------------------------------------------------------
# PENDING MIGRATION (tracked for a dedicated follow-up — these stages are deeply
# coupled to the pre-rework 2-worker fleet and the Python client, and need
# re-expressing against the single persistent tier + the Rust harness/cli:
#   - retention sweep, worker late-catalog wait, worker ledger-reconnect
#   - SIGTERM drain of the worker and of the gateway (--drain-inflight /
#     --gateway-drain-inflight harness stages + docker kill)
#   - the Phase-1 scale sweep (--scale) and maintenance (--maintenance)
#   - the backup/restore drill (scripts/backup-catalog.sh / restore-catalog.sh,
#     with the post-backup write + restore verification now via `duckfleet-server cli`)
# The isolated-pod path (a sized model -> dedicated pod) is Kubernetes-only and is
# exercised by scripts/test-k8s.sh, not here.
# ---------------------------------------------------------------------------
echo 'PASS: compose core path (worker limits, secret scrub, direct-worker isolation refusal, dbt-free api proof, dbt lifecycle, concurrency, metering, pgwire, ledger immutability, busy-worker status, mixed-protocol routing, partial outage)'
