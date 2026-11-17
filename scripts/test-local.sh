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
# dbt must be an option, not a dependency. This runs the whole workflow from an image
# where dbt is not installed, and asserts that before doing anything else.
docker compose run --rm api
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

# A worker holds one PostgreSQL connection for its whole life, and the server
# restarting does not tell it. Every statement afterwards failed with a generic 503
# while /healthz still answered 200, so the container kept taking traffic it could not
# serve and nothing replaced it. Restart PostgreSQL underneath a running worker and
# require it to recover on its own.
docker compose restart postgres
docker compose up -d --wait postgres
running_id=$(docker compose ps -q worker-small)
# The first statement is the one that discovers the dead connection. It must succeed,
# not merely be reported honestly: reconnecting in place is what keeps open sessions.
docker compose run --rm --no-deps dbt python - <<'PY'
import os, uuid
from duckfleet import Client, RemoteConnection
url, token = os.environ["DUCKFLEET_URL"], os.environ["DUCKFLEET_TOKEN"]
connection = RemoteConnection(url, token, compute={"cpu": 1, "memory": "1Gi"})
try:
    rows = connection.cursor().execute("select 1 as after_restart").fetchall()
    assert rows == [(1,)], rows
finally:
    connection.close()
# History is the ledger, so a row proves the reconnect reached PostgreSQL rather than
# the engine answering from memory while the write was silently dropped.
history = Client(url, token).request("GET", "/v1/queries?limit=5")
assert b"after_restart" in history, history[:200]
PY
# The same container, never restarted: it replaced its own connection rather than
# being replaced by the probe.
test "$(docker compose ps -q worker-small)" = "$running_id"
test "$(docker inspect -f '{{.RestartCount}}' "$running_id")" = 0
test "$(docker compose exec -T worker-small curl -sS -o /dev/null -w '%{http_code}' http://localhost:8080/healthz)" = 200
docker compose logs worker-small --since 2m > artifacts/worker-ledger-reconnect.log 2>&1
grep -q 'ledger connection' artifacts/worker-ledger-reconnect.log
# The cause reaches the log, and the credential in the DSN does not (#20).
! grep -q 'local-catalog-password' artifacts/worker-ledger-reconnect.log
echo 'PASS: a worker replaced its own ledger connection after PostgreSQL restarted'
# Kubernetes terminates a pod with SIGTERM. The worker previously waited on ctrl_c
# alone — SIGINT — so the graceful path never ran on a pod deletion and it kept
# admitting new sessions until it was killed. Drain it with a statement in flight and
# require three things at once: the statement finishes, new sessions are refused with a
# classified reason, and the process leaves of its own accord.
rm -f artifacts/drain-inflight.started
docker compose run --rm --no-deps -T dbt python /app/tests/harness.py --drain-inflight \
  > artifacts/drain-inflight.log 2>&1 &
inflight=$!
for _ in $(seq 1 60); do
  [ -s artifacts/drain-inflight.started ] && break
  sleep 1
done
test -s artifacts/drain-inflight.started
draining_id=$(docker compose ps -q worker-small)
docker kill -s TERM "$draining_id" > /dev/null
# Reported at once, so a Service drops this endpoint before the process goes away.
for _ in $(seq 1 15); do
  status=$(docker compose exec -T worker-small curl -sS http://localhost:8080/healthz 2>/dev/null || true)
  case "$status" in *draining*) break ;; esac
  sleep 1
done
case "$status" in
  *draining*) ;;
  *) echo "healthz did not report draining: $status" >&2; exit 1 ;;
esac
# A new session is refused with a reason a caller can act on, not a closed connection.
docker compose exec -T worker-small sh -c 'curl -sS \
  -X POST http://localhost:8080/v1/sessions \
  -H "Authorization: Bearer $DUCKFLEET_TOKEN" -H "Content-Type: application/json" -d "{}"' \
  > artifacts/drain-refusal.json
python3 scripts/assert_draining.py artifacts/drain-refusal.json
# And the statement that was already running finished.
wait "$inflight"
grep -q 'PASS: a statement already in flight finished' artifacts/drain-inflight.log
# The settle window runs after the last statement, so wait for the process to leave
# before reading its log: the "drain complete" line is written on the way out, and
# reading too early fails the order assertion on a drain that was in fact correct.
for _ in $(seq 1 90); do
  [ "$(docker inspect -f '{{.State.Running}}' "$draining_id")" = false ] && break
  sleep 1
done
test "$(docker inspect -f '{{.State.Running}}' "$draining_id")" = false
# The log must show the signal arriving before that statement finished. Without this
# the check above passes whenever the statement is simply fast, which is how the first
# version of this test proved nothing at all.
docker logs "$draining_id" > artifacts/drain-order.log 2>&1
python3 scripts/assert_drain_order.py artifacts/drain-order.log
# The container exited because it was asked to, not because it was killed.
test "$(docker inspect -f '{{.State.ExitCode}}' "$draining_id")" = 0
# Force-recreated: the drained container exited cleanly, and starting that same
# stopped container back up is not what a replacement does.
docker compose up -d --force-recreate --no-deps --wait worker-small
docker compose up -d --wait server
echo 'PASS: a worker drained on SIGTERM, refusing new sessions and finishing the one in flight'
# The gateway is the other half of that: a rollout used to cut every proxied request
# mid-flight into a transport failure the caller cannot classify. Drain it the same
# way — the statement in flight finishes, a new session is refused with the worker's
# own draining shape, and the process leaves of its own accord.
rm -f artifacts/gateway-drain-inflight.started
docker compose run --rm --no-deps -T dbt python /app/tests/harness.py --gateway-drain-inflight \
  > artifacts/gateway-drain-inflight.log 2>&1 &
gateway_inflight=$!
for _ in $(seq 1 60); do
  [ -s artifacts/gateway-drain-inflight.started ] && break
  sleep 1
done
test -s artifacts/gateway-drain-inflight.started
gateway_id=$(docker compose ps -q server)
docker kill -s TERM "$gateway_id" > /dev/null
# Reported at once, so a Service drops this endpoint before the process goes away.
for _ in $(seq 1 15); do
  status=$(docker compose exec -T server curl -sS http://localhost:8080/healthz 2>/dev/null || true)
  case "$status" in *draining*) break ;; esac
  sleep 1
done
case "$status" in
  *draining*) ;;
  *) echo "gateway healthz did not report draining: $status" >&2; exit 1 ;;
esac
# A new session through the draining gateway is refused in the worker's own terms.
docker compose exec -T server sh -c 'curl -sS \
  -X POST http://localhost:8080/v1/sessions \
  -H "Authorization: Bearer $DUCKFLEET_TOKEN" -H "Content-Type: application/json" -d "{}"' \
  > artifacts/gateway-drain-refusal.json
python3 scripts/assert_draining.py artifacts/gateway-drain-refusal.json
# And the statement that was already in flight through it finished.
wait "$gateway_inflight"
grep -q 'PASS: a statement already in flight finished' artifacts/gateway-drain-inflight.log
# Same as the worker: let the settle window end and the process leave before reading
# the log it writes on the way out.
for _ in $(seq 1 90); do
  [ "$(docker inspect -f '{{.State.Running}}' "$gateway_id")" = false ] && break
  sleep 1
done
test "$(docker inspect -f '{{.State.Running}}' "$gateway_id")" = false
# The gateway's own log must show the signal arriving before that request completed.
# Without the ordering, the checks above pass whenever the statement is simply fast —
# the same hollowness the worker's first drain test had.
docker logs "$gateway_id" > artifacts/gateway-drain-order.log 2>&1
python3 scripts/assert_drain_order.py artifacts/gateway-drain-order.log \
  'termination signal received' \
  'a request in flight when the signal arrived has completed' \
  'drain complete'
test "$(docker inspect -f '{{.State.ExitCode}}' "$gateway_id")" = 0
docker compose up -d --force-recreate --no-deps --wait server
echo 'PASS: the gateway drained on SIGTERM, refusing new sessions and finishing the request in flight'

# The catalog IS the lake, and a backup that has never been restored is a hope, not
# a backup. Full drill: dump both databases, commit more work, destroy PostgreSQL's
# volume outright, restore into the fresh instance, and prove the platform serves
# from the restored map — pre-backup data reads from the bucket, post-backup work is
# honestly gone (that is what a recovery point means), history and the ledger are
# back, and a new write commits cleanly against the restored snapshot chain.
rm -rf artifacts/catalog-backup
bash scripts/backup-catalog.sh compose artifacts/catalog-backup
test -s artifacts/catalog-backup/lake.dump
test -s artifacts/catalog-backup/platform.dump
# Work the restore must forget.
docker compose run --rm --no-deps dbt python - <<'PY'
import os
from duckfleet import RemoteConnection
connection = RemoteConnection(os.environ["DUCKFLEET_URL"], os.environ["DUCKFLEET_TOKEN"])
try:
    cursor = connection.cursor()
    cursor.execute("create schema if not exists api_demo")
    cursor.execute("create or replace table api_demo.after_backup as select 42 as n")
    assert cursor.execute("select count(*) from api_demo.after_backup").fetchall() == [(1,)]
finally:
    connection.close()
PY
docker compose stop server worker-small worker-medium
docker compose rm -sf postgres
docker volume rm duckfleet-local_postgres-data > /dev/null
docker compose up -d --no-deps --wait postgres
DUCKFLEET_CONFIRM_RESTORE=yes bash scripts/restore-catalog.sh compose artifacts/catalog-backup
docker compose up -d --wait server
docker compose run --rm --no-deps dbt python - <<'PY'
import json
import os
from duckfleet import Client, RemoteConnection, RemoteError
url, token = os.environ["DUCKFLEET_URL"], os.environ["DUCKFLEET_TOKEN"]
connection = RemoteConnection(url, token)
try:
    cursor = connection.cursor()
    # Data committed before the backup reads through the restored catalog: the
    # Parquet never left the bucket, and the restored rows still name it.
    rows = cursor.execute("select count(*) from lake.analytics.customer_totals").fetchall()
    assert rows == [(2,)], rows
    # The recovery point is real: work committed after the backup is gone.
    try:
        cursor.execute("select count(*) from api_demo.after_backup")
        raise AssertionError("a post-backup table survived the restore")
    except RemoteError as error:
        assert error.failure_kind == "sql_error", error.failure_kind
    # And the restored snapshot chain accepts new commits.
    cursor.execute("create or replace table api_demo.after_restore as select 1 as n")
    assert cursor.execute("select count(*) from api_demo.after_restore").fetchall() == [(1,)]
finally:
    connection.close()
# History and the ledger came back with the platform database.
client = Client(url, token)
assert b"query_id" in client.request("GET", "/v1/queries?limit=5")
budget = json.loads(client.request("GET", "/v1/budget"))
assert budget["usage_events"] > 0, budget
PY
echo 'PASS: the catalog was destroyed and restored from backup; old data read, newer work was gone, and a fresh write committed'

