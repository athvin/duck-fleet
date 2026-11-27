#!/usr/bin/env bash
# Workers expose only the lake and private import staging to SQL. Proves on
# real Compose, over HTTP sessions, durable jobs, and pgwire, that worker-local
# files (including the environment holding platform credentials), stdio COPY,
# arbitrary URLs, and other object prefixes are refused, that no marker reaches
# worker logs, that the lockdown cannot be undone from SQL, and that lake work
# is unaffected.
set -euo pipefail
trap 'echo "local I/O test failed at line $LINENO" >&2' ERR

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
(cd "$root/server" && cargo build --locked -p duckfleet-cli)
binary="$root/server/target/debug/duckfleet"
url=${DUCKFLEET_URL:-http://127.0.0.1:18080}
token=${DUCKFLEET_TOKEN:-duckfleet-local-development-token}
project="$root/artifacts/local-io-test"
marker="local_io_marker_$$_$(date +%s)"
cleanup() {
  (cd "$project" 2>/dev/null && "$binary" sql "drop schema if exists local_io cascade" >/dev/null 2>&1) || true
  rm -rf "$project"
}
trap cleanup EXIT
rm -rf "$project"
mkdir -p "$project"
cd "$project"
"$binary" init --url "$url" --token "$token" >/dev/null
network=$(docker network ls --format '{{.Name}}' | grep -m1 'duckfleet-local_default')
since=$(date -u +%Y-%m-%dT%H:%M:%SZ)

refused_http() {
  local code=0
  "$binary" sql "$1" >refused.out 2>&1 || code=$?
  [[ "$code" -ne 0 ]]
  grep -q 'file system operations are disabled by configuration' refused.out
  grep -q 'duckfleet export' refused.out
}
statements=(
  "select length(content) from read_text('/proc/self/environ')"
  "select * from read_csv('/etc/passwd')"
  "copy (select '$marker') to stdout"
  "copy (select '$marker') to '/dev/stderr'"
  "copy (select '$marker') to '/tmp/$marker.csv'"
  "select * from read_csv('https://example.com/data.csv')"
  "select * from read_parquet('s3://duckfleet/not-the-lake/x.parquet')"
)
for statement in "${statements[@]}"; do
  refused_http "$statement"
done
"$binary" sql "create schema if not exists local_io" >/dev/null
"$binary" sql "create table local_io.t(v varchar)" >/dev/null
code=0
"$binary" sql "copy local_io.t from stdin" >refused.out 2>&1 || code=$?
[[ "$code" -ne 0 ]]

# The restriction is permanent for the engine's lifetime.
for statement in "set enable_external_access=true" "reset enable_external_access" "set allowed_directories=['/']"; do
  code=0
  "$binary" sql "$statement" >undo.out 2>&1 || code=$?
  [[ "$code" -ne 0 ]]
done
refused_http "select length(content) from read_text('/proc/self/environ')"

# Durable jobs run on the same engine and are refused the same way.
job=$("$binary" job submit "select length(content) from read_text('/proc/self/environ')" 2>/dev/null | jq -r .request_id)
for _ in {1..30}; do
  status=$("$binary" job status "$job" | jq -r .status)
  [[ "$status" != queued && "$status" != running && "$status" != preparing && "$status" != executing ]] && break
  sleep 1
done
# Refused before engine entry (status refused) or failed in the engine; never succeeded.
[[ "$status" == failed || "$status" == refused ]]

# pgwire: the same refusal, and the stdio COPY marker never reaches the worker.
pg() {
  printf '%s\n' "$1" | docker run -i --rm --network "$network" -e PGPASSWORD="$token" postgres:17.6-bookworm \
    psql "host=server port=5432 user=duckfleet dbname=lake sslmode=disable" -X -At 2>&1 || true
}
[[ "$(pg "\\copy (select '$marker') to stdout")" == *"file system operations are disabled"* ]]
[[ "$(pg "select length(content) from read_text('/proc/self/environ');")" == *"file system operations are disabled"* ]]

# Lake reads and writes, spill, and JSON still work.
"$binary" sql "insert into local_io.t values ('ok')" >/dev/null
"$binary" sql "select count(*)::bigint as n from local_io.t" --format json | jq -e '.[0].n == 1' >/dev/null
"$binary" sql "select max(rn)::bigint as n from (select row_number() over (order by md5(range::varchar)) rn from range(6000000))" --format json | jq -e '.[0].n == 6000000' >/dev/null
"$binary" sql "select to_json({'a': 1})::varchar as j" --format json | jq -e '.[0].j == "{\"a\":1}"' >/dev/null
"$binary" history --limit 1 --format json | jq -e '.queries[0].profile_status == "captured"' >/dev/null

if (cd "$root" && docker compose logs --since "$since" worker-small server 2>&1) | grep -q "$marker"; then
  echo "query rows reached container logs" >&2
  exit 1
fi
printf 'PASS: worker-local files, environment, stdio COPY, URLs, and foreign prefixes refused over HTTP, jobs, and pgwire; lockdown permanent; lake, spill, JSON, and profiles intact\n'
