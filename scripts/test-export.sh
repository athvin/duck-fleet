#!/usr/bin/env bash
# Real Compose export tests: every format against an independent DuckDB reader,
# an over-interactive-limit result through the durable path with measured CLI
# memory, existing-file protection, resume, corruption, expiry, cancellation,
# and cross-principal denial.
set -euo pipefail
trap 'echo "export test failed at line $LINENO" >&2' ERR

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
(cd "$root/server" && cargo build --locked -p duckfleet-cli)
binary="$root/server/target/debug/duckfleet"
url=${DUCKFLEET_URL:-http://127.0.0.1:18080}
token=${DUCKFLEET_TOKEN:-duckfleet-local-development-token}
project="$root/artifacts/export-test"
credential_id=""
active_pid=""
cleanup() {
  if [[ -n "$active_pid" ]]; then
    kill -KILL "$active_pid" >/dev/null 2>&1 || true
    wait "$active_pid" >/dev/null 2>&1 || true
  fi
  if [[ -n "$credential_id" ]]; then
    curl -fsS -X DELETE -H "Authorization: Bearer $token" "$url/v1/credentials/$credential_id" >/dev/null 2>&1 || true
  fi
  rm -rf "$project"
}
trap cleanup EXIT
rm -rf "$project"
mkdir -p "$project"
cd "$project"
"$binary" init --url "$url" --token "$token" >/dev/null
psql() {
  (cd "$root" && docker compose exec -T postgres psql -U duckfleet -d platform -v ON_ERROR_STOP=1 -qtAX -c "$1")
}
expect_failure() {
  local code=0
  "$@" >expected-failure.out 2>&1 || code=$?
  [[ "$code" -ne 0 ]]
}
request_id() { grep -o 'request_id [0-9a-f-]*' "$1" | head -1 | cut -d' ' -f2; }
peak_rss_kb() {
  # Linux GNU time reports KiB; macOS BSD time reports bytes.
  if /usr/bin/time -v true >/dev/null 2>&1; then
    /usr/bin/time -v "$@" 2>time.out >/dev/null
    awk -F: '/Maximum resident set size/ {gsub(/ /,"",$2); print $2}' time.out
  else
    /usr/bin/time -l "$@" 2>time.out >/dev/null
    awk '/maximum resident set size/ {print int($1 / 1024)}' time.out
  fi
}

typed="select * from (values
  (1::bigint, 'alpha', 12.34::decimal(10,2), true, timestamp '2026-01-02 03:04:05.123456', {'k': 'a', 'v': [1, 2]}),
  (2::bigint, null, -0.01::decimal(10,2), false, timestamp '2026-02-03 04:05:06.654321', {'k': 'b', 'v': [3]}),
  (3::bigint, 'gamma', null, null, null, null)
) t(id, name, amount, active, happened_at, nested) order by id"
flat="select id, name, amount, active, happened_at from ($typed) order by id"

# Parquet and Arrow keep types, JSON keeps nesting, CSV refuses nesting before
# publishing anything.
"$binary" export "$typed" --output typed.parquet >parquet.out 2>parquet.err
[[ "$(<parquet.err)" == *"sql: succeeded"* && "$(<parquet.out)" == *"exported 3 rows as parquet"* ]]
[[ ! -e typed.parquet.duckfleet-partial ]]
duckdb -json -c "select id, name, amount::varchar as amount, active, strftime(happened_at, '%Y-%m-%d %H:%M:%S.%f') as happened_at, to_json(nested)::varchar as nested from 'typed.parquet' order by id" > parquet.json
duckdb -json -c "select id, name, amount::varchar as amount, active, strftime(happened_at, '%Y-%m-%d %H:%M:%S.%f') as happened_at, to_json(nested)::varchar as nested from ($typed) order by id" > expected.json
diff -u <(jq -S . expected.json) <(jq -S . parquet.json)
duckdb -list -noheader -c "select column_name || ':' || column_type from (describe select * from 'typed.parquet')" > parquet-schema.txt
grep -qx 'id:BIGINT' parquet-schema.txt
grep -qx 'amount:DECIMAL(10,2)' parquet-schema.txt
grep -qx 'active:BOOLEAN' parquet-schema.txt
grep -q '^happened_at:TIMESTAMP' parquet-schema.txt
grep -q '^nested:STRUCT' parquet-schema.txt

"$binary" export "$typed" --output typed.json >/dev/null 2>json.err
jq -e 'length == 3 and .[0].id == 1 and .[0].nested.v == [1,2] and .[1].name == null and (.[2] | has("amount") | not)' typed.json >/dev/null
"$binary" export "$typed" --output typed.arrow >/dev/null 2>arrow.err
[[ "$(head -c 6 typed.arrow)" == ARROW1 && "$(tail -c 6 typed.arrow)" == ARROW1 ]]
expect_failure "$binary" export "$typed" --output typed.csv
[[ "$(<expected-failure.out)" == *"CSV cannot represent nested column nested"* ]]
[[ ! -e typed.csv ]]
"$binary" export "$flat" --output flat.csv >/dev/null 2>csv.err
duckdb -json -c "select id, name, amount::varchar as amount, active, strftime(happened_at, '%Y-%m-%d %H:%M:%S.%f') as happened_at from read_csv('flat.csv', header=true, nullstr='', types={'amount':'DECIMAL(10,2)'}) order by id" > csv.json
duckdb -json -c "select id, name, amount::varchar as amount, active, strftime(happened_at, '%Y-%m-%d %H:%M:%S.%f') as happened_at from ($flat) order by id" > expected-csv.json
diff -u <(jq -S . expected-csv.json) <(jq -S . csv.json)
"$binary" export "$flat" --output flat.psv --format csv --delimiter '|' >/dev/null 2>/dev/null
[[ "$(head -1 flat.psv)" == 'id|name|amount|active|happened_at' ]]

# Empty results still carry their schema where the format has one.
empty="select 1::int as a, 'x'::varchar as b where false"
"$binary" export "$empty" --output empty.parquet >/dev/null 2>/dev/null
"$binary" export "$empty" --output empty.json >/dev/null 2>/dev/null
"$binary" export "$empty" --output empty.csv >/dev/null 2>/dev/null
[[ "$(duckdb -csv -noheader -c "select count(*) from 'empty.parquet'")" == 0 ]]
duckdb -csv -noheader -c "select column_name from (describe select * from 'empty.parquet')" | paste -sd, - | grep -qx 'a,b'
jq -e '. == []' empty.json >/dev/null
[[ "$(<empty.csv)" == 'a,b' ]]

# Existing destinations survive without --force; --force replaces only after a
# verified download.
printf 'keep me' > flat.csv
expect_failure "$binary" export "$flat" --output flat.csv
[[ "$(<expected-failure.out)" == *"already exists"* && "$(<flat.csv)" == 'keep me' ]]
"$binary" export "$flat" --output flat.csv --force >/dev/null 2>/dev/null
[[ "$(head -1 flat.csv)" == 'id,name,amount,active,happened_at' ]]

# Over the interactive limits (100,000 rows / 16 MiB) through the durable path,
# with the CLI's peak memory bounded by chunks, not the whole result.
large="select i as id, repeat('x', 120) as payload from range(250000) t(i)"
rss=$(peak_rss_kb "$binary" export "$large" --output large.parquet)
duckdb -csv -noheader -c "select count(*), sum(length(payload)) from 'large.parquet'" | grep -qx '250000,30000000'
[[ "$rss" -lt $((256 * 1024)) ]] || { echo "peak CLI RSS ${rss} KiB exceeded 256 MiB" >&2; exit 1; }
"$binary" export "$large" --output large.csv >/dev/null 2>large-csv.err
[[ "$(wc -c < large.csv)" -gt $((16 * 1024 * 1024)) ]]
duckdb -csv -noheader -c "select count(*) from read_csv('large.csv', header=true)" | grep -qx 250000
echo "large export peak CLI RSS: ${rss} KiB"

# --job exports a finished durable job; resume reuses verified chunks and
# validates identity; corruption never publishes the final path.
"$binary" job submit "$large" > submitted.out 2>submitted.err
job=$(jq -r .request_id submitted.out)
for _ in {1..60}; do
  [[ "$("$binary" job status "$job" | jq -r .state)" == terminal ]] && break
  sleep 1
done
"$binary" job result "$job" --output resumed.csv.duckfleet-partial >/dev/null 2>&1
[[ "$(ls resumed.csv.duckfleet-partial/*.arrow | wc -l)" -gt 1 ]]
rm resumed.csv.duckfleet-partial/000001.arrow
expect_failure "$binary" export --job "$job" --output resumed.csv
[[ "$(<expected-failure.out)" == *"partial export already exists"* ]]
"$binary" export --job "$job" --output resumed.csv --resume >/dev/null 2>resume.err
[[ ! -e resumed.csv.duckfleet-partial ]]
duckdb -csv -noheader -c "select count(*) from read_csv('resumed.csv', header=true)" | grep -qx 250000
"$binary" job result "$job" --output corrupt.parquet.duckfleet-partial >/dev/null 2>&1
printf 'x' | dd of=corrupt.parquet.duckfleet-partial/000000.arrow bs=1 seek=100 conv=notrunc 2>/dev/null
expect_failure "$binary" export --job "$job" --output corrupt.parquet --resume
[[ "$(<expected-failure.out)" == *"--resume"* && ! -e corrupt.parquet ]]
other_job=$(jq -r .request_id < <("$binary" job submit "select 1 as a" 2>/dev/null))
sleep 2
"$binary" job result "$other_job" --output mismatch.json.duckfleet-partial >/dev/null 2>&1
expect_failure "$binary" export --job "$job" --output mismatch.json --resume
[[ ! -e mismatch.json ]]
expect_failure "$binary" export "$flat" --output never.csv --resume

# Another principal cannot export this result by knowing its ID.
principal="export-other-$(date +%s)-$$"
issued=$(curl -fsS -X POST -H "Authorization: Bearer $token" -H 'Content-Type: application/json' --data "{\"principal\":\"$principal\"}" "$url/v1/credentials")
other_token=$(jq -r '.token' <<<"$issued")
credential_id=$(jq -r '.credential_id' <<<"$issued")
expect_failure env DUCKFLEET_TOKEN="$other_token" "$binary" export --job "$job" --output stolen.csv
[[ ! -e stolen.csv && ! -e stolen.csv.duckfleet-partial ]]

# Expired results are refused on every manifest/chunk read.
psql "update query_jobs set result_expires_at=clock_timestamp()-interval '1 second' where request_id='$job'" >/dev/null
expect_failure "$binary" export --job "$job" --output expired.csv
[[ ! -e expired.csv ]]

# Ctrl-C cancels the durable job, reports it, and publishes nothing.
"$binary" export "select count(*) from range(100000000000) a" --output canceled.csv >cancel.out 2>cancel.err &
active_pid=$!
for _ in {1..100}; do
  grep -q 'request_id' cancel.err 2>/dev/null && break
  sleep 0.1
done
sleep 1
kill -INT "$active_pid"
cancel_code=0
wait "$active_pid" || cancel_code=$?
active_pid=""
[[ "$cancel_code" -ne 0 && "$(<cancel.err)" == *export_canceled* ]]
[[ ! -e canceled.csv && ! -e canceled.csv.duckfleet-partial ]]
canceled_job=$(request_id cancel.err)
for _ in {1..30}; do
  state=$("$binary" job status "$canceled_job" | jq -r .status)
  [[ "$state" != null && "$state" != running && "$state" != queued ]] && break
  sleep 1
done
[[ "$state" != succeeded ]]

if grep -E 'local-object-store-password|X-Amz-Signature' ./*.out ./*.err; then
  echo 'credential or signed URL leaked' >&2
  exit 1
fi
printf 'PASS: csv/json/arrow/parquet exports, independent parquet/csv readers, empty schemas, durable over-limit export, bounded memory, force protection, resume, corruption, identity, expiry, cancellation, and cross-principal denial\n'
