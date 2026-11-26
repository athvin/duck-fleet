#!/usr/bin/env bash
set -euo pipefail
trap 'echo "import test failed at line $LINENO" >&2' ERR

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
(cd "$root/server" && cargo build --locked -p duckfleet-cli)
binary="$root/server/target/debug/duckfleet"
url=${DUCKFLEET_URL:-http://127.0.0.1:18080}
token=${DUCKFLEET_TOKEN:-duckfleet-local-development-token}
project="$root/artifacts/import-test"
credential_id=""
active_pid=""
cleanup() {
  if [[ -n "$active_pid" ]]; then
    kill -KILL "$active_pid" >/dev/null 2>&1 || true
    wait "$active_pid" >/dev/null 2>&1 || true
  fi
  (cd "$root" && docker compose up -d --wait worker-small server >/dev/null 2>&1) || true
  if [[ -n "$credential_id" ]]; then
    curl -fsS -X DELETE -H "Authorization: Bearer $token" "$url/v1/credentials/$credential_id" >/dev/null 2>&1 || true
  fi
  (cd "$project" 2>/dev/null && "$binary" sql 'drop schema if exists import_fixture cascade' >/dev/null 2>&1) || true
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
expect_cli_failure() {
  local code=0
  "$@" >expected-failure.out 2>&1 || code=$?
  [[ "$code" -ne 0 ]]
  [[ "$(<expected-failure.out)" != *local-object-store-password* ]]
}

cat > typed.csv <<'CSV'
id,name,amount,active,created_at,note
1,alpha,12.50,true,2026-01-02 03:04:05,
2,beta,-3.25,false,2026-02-03 04:05:06,hello
CSV
"$binary" import typed.csv --table import_fixture.typed --create >create.out 2>create.err
[[ "$(<create.err)" == *transfer:* && "$(<create.err)" == *execution:* ]]
"$binary" sql 'select count(*)::bigint as rows, sum(amount)::varchar as amount, count(note)::bigint as notes from import_fixture.typed' --format json > typed.json
jq -e '. == [{"rows":2,"amount":"9.25","notes":1}]' typed.json >/dev/null
"$binary" describe import_fixture.typed --format json > typed-schema.json
jq -e 'map(select(.column_name == "id"))[0].data_type == "BIGINT"' typed-schema.json >/dev/null
jq -e 'map(select(.column_name == "active"))[0].data_type == "BOOLEAN"' typed-schema.json >/dev/null
jq -e 'map(select(.column_name == "created_at"))[0].data_type | contains("TIMESTAMP")' typed-schema.json >/dev/null
"$binary" import typed.csv --table import_fixture.typed --append >/dev/null 2>append.err
"$binary" sql 'select count(*)::bigint as rows from import_fixture.typed' --format json | jq -e '. == [{"rows":4}]' >/dev/null

cat > delimited.csv <<'CSV'
a|b
7|x
8|y
CSV
"$binary" import delimited.csv --format csv --delimiter '|' --table import_fixture.delimited --create >/dev/null 2>delimiter.err
"$binary" sql 'select sum(a)::bigint as total from import_fixture.delimited' --format json | jq -e '. == [{"total":15}]' >/dev/null
cat > headerless.csv <<'CSV'
9|z
CSV
"$binary" import headerless.csv --format csv --delimiter '|' --no-header --table import_fixture.delimited --append >/dev/null 2>headerless.err
"$binary" sql 'select count(*)::bigint as rows, sum(a)::bigint as total from import_fixture.delimited' --format json | jq -e '. == [{"rows":3,"total":24}]' >/dev/null

# Independent local DuckDB is the Parquet writer and semantic reference.
duckdb fixture.duckdb "copy (select * from (values
  (1, timestamp '2026-01-02 03:04:05.123456', decimal '12.34', null, {'name':'alpha','values':[1,2]}),
  (2, timestamp '2026-02-03 04:05:06.654321', decimal '-0.01', 'hello', {'name':'beta','values':[3]})
) t(id, happened_at, amount, note, nested)) to 'fixture.parquet' (format parquet);"
"$binary" import fixture.parquet --table import_fixture.parquet --create >/dev/null 2>parquet.err
"$binary" sql "select strftime(happened_at, '%Y-%m-%d %H:%M:%S.%f') as happened_at, amount::varchar as amount, note, to_json(nested)::varchar as nested from import_fixture.parquet order by id" --format json > parquet.json
duckdb -json fixture.duckdb "select strftime(happened_at, '%Y-%m-%d %H:%M:%S.%f') as happened_at, amount::varchar as amount, note, to_json(nested)::varchar as nested from 'fixture.parquet' order by id" > expected-parquet.json
jq -S . parquet.json > parquet-normalized.json
jq -S . expected-parquet.json > expected-parquet-normalized.json
diff -u expected-parquet-normalized.json parquet-normalized.json

# More than one 16 MiB upload part and far beyond the interactive row cap.
duckdb fixture.duckdb "copy (select i as id, repeat('x', 80) as payload from range(350000) t(i)) to 'large.csv' (format csv, header true);"
[[ "$(wc -c < large.csv)" -gt $((16 * 1024 * 1024)) ]]
"$binary" import large.csv --table import_fixture.large --create >/dev/null 2>large.err
"$binary" sql 'select count(*)::bigint as rows, sum(length(payload))::bigint as bytes from import_fixture.large' --format json | jq -e '. == [{"rows":350000,"bytes":28000000}]' >/dev/null

# Cancellation runs through the existing request cancellation path and never
# retries the transaction. Keep a deterministic large source for the race.
"$binary" import large.csv --table import_fixture.canceled --create >cancel.out 2>cancel.err &
active_pid=$!
for _ in {1..100}; do
  grep -q 'execution:' cancel.err 2>/dev/null && break
  kill -0 "$active_pid" 2>/dev/null || break
  sleep 0.1
done
kill -INT "$active_pid"
cancel_code=0
wait "$active_pid" || cancel_code=$?
active_pid=""
if [[ "$cancel_code" -ne 0 ]]; then
  [[ "$(<cancel.err)" == *import_canceled* ]]
fi
"$binary" sql "select count(*)::bigint as rows from information_schema.tables where table_schema='import_fixture' and table_name='canceled'" --format json > canceled-table.json
if [[ "$(jq -r '.[0].rows' canceled-table.json)" == 1 ]]; then
  "$binary" sql 'select count(*)::bigint as rows from import_fixture.canceled' --format json | jq -e '. == [{"rows":350000}]' >/dev/null
fi

# Losing the worker during ingestion makes the SQL outcome unknown, not a reason
# to replay it. Private staging cleanup still belongs to the gateway.
"$binary" import large.csv --table import_fixture.interrupted --create >interrupt.out 2>interrupt.err &
active_pid=$!
for _ in {1..100}; do
  grep -q 'execution:' interrupt.err 2>/dev/null && break
  kill -0 "$active_pid" 2>/dev/null || break
  sleep 0.1
done
(cd "$root" && docker compose kill -s SIGKILL worker-small >/dev/null)
interrupt_code=0
wait "$active_pid" || interrupt_code=$?
active_pid=""
(cd "$root" && docker compose up -d --wait worker-small server >/dev/null)
"$binary" sql "select count(*)::bigint as rows from information_schema.tables where table_schema='import_fixture' and table_name='interrupted'" --format json > interrupted-table.json
if [[ "$(jq -r '.[0].rows' interrupted-table.json)" == 1 ]]; then
  "$binary" sql 'select count(*)::bigint as rows from import_fixture.interrupted' --format json | jq -e '. == [{"rows":350000}]' >/dev/null
fi

printf 'not a parquet file' > malformed.parquet
expect_cli_failure "$binary" import malformed.parquet --table import_fixture.must_not_exist --create
"$binary" sql "select count(*)::bigint as tables from information_schema.tables where table_schema='import_fixture' and table_name='must_not_exist'" --format json | jq -e '. == [{"tables":0}]' >/dev/null
expect_cli_failure "$binary" import typed.csv --table import_fixture.parquet --append
"$binary" sql 'select count(*)::bigint as rows from import_fixture.parquet' --format json | jq -e '. == [{"rows":2}]' >/dev/null
expect_cli_failure "$binary" import typed.csv --table import_fixture.typed
expect_cli_failure "$binary" import typed.csv --table import_fixture.typed --create --append
printf x > unsupported.bin
expect_cli_failure "$binary" import unsupported.bin --table import_fixture.bad --create
truncate -s $((1024 * 1024 * 1024 + 1)) oversized.csv
expect_cli_failure "$binary" import oversized.csv --table import_fixture.bad --create

# Exercise the streaming API directly for checksum rejection and owner fencing.
principal="import-other-$(date +%s)-$$"
issued=$(curl -fsS -X POST -H "Authorization: Bearer $token" -H 'Content-Type: application/json' --data "{\"principal\":\"$principal\"}" "$url/v1/credentials")
other_token=$(jq -r '.token' <<<"$issued")
credential_id=$(jq -r '.credential_id' <<<"$issued")
upload() {
  local id=$1 digest=$2
  curl -fsS -X POST -H "Authorization: Bearer $token" --data-binary @typed.csv \
    "$url/v1/imports/$id/upload?destination=import_fixture.unused&mode=create&format=csv&bytes=$(wc -c < typed.csv | tr -d " ")&sha256=$digest&options=%7B%7D"
}
checksum_id=$(uuidgen | tr '[:upper:]' '[:lower:]')
set +e
curl -sS -o checksum-failure.json -w '%{http_code}' -X POST -H "Authorization: Bearer $token" --data-binary @typed.csv \
  "$url/v1/imports/$checksum_id/upload?destination=import_fixture.unused&mode=create&format=csv&bytes=$(wc -c < typed.csv | tr -d " ")&sha256=$(printf '0%.0s' {1..64})&options=%7B%7D" > checksum-status.out
set -e
[[ "$(<checksum-status.out)" == 400 ]]
curl -fsS -X DELETE -H "Authorization: Bearer $token" "$url/v1/imports/$checksum_id" >/dev/null
staged_id=$(uuidgen | tr '[:upper:]' '[:lower:]')
upload "$staged_id" "$(shasum -a 256 typed.csv | cut -d' ' -f1)" > staged.json
jq -e --arg id "$staged_id" '.request_id == $id and .state == "staged" and (.staging_url | startswith("s3://")) and (.staging_url | contains("X-Amz-Signature") | not)' staged.json >/dev/null
[[ "$(curl -sS -o other-get.json -w '%{http_code}' -H "Authorization: Bearer $other_token" "$url/v1/imports/$staged_id")" == 404 ]]
[[ "$(curl -sS -o other-delete.json -w '%{http_code}' -X DELETE -H "Authorization: Bearer $other_token" "$url/v1/imports/$staged_id")" == 404 ]]
psql "update import_requests set expires_at=clock_timestamp()-interval '1 second' where request_id='$staged_id'" >/dev/null
# The gateway's maintenance tick is 60 seconds; make the artifacts task due now
# so the next tick reclaims the expired upload instead of waiting two ticks.
psql "update platform_maintenance_tasks set next_due_at=clock_timestamp() where task='artifacts'" >/dev/null
for _ in {1..90}; do
  state=$(psql "select state from import_requests where request_id='$staged_id'")
  [[ "$state" == deleted ]] && break
  sleep 2
done
[[ "$state" == deleted ]]
remaining=$(psql "select count(*) from import_requests where state <> 'deleted' and (expires_at < clock_timestamp() or delete_after < clock_timestamp()-interval '2 minutes')")
[[ "$remaining" == 0 ]]
"$binary" history --limit 100 --format json > import-history.json
jq -e 'any(.queries[]; .context.connection_name == "import" and .context.import_id != null)' import-history.json >/dev/null
if grep -E 'DUCKFLEETLOCAL|local-object-store-password|X-Amz-Signature' ./*.out ./*.err ./*.json; then
  echo 'credential or signed URL leaked' >&2
  exit 1
fi

printf 'PASS: real CSV/Parquet create and append, inferred and nested values, multipart transfer, rollback, owner fencing, checksum rejection, and leased cleanup\n'
