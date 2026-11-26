#!/usr/bin/env bash
set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
(cd "$root/server" && cargo build --locked -p duckfleet-cli)
binary="$root/server/target/debug/duckfleet"
url=${DUCKFLEET_URL:-http://127.0.0.1:18080}
token=${DUCKFLEET_TOKEN:-duckfleet-local-development-token}
project="$root/artifacts/history-test"
credential_id=""
cleanup() {
  (cd "$root" && DUCKFLEET_PROFILE_SAMPLE_PERCENT=100 docker compose up -d --wait worker-small server >/dev/null 2>&1) || true
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

"$binary" sql 'select sum(i) as total from range(1000000) t(i)' >/dev/null
id="$("$binary" history --limit 1 --format json | jq -r '.queries[0].query_id')"
"$binary" history --limit 5 > history-table.out
"$binary" history --limit 5 --format json > history.json
"$binary" profile "$id" > profile.out
"$binary" profile "$id" --metrics > metrics.out
"$binary" profile "$id" --raw > raw.json
jq -e '.operator | length > 0' raw.json >/dev/null
[[ "$(<profile.out)" == *'most_expensive:'* ]]
[[ "$(<profile.out)" == *'engine_execution_ms:'* ]]
[[ "$(<profile.out)" == *'scope: DuckDB engine only; not container RSS or provider cost'* ]]
[[ "$(<metrics.out)" == *'engine_cpu_seconds:'* ]]
[[ "$(<history-table.out)" == *"$id"* ]]
"$binary" history --status succeeded --worker-id small --format json > filtered.json
jq -e '.queries | length > 0' filtered.json >/dev/null

page1="$("$binary" history --limit 5 --format json)"
next="$(jq -r '.next_before' <<<"$page1")"
[[ "$next" != null ]]
for n in 1 2 3 4 5; do "$binary" sql "select $n" >/dev/null & done
wait
page2="$("$binary" history --limit 5 --before "$next" --format json)"
jq -n --argjson a "$page1" --argjson b "$page2" -e '($a.queries|map(.query_id)) as $ids | all($b.queries[]; .query_id as $id | $ids | index($id) | not)' >/dev/null

"$binary" history --watch --interval 1 > watch.out 2>&1 &
watch_pid=$!
sleep 3
kill -INT "$watch_pid"
wait "$watch_pid"
[[ "$(<watch.out)" == *'history watch stopped'* ]]

set +e
"$binary" sql 'select * from missing_history_fixture' >/dev/null 2>&1
failed_code=$?
set -e
[[ "$failed_code" -eq 1 ]]
failed_id="$("$binary" history --status failed --limit 1 --format json | jq -r '.queries[0].query_id')"
"$binary" profile "$failed_id" --metrics > failed-profile.out
[[ "$(<failed-profile.out)" == *'status: failed'* ]]

long_sql='select sum(sin(i::double)) from range(100000000000) t(i)'
"$binary" job submit --sql "$long_sql" --timeout 1 > timeout-submit.json 2> timeout-submit.err
timeout_job=$(jq -r '.request_id' timeout-submit.json)
"$binary" job status "$timeout_job" --wait > timeout-status.json
timeout_id="$("$binary" history --failure-kind timeout --limit 1 --format json | jq -r '.queries[0].query_id')"
[[ "$timeout_id" != null ]]
"$binary" profile "$timeout_id" --metrics > timeout-profile.out
[[ "$(<timeout-profile.out)" == *'status: timed_out'* ]]

"$binary" job submit --sql "$long_sql" --timeout 60 > cancel-submit.json 2> cancel-submit.err
cancel_job=$(jq -r '.request_id' cancel-submit.json)
"$binary" job cancel "$cancel_job" >/dev/null
"$binary" job status "$cancel_job" --wait > cancel-status.json
cancel_id="$("$binary" history --status canceled --limit 1 --format json | jq -r '.queries[0].query_id')"
[[ "$cancel_id" != null ]]
"$binary" profile "$cancel_id" --metrics > cancel-profile.out
[[ "$(<cancel-profile.out)" == *'status: canceled'* ]]

cd "$root"
DUCKFLEET_PROFILE_SAMPLE_PERCENT=0 docker compose up -d --wait worker-small server >/dev/null
cd "$project"
"$binary" sql 'select 42 as sampled_fixture' >/dev/null
sampled_id="$("$binary" history --limit 1 --format json | jq -r '.queries[0].query_id')"
"$binary" profile "$sampled_id" --metrics > sampled-profile.out
[[ "$(<sampled-profile.out)" == *'profile_status: sampled_out'* ]]
set +e
sampled_raw="$("$binary" profile "$sampled_id" --raw 2>&1)"
sampled_code=$?
set -e
[[ "$sampled_code" -eq 1 && "$sampled_raw" == *sampled_out* ]]
cd "$root"
DUCKFLEET_PROFILE_SAMPLE_PERCENT=100 docker compose up -d --wait worker-small server >/dev/null
cd "$project"

principal="history-metadata-$(date +%s)-$$"
issued=$(curl -fsS -X POST -H "Authorization: Bearer $token" -H 'Content-Type: application/json' --data "{\"principal\":\"$principal\"}" "$url/v1/credentials")
metadata_token=$(jq -r '.token' <<<"$issued")
credential_id=$(jq -r '.credential_id' <<<"$issued")
policy=$(curl -fsS -H "Authorization: Bearer $token" "$url/v1/principals/$principal/policy")
body=$(jq '{roles:["viewer"],permissions:[{"action":"history.read","scope":"all"}],compute:.compute,disabled:false}' <<<"$policy")
curl -fsS -X PUT -H "Authorization: Bearer $token" -H 'Content-Type: application/json' --data "$body" "$url/v1/principals/$principal/policy" >/dev/null
"$binary" --url "$url" --token "$metadata_token" history --limit 1 --format json > metadata.json
jq -e '.queries[0].sql == null and .queries[0].error == null' metadata.json >/dev/null
set +e
metadata_profile="$("$binary" --url "$url" --token "$metadata_token" profile "$id" --raw 2>&1)"
metadata_code=$?
set -e
[[ "$metadata_code" -eq 1 && "$metadata_profile" == *profile_unavailable* ]]
[[ "$metadata_profile" != *'select sum(i)'* ]]

printf 'PASS: real history/profile lifecycle, filters, pagination, watch, sampling, and metadata privacy\n'
