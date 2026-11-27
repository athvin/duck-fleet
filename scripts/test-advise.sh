#!/usr/bin/env bash
# Real Compose advisor tests with named fixtures: CPU saturation, memory/spill,
# queue contention, a dbt model aggregate, insufficient evidence, a
# metadata-only caller, and advice served while the execution worker is stopped.
set -euo pipefail
trap 'echo "advise test failed at line $LINENO" >&2' ERR

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
(cd "$root/server" && cargo build --locked -p duckfleet-cli)
binary="$root/server/target/debug/duckfleet"
url=${DUCKFLEET_URL:-http://127.0.0.1:18080}
token=${DUCKFLEET_TOKEN:-duckfleet-local-development-token}
project="$root/artifacts/advise-test"
operator_id=""
cleanup() {
  (cd "$root" && docker compose up -d --wait worker-small >/dev/null 2>&1) || true
  if [[ -n "$operator_id" ]]; then
    curl -fsS -X DELETE -H "Authorization: Bearer $token" "$url/v1/credentials/$operator_id" >/dev/null 2>&1 || true
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
tag="advise_$$_$(date +%s)"
last_id() {
  "$binary" history --limit 20 --format json | jq -r --arg tag "$1" '[.queries[] | select(.sql != null and (.sql | contains($tag)))][0].query_id'
}
advise() { "$binary" advise "$@" --format json; }

# CPU saturation without memory pressure.
for _ in 1 2 3; do
  "$binary" sql "select sum(sin(range)) as cpu_$tag from range(120000000)" >/dev/null
done
cpu=$(last_id "cpu_$tag")
advise query "$cpu" > cpu.json
jq -e '.groups | length == 1' cpu.json >/dev/null
jq -e '.groups[0] | .recommendation.kind == "try_more_cpu" and .recommendation.confidence == "supported" and .sample_count >= 3' cpu.json >/dev/null
jq -e --arg id "$cpu" '.groups[0].source_query_ids | index($id) != null' cpu.json >/dev/null
jq -e '.groups[0].evidence.cpu_utilization >= 0.8 and .groups[0].allocation.cpu == 1' cpu.json >/dev/null
jq -e '.groups[0].recommendation.next_experiment | contains("--cpu 2")' cpu.json >/dev/null
"$binary" advise query "$cpu" > cpu.txt
grep -q '^recommendation: try_more_cpu (supported)' cpu.txt
grep -q 'delivery=unknown' cpu.txt
grep -q 'not container RSS or provider cost; nothing was changed' cpu.txt

# A sort larger than the 512 MiB engine limit spills.
for _ in 1 2 3; do
  "$binary" sql "select max(rn) as mem_$tag from (select row_number() over (order by md5(range::varchar)) rn from range(8000000))" >/dev/null
done
memory=$(last_id "mem_$tag")
advise query "$memory" > memory.json
jq -e '.groups[0] | .recommendation.kind == "try_more_memory" and .evidence.peak_temp_dir_bytes > 0' memory.json >/dev/null
jq -e '.groups[0].recommendation.next_experiment | contains("--memory 2.0Gi")' memory.json >/dev/null

# Queue contention: a short durable job waits behind a long one on the
# single-statement worker, so its queue time is retained in history.
"$binary" job submit "select sum(sin(range)) as hold_$tag from range(250000000)" >/dev/null 2>&1
probe=$("$binary" job submit "select 42 as queued_$tag" --queue-timeout 120 2>/dev/null | jq -r .request_id)
for _ in {1..60}; do
  [[ "$("$binary" job status "$probe" | jq -r .state)" == terminal ]] && break
  sleep 1
done
queued=$(last_id "queued_$tag")
advise query "$queued" --thresholds min_samples=1 > queue.json
jq -e '.groups[0] | .recommendation.kind == "investigate_queueing" and .phases_ms.queue > .phases_ms.engine' queue.json >/dev/null
jq -e '.groups[0].recommendation.summary | contains("would not shorten")' queue.json >/dev/null

# Few samples are qualified, never presented as supported.
"$binary" sql "select sum(sin(range)) as once_$tag from range(60000000)" >/dev/null
advise query "$(last_id "once_$tag")" > once.json
jq -e '.groups[0].recommendation.confidence == "qualified" and (.groups[0].recommendation.qualifications | any(contains("sample")))' once.json >/dev/null

# A failed run carries no usable timing evidence.
"$binary" sql "select * from missing_relation_$tag" >/dev/null 2>&1 || true
failed=$("$binary" history --limit 5 --format json | jq -r --arg tag "$tag" '[.queries[] | select(.sql != null and (.sql | contains("missing_relation_" + $tag)))][0].query_id')
advise query "$failed" > failed.json
jq -e '.groups[0].recommendation.kind == "insufficient_evidence" and (.skipped | length == 1)' failed.json >/dev/null

# A dbt model aggregate groups only compatible allocations.
model="model.advise_fixture.$tag"
session=$(curl -fsS -X POST -H "Authorization: Bearer $token" -H 'Content-Type: application/json' \
  --data "{\"compute\":{},\"context\":{\"dbt\":{\"node_id\":\"$model\"}}}" "$url/v1/sessions" | jq -r .session_id)
for _ in 1 2 3; do
  curl -fsS -o /dev/null -X POST -H "Authorization: Bearer $token" -H 'Content-Type: application/json' \
    --data "{\"request_id\":\"$(uuidgen | tr '[:upper:]' '[:lower:]')\",\"sql\":\"select sum(sin(range)) from range(80000000)\",\"context\":{\"dbt\":{\"node_id\":\"$model\"}},\"timeout_seconds\":60}" \
    "$url/v1/sessions/$session/queries"
done
curl -fsS -o /dev/null -X DELETE -H "Authorization: Bearer $token" "$url/v1/sessions/$session"
advise model "$model" > model.json
jq -e '.mixed_allocations == false and (.groups | length == 1) and .groups[0].sample_count == 3' model.json >/dev/null
jq -e '.groups[0].recommendation.kind == "try_more_cpu"' model.json >/dev/null

# A metadata-only operator sees timing advice but no SQL or profile content.
principal="advise-operator-$(date +%s)-$$"
issued=$(curl -fsS -X POST -H "Authorization: Bearer $token" -H 'Content-Type: application/json' --data "{\"principal\":\"$principal\"}" "$url/v1/credentials")
operator_token=$(jq -r '.token' <<<"$issued")
operator_id=$(jq -r '.credential_id' <<<"$issued")
policy=$(curl -fsS -H "Authorization: Bearer $token" "$url/v1/principals/$principal/policy")
body=$(jq '{roles:["operator"],permissions:[],compute:.compute,disabled:false}' <<<"$policy")
curl -fsS -X PUT -H "Authorization: Bearer $token" -H 'Content-Type: application/json' --data "$body" "$url/v1/principals/$principal/policy" >/dev/null
"$binary" --token "$operator_token" advise query "$memory" --format json > operator.json
jq -e '.groups[0].sample_count == 1 and .groups[0].recommendation.kind != "try_more_memory"' operator.json >/dev/null
jq -e '.groups[0].recommendation.confidence != "supported"' operator.json >/dev/null
if grep -q "mem_$tag\|md5\|row_number" operator.json; then
  echo 'advisor exposed SQL text to a metadata-only caller' >&2
  exit 1
fi

# Advice reads only retained metadata: it works with the worker stopped and
# leaves no session, request, or configuration behind.
(cd "$root" && docker compose stop worker-small >/dev/null)
before=$(psql "select (select count(*) from requests) || ',' || (select count(*) from gateway_sessions) || ',' || (select string_agg(scope || max_running, ',' order by scope) from admission_policies)")
advise query "$cpu" > stopped.json
"$binary" advise model "$model" >/dev/null
after=$(psql "select (select count(*) from requests) || ',' || (select count(*) from gateway_sessions) || ',' || (select string_agg(scope || max_running, ',' order by scope) from admission_policies)")
(cd "$root" && docker compose up -d --wait worker-small >/dev/null)
[[ "$before" == "$after" ]]
diff <(jq -S '.groups[0].recommendation' cpu.json) <(jq -S '.groups[0].recommendation' stopped.json)

printf 'PASS: CPU, memory/spill, queueing, model aggregate, qualified and insufficient evidence, metadata-only privacy, and read-only advice with workers stopped\n'
