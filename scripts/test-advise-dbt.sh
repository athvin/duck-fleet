#!/usr/bin/env bash
# Real dbt artifacts for the conflict advisor: the fixture project compiled by
# the pinned dbt binary, the retail demo as a false-positive check, offline
# operation, strict/malformed exit codes, and a real transaction_conflict
# recorded under the two models' identities and correlated from history.
set -euo pipefail
trap 'echo "advise dbt test failed at line $LINENO" >&2' ERR

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
(cd "$root/server" && cargo build --locked -p duckfleet-cli)
binary="$root/server/target/debug/duckfleet"
url=${DUCKFLEET_URL:-http://127.0.0.1:18080}
token=${DUCKFLEET_TOKEN:-duckfleet-local-development-token}
work="$root/artifacts/advise-dbt-test"
operator_id=""
cleanup() {
  if [[ -n "$operator_id" ]]; then
    curl -fsS -X DELETE -H "Authorization: Bearer $token" "$url/v1/credentials/$operator_id" >/dev/null 2>&1 || true
  fi
  "$binary" --url "$url" --token "$token" sql 'drop schema if exists conflict_fixture cascade' >/dev/null 2>&1 || true
  # dbt runs as root in its container, so its target/ and logs/ are not ours.
  docker run --rm -v "$root/artifacts:/artifacts" alpine:3.20 rm -rf /artifacts/advise-dbt-test >/dev/null 2>&1 || true
  rm -rf "$work"
}
trap cleanup EXIT
docker run --rm -v "$root/artifacts:/artifacts" alpine:3.20 rm -rf /artifacts/advise-dbt-test >/dev/null 2>&1 || true
rm -rf "$work"
mkdir -p "$work"
cd "$work"
cp -R "$root/tests/dbt-conflicts" "$work/conflicts"
cp -R "$root/examples/retail-demo" "$work/retail"
rm -rf "$work/conflicts/target" "$work/retail/target"
dbt() {
  (cd "$root" && docker compose --profile test run --rm --no-deps -T -v "$1:/project" -w /project dbt dbt "${@:2}" --project-dir /project --profiles-dir /project >/dev/null)
}
psql() {
  (cd "$root" && docker compose exec -T postgres psql -U duckfleet -d platform -v ON_ERROR_STOP=1 -qtAX -c "$1")
}
dbt "$work/conflicts" compile
dbt "$work/retail" compile
models_before=$(cd "$work/conflicts/models" && shasum ./*.sql | shasum)

# Static analysis needs no warehouse, credentials, or configuration.
offline() { env -u DUCKFLEET_URL -u DUCKFLEET_TOKEN HOME="$work/empty-home" XDG_CONFIG_HOME="$work/empty-home" "$binary" "$@"; }
mkdir -p "$work/empty-home"
requests_before=$(psql "select count(*) from requests")
offline advise dbt --project-dir "$work/conflicts" --format json > static.json
offline advise dbt --project-dir "$work/conflicts" > static.txt
[[ "$(psql "select count(*) from requests")" == "$requests_before" ]]
jq -e '.warnings | length == 1' static.json >/dev/null 2>&1 || { jq '.warnings' static.json >&2; exit 1; }
jq -e '.warnings[0].relation == "lake.conflict_fixture.shared_log"' static.json >/dev/null
jq -e '.warnings[0].models == ["model.conflict_fixture.branch_a", "model.conflict_fixture.branch_b"]' static.json >/dev/null
jq -e '.warnings[0].writes | all(.kind == "rewrite")' static.json >/dev/null
jq -e '.warnings[0].remedy | contains("ref(") and contains("models/")' static.json >/dev/null
for clean in ordered_c own_target appender_1 appender_2 reader_view events_log; do
  jq -e --arg m "model.conflict_fixture.$clean" '[.warnings[].models[]] | index($m) == null' static.json >/dev/null
done
jq -e '.analyzed == 9 and .unknown == 1 and .unknown_coverage[0].model == "model.conflict_fixture.dynamic_hook"' static.json >/dev/null
grep -q '^summary: 1 warning(s); 9 node(s) analyzed, 1 with unknown coverage' static.txt
grep -q '^UNKNOWN model.conflict_fixture.dynamic_hook: post-hook 1 is dynamic' static.txt

# The retail demo writes only its own tables: no false positives.
offline advise dbt --project-dir "$work/retail" --format json > retail.json
jq -e '.warnings == [] and .unknown == 0 and .analyzed >= 6' retail.json >/dev/null

# Exit codes: warnings succeed by default and fail under --strict; unusable
# artifacts fail either way.
offline advise dbt --project-dir "$work/retail" --strict >/dev/null
strict=0
offline advise dbt --project-dir "$work/conflicts" --strict >/dev/null || strict=$?
[[ "$strict" -eq 1 ]]
mkdir -p "$work/malformed/target" "$work/missing"
printf '{"nodes":' > "$work/malformed/target/manifest.json"
for broken in "$work/malformed" "$work/missing"; do
  code=0
  offline advise dbt --project-dir "$broken" >/dev/null 2>broken.err || code=$?
  [[ "$code" -eq 2 && "$(<broken.err)" == invalid_dbt_artifacts* ]]
done

# A real overlapping write under the two models' identities: both delete from
# shared_log in open transactions; the second commit loses.
dbt "$work/conflicts" run --threads 1
open_session() {
  curl -fsS -X POST -H "Authorization: Bearer $token" -H 'Content-Type: application/json' \
    --data "{\"compute\":{},\"context\":{\"dbt\":{\"node_id\":\"$1\",\"invocation_id\":\"advise-$$\"}}}" "$url/v1/sessions" | jq -r .session_id
}
statement() {
  curl -sS -o "$work/statement.json" -w '%{http_code}' -X POST -H "Authorization: Bearer $token" -H 'Content-Type: application/json' \
    --data "{\"request_id\":\"$(uuidgen | tr '[:upper:]' '[:lower:]')\",\"sql\":\"$2\",\"context\":{\"dbt\":{\"node_id\":\"$3\",\"invocation_id\":\"advise-$$\"}}}" \
    "$url/v1/sessions/$1/queries"
}
a=$(open_session model.conflict_fixture.branch_a)
b=$(open_session model.conflict_fixture.branch_b)
statement "$a" "begin" model.conflict_fixture.branch_a >/dev/null
statement "$b" "begin" model.conflict_fixture.branch_b >/dev/null
# Both rewrite the same data file (the seed row); DuckLake conflicts per file.
statement "$a" "delete from lake.conflict_fixture.shared_log where source = 'seed'" model.conflict_fixture.branch_a >/dev/null
statement "$b" "delete from lake.conflict_fixture.shared_log where source = 'seed'" model.conflict_fixture.branch_b >/dev/null
[[ "$(statement "$a" "commit" model.conflict_fixture.branch_a)" == 200 ]]
[[ "$(statement "$b" "commit" model.conflict_fixture.branch_b)" == 409 ]]
jq -e '.failure_kind == "transaction_conflict"' "$work/statement.json" >/dev/null
for session in "$a" "$b"; do
  curl -fsS -o /dev/null -X DELETE -H "Authorization: Bearer $token" "$url/v1/sessions/$session"
done
"$binary" --url "$url" --token "$token" advise dbt --project-dir "$work/conflicts" --history --format json > history.json
jq -e '.warnings[0].confidence == "observed_race"' history.json >/dev/null
jq -e '.warnings[0].observed_conflicts | any(.model == "model.conflict_fixture.branch_b" and .invocation_id != null)' history.json >/dev/null

# A metadata-only operator gets the static warning but no model identity or
# SQL from history.
principal="advise-dbt-operator-$(date +%s)-$$"
issued=$(curl -fsS -X POST -H "Authorization: Bearer $token" -H 'Content-Type: application/json' --data "{\"principal\":\"$principal\"}" "$url/v1/credentials")
operator_token=$(jq -r '.token' <<<"$issued")
operator_id=$(jq -r '.credential_id' <<<"$issued")
policy=$(curl -fsS -H "Authorization: Bearer $token" "$url/v1/principals/$principal/policy")
body=$(jq '{roles:["operator"],permissions:[],compute:.compute,disabled:false}' <<<"$policy")
curl -fsS -X PUT -H "Authorization: Bearer $token" -H 'Content-Type: application/json' --data "$body" "$url/v1/principals/$principal/policy" >/dev/null
"$binary" --url "$url" --token "$operator_token" advise dbt --project-dir "$work/conflicts" --history --format json > operator.json
jq -e '.warnings | length == 1 and .[0].observed_conflicts == [] and .[0].confidence == "static_possibility"' operator.json >/dev/null
if grep -q 'delete from lake' operator.json; then
  echo 'restricted SQL text reached a metadata-only caller' >&2
  exit 1
fi

# Analysis never edits the project.
[[ "$(cd "$work/conflicts/models" && shasum ./*.sql | shasum)" == "$models_before" ]]

printf 'PASS: dbt conflict advice on real artifacts: true positive, ordered/different/append/read-only negatives, dynamic coverage, retail false-positive check, offline, strict and malformed exits, observed-race correlation, and metadata-only privacy\n'
