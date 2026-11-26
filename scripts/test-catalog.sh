#!/usr/bin/env bash
set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
(cd "$root/server" && cargo build --locked -p duckfleet-cli)
binary="$root/server/target/debug/duckfleet"
url=${DUCKFLEET_URL:-http://127.0.0.1:18080}
token=${DUCKFLEET_TOKEN:-duckfleet-local-development-token}
project="$root/artifacts/catalog-test"
reader_id=""
cleanup() {
  cd "$project" 2>/dev/null || true
  "$binary" sql 'drop schema if exists catalog_fixture cascade' 'drop schema if exists other_fixture cascade' 'drop schema if exists "Empty Schema" cascade' 'drop schema if exists "Odd.Schema" cascade' >/dev/null 2>&1 || true
  if [[ -n "$reader_id" ]]; then
    curl -fsS -X DELETE -H "Authorization: Bearer $token" "$url/v1/credentials/$reader_id" >/dev/null 2>&1 || true
  fi
  (cd "$root" && docker compose --profile readers stop server-readers worker-small-reader >/dev/null 2>&1) || true
  rm -rf "$project"
}
trap cleanup EXIT
rm -rf "$project"
mkdir -p "$project"
cd "$project"
"$binary" init --url "$url" --token "$token" >/dev/null

"$binary" sql \
  'create schema if not exists catalog_fixture' \
  'create or replace table catalog_fixture.events (id integer, "mixed.Name" varchar, tags varchar[])' \
  "insert into catalog_fixture.events values (1, 'alpha', ['x','y']), (2, 'beta', ['z'])" \
  'create or replace view catalog_fixture.event_names as select id, "mixed.Name" from catalog_fixture.events' \
  'create or replace table catalog_fixture."dot.name" (id integer)' \
  'insert into catalog_fixture."dot.name" values (7)' \
  'create or replace table catalog_fixture."quote""name" (id integer)' \
  'create schema if not exists "Empty Schema"' \
  'create schema if not exists other_fixture' \
  'create or replace table other_fixture.events (id integer)' \
  'create schema if not exists "Odd.Schema"' \
  'create or replace table "Odd.Schema"."space table" (id integer)' >/dev/null

"$binary" schemas --format json > schemas.json
"$binary" tables --schema catalog_fixture --format json > tables.json
"$binary" views --schema catalog_fixture --format json > views.json
"$binary" describe catalog_fixture.events --format json > describe.json
"$binary" describe 'catalog_fixture."dot.name"' > dot.out
"$binary" describe 'catalog_fixture."quote""name"' > quote.out
"$binary" describe '"Odd.Schema"."space table"' > space.out
"$binary" preview catalog_fixture.events --limit 1 --format json > preview.json 2>preview.err
"$binary" tables --schema 'Empty Schema' > empty.out
jq -e '.[] | select(.name == "Empty Schema")' schemas.json >/dev/null
jq -e '.[] | select(.relation_name == "events")' tables.json >/dev/null
jq -e '.[] | select(.relation_name == "event_names")' views.json >/dev/null
jq -e '.[] | select(.column_name == "mixed.Name" and .is_nullable == "YES")' describe.json >/dev/null
jq -e 'length == 1' preview.json >/dev/null
[[ "$(<preview.err)" == *'preview_limit: 1'* ]]
[[ "$(<dot.out)" == *'dot.name'* ]]
[[ "$(<quote.out)" == *'quote"name'* ]]
[[ "$(<space.out)" == *'space table'* ]]
[[ "$(<empty.out)" == *'(0 rows)'* ]]

set +e
ambiguous="$("$binary" describe events 2>&1)"; ambiguous_code=$?
missing="$("$binary" describe catalog_fixture.missing 2>&1)"; missing_code=$?
missing_schema="$("$binary" tables --schema no_such_schema 2>&1)"; missing_schema_code=$?
injection="$("$binary" preview 'catalog_fixture.events; DROP TABLE events' 2>&1)"; injection_code=$?
invalid_limit="$("$binary" --url http://127.0.0.1:1 --token invalid preview catalog_fixture.events --limit 0 2>&1)"; invalid_limit_code=$?
set -e
[[ "$ambiguous_code" -eq 1 && "$ambiguous" == *ambiguous_relation* ]]
[[ "$missing_code" -eq 1 && "$missing" == *missing_relation* ]]
[[ "$missing_schema_code" -eq 1 && "$missing_schema" == *missing_schema* ]]
[[ "$injection_code" -eq 1 && "$injection" == *missing_relation* ]]
[[ "$invalid_limit_code" -eq 2 && "$invalid_limit" == *'--limit must be'* ]]
"$binary" preview catalog_fixture.events --limit 1 >/dev/null

cd "$root"
docker compose --profile readers up -d --wait server-readers >/dev/null
principal="catalog-reader-$(date +%s)-$$"
issued=$(curl -fsS -X POST -H "Authorization: Bearer $token" -H 'Content-Type: application/json' \
  --data "{\"principal\":\"$principal\"}" "$url/v1/credentials")
reader_token=$(jq -r '.token' <<<"$issued")
reader_id=$(jq -r '.credential_id' <<<"$issued")
policy=$(curl -fsS -H "Authorization: Bearer $token" "$url/v1/principals/$principal/policy")
body=$(jq '.compute[0].pool = "small-reader" | {roles:["viewer"],permissions:[],compute:.compute,disabled:false}' <<<"$policy")
curl -fsS -X PUT -H "Authorization: Bearer $token" -H 'Content-Type: application/json' \
  --data "$body" "$url/v1/principals/$principal/policy" >/dev/null
reader_url=http://127.0.0.1:18082
"$binary" --url "$reader_url" --token "$reader_token" schemas --format json > "$project/reader-schemas.json"
"$binary" --url "$reader_url" --token "$reader_token" describe catalog_fixture.events --format json > "$project/reader-describe.json"
"$binary" --url "$reader_url" --token "$reader_token" preview catalog_fixture.events --limit 1 --format json > "$project/reader-preview.json" 2>/dev/null
jq -e 'length == 1' "$project/reader-preview.json" >/dev/null
set +e
"$binary" --url "$reader_url" --token "$reader_token" sql 'create table catalog_fixture.reader_escape(id integer)' > "$project/reader-denied.out" 2>&1
reader_denied=$?
set -e
[[ "$reader_denied" -eq 1 ]]
[[ "$(<"$project/reader-denied.out")" != *password* ]]

printf 'PASS: real catalog discovery, bounded previews, quoted identifiers, injection refusal, and reader authority\n'
