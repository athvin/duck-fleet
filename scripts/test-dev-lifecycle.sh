#!/usr/bin/env bash
set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
binary=${DUCKFLEET_BINARY:-"$root/server/target/debug/duckfleet"}
(cd "$root/server" && cargo build --locked -p duckfleet-cli)

base="$root/artifacts/dev-lifecycle-test"
a="$base/a"
b="$base/b"
port_a=${DUCKFLEET_TEST_DEV_PORT_A:-19180}
port_b=${DUCKFLEET_TEST_DEV_PORT_B:-19181}
cleanup() {
  for project in "$a" "$b"; do
    if [[ -f "$project/duckfleet.json" ]]; then
      (cd "$project" && "$binary" reset --yes >/dev/null 2>&1) || true
    fi
  done
  rm -rf "$base"
}
trap cleanup EXIT
rm -rf "$base"
mkdir -p "$a" "$b"

cd "$a"
"$binary" init --url "http://127.0.0.1:$port_a"
"$binary" dev
"$binary" sql 'select 42 as lifecycle_answer' > first-query.out
[[ "$(<first-query.out)" == *42* ]]
"$binary" status > ready.out
[[ "$(<ready.out)" == *'state: ready'* ]]
[[ "$(<ready.out)" != *duckfleet-local-development-token* ]]
"$binary" dev >/dev/null

"$binary" sql 'create schema if not exists analytics' \
  'create or replace table analytics.lifecycle_persist (id integer)' \
  'insert into analytics.lifecycle_persist values (42)' >/dev/null
"$binary" stop >/dev/null
set +e
"$binary" status > stopped.out 2>&1
stopped_code=$?
set -e
[[ "$stopped_code" -eq 1 ]]
[[ "$(<stopped.out)" == *'state: stopped'* ]]
"$binary" dev >/dev/null
"$binary" sql 'select * from analytics.lifecycle_persist' > persisted.out
[[ "$(<persisted.out)" == *42* ]]
"$binary" history --limit 5 --format json > history.json
[[ "$(<history.json)" == *lifecycle_persist* ]]

project_a="$("$binary" config show | jq -r '.compose_project')"
docker compose --project-name "$project_a" --file "$root/compose.yaml" stop worker-small >/dev/null
set +e
"$binary" status > degraded.out 2>&1
degraded_code=$?
set -e
[[ "$degraded_code" -eq 3 ]]
[[ "$(<degraded.out)" == *'state: degraded'* ]]
"$binary" dev >/dev/null

set +e
DUCKFLEET_POSTGRES_ADMIN_PASSWORD=wrong-local-password "$binary" dev > failed-dependency.out 2>&1
failed_dependency_code=$?
set -e
[[ "$failed_dependency_code" -eq 1 ]]
[[ "$(<failed-dependency.out)" == *platform-provision* ]]
[[ "$(<failed-dependency.out)" == *logs* ]]
"$binary" dev >/dev/null
"$binary" sql 'select * from analytics.lifecycle_persist' >/dev/null

"$binary" dev --demo >/dev/null
"$binary" sql 'select count(*) from retail.fct_orders' > demo.out
[[ "$(<demo.out)" == *20* ]]
"$binary" dev --demo > demo-repeat.out
[[ "$(<demo-repeat.out)" == *'retail demo already loaded'* ]]

cd "$b"
"$binary" init --url "http://127.0.0.1:$port_b"
"$binary" dev >/dev/null
"$binary" sql 'select 81 as project_b' >/dev/null
project_b="$("$binary" config show | jq -r '.compose_project')"
[[ "$project_a" != "$project_b" ]]
set +e
"$binary" reset > reset-refused.out 2>&1 </dev/null
reset_refused_code=$?
set -e
[[ "$reset_refused_code" -eq 2 ]]
[[ "$(<reset-refused.out)" == *--yes* ]]
"$binary" reset --yes >/dev/null
[[ -z "$(docker compose --project-name "$project_b" --file "$root/compose.yaml" ps --all --quiet)" ]]
[[ -z "$(docker volume ls --filter "label=com.docker.compose.project=$project_b" --quiet)" ]]

cd "$a"
"$binary" sql 'select * from analytics.lifecycle_persist' >/dev/null
[[ -n "$(docker compose --project-name "$project_a" --file "$root/compose.yaml" ps --status running --quiet)" ]]
[[ -n "$(docker volume ls --filter "label=com.docker.compose.project=$project_a" --quiet)" ]]
"$binary" reset --yes >/dev/null
[[ ! -e .duckfleet/demo.json ]]
[[ -z "$(docker volume ls --filter "label=com.docker.compose.project=$project_a" --quiet)" ]]

printf 'PASS: fresh/warm/failed lifecycle, demo, status, and two-project reset isolation\n'
