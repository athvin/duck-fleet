#!/usr/bin/env bash
set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
(cd "$root/server" && cargo build --locked -p duckfleet-cli)
binary="$root/server/target/debug/duckfleet"
url=${DUCKFLEET_URL:-http://127.0.0.1:18080}
token=${DUCKFLEET_TOKEN:-duckfleet-local-development-token}
project="$root/artifacts/shell-test"
rm -rf "$project"
mkdir -p "$project"
trap 'rm -rf "$project"' EXIT
cd "$project"
"$binary" init --url "$url" --token "$token" >/dev/null

printf '%s\n' \
  'create temporary table shell_file (id integer, note varchar);' \
  "insert into shell_file values (1, 'semi;colon');" \
  'select * from shell_file' > session.sql
"$binary" sql --file session.sql > file.out
[[ "$(<file.out)" == *'semi;colon'* ]]
printf 'create temporary table shell_stdin (id integer); insert into shell_stdin values (42); select * from shell_stdin;' | "$binary" sql > stdin.out
[[ "$(<stdin.out)" == *42* ]]

printf '%s\n' \
  'select 1 as before_error;' \
  'select * from definitely_missing_shell_table;' \
  'select 2 as after_error;' > errors.sql
set +e
"$binary" sql --file errors.sql > stopped.out 2>&1
stopped=$?
"$binary" sql --file errors.sql --continue-on-error > continued.out 2>&1
continued=$?
set -e
[[ "$stopped" -eq 1 ]]
[[ "$continued" -eq 1 ]]
[[ "$(<stopped.out)" == *before_error* ]]
[[ "$(<stopped.out)" != *after_error* ]]
[[ "$(<continued.out)" == *after_error* ]]

cd "$root/server"
DUCKFLEET_URL="$url" DUCKFLEET_TOKEN="$token" \
  cargo test --locked -p duckfleet-cli --test shell_pty -- --ignored --test-threads=1
printf 'PASS: SQL file/stdin and real PTY shell lifecycle\n'
