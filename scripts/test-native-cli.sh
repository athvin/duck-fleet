#!/usr/bin/env bash
set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
binary=${DUCKFLEET_BINARY:-"$root/server/target/debug/duckfleet"}
(cd "$root/server" && cargo build --locked -p duckfleet-cli)

project=$(mktemp -d "${TMPDIR:-/tmp}/duckfleet-native-cli.XXXXXX")
trap 'rm -rf "$project"' EXIT
cd "$project"

DUCKFLEET_TOKEN=${DUCKFLEET_TOKEN:-duckfleet-local-development-token} "$binary" init
unset DUCKFLEET_URL DUCKFLEET_TOKEN

"$binary" version
"$binary" config show > config.json
"$binary" sql 'select 42 as answer' > sql.out
"$binary" history --limit 5 > history.json
"$binary" workers > workers.json
"$binary" pools > pools.json

[[ "$(<sql.out)" == *42* ]]
[[ "$(<config.json)" == *'<redacted>'* ]]
[[ "$(<config.json)" != *duckfleet-local-development-token* ]]
[[ "$(<duckfleet.json)" != *duckfleet-local-development-token* ]]
[[ "$(<history.json)" == *'select 42 as answer'* ]]

DUCKFLEET_URL=http://environment.invalid DUCKFLEET_TOKEN=environment-secret "$binary" config show > environment.json
[[ "$(<environment.json)" == *'"url_source": "environment"'* ]]
[[ "$(<environment.json)" != *environment-secret* ]]
"$binary" --url http://flag.invalid --token flag-secret config show > flag.json
[[ "$(<flag.json)" == *'"url_source": "flag"'* ]]
[[ "$(<flag.json)" != *flag-secret* ]]

printf '{broken\n' > duckfleet.json
if "$binary" workers > malformed.out 2>&1; then
  exit 1
fi
[[ "$(<malformed.out)" == *malformed* ]]

printf 'PASS: native CLI project configuration and real Compose commands\n'
