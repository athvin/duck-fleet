#!/usr/bin/env bash
# Real PostgreSQL clients against the pgwire front on the Compose network:
# psql 17.6 and psycopg 3.3.9, both in pinned containers.
set -euo pipefail
trap 'echo "pgwire client test failed at line $LINENO" >&2' ERR

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
token=${DUCKFLEET_TOKEN:-duckfleet-local-development-token}
network=$(docker network ls --format '{{.Name}}' | grep -m1 'duckfleet-local_default')
dsn="host=server port=5432 user=duckfleet dbname=lake sslmode=disable password=$token"

psql_run() {
  printf '%s\n' "$1" | docker run -i --rm --network "$network" postgres:17.6-bookworm \
    psql "$dsn" -X -At -v VERBOSITY=verbose 2>&1 | grep -v '^NOTICE:  query submitted' || true
}

# An aborted transaction reports 25P02 until it ends; COMMIT ends it.
output=$(psql_run $'begin;\nselect * from missing_relation;\nselect 1;\n\\echo state :LAST_ERROR_SQLSTATE\ncommit;\nselect \'after_\' || 42;')
[[ "$output" == *"state 25P02"* ]]
[[ "$output" == *"after_42"* ]]

# A nested result is refused with 0A000 and the same connection keeps working.
output=$(psql_run $'select [1, 2] as l;\n\\echo state :LAST_ERROR_SQLSTATE\nselect \'still_\' || 42;')
[[ "$output" == *"state 0A000"* && "$output" == *"cast it to VARCHAR"* && "$output" == *"still_42"* ]]

# psql Ctrl-C sends CancelRequest; the statement stops well before its deadline.
started=$(date +%s)
output=$(printf 'select sum(sin(range)) from range(100000000000);\n' | docker run -i --rm --network "$network" postgres:17.6-bookworm \
  timeout -k 20 -s INT 8 psql "$dsn" -X -At -v VERBOSITY=verbose 2>&1 || true)
# The interrupt lands at 8 s, after container start and session open; the
# statement's own deadline is 60 s, so finishing well before that is the cancel.
[[ "$(( $(date +%s) - started ))" -lt 40 ]]
[[ "$output" == *"57014"* && "$output" == *"[canceled]"* ]]

# Third-party client compatibility runs only in a pinned container; the
# repository's own tooling stays Rust-only (see compat/README.md).
docker run --rm --network "$network" -e PGWIRE_DSN="$dsn" -v "$root/compat:/compat:ro" \
  ghcr.io/astral-sh/uv:0.9.13-python3.12-bookworm-slim \
  uv run --quiet --no-project --with 'psycopg[binary]==3.3.9' /compat/pgwire_clients.py

curl -fsS -H "Authorization: Bearer $token" "${DUCKFLEET_URL:-http://127.0.0.1:18080}/v1/queries?status=canceled&limit=5" \
  | jq -e 'any(.queries[]; .context.operation == "pgwire")' >/dev/null

printf 'PASS: psql 17.6 and psycopg 3.3.9 see truthful transaction status, 25P02, commit/rollback, 40001, classified unsupported types, and CancelRequest\n'
