#!/usr/bin/env bash
# Reproduces the #172 connectivity matrix against the running Compose stack:
#   W1/W2  prototype ADBC driver via adbc-driver-manager 1.13.0, pyarrow 26.0.1, pandas 3.1.1
#   W3     psql 17.6 (the postgres:17.6-bookworm image) against the pgwire front
#   W4     psycopg 3.3.9 against the pgwire front
# Writes prototypes/connectivity/matrix.json. Requires: uv, docker, cargo, jq.
set -euo pipefail
root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
token=${DUCKFLEET_TOKEN:-duckfleet-local-development-token}
out="$root/prototypes/connectivity/matrix.json"
network=$(docker network ls --format '{{.Name}}' | grep -m1 'duckfleet-local_default')

(cd "$root/prototypes/adbc-duckfleet" && cargo build --release --quiet)
driver=$(ls "$root"/prototypes/adbc-duckfleet/target/release/libadbc_duckfleet.{dylib,so} 2>/dev/null | head -1 || true)

docker rm -f duckfleet-pgwire-forward >/dev/null 2>&1 || true
docker run -d --rm --name duckfleet-pgwire-forward --network "$network" -p 127.0.0.1:15432:5432 \
  alpine/socat:1.8.0.3 tcp-listen:5432,fork,reuseaddr tcp-connect:server:5432 >/dev/null
trap 'docker rm -f duckfleet-pgwire-forward >/dev/null 2>&1 || true' EXIT
sleep 1

python_results=$(uv run --quiet --python 3.12 --no-project \
  --with adbc-driver-manager==1.13.0 --with pyarrow==26.0.1 --with pandas==3.1.1 --with 'psycopg[binary]==3.3.9' \
  "$root/prototypes/connectivity/workflows.py" --driver "$driver" --token "$token")

w3="W3 psql 17.6"
psql_results="[]"
# psql_check <capability> <pass-marker> <script on stdin> [notes]
psql_check() {
  local capability=$1 expect=$2 script=$3 note=${4:-}
  local output code=0 started ended
  started=$(date +%s)
  output=$(printf '%s\n' "$script" | docker run -i --rm --network "$network" -e PGPASSWORD="$token" postgres:17.6-bookworm \
    timeout -k 5 -s INT "${PSQL_TIMEOUT:-20}" psql "host=server port=5432 user=duckfleet dbname=lake ${PSQL_SSL:-sslmode=disable}" -X -At 2>&1 | grep -v '^NOTICE:  query submitted') || code=$?
  ended=$(date +%s)
  local outcome=fails
  if [[ "$output" == *"$expect"* ]]; then outcome=works; fi
  psql_results=$(jq --arg w "$w3" --arg c "$capability" --arg o "$outcome" \
    --arg d "${note}exit $code after $((ended - started))s: ${output:0:300}" \
    '. + [{workflow: $w, capability: $c, outcome: $o, detail: $d}]' <<<"$psql_results")
}
psql_check "simple query" "simple_ok_42" "select 'simple_ok_' || (41 + 1);"
psql_check "extended bind (\\bind)" "bind_ok_42" 'select '"'"'bind_ok_'"'"' || ($1::int + 1) \bind 41 \g'
psql_check "COPY TO STDOUT (\\copy)" "copy_ok" '\copy (select '"'"'copy_ok'"'"') to stdout' \
  "no CopyOut reaches the client: the worker executes DuckDB COPY ... TO STDOUT itself, writing the rows to its own stdout (container logs), and returns only a row count. "
psql_check "null and nested types" "[1, 2]" "select [1, 2] as l;"
psql_check "large results (90k rows)" "89999" "select i from range(90000) t(i);"
psql_check "aborted-transaction SQLSTATE (25P02)" "status 25P02" $'begin;\nselect * from missing_relation;\nselect 1;\n\\echo status :LAST_ERROR_SQLSTATE' \
  "the engine enforces the aborted transaction, but the error is reported with SQLSTATE 42601 and ReadyForQuery stays I. "
PSQL_TIMEOUT=3 psql_check "native cancellation (Ctrl-C sends CancelRequest)" "canceling statement" \
  "select sum(sin(range)) from range(100000000000);"
PSQL_SSL=sslmode=require psql_check "TLS (sslmode=require)" "tls_ok" "select 'tls_ok';"

jq -s 'add' <(printf '%s' "$python_results") <(printf '%s' "$psql_results") > "$out"
jq -r '.[] | "\(.outcome)\t\(.workflow)\t\(.capability)"' "$out"
