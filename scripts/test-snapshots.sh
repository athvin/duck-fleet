#!/usr/bin/env bash
# Real Compose snapshot tests: a known multi-snapshot table, listing and
# pagination, time travel against direct DuckLake SQL, a net diff against an
# independent rowid join, empty and schema-change diffs, classified refusals,
# reader authority, cancellation, and proof that no read changes the lake.
set -euo pipefail
trap 'echo "snapshot test failed at line $LINENO" >&2' ERR

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
(cd "$root/server" && cargo build --locked -p duckfleet-cli)
binary="$root/server/target/debug/duckfleet"
url=${DUCKFLEET_URL:-http://127.0.0.1:18080}
token=${DUCKFLEET_TOKEN:-duckfleet-local-development-token}
project="$root/artifacts/snapshot-test"
schema="snap_fixture_$$"
reader_id=""
active_pid=""
cleanup() {
  if [[ -n "$active_pid" ]]; then
    kill -KILL "$active_pid" >/dev/null 2>&1 || true
  fi
  if [[ -n "$reader_id" ]]; then
    curl -fsS -X DELETE -H "Authorization: Bearer $token" "$url/v1/credentials/$reader_id" >/dev/null 2>&1 || true
  fi
  (cd "$root" && docker compose --profile readers stop server-readers worker-small-reader >/dev/null 2>&1) || true
  (cd "$project" 2>/dev/null && "$binary" sql "drop schema if exists $schema cascade" >/dev/null 2>&1) || true
  rm -rf "$project"
}
trap cleanup EXIT
rm -rf "$project"
mkdir -p "$project"
cd "$project"
"$binary" init --url "$url" --token "$token" >/dev/null
sql() { "$binary" sql "$1" --format json; }
latest() { sql "select max(snapshot_id)::bigint as id from ducklake_snapshots('lake')" | jq -r '.[0].id'; }
expect_failure() {
  local code=0
  "$@" >expected-failure.out 2>&1 || code=$?
  [[ "$code" -ne 0 ]]
}

sql "create schema $schema" >/dev/null
sql "create table $schema.t as select i as id, 'v' || i as v from range(5) r(i)" >/dev/null
s1=$(latest)
sql "insert into $schema.t values (10, 'ten'), (11, 'eleven')" >/dev/null
s2=$(latest)
sql "update $schema.t set v = 'updated' where id in (1, 10)" >/dev/null
s3=$(latest)
sql "delete from $schema.t where id in (0, 11)" >/dev/null
s4=$(latest)
sql "create view $schema.view_t as select * from $schema.t" >/dev/null
[[ "$s1" -lt "$s2" && "$s2" -lt "$s3" && "$s3" -lt "$s4" ]]
before_current=$(sql "select * from $schema.t order by id")
before_latest=$(latest)

# Listing is newest first and paginates without duplicates or gaps.
"$binary" snapshots --limit 3 --format json > page1.json
"$binary" snapshots --limit 3 --before "$(jq -r .next_before page1.json)" --format json > page2.json
jq -s '[.[].snapshots[].snapshot_id]' page1.json page2.json > ids.json
jq -e 'length == 6 and (. == (unique | reverse)) and (.[0] - .[5] == 5)' ids.json >/dev/null
for id in "$s1" "$s2" "$s3" "$s4"; do
  "$binary" snapshots --limit 500 --format json | jq -e --argjson id "$id" 'any(.snapshots[]; .snapshot_id == $id)' >/dev/null
done
"$binary" snapshot "$s3" --format json | jq -e --argjson id "$s3" '.[0].snapshot_id == $id and (.[0].changes | contains("tables_inserted_into")) and (.[0].changes | contains("tables_deleted_from"))' >/dev/null

# Time travel returns exactly the rows direct DuckLake SQL returns.
for id in "$s1" "$s2" "$s3" "$s4"; do
  "$binary" snapshot preview "$schema.t" --at "$id" --limit 500 --format json 2>preview.err | jq -S 'sort_by(.id)' > preview.json
  [[ "$(<preview.err)" == *"snapshot: $id; preview_limit: 500"* ]]
  sql "select * from $schema.t at (version => $id)" | jq -S 'sort_by(.id)' > direct.json
  diff -u direct.json preview.json
done
[[ "$("$binary" snapshot preview "$schema.t" --at "$s1" --limit 2 --format json 2>/dev/null | jq length)" == 2 ]]

# The net diff agrees with an independent full outer join on DuckLake row IDs.
expected_diff() {
  sql "select
     count(*) filter (where a.rowid is null)::bigint as added,
     count(*) filter (where b.rowid is null)::bigint as deleted,
     count(*) filter (where a.rowid is not null and b.rowid is not null and (a.id, a.v) is distinct from (b.id, b.v))::bigint as updated
   from (select rowid, * from $schema.t at (version => $1)) a
   full outer join (select rowid, * from $schema.t at (version => $2)) b on a.rowid = b.rowid" | jq -c '.[0]'
}
for range in "$s1 $s4" "$s1 $s2" "$s2 $s3" "$s3 $s4" "$s2 $s4"; do
  read -r from to <<<"$range"
  "$binary" snapshot diff "$schema.t" --from "$from" --to "$to" --format json > diff.json
  [[ "$(jq -c '{added, deleted, updated}' diff.json)" == "$(expected_diff "$from" "$to")" ]]
done
"$binary" snapshot diff "$schema.t" --from "$s1" --to "$s4" --format json > full.json
jq -e '.added == 1 and .deleted == 1 and .updated == 1' full.json >/dev/null
jq -e 'any(.examples[]; .change == "updated" and .before.v == "v1" and .after.v == "updated")' full.json >/dev/null
jq -e 'any(.examples[]; .change == "added" and .after.id == 10 and .after.v == "updated")' full.json >/dev/null
"$binary" snapshot diff "$schema.t" --from "$s2" --to "$s2" --format json | jq -e '.added == 0 and .deleted == 0 and .updated == 0 and .examples == []' >/dev/null
"$binary" snapshot diff "$schema.t" --from "$s1" --to "$s4" --limit 1 --format json | jq -e '[.examples[] | .change] | group_by(.) | all(length == 1)' >/dev/null
"$binary" snapshot diff "$schema.t" --from "$s1" --to "$s4" > diff-table.out 2>/dev/null
[[ "$(head -1 diff-table.out)" == "$schema.t from snapshot $s1 to $s4: 1 added, 1 deleted, 1 updated" ]]

# A schema change renders older images in the current schema.
sql "alter table $schema.t add column note varchar" >/dev/null
sql "insert into $schema.t values (20, 'twenty', 'new')" >/dev/null
s6=$(latest)
"$binary" snapshot diff "$schema.t" --from "$s4" --to "$s6" --format json > schema-change.json
jq -e '.added == 1 and .deleted == 0 and .updated == 0 and .examples[0].after.note == "new"' schema-change.json >/dev/null

# Classified refusals happen before any scan.
expect_failure "$binary" snapshot 999999999
[[ "$(<expected-failure.out)" == *snapshot_not_found* ]]
expect_failure "$binary" snapshot preview "$schema.t" --at 999999999
[[ "$(<expected-failure.out)" == *snapshot_not_found* ]]
expect_failure "$binary" snapshot diff "$schema.t" --from "$s4" --to "$s1"
[[ "$(<expected-failure.out)" == *"is after --to"* ]]
expect_failure "$binary" snapshot preview "$schema.t" --at abc
expect_failure "$binary" snapshot diff "other.$schema.t" --from "$s1" --to "$s4"
[[ "$(<expected-failure.out)" == *cross_catalog* ]]
expect_failure "$binary" snapshot diff "$schema.view_t" --from "$s1" --to "$s4"
[[ "$(<expected-failure.out)" == *not_a_table* ]]
expect_failure "$binary" snapshots --limit 501
expect_failure "$binary" snapshot diff "$schema.t" --from "$s1" --to "$s4" --limit 101
expect_failure "$binary" snapshot "$s1" --at "$s1"

# A large diff is cancelable and bounded by the statement deadline.
sql "create table $schema.big as select i as id, i as v from range(4000000) r(i)" >/dev/null
b1=$(latest)
sql "update $schema.big set v = v + 1" >/dev/null
b2=$(latest)
"$binary" snapshot diff "$schema.big" --from "$b1" --to "$b2" >cancel.out 2>cancel.err &
active_pid=$!
sleep 1.5
kill -INT "$active_pid" 2>/dev/null || true
cancel_code=0
wait "$active_pid" || cancel_code=$?
active_pid=""
if [[ "$cancel_code" -ne 0 ]]; then
  [[ "$(<cancel.err)" == *canceled* ]]
else
  [[ "$(head -1 cancel.out)" == *"0 added, 0 deleted, 4000000 updated"* ]]
fi
sql "drop table $schema.big" >/dev/null

# A reader principal reads snapshots through its own catalog credentials.
docker compose -f "$root/compose.yaml" --profile readers up -d --wait server-readers >/dev/null
principal="snapshot-reader-$(date +%s)-$$"
issued=$(curl -fsS -X POST -H "Authorization: Bearer $token" -H 'Content-Type: application/json' --data "{\"principal\":\"$principal\"}" "$url/v1/credentials")
reader_token=$(jq -r '.token' <<<"$issued")
reader_id=$(jq -r '.credential_id' <<<"$issued")
policy=$(curl -fsS -H "Authorization: Bearer $token" "$url/v1/principals/$principal/policy")
body=$(jq '.compute[0].pool = "small-reader" | {roles:["viewer"],permissions:[],compute:.compute,disabled:false}' <<<"$policy")
curl -fsS -X PUT -H "Authorization: Bearer $token" -H 'Content-Type: application/json' --data "$body" "$url/v1/principals/$principal/policy" >/dev/null
reader=("$binary" --url http://127.0.0.1:18082 --token "$reader_token")
"${reader[@]}" snapshots --limit 5 --format json | jq -e '.snapshots | length == 5' >/dev/null
"${reader[@]}" snapshot preview "$schema.t" --at "$s2" --limit 500 --format json 2>/dev/null | jq -e 'length == 7' >/dev/null
"${reader[@]}" snapshot diff "$schema.t" --from "$s1" --to "$s4" --format json | jq -e '.updated == 1' >/dev/null
expect_failure "${reader[@]}" sql "delete from $schema.t"

# No read changed current data or created a snapshot.
sql "alter table $schema.t drop column note" >/dev/null
sql "delete from $schema.t where id = 20" >/dev/null
after_reads=$(latest)
"$binary" snapshots --limit 10 >/dev/null
"$binary" snapshot preview "$schema.t" --at "$s1" >/dev/null 2>&1
"$binary" snapshot diff "$schema.t" --from "$s1" --to "$s4" >/dev/null 2>&1
[[ "$(latest)" == "$after_reads" ]]
[[ "$(sql "select * from $schema.t order by id")" == "$before_current" ]]
[[ "$before_latest" -lt "$after_reads" ]]

printf 'PASS: snapshot listing/pagination, time travel, net diffs vs independent rowid joins, empty and schema-change diffs, refusals, cancellation, reader authority, and read-only behavior\n'
