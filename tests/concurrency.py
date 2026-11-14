"""Concurrent DuckLake write correctness: real workers, real catalog, no mocks.

Statements serialize inside a worker, so overlapping transactions are built by
interleaving requests rather than by racing threads. The conflict window is then
exact, and a failure here is a real regression instead of a flake.

Every assertion below was first measured against DuckDB 1.5.4 with a DuckLake
PostgreSQL catalog. This file is the executable record of that behavior.
"""
import json
import uuid

from dbt.adapters.duckfleet.client import RemoteConnection, RemoteError

SMALL = {"cpu": 1, "memory": "1Gi"}
MEDIUM = {"cpu": 2, "memory": "2Gi"}


def require(condition, message):
    if not condition:
        raise AssertionError(message)


def sql(connection, statement, parameters=None):
    return connection.cursor().execute(statement, parameters).fetchall()


def expect_conflict(connection, statement, context):
    """Asserts the statement lost a commit race and says so in a usable way."""
    try:
        sql(connection, statement)
    except RemoteError as error:
        require(error.status == 409, f"{context}: expected HTTP 409, got {error.status}: {error}")
        require(error.failure_kind == "transaction_conflict", f"{context}: kind {error.failure_kind}")
        require(error.retry_safe, f"{context}: a discarded transaction must be retry-safe")
        require("Transaction conflict" in str(error), f"{context}: unexpected message: {error}")
        return error
    raise AssertionError(f"{context}: expected a commit conflict")


def expect_no_conflict(connection, statement, context):
    try:
        sql(connection, statement)
    except RemoteError as error:
        raise AssertionError(f"{context}: unexpected conflict: {error}") from error


class Writers:
    """Two sessions whose transactions overlap, on the requested worker sizes."""

    def __init__(self, url, token, first=SMALL, second=MEDIUM, label="writers"):
        self.url, self.token = url, token
        self.first = RemoteConnection(url, token, name=f"{label}-first", compute=first)
        self.second = RemoteConnection(url, token, name=f"{label}-second", compute=second)

    def __enter__(self):
        return self

    def __exit__(self, *_):
        for connection in (self.first, self.second):
            try:
                connection.close()
            except RemoteError:
                pass
        return False


def admin(url, token):
    return RemoteConnection(url, token, name="concurrency-admin", compute=SMALL)


def reset(url, token, columns="id integer, amount integer", rows="(1,10),(2,20),(3,30)"):
    connection = admin(url, token)
    try:
        sql(connection, "create schema if not exists concurrency")
        sql(connection, "drop table if exists concurrency.target")
        sql(connection, f"create table concurrency.target ({columns})")
        if rows:
            sql(connection, f"insert into concurrency.target values {rows}")
    finally:
        connection.close()


def read_target(url, token, projection="id,amount"):
    connection = admin(url, token)
    try:
        return sql(connection, f"select {projection} from concurrency.target order by id")
    finally:
        connection.close()


def target_columns(url, token):
    connection = admin(url, token)
    try:
        rows = sql(connection, """select column_name from information_schema.columns
            where table_schema = 'concurrency' and table_name = 'target' order by column_name""")
        return [name for (name,) in rows]
    finally:
        connection.close()


def appends_never_conflict(url, token, evidence):
    """Append-only writers are compatible: both commit and no row is lost."""
    reset(url, token, rows=None)
    with Writers(url, token, label="append") as writers:
        sql(writers.first, "begin")
        sql(writers.second, "begin")
        sql(writers.first, "insert into concurrency.target values (1,10)")
        sql(writers.second, "insert into concurrency.target values (2,20)")
        sql(writers.first, "commit")
        expect_no_conflict(writers.second, "commit", "concurrent appends")
    rows = read_target(url, token)
    require(rows == [(1, 10), (2, 20)], f"appends lost or duplicated rows: {rows}")
    evidence.append({"case": "concurrent appends to one table", "outcome": "both committed", "rows": rows})
    print("PASS: concurrent appends to one table both commit and keep every row")


def disjoint_tables_never_conflict(url, token, evidence):
    reset(url, token, rows=None)
    connection = admin(url, token)
    try:
        sql(connection, "drop table if exists concurrency.other")
        sql(connection, "create table concurrency.other (id integer)")
    finally:
        connection.close()
    with Writers(url, token, label="disjoint") as writers:
        sql(writers.first, "begin")
        sql(writers.second, "begin")
        sql(writers.first, "insert into concurrency.target values (1,10)")
        sql(writers.second, "insert into concurrency.other values (1)")
        sql(writers.first, "commit")
        expect_no_conflict(writers.second, "commit", "disjoint tables")
    evidence.append({"case": "writes to different tables", "outcome": "both committed"})
    print("PASS: overlapping writes to different tables both commit")


def delete_insert_conflicts_atomically(url, token, evidence):
    """The dbt delete+insert pattern, concurrent on one table.

    The losing transaction is discarded whole, so the winner's change stands alone
    and no half-applied state is visible. Note that the two writers touch different
    rows: DuckLake detects the conflict per data file, not per row.
    """
    reset(url, token)
    with Writers(url, token, label="delete-insert") as writers:
        sql(writers.first, "begin")
        sql(writers.second, "begin")
        sql(writers.first, "delete from concurrency.target where id = 1")
        sql(writers.first, "insert into concurrency.target values (1,11)")
        sql(writers.second, "delete from concurrency.target where id = 2")
        sql(writers.second, "insert into concurrency.target values (2,22)")
        sql(writers.first, "commit")
        error = expect_conflict(writers.second, "commit", "delete+insert on one table")
        # The engine already discarded the transaction, so there is nothing to roll
        # back. A client that sends ROLLBACK here gets an error, not a clean slate.
        try:
            sql(writers.second, "rollback")
            raise AssertionError("expected no active transaction after a losing commit")
        except RemoteError as rollback_error:
            require("no transaction is active" in str(rollback_error), f"unexpected rollback error: {rollback_error}")
        # The session itself survives and is immediately reusable.
        require(sql(writers.second, "select 1") == [(1,)], "a lost commit poisoned the session")
        rows = read_target(url, token)
        require(rows == [(1, 11), (2, 20), (3, 30)], f"losing transaction was not discarded whole: {rows}")
        # Replaying the loser's whole transaction converges on the intended state.
        sql(writers.second, "begin")
        sql(writers.second, "delete from concurrency.target where id = 2")
        sql(writers.second, "insert into concurrency.target values (2,22)")
        expect_no_conflict(writers.second, "commit", "replay after conflict")
    rows = read_target(url, token)
    require(rows == [(1, 11), (2, 22), (3, 30)], f"replay did not converge: {rows}")
    evidence.append({
        "case": "delete+insert on one table, different rows",
        "outcome": "first commit wins, loser discarded whole, replay converges",
        "failure_kind": error.failure_kind, "retry_safe": error.retry_safe,
        "error": str(error), "rows_after_replay": rows,
    })
    print("PASS: a losing delete+insert is discarded whole and converges when replayed")


def update_and_schema_changes_conflict(url, token, evidence):
    """Updates and schema changes conflict the same way, and classify the same."""
    for case, first, second, check in [
        ("update vs update",
         "update concurrency.target set amount = 111 where id = 1",
         "update concurrency.target set amount = 222 where id = 2",
         lambda: read_target(url, token) == [(1, 111), (2, 20), (3, 30)]),
        ("insert vs delete",
         "insert into concurrency.target values (4,40)",
         "delete from concurrency.target where id = 3",
         lambda: read_target(url, token) == [(1, 10), (2, 20), (3, 30), (4, 40)]),
        # The winner's column must exist and the loser's must not.
        ("alter vs alter",
         "alter table concurrency.target add column tag varchar",
         "alter table concurrency.target add column note varchar",
         lambda: target_columns(url, token) == ["amount", "id", "tag"]),
    ]:
        reset(url, token)
        with Writers(url, token, label="shapes") as writers:
            sql(writers.first, "begin")
            sql(writers.second, "begin")
            sql(writers.first, first)
            sql(writers.second, second)
            sql(writers.first, "commit")
            error = expect_conflict(writers.second, "commit", case)
        require(check(), f"{case}: winner's change did not stand alone")
        evidence.append({"case": case, "outcome": "classified conflict", "error": str(error)})
        print(f"PASS: {case} is reported as a retry-safe conflict")


def same_worker_behaves_like_cross_worker(url, token, evidence):
    """Two sessions on one worker share a DuckDB instance but keep the same rule:
    the conflict surfaces at commit, not mid-statement."""
    reset(url, token)
    with Writers(url, token, first=SMALL, second=SMALL, label="same-worker") as writers:
        require(
            writers.first.placement["worker_id"] == writers.second.placement["worker_id"] == "small",
            "expected both sessions on the small worker",
        )
        sql(writers.first, "begin")
        sql(writers.second, "begin")
        sql(writers.first, "update concurrency.target set amount = 111 where id = 1")
        # Same row, same process: this still succeeds, because detection is at commit.
        expect_no_conflict(writers.second, "update concurrency.target set amount = 222 where id = 1",
                           "same-worker statement before commit")
        sql(writers.first, "commit")
        error = expect_conflict(writers.second, "commit", "same-worker conflict")
    rows = read_target(url, token)
    require(rows == [(1, 111), (2, 20), (3, 30)], f"same-worker conflict lost the winner's change: {rows}")
    evidence.append({"case": "two sessions on one worker", "outcome": "conflict at commit, not mid-statement",
                     "error": str(error)})
    print("PASS: a same-worker conflict also surfaces only at commit")


def committed_writes_are_visible_across_workers(url, token, evidence):
    """A commit reaches the other worker at once, and does not disturb a transaction
    that opened before it: that one keeps the snapshot it started with."""
    reset(url, token, rows=None)
    with Writers(url, token, label="visibility") as writers:
        sql(writers.first, "insert into concurrency.target values (1,10)")
        immediate = sql(writers.second, "select count(*) from concurrency.target")
        require(immediate == [(1,)], f"committed write was not visible to the other worker: {immediate}")
        sql(writers.second, "begin")
        in_transaction = sql(writers.second, "select count(*) from concurrency.target")
        require(in_transaction == [(1,)], f"a transaction opened after the commit read a stale snapshot: {in_transaction}")
        # Now commit on the other worker while this transaction is still open.
        sql(writers.first, "insert into concurrency.target values (2,20)")
        during = sql(writers.second, "select count(*) from concurrency.target")
        require(during == [(1,)], f"an open transaction saw a commit made after it started: {during}")
        # Reading a snapshot that fell behind is not a conflict.
        expect_no_conflict(writers.second, "commit", "read-only transaction over an older snapshot")
        after = sql(writers.second, "select count(*) from concurrency.target")
        require(after == [(2,)], f"a new statement did not see the committed row: {after}")
    evidence.append({"case": "cross-worker visibility",
                     "outcome": "visible at once to later transactions; an open transaction keeps its snapshot"})
    print("PASS: commits are visible at once, and an open transaction keeps its snapshot")


def conflicts_are_recorded_and_filterable(url, token, evidence):
    """A conflict must be findable in history as a conflict, not as generic failure."""
    reset(url, token)
    marker = f"conflict-probe-{uuid.uuid4()}"
    with Writers(url, token, label="history") as writers:
        sql(writers.first, "begin")
        sql(writers.second, "begin")
        sql(writers.first, "delete from concurrency.target where id = 1")
        sql(writers.second, "delete from concurrency.target where id = 2")
        sql(writers.first, "commit")
        # The marker rides along in the request context so the record is identifiable.
        writers.second.context["workload"] = marker
        expect_conflict(writers.second, "commit", "history conflict")
        client = writers.second.client

    listed = json.loads(client.request("GET", "/v1/queries?status=conflict&limit=100"))["queries"]
    require(listed, "no records with status=conflict")
    mine = [q for q in listed if q["context"].get("workload") == marker]
    require(len(mine) == 1, f"expected exactly one conflict record for this run, got {len(mine)}")
    record = mine[0]
    require(record["status"] == "conflict", f"status {record['status']}")
    require(record["failure_kind"] == "transaction_conflict", f"failure_kind {record['failure_kind']}")
    require(record["retry_safe"] is True, f"retry_safe {record['retry_safe']}")
    require(record["sql"].strip().lower() == "commit", f"conflict was attributed to {record['sql']!r}")

    by_kind = json.loads(client.request("GET", "/v1/queries?failure_kind=transaction_conflict&limit=100"))["queries"]
    require(any(q["query_id"] == record["query_id"] for q in by_kind), "failure_kind filter missed the record")
    detail = json.loads(client.request("GET", "/v1/queries/" + record["query_id"]))
    require(detail["failure_kind"] == "transaction_conflict" and detail["retry_safe"] is True, str(detail))

    # A conflict is a distinct state: it must not also appear as a plain failure.
    failures = json.loads(client.request("GET", "/v1/queries?status=failed&limit=200"))["queries"]
    require(
        not any(q["query_id"] == record["query_id"] for q in failures),
        "a conflict was also reported as a generic failure",
    )
    evidence.append({"case": "history record", "outcome": "status=conflict, retry_safe=true, filterable",
                     "query_id": record["query_id"]})
    print("PASS: conflicts are durable, classified, and filterable in query history")


def ordinary_sql_errors_stay_unsafe(url, token, evidence):
    """The point of the classification is the contrast: bad SQL is never retry-safe."""
    connection = admin(url, token)
    try:
        for statement, kind in [
            ("select * from concurrency.does_not_exist", "sql_error"),
            ("select missing_column", "sql_error"),
        ]:
            try:
                sql(connection, statement)
                raise AssertionError(f"expected an error from {statement!r}")
            except RemoteError as error:
                require(error.status == 400, f"{statement}: HTTP {error.status}")
                require(error.failure_kind == kind, f"{statement}: kind {error.failure_kind}")
                require(not error.retry_safe, f"{statement}: must not be retry-safe")
        # A duplicate request is a conflict in HTTP terms but must never be replayed.
        request = {"request_id": str(uuid.uuid4()), "sql": "select 1"}
        connection.client.request("POST", connection.path + "/queries", request)
        try:
            connection.client.request("POST", connection.path + "/queries", request)
            raise AssertionError("expected a duplicate-request rejection")
        except RemoteError as error:
            require(error.status == 409, f"duplicate request: HTTP {error.status}")
            require(error.failure_kind == "duplicate_request", f"duplicate request: {error.failure_kind}")
            require(not error.retry_safe, "a duplicate request must not be retry-safe")
    finally:
        connection.close()
    evidence.append({"case": "non-conflict failures", "outcome": "classified and not retry-safe"})
    print("PASS: SQL errors and duplicate requests are classified and not retry-safe")


def checks(url, token, artifacts):
    evidence = []
    appends_never_conflict(url, token, evidence)
    disjoint_tables_never_conflict(url, token, evidence)
    delete_insert_conflicts_atomically(url, token, evidence)
    update_and_schema_changes_conflict(url, token, evidence)
    same_worker_behaves_like_cross_worker(url, token, evidence)
    committed_writes_are_visible_across_workers(url, token, evidence)
    conflicts_are_recorded_and_filterable(url, token, evidence)
    ordinary_sql_errors_stay_unsafe(url, token, evidence)
    (artifacts / "write-conflicts.json").write_text(json.dumps(evidence, indent=2))
    connection = admin(url, token)
    try:
        for table in ["target", "other"]:
            sql(connection, f"drop table if exists concurrency.{table}")
    finally:
        connection.close()
    print(f"PASS: {len(evidence)} concurrent-write cases match the documented DuckLake semantics")
