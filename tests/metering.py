"""Metering and budget gate: no execution escapes the ledger, and caps are real.

The budget is shared state for the whole stack, so every check that changes it
restores it in a finally block. Leaving the balance at zero would refuse every
later stage of the harness.
"""
import json
import math
import uuid

from dbt.adapters.duckfleet.client import Client, RemoteConnection, RemoteError

SMALL = {"cpu": 1, "memory": "1Gi"}
MEDIUM = {"cpu": 2, "memory": "2Gi"}
# Large enough to need interrupting, without allocating a huge table.
LONG_SQL = "select sum(a.i * b.i) from range(1000000000) a(i), range(1000000000) b(i)"


def require(condition, message):
    if not condition:
        raise AssertionError(message)


def sql(connection, statement, parameters=None):
    return connection.cursor().execute(statement, parameters).fetchall()


def budget(client):
    return json.loads(client.request("GET", "/v1/budget"))


def set_budget(client, **limits):
    return json.loads(client.request("PUT", "/v1/budget", limits))


def events_for(client, execution_id):
    return json.loads(
        client.request("GET", f"/v1/usage?execution_id={execution_id}")
    )["events"]


def history_for(client, query_id):
    return json.loads(client.request("GET", "/v1/queries/" + query_id))


def expected_charge(units, elapsed_ms, per_unit_hour):
    """The same ceiling arithmetic the server uses, recomputed independently."""
    return math.ceil(units * elapsed_ms * per_unit_hour / 3_600_000)


def one_statement_is_metered_once(client, url, token, evidence):
    position = budget(client)
    connection = RemoteConnection(url, token, name="metering", compute=MEDIUM)
    try:
        cursor = connection.cursor().execute("select sum(i) from range(200000) t(i)")
        query_id = cursor.query_id
    finally:
        connection.close()

    events = events_for(client, query_id)
    require(len(events) == 1, f"expected exactly one usage event, got {len(events)}")
    event = events[0]
    record = history_for(client, query_id)
    require(record["status"] == "succeeded", str(record["status"]))
    require(event["meter_name"] == "compute" and event["unit"] == "millisecond", str(event))
    require(event["source"] == "worker", str(event["source"]))
    require(event["worker_id"] == "medium", str(event["worker_id"]))
    # The medium class is 2 vCPU / 2 GiB, so max(2, 2/4) normalizes to 2 units.
    require(event["resource_units"] == 2, str(event["resource_units"]))
    require(event["quantity"] == record["elapsed_ms"], f"metered {event['quantity']}ms, history says {record['elapsed_ms']}ms")
    require(event["rate_card_version"] == position["rate_card_version"], str(event))
    expected = expected_charge(2, event["quantity"], position["compute_micro_usd_per_unit_hour"])
    require(event["amount_micro_usd"] == expected, f"charged {event['amount_micro_usd']}, expected {expected}")

    after = budget(client)
    require(
        after["spent_micro_usd"] == position["spent_micro_usd"] + event["amount_micro_usd"],
        f"spend moved from {position['spent_micro_usd']} to {after['spent_micro_usd']}",
    )
    # The hold is released at settlement, not left outstanding.
    require(after["held_micro_usd"] == position["held_micro_usd"], f"reservation leaked: {after['held_micro_usd']}")
    require(after["held_reservations"] == position["held_reservations"], str(after))
    evidence.append({"case": "one statement", "units": event["resource_units"],
                     "elapsed_ms": event["quantity"], "amount_micro_usd": event["amount_micro_usd"]})
    print("PASS: a statement is metered exactly once, priced from the recorded rate card")


def submit(connection, statement, expect_kind=None, timeout_seconds=30):
    """Runs one statement under a request_id the caller knows, and returns it."""
    query_id = str(uuid.uuid4())
    try:
        connection.client.request("POST", connection.path + "/queries", {
            "request_id": query_id, "sql": statement, "timeout_seconds": timeout_seconds,
        })
        require(expect_kind is None, f"expected {expect_kind} from {statement!r}")
    except RemoteError as error:
        require(expect_kind is not None, f"unexpected failure from {statement!r}: {error}")
        require(error.failure_kind == expect_kind, f"{statement!r} failed as {error.failure_kind}")
    return query_id


def every_terminal_status_is_metered(client, url, token, evidence):
    """The gate itself. Success, SQL error, timeout, and a lost commit race all
    consume compute, so each one has to leave exactly one usage event."""
    produced = {}
    connection = RemoteConnection(url, token, name="metering-statuses", compute=SMALL)
    try:
        produced["succeeded"] = submit(connection, "select 1")
        produced["failed"] = submit(
            connection, "select * from a_table_that_does_not_exist", "sql_error"
        )
        produced["timed_out"] = submit(connection, LONG_SQL, "timeout", timeout_seconds=1)
    finally:
        connection.close()

    # A conflict needs a second worker committing first; see tests/concurrency.py
    # for the full semantics. Here it only has to prove the loser is still metered.
    first = RemoteConnection(url, token, name="metering-conflict-first", compute=SMALL)
    second = RemoteConnection(url, token, name="metering-conflict-second", compute=MEDIUM)
    try:
        sql(first, "create schema if not exists metering")
        sql(first, "drop table if exists metering.contended")
        sql(first, "create table metering.contended (id integer)")
        sql(first, "insert into metering.contended values (1),(2)")
        sql(first, "begin")
        sql(second, "begin")
        sql(first, "delete from metering.contended where id = 1")
        sql(second, "delete from metering.contended where id = 2")
        sql(first, "commit")
        produced["conflict"] = submit(second, "commit", "transaction_conflict")
        sql(first, "drop table if exists metering.contended")
    finally:
        first.close()
        second.close()

    for status, query_id in produced.items():
        record = history_for(client, query_id)
        require(record["status"] == status, f"{status}: history says {record['status']}")
        events = events_for(client, query_id)
        require(len(events) == 1, f"{status}: {len(events)} usage events, expected exactly 1")
        event = events[0]
        # Even a failure is charged for the time it consumed, at its own size.
        expected = expected_charge(event["resource_units"], event["quantity"], 100_000)
        require(event["amount_micro_usd"] == expected, f"{status}: {event}")
        require(event["quantity"] == record["elapsed_ms"], f"{status}: {event['quantity']} vs {record['elapsed_ms']}")
        evidence.append({"case": f"status {status}", "query_id": query_id,
                         "amount_micro_usd": event["amount_micro_usd"]})
    # A timed-out statement ran for about a second, so it cannot be free.
    timed_out = events_for(client, produced["timed_out"])[0]
    require(timed_out["amount_micro_usd"] > 0, f"an interrupted statement was free: {timed_out}")
    print(f"PASS: every terminal status is metered exactly once ({', '.join(sorted(produced))})")


def a_duplicate_request_is_not_metered_twice(client, url, token, evidence):
    connection = RemoteConnection(url, token, name="metering-duplicate", compute=SMALL)
    try:
        request = {"request_id": str(uuid.uuid4()), "sql": "select 1"}
        connection.client.request("POST", connection.path + "/queries", request)
        try:
            connection.client.request("POST", connection.path + "/queries", request)
            raise AssertionError("expected the duplicate to be rejected")
        except RemoteError as error:
            require(error.failure_kind == "duplicate_request", str(error.failure_kind))
        events = events_for(client, request["request_id"])
        require(len(events) == 1, f"a duplicate request produced {len(events)} usage events")
    finally:
        connection.close()
    evidence.append({"case": "duplicate request", "outcome": "metered once"})
    print("PASS: a rejected duplicate request does not meter a second time")


def a_per_job_cap_stops_a_running_statement(client, url, token, evidence):
    """The per-job cost cap becomes a shorter deadline, reported as a budget stop."""
    position = budget(client)
    per_second = math.ceil(position["compute_micro_usd_per_unit_hour"] / 3600)
    try:
        # Two seconds of the small class, and nothing more.
        set_budget(client, max_job_micro_usd=per_second * 2)
        connection = RemoteConnection(url, token, name="metering-cap", compute=SMALL)
        try:
            query_id = str(uuid.uuid4())
            try:
                connection.client.request("POST", connection.path + "/queries", {
                    "request_id": query_id, "sql": LONG_SQL, "timeout_seconds": 60,
                })
                raise AssertionError("expected the cost cap to stop the statement")
            except RemoteError as error:
                require(error.failure_kind == "budget_exceeded", f"stopped as {error.failure_kind}")
                require(not error.retry_safe, "an interrupted statement is not retry-safe")
            record = history_for(client, query_id)
            require(record["status"] == "budget_exceeded", str(record["status"]))
            # It was stopped near the cap, not at the 60-second timeout it asked for.
            require(record["elapsed_ms"] < 20_000, f"ran for {record['elapsed_ms']}ms despite the cap")
            events = events_for(client, query_id)
            require(len(events) == 1, f"{len(events)} usage events for a capped statement")
            require(events[0]["amount_micro_usd"] > 0, "a capped statement was free")
            evidence.append({"case": "per-job cost cap", "elapsed_ms": record["elapsed_ms"],
                             "amount_micro_usd": events[0]["amount_micro_usd"]})
            # The session survives, like any other interruption.
            require(sql(connection, "select 42") == [(42,)], "the cap poisoned the session")
        finally:
            connection.close()
    finally:
        set_budget(client, max_job_micro_usd=position["max_job_micro_usd"])
    print("PASS: the per-job cost cap stops a statement and still meters what it used")


def an_exhausted_balance_refuses_admission(client, url, token, evidence):
    position = budget(client)
    connection = RemoteConnection(url, token, name="metering-exhausted", compute=SMALL)
    try:
        # Spending already exceeds this, so nothing can be admitted.
        set_budget(client, prepaid_micro_usd=0)
        exhausted = budget(client)
        require(exhausted["available_micro_usd"] <= 0, str(exhausted))
        query_id = str(uuid.uuid4())
        try:
            connection.client.request("POST", connection.path + "/queries", {
                "request_id": query_id, "sql": "create table never_created_by_a_refusal (n integer)",
            })
            raise AssertionError("expected the budget to refuse this execution")
        except RemoteError as error:
            require(error.status == 402, f"expected HTTP 402, got {error.status}")
            require(error.failure_kind == "budget_exhausted", str(error.failure_kind))
            # Nothing ran, so sending it again once funded is legitimate.
            require(error.retry_safe, "a refusal before execution should be retry-safe")
        record = history_for(client, query_id)
        require(record["status"] == "refused", str(record["status"]))
        require(record["failure_kind"] == "budget_exhausted", str(record["failure_kind"]))
        require(record["elapsed_ms"] == 0, str(record["elapsed_ms"]))
        # A refused execution ran no SQL, so it is not charged for anything.
        require(not events_for(client, query_id), "a refused execution was metered")
        evidence.append({"case": "exhausted balance", "outcome": "refused with HTTP 402, nothing metered"})
    finally:
        set_budget(client, prepaid_micro_usd=position["prepaid_micro_usd"])
        try:
            # Proof the refusal was the budget and not a broken session.
            require(sql(connection, "select 1") == [(1,)], "session unusable after a refusal")
        finally:
            connection.close()
    restored = budget(client)
    require(restored["available_micro_usd"] > 0, f"budget was not restored: {restored}")
    require(
        sql_table_absent(url, token, "never_created_by_a_refusal"),
        "the refused statement created its table anyway",
    )
    print("PASS: an exhausted balance refuses admission before any SQL runs")


def sql_table_absent(url, token, name):
    connection = RemoteConnection(url, token, name="metering-verify", compute=SMALL)
    try:
        found = sql(connection, """select count(*) from information_schema.tables
            where table_name = ?""", [name])
        return found == [(0,)]
    finally:
        connection.close()


def the_ledger_only_grows(client, evidence):
    """Corrections are new rows, so the event count never decreases."""
    before = budget(client)["usage_events"]
    listing = json.loads(client.request("GET", "/v1/usage?limit=5"))
    require(listing["events"], "no usage events listed")
    newest = listing["events"][0]
    require(newest["corrects"] is None, "unexpected correction event in this run")
    require(newest["ledger_id"] > 0, str(newest))
    after = budget(client)["usage_events"]
    require(after >= before, f"the ledger shrank from {before} to {after}")
    evidence.append({"case": "ledger ordering", "events": after})
    print(f"PASS: the usage ledger exposes a durable order and only grows ({after} events)")


def checks(url, token, artifacts):
    client = Client(url, token)
    opening = budget(client)
    require(opening["available_micro_usd"] > 0, f"no budget to start from: {opening}")
    evidence = []
    try:
        one_statement_is_metered_once(client, url, token, evidence)
        every_terminal_status_is_metered(client, url, token, evidence)
        a_duplicate_request_is_not_metered_twice(client, url, token, evidence)
        a_per_job_cap_stops_a_running_statement(client, url, token, evidence)
        an_exhausted_balance_refuses_admission(client, url, token, evidence)
        the_ledger_only_grows(client, evidence)
    finally:
        # However this ends, the next stage of the harness needs a working budget.
        set_budget(client, prepaid_micro_usd=opening["prepaid_micro_usd"],
                   max_job_micro_usd=opening["max_job_micro_usd"])
    closing = budget(client)
    (artifacts / "metering.json").write_text(json.dumps(
        {"opening": opening, "closing": closing, "cases": evidence}, indent=2))
    print(f"PASS: {len(evidence)} metering cases; {closing['spent_micro_usd']} micro-USD metered in total")
