"""Real dbt + HTTP + Postgres + S3 integration gate; no mocks."""
import concurrent.futures
import datetime
import decimal
import json
import os
from pathlib import Path
import subprocess
import sys
import time
import uuid

from dbt.adapters.duckfleet.client import Client, RemoteConnection, RemoteError

URL = os.environ["DUCKFLEET_URL"]
TOKEN = os.environ["DUCKFLEET_TOKEN"]
ARTIFACTS = Path("/artifacts")


def require(condition, message):
    if not condition:
        raise AssertionError(message)


def expect_error(status, action):
    try:
        action()
    except RemoteError as error:
        require(error.status == status, f"expected HTTP {status}, got {error.status}: {error}")
    else:
        raise AssertionError(f"expected HTTP {status}")


def sql(connection, statement, parameters=None):
    return connection.cursor().execute(statement, parameters).fetchall()


def dbt(stage, *arguments, expected_code=0):
    env = dict(os.environ, PYTHONPATH="/app/tests/guard", DBT_TARGET_PATH=str(ARTIFACTS / stage / "target"))
    result = subprocess.run([
        "dbt", "--no-use-colors", "--log-path", str(ARTIFACTS / stage / "logs"),
        *arguments,
    ], check=False, env=env)
    require(result.returncode == expected_code, f"dbt {stage}: exit {result.returncode}, expected {expected_code}")


def results(connection):
    actual = sql(connection, "select customer_id,lifetime_value,order_count from lake.analytics.customer_totals order by customer_id")
    require(actual == [(10, decimal.Decimal("125.00"), 2), (20, decimal.Decimal("90.00"), 2)], str(actual))
    require(sql(connection, "select count(*),count(distinct order_id) from lake.analytics.fct_orders") == [(4, 4)], "incremental duplicate or missing rows")


def resource_checks(connection):
    workers = json.loads(connection.client.request("GET", "/v1/workers"))["workers"]
    require({w["worker_id"] for w in workers} == {"small", "medium"}, "missing sized workers")
    for worker in workers:
        cpu = 1 if worker["worker_id"] == "small" else 2
        require(worker["cpu"] == cpu == worker["duckdb_threads"], str(worker))
        require(worker["cgroup_cpu_quota"] == cpu * worker["cgroup_cpu_period"], str(worker))
        require(worker["memory_bytes"] == worker["cgroup_memory_max"] == cpu * 1024**3, str(worker))
    (ARTIFACTS / "worker-resources.json").write_text(json.dumps(workers, indent=2))
    require(len({w["worker_boot_id"] for w in workers}) == 2, "workers share one process")
    for compute in [{"cpu":32,"memory":"128Gi"}, {"cpu":2}, {"cpu":1,"memory":"2Gi"}, {"compute":"burst"}]:
        expect_error(400, lambda: connection.client.request("POST", "/v1/sessions", {"compute":compute}))
    medium = RemoteConnection(URL, TOKEN, compute={"cpu":2,"memory":"2Gi"})
    try:
        require(medium.placement["worker_id"] == "medium", "medium request silently fell back")
        require(sql(medium, "select current_setting('threads')::integer") == [(2,)], "wrong execution size")
        # A session's placement is fixed. Unknown policy fields on SQL are rejected.
        expect_error(422, lambda: medium.client.request("POST", medium.path + "/queries", {
            "request_id":str(uuid.uuid4()), "sql":"select 1", "compute":{"cpu":1,"memory":"1Gi"},
        }))
    finally:
        medium.close()


def model_resources(connection, table, cpu, memory):
    actual = sql(connection, f"select distinct worker_threads,worker_memory from lake.analytics.{table}")
    require(actual == [(cpu, memory)], f"{table} ran with {actual}, expected {(cpu,memory)}")


def verify_model_placement(history):
    evidence = []
    for stage in ["initial", "incremental", "repeat", "resize-small", "resize-medium"]:
        run = json.loads((ARTIFACTS / stage / "target/run_results.json").read_text())
        invocation = run["metadata"]["invocation_id"]
        nodes = ["model.duckfleet_harness.fct_orders"]
        if not stage.startswith("resize"):
            nodes.append("model.duckfleet_harness.customer_totals")
        for node in nodes:
            queries = [q for q in history if q["context"].get("dbt", {}).get("invocation_id") == invocation and q["context"]["dbt"].get("node_id") == node]
            require(queries, f"no query evidence for {stage}/{node}")
            expected = "medium" if node.endswith("fct_orders") and stage != "resize-small" else "small"
            require({q["placement"]["worker_id"] for q in queries} == {expected}, f"wrong placement for {stage}/{node}")
            require(len({q["session_id"] for q in queries}) == 1, f"model moved sessions: {stage}/{node}")
            require(all(q["context"]["dbt"]["platform"]["tags"]["team"] == "harness" for q in queries), "missing tags")
            require(all(q["placement"]["cpu"] == q["context"]["requested_compute"]["cpu"] for q in queries), "request/placement mismatch")
            if node.endswith("fct_orders"):
                require(any("hook_marker" in q["sql"] for q in queries), "missing hooks in pinned session")
                require(any(q["sql"].strip().upper() == "COMMIT" for q in queries), "missing commit in pinned session")
            evidence.append({"stage":stage, "model":node, "worker":expected, "session_id":queries[0]["session_id"], "placement":queries[0]["placement"]})
    (ARTIFACTS / "model-placement.json").write_text(json.dumps(evidence, indent=2))
    print("PASS: model config selected small and medium workers; hooks and commits stayed pinned")


def protocol_checks(connection):
    client = connection.client
    expect_error(401, lambda: Client(URL, "wrong-token").request("POST", "/v1/sessions"))
    expect_error(404, lambda: client.request("POST", f"/v1/sessions/{uuid.uuid4()}/queries", {"request_id":str(uuid.uuid4()), "sql":"select 1"}))
    expect_error(400, lambda: sql(connection, "select missing_column"))
    require(sql(connection, "select 1") == [(1,)], "SQL error poisoned session")
    typed = sql(connection, "select ?::decimal(20,2), ?::date, ?::boolean, ?::varchar, ?::bigint", ["123456789012345678.90", "2026-11-09", True, None, 9223372036854775807])
    require(typed == [(decimal.Decimal("123456789012345678.90"), datetime.date(2026,11,9), True, None, 9223372036854775807)], str(typed))
    sql(connection, "create schema if not exists checks")
    sql(connection, "create or replace table checks.replay (n integer)")
    request = {"request_id":str(uuid.uuid4()), "sql":"insert into checks.replay values (1)"}
    client.request("POST", connection.path + "/queries", request)
    expect_error(409, lambda: client.request("POST", connection.path + "/queries", request))
    require(sql(connection, "select count(*) from checks.replay") == [(1,)], "duplicate request executed twice")
    sql(connection, "begin")
    sql(connection, "insert into checks.replay values (2)")
    sql(connection, "rollback")
    require(sql(connection, "select count(*) from checks.replay") == [(1,)], "rollback failed")
    sql(connection, "create temp table session_only as select 99 as n")
    other = RemoteConnection(URL, TOKEN)
    try:
        expect_error(400, lambda: sql(other, "select * from session_only"))
        sql(other, "begin")
        sql(other, "insert into checks.replay values (3)")
    finally:
        other.close()
    require(sql(connection, "select count(*) from checks.replay") == [(1,)], "close did not roll back")
    expect_error(400, lambda: sql(connection, "select * from range(100001)"))
    # Large enough to require interruption, without allocating a huge table.
    long_sql = "select sum(a.i * b.i) from range(1000000000) a(i), range(1000000000) b(i)"
    timeout_id = str(uuid.uuid4())
    expect_error(400, lambda: client.request("POST", connection.path + "/queries", {"request_id":timeout_id, "sql":long_sql, "timeout_seconds":1}))
    with concurrent.futures.ThreadPoolExecutor() as pool:
        future = pool.submit(sql, connection, long_sql)
        canceled = False
        for _ in range(100):
            if connection.cancel()["interrupted_active_query"]:
                canceled = True
                break
            time.sleep(0.05)
        require(canceled, "query never became cancelable")
        expect_error(400, future.result)
    require(sql(connection, "select 42") == [(42,)], "interruption poisoned session")
    history = json.loads(client.request("GET", "/v1/queries"))["queries"]
    require(any(q["query_id"] == timeout_id and q["status"] == "timed_out" for q in history), "missing timed-out history")
    require(any(q["status"] == "canceled" for q in history), "missing cancellation history")


def main():
    ARTIFACTS.mkdir(exist_ok=True)
    import observability
    if "--history-without-workers" in sys.argv:
        observability.history_without_workers(URL, TOKEN)
        return
    if "--unavailable-medium" in sys.argv:
        expect_error(503, lambda: RemoteConnection(URL, TOKEN, compute={"cpu":2,"memory":"2Gi"}))
        small = RemoteConnection(URL, TOKEN, compute={"cpu":1,"memory":"1Gi"})
        try:
            require(sql(small, "select 1") == [(1,)], "small worker unavailable")
        finally:
            small.close()
        print("PASS: unavailable medium worker did not fall back to small")
        return
    connection = RemoteConnection(URL, TOKEN)
    try:
        if "--after-restart" in sys.argv:
            results(connection)
            profile_query_id = (ARTIFACTS / "profile-query-id.txt").read_text()
            profile = json.loads(connection.client.request("GET", "/v1/queries/"+profile_query_id))
            require(profile["profile_status"] == "captured" and profile["profile"], "profile did not survive restart")
            history = json.loads(connection.client.request("GET", "/v1/queries"))["queries"]
            require(any(q["context"].get("model_run_id") for q in history), "model history did not survive restart")
            model_resources(connection, "fct_orders", 2, "1.0 GiB")
            model_resources(connection, "customer_totals", 1, "512.0 MiB")
            resource_checks(connection)
            print("PASS: data and query history survived worker and gateway restart")
            return
        resource_checks(connection)
        protocol_checks(connection)
        profile_query_id = observability.checks(URL, TOKEN)
        (ARTIFACTS / "profile-query-id.txt").write_text(profile_query_id)
        dbt("debug", "debug")
        dbt("initial", "build", "--full-refresh")
        require(sql(connection, "select sum(amount) from lake.analytics.fct_orders") == [(decimal.Decimal("170.00"),)], "initial models incorrect")
        seed = Path("seeds/orders.csv")
        original = seed.read_text()
        try:
            seed.write_text(original.replace("2,10,20.00,2026-01-02", "2,10,25.00,2026-01-04") + "4,20,40.00,2026-01-05 00:00:00\n")
            dbt("reseed", "seed", "--full-refresh")
            dbt("incremental", "run")
            dbt("tests", "test")
            results(connection)
            dbt("repeat", "build")
            results(connection)
            model_resources(connection, "fct_orders", 2, "1.0 GiB")
            model_resources(connection, "customer_totals", 1, "512.0 MiB")
            dbt("resize-small", "run", "--select", "fct_orders", "--full-refresh", "--vars", '{"fct_cpu":1,"fct_memory":"1Gi"}')
            model_resources(connection, "fct_orders", 1, "512.0 MiB")
            dbt("resize-medium", "run", "--select", "fct_orders", "--full-refresh")
            model_resources(connection, "fct_orders", 2, "1.0 GiB")
            results(connection)
            dbt("docs", "docs", "generate")
        finally:
            seed.write_text(original)
        dbt("expected-failure", "run", "--select", "failing_model", "--vars", '{"exercise_failure":true}', expected_code=1)
        require(sql(connection, "select count(*) from checks.replay") == [(1,)], "failed dbt model did not roll back its pre-hook")
        failed_run = json.loads((ARTIFACTS / "expected-failure/target/run_results.json").read_text())
        require(len(failed_run["results"]) == 1 and failed_run["results"][0]["status"] == "error", "expected a real model error")
        dbt("unsupported-size", "run", "--select", "unsupported_size", "--vars", '{"exercise_unsupported_size":true}', expected_code=1)
        require(sql(connection, "select count(*) from checks.replay") == [(1,)], "unsupported size executed its pre-hook")
        history = json.loads(connection.client.request("GET", "/v1/queries"))["queries"]
        bad_invocation = json.loads((ARTIFACTS / "unsupported-size/target/run_results.json").read_text())["metadata"]["invocation_id"]
        require(not any(q["context"].get("dbt", {}).get("node_id") == "model.duckfleet_harness.unsupported_size" and q["context"]["dbt"]["invocation_id"] == bad_invocation for q in history), "unsupported model ran SQL")
        verify_model_placement(history)
        observability.save_model_profile(connection.client, history, ARTIFACTS)
        model_queries = [q for q in history if q["context"].get("dbt", {}).get("node_id") == "model.duckfleet_harness.fct_orders"]
        require(model_queries, "no server-side dbt model queries")
        groups = {}
        for query in model_queries:
            context = query["context"]
            require(context["dbt"]["platform"]["tags"]["team"] == "harness", "model tags missing")
            groups.setdefault(context["model_run_id"], set()).add(query["session_id"])
        require(all(len(sessions) == 1 for sessions in groups.values()), "model lifecycle crossed sessions")
        require(all(q["status"] != "running" for q in history), "missing terminal query state")
        (ARTIFACTS / "query-history.json").write_text(json.dumps(history, indent=2))
        print(f"PASS: protocol checks and dbt lifecycle; {len(history)} durable query records")
    finally:
        connection.close()


if __name__ == "__main__":
    main()
