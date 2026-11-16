"""Real dbt + HTTP + Postgres + S3 integration gate; no mocks."""
import concurrent.futures
import datetime
import decimal
import json
import os
from pathlib import Path
import subprocess
import sys
import threading
import time
import uuid

from duckfleet import Client, RemoteConnection, RemoteError

URL = os.environ["DUCKFLEET_URL"]
TOKEN = os.environ["DUCKFLEET_TOKEN"]
ARTIFACTS = Path("/artifacts")


def require(condition, message):
    if not condition:
        raise AssertionError(message)


def expect_error(status, action):
    """Returns the classified error, so a caller can assert on how it was classified."""
    try:
        action()
    except RemoteError as error:
        require(error.status == status, f"expected HTTP {status}, got {error.status}: {error}")
        return error
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
    # A malformed request is the caller's to fix, so it stays a classified 400.
    for compute in [{"cpu":2}, {"memory":"2Gi"}, {"cpu":0,"memory":"1Gi"},
                    {"cpu":1,"memory":"1GB"}, {"compute":"burst"}]:
        refused = expect_error(400, lambda: connection.client.request(
            "POST", "/v1/sessions", {"compute":compute}))
        require(refused.failure_kind == "invalid_request", f"{compute}: {refused}")
    # A size nothing runs is a different thing: the request is well formed and this
    # deployment simply has no worker for it. It must say so, and must never be
    # served by a worker of another size.
    absent = expect_error(503, lambda: connection.client.request(
        "POST", "/v1/sessions", {"compute":{"cpu":32,"memory":"128Gi"}}))
    require("no worker provides cpu=32, memory=128Gi" in str(absent), str(absent))
    # A size this deployment does run resolves even though nothing was taught it:
    # 1Gi at 1 CPU is the default pair expressed the long way.
    exact = RemoteConnection(URL, TOKEN, compute={"cpu":1,"memory":"1024Mi"})
    try:
        require(exact.placement["worker_id"] == "small", str(exact.placement))
        require(exact.placement["memory_bytes"] == 1024**3, str(exact.placement))
    finally:
        exact.close()
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


def routing_contract_checks(connection):
    """The compute contract over the wire, with no dbt involved.

    Whether isolated compute can run is a property of the deployment, so this asserts
    against what the gateway reports about itself rather than assuming either answer.
    Most deployments cannot create pods, and there asking for isolation must be
    refused explicitly rather than quietly served by a shared worker.
    """
    client = connection.client
    available = json.loads(client.request("GET", "/v1/workers"))["isolated_compute"]
    isolated = {"compute": "isolated", "cpu": 1, "memory": "1Gi"}
    if available:
        # Covered in depth by --isolated against a real cluster; here it only has to
        # be reachable, so that "available" is not a claim nothing checks.
        opened = json.loads(client.request("POST", "/v1/sessions", {"compute": isolated}))
        try:
            require(opened["placement"]["compute"] == "isolated", str(opened["placement"]))
        finally:
            client.request("DELETE", "/v1/sessions/" + opened["session_id"])
    else:
        refused = expect_error(501, lambda: client.request(
            "POST", "/v1/sessions", {"compute": isolated}))
        require(refused.failure_kind == "compute_unavailable", str(refused))
        # Nothing ran, so the caller may send it again somewhere that supports it.
        require(refused.retry_safe is True, str(refused))
    # An unrecognised mode is a different problem: fix the request, not the platform.
    # It must stay a *classified* 400 rather than becoming an extractor rejection,
    # which would be the one failure here carrying no failure_kind to act on.
    unknown = expect_error(400, lambda: client.request(
        "POST", "/v1/sessions", {"compute": {"compute": "burst"}}))
    require(unknown.failure_kind == "invalid_request", str(unknown))
    require("persistent or isolated" in str(unknown), str(unknown))
    # The resolved mode is reported back, so a caller can see where it landed.
    opened = json.loads(client.request("POST", "/v1/sessions",
                                       {"compute": {"cpu": 2, "memory": "2Gi"}}))
    try:
        placement = opened["placement"]
        require(placement["compute"] == "persistent", str(placement))
        require(placement["worker_id"] == "medium", str(placement))
    finally:
        client.request("DELETE", "/v1/sessions/" + opened["session_id"])
    print("PASS: compute routing contract names its mode and refuses what it cannot run")


def protocol_checks(connection):
    client = connection.client
    routing_contract_checks(connection)
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


def isolated_session(compute):
    """Opens an isolated session over the adapter, as a caller would.

    Through `RemoteConnection` rather than raw HTTP so results come back decoded:
    the worker answers in Arrow IPC, where a scalar is binary and asserting on the
    bytes proves nothing about the value.

    The pod name arrives as the placement's `worker_id`, which is the pod's own name,
    because reservation recovery keys on worker identity.
    """
    connection = RemoteConnection(URL, TOKEN, compute=compute)
    placement = connection.placement
    require(placement["compute"] == "isolated", str(placement))
    require(placement["worker_id"].startswith("duckfleet-session-"), str(placement))
    return connection, placement


def isolated_checks():
    """On-demand compute, against a real cluster.

    Everything here is observable through the API. Whether the pod is actually gone
    afterwards is a cluster fact, so test-k8s.sh asserts that with kubectl; what this
    proves is that the platform reports the placement honestly and refuses what it
    cannot place.
    """
    # A size no persistent worker in this release runs, so it cannot be satisfied by
    # a shared worker even by accident.
    compute = {"compute": "isolated", "cpu": 1, "memory": "768Mi"}
    connection, placement = isolated_session(compute)
    pod = placement["worker_id"]
    (ARTIFACTS / "isolated-pod.txt").write_text(pod)
    try:
        # The cgroup the pod actually got, not what was asked for. These come from the
        # pod reading its own limits, so they are the kubelet's answer rather than the
        # chart's intention.
        require(placement["cpu"] == 1, str(placement))
        require(placement["memory_bytes"] == 768 * 1024 * 1024, str(placement))
        require(placement["cgroup_memory_max"] == 768 * 1024 * 1024, str(placement))
        require(placement["cgroup_cpu_quota"] // placement["cgroup_cpu_period"] == 1,
                str(placement))
        # The engine is sized from the pod's own limits, as on a persistent worker.
        require(sql(connection, "select 21 * 2") == [(42,)], "isolated session did not answer")
        require(sql(connection, "select current_setting('threads')::integer") == [(1,)],
                "isolated engine was not sized from its pod")
        # No sharing: an identical second request gets a pod of its own rather than
        # joining this one. Asking for isolation twice must not quietly produce one
        # pod serving two callers, which is what a shared worker would do.
        other, other_placement = isolated_session(compute)
        try:
            require(other_placement["worker_id"] != pod, f"two isolated sessions shared {pod}")
            require(sql(other, "select 1") == [(1,)], "second isolated session did not answer")
        finally:
            other.close()
    finally:
        connection.close()

    # History records an isolated placement the same way it records a shared one, so
    # a bill can be traced to the pod that earned it.
    history = json.loads(Client(URL, TOKEN).request("GET", "/v1/queries?worker_id=" + pod))
    rows = history["queries"]
    require(rows, f"no history rows for {pod}")
    for row in rows:
        require(row["placement"]["compute"] == "isolated", str(row["placement"]))
        require(row["placement"]["worker_id"] == pod, str(row["placement"]))
    print(f"PASS: an isolated session ran on its own pod {pod} and reported its real cgroup")

    # A size no node can hold must be refused with the scheduler's reason, and must
    # never be quietly served by a persistent worker.
    refused = expect_error(503, lambda: Client(URL, TOKEN).request(
        "POST", "/v1/sessions",
        {"compute": {"compute": "isolated", "cpu": 64, "memory": "512Gi"}}))
    require("not downgraded" in str(refused), str(refused))
    # The scheduler's own words, so the shortfall is actionable rather than guessed.
    require("Insufficient" in str(refused), str(refused))
    # Its own kind, not the generic one an internal failure shares: nothing ran, and
    # unlike a deployment that cannot isolate at all, capacity may differ later.
    require(refused.failure_kind == "not_placeable", str(refused))
    require(refused.retry_safe is True, str(refused))
    print("PASS: an unplaceable isolated request was refused, not downgraded")

    # Metered under the pod's own identity, which is what makes an isolated bill
    # attributable: the pod is gone, so this row is the only remaining record of it.
    metered = json.loads(Client(URL, TOKEN).request("GET", "/v1/usage"))["events"]
    mine = [event for event in metered if event["worker_id"] == pod]
    require(mine, f"the isolated session was not metered under {pod}")
    require(all(event["amount_micro_usd"] >= 0 for event in mine), str(mine))
    print(f"PASS: the isolated session was metered under its own pod ({len(mine)} events)")


def leak_isolated():
    """Abandons an isolated session mid-statement, with funds still reserved.

    Leaves the pod running and never closes it, so the caller can destroy the gateway
    and assert the pod is collected and the ledger settled anyway.

    The statement matters. A reservation is taken per statement and released when it
    reaches a terminal status, so abandoning an idle session holds nothing and would
    make the settlement assertion pass without proving anything. Money is only at
    risk while a statement is still running, because an isolated pod never restarts
    to recover its own reservation the way a persistent worker does.
    """
    connection, placement = isolated_session(
        {"compute": "isolated", "cpu": 1, "memory": "768Mi"})
    (ARTIFACTS / "leaked-pod.txt").write_text(placement["worker_id"])

    def long_statement():
        try:
            connection.client.request("POST", connection.path + "/queries", {
                "request_id": str(uuid.uuid4()),
                "sql": "select sum(a.range * b.range) from range(200000) a, range(200000) b",
                "timeout_seconds": 60,
            })
        except Exception:
            # Expected: this process exits while the statement is still running, and
            # the pod running it is about to be destroyed underneath it.
            pass

    threading.Thread(target=long_statement, daemon=True).start()
    # Long enough for the worker to have admitted the statement and taken its hold.
    time.sleep(8)
    print(f"left a statement running on {placement['worker_id']}")


def main():
    ARTIFACTS.mkdir(exist_ok=True)
    import observability
    if "--history-without-workers" in sys.argv:
        observability.history_without_workers(URL, TOKEN)
        return
    if "--concurrency" in sys.argv:
        import concurrency
        concurrency.checks(URL, TOKEN, ARTIFACTS)
        return
    if "--metering" in sys.argv:
        import metering
        metering.checks(URL, TOKEN, ARTIFACTS)
        return
    if "--isolated" in sys.argv:
        isolated_checks()
        return
    if "--leak-isolated" in sys.argv:
        leak_isolated()
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
