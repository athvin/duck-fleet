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
        # be reachable, so that "available" is not a claim nothing checks. A gateway
        # that reports isolation available must actually place one: the pod is created,
        # scheduled, and its session opened labelled isolated. Scheduling is proven to
        # provision on demand both here (kind) and on EKS Auto Mode's duckfleet
        # NodePool (duck-fleet#90, resolved), so an open that cannot place is a defect,
        # not a tolerated capacity fact.
        # The open holds while a node is provisioned on a cluster that adds them on
        # demand, so it gets the session-open timeout rather than the statement one.
        opened = json.loads(client.request("POST", "/v1/sessions", {"compute": isolated},
                                           timeout=RemoteConnection.OPEN_TIMEOUT))
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


def cold_start_checks():
    """Measures what an isolated pod costs in latency against a persistent worker.

    The routing choice — shared worker vs a pod of one's own — only makes sense if
    its price is known, and the blueprint flags worker economics as a feasibility
    gate. This times three opens: a persistent session (no pod, the warm baseline);
    the first isolated session (cold — a pod is created, scheduled, and pulled to
    ready before the session answers); and a second isolated session right after
    (warm-node — the image is cached and a node already stands, so this isolates
    pod-lifecycle cost from node scale-up). Each number is wall-clock from the
    client, which is the latency a caller actually waits, and all three land in an
    evidence artifact rather than a log line. The persistent elapsed_ms the ledger
    records for the same query gives the per-execution cost side its anchor.
    """
    def timed_open(compute):
        started = time.monotonic()
        connection = RemoteConnection(URL, TOKEN, compute=compute)
        opened = time.monotonic() - started
        require(sql(connection, "select 1") == [(1,)], "session did not answer")
        return connection, opened

    persistent, warm_persistent = timed_open({"cpu": 1, "memory": "1Gi"})
    persistent.close()

    isolated_compute = {"compute": "isolated", "cpu": 1, "memory": "1Gi"}
    first, cold_isolated = timed_open(isolated_compute)
    first_pod = first.placement["worker_id"]
    first.close()
    # The second open lands on the warm node the first left behind — genuinely a fresh
    # pod, not a reuse. A closed pod is not instantly reclaimed: it terminates, and
    # until it does it still counts against the namespace quota, which is sized to one
    # isolated pod beyond the persistent workers. The gateway waits that overlap out
    # itself (quotaWaitSeconds, #94), so this open is deliberately bare: a
    # not_placeable here is a regression in that wait, not a race for this test to
    # absorb. The warm-node measurement honestly includes any quota wait the gateway
    # paid — that is the time a caller actually waits for a second isolated session.
    second, warm_isolated = timed_open(isolated_compute)
    require(second.placement["worker_id"] != first_pod, "the second isolated pod reused the first")
    second.close()

    evidence = {
        "persistent_open_seconds": round(warm_persistent, 2),
        "isolated_cold_open_seconds": round(cold_isolated, 2),
        "isolated_warm_node_open_seconds": round(warm_isolated, 2),
        "isolated_over_persistent_seconds": round(cold_isolated - warm_persistent, 2),
    }
    (ARTIFACTS / "cold-start.json").write_text(json.dumps(evidence, indent=2) + "\n")
    # Not a performance threshold — hardware and the scheduler make the absolute
    # numbers vary — but the ordering is a real invariant: a pod that must be created
    # cannot open faster than a worker that already exists.
    require(cold_isolated >= warm_persistent,
            f"isolated open {cold_isolated:.2f}s beat persistent {warm_persistent:.2f}s")
    print(f"PASS: cold start measured — persistent {warm_persistent:.2f}s, "
          f"isolated cold {cold_isolated:.2f}s, isolated warm-node {warm_isolated:.2f}s")


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

    # A size that cannot be served must be refused, must say which ceiling it hit, and
    # must never be quietly served by a persistent worker. There are two ceilings and
    # they fail at different moments, so both are exercised: the namespace quota
    # refuses at admission before a pod exists, while a size within the quota but
    # larger than any node is refused by the scheduler after one does.
    for wanted, expected, what in [
        ({"cpu": 64, "memory": "512Gi"}, "exceeded quota", "the namespace quota"),
        # Comfortably inside the quota — it has headroom for several more pods this
        # size — and far beyond what one node holds, so which ceiling answers is not a
        # matter of timing.
        ({"cpu": 11, "memory": "12Gi"}, "Insufficient", "the scheduler"),
    ]:
        refused = expect_error(503, lambda w=wanted: Client(URL, TOKEN).request(
            "POST", "/v1/sessions", {"compute": {"compute": "isolated", **w}}))
        require("not downgraded" in str(refused), str(refused))
        # The refusing component's own words, so the shortfall is actionable rather
        # than guessed.
        require(expected in str(refused), f"expected {what}: {refused}")
        # Its own kind, not the generic one an internal failure shares: nothing ran,
        # and unlike a deployment that cannot isolate at all, capacity may differ
        # later. A quota refusal reaches this too — it changed nothing, so retrying a
        # smaller request is safe.
        require(refused.failure_kind == "not_placeable", str(refused))
        require(refused.retry_safe is True, str(refused))
        print(f"PASS: an isolated request {what} cannot serve was refused, not downgraded")

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


def drain_inflight(marker_name="drain-inflight.started", drained="its worker"):
    """Holds a statement in flight while its worker — or the gateway — is terminated.

    The same scenario proves both drains: the statement flows through the gateway to a
    worker, so whichever of the two receives the signal must keep it alive. Which one
    is terminated is the harness script's choice; this function only has to leave a
    statement genuinely running when the marker file appears.

    The caller signals readiness through a file because the drain has to begin while
    the engine is genuinely busy. Getting that wrong is easy and silent: the first
    version of this used a statement that finished in a second, four seconds before
    the signal, so the assertion passed without the drain ever having waited for
    anything.

    What proves the statement was in flight is `scripts/assert_drain_order.py`, which
    reads the worker's own log and requires the signal to appear *before* the statement
    completed. That is hardware-independent, which matters because the row count is not:
    the same statement measured 25.8s on this laptop, 43.2s on a GitHub runner, and 58.0s
    on a slower one, where it hit its own timeout and the test failed having proved
    nothing about draining. Sizing a statement by wall-clock across unknown hardware is
    the mistake; the count below targets the low end of the window on the slowest machine
    seen, and the ordering assertion is what keeps it honest on the fastest.
    """
    marker = ARTIFACTS / marker_name
    marker.unlink(missing_ok=True)
    connection = RemoteConnection(URL, TOKEN, compute={"cpu": 1, "memory": "1Gi"})
    outcome = {}

    def long_statement():
        started = time.monotonic()
        try:
            connection.client.request("POST", connection.path + "/queries", {
                "request_id": str(uuid.uuid4()),
                # About 8s on a laptop and under 30s on the slowest runner measured,
                # against the 58s timeout below. Lower than it was: at 12e9 a slow
                # runner reached 58.0s and was interrupted.
                "sql": "select count(*) from range(4000000000) where range % 7 = 3",
                # Inside the 75-second drain window, so finishing is the drain's doing
                # rather than the statement being cut short.
                "timeout_seconds": 58,
            })
            outcome["elapsed"] = time.monotonic() - started
        except Exception as error:  # noqa: BLE001 - reported, not handled
            outcome["error"] = f"{error} after {time.monotonic() - started:.1f}s"

    thread = threading.Thread(target=long_statement)
    thread.start()
    # Long enough for the statement to have been admitted and reached the engine.
    time.sleep(4)
    marker.write_text(connection.placement["worker_id"])
    thread.join(timeout=120)
    require("elapsed" in outcome,
            f"a statement in flight did not survive the drain: {outcome.get('error')}")
    # A sanity floor only — the marker is written four seconds in, so anything shorter
    # than this did not reach the engine at all. Whether the statement actually spanned
    # the signal is settled by scripts/assert_drain_order.py from the worker's log, not
    # by its duration; a stricter floor here would only add a flake on fast hardware in
    # exchange for a proof that is already made properly elsewhere.
    require(outcome["elapsed"] > 4,
            f"the statement finished in {outcome['elapsed']:.1f}s, which is less than "
            "the delay before the signal, so it cannot have been running")
    print(f"PASS: a statement already in flight finished while {drained} drained "
          f"({outcome['elapsed']:.1f}s)")


def scale_checks():
    """The blueprint's Phase 1 acceptance bar, counted rather than assumed.

    A generated 110-model project builds through the platform, and then three
    claims are proven by arithmetic: every statement of the run reached a
    terminal history state, every one of them settled exactly one usage event,
    and the whole run's wall time and attributed cost are written down as
    evidence rather than remembered as an impression.
    """
    project = Path("/tmp/duckfleet-scale")
    subprocess.run([sys.executable, "/app/tests/dbt/scale/generate.py", str(project)],
                   check=True)
    started = time.monotonic()
    build = subprocess.run(
        ["dbt", "--no-use-colors", "--log-path", str(ARTIFACTS / "scale/logs"),
         "build", "--project-dir", str(project), "--profiles-dir", "/app/tests/dbt"],
        check=False,
        env=dict(os.environ, DBT_TARGET_PATH=str(ARTIFACTS / "scale/target")))
    wall = time.monotonic() - started
    require(build.returncode == 0, f"scale dbt build exited {build.returncode}")
    run = json.loads((ARTIFACTS / "scale/target/run_results.json").read_text())
    invocation = run["metadata"]["invocation_id"]
    models = [n for n in run["results"] if n["unique_id"].startswith("model.")]
    require(len(models) >= 100, f"only {len(models)} models ran; the bar is 100")
    failed = [n["unique_id"] for n in run["results"]
              if n["status"] not in ("success", "pass")]
    require(not failed, f"nodes did not succeed: {failed[:5]}")

    client = Client(URL, TOKEN)
    records, cursor = [], None
    while True:
        path = f"/v1/queries?limit=500&invocation_id={invocation}"
        page = json.loads(client.request(
            "GET", path + (f"&before={cursor}" if cursor else "")))
        records.extend(page["queries"])
        cursor = page.get("next_before")
        if not cursor:
            break
    require(len(records) >= len(models),
            f"{len(records)} history records for {len(models)} models")
    require(all(q["status"] != "running" for q in records),
            "a scale statement never reached a terminal state")

    # Every execution settled exactly one usage event — found by walking the
    # ledger backwards, since the run's events are its newest.
    wanted = {q["query_id"] for q in records}
    events, cursor = {}, None
    total_micro_usd = 0
    while wanted - set(events):
        page = json.loads(client.request(
            "GET", "/v1/usage?limit=500" + (f"&before={cursor}" if cursor else "")))
        for event in page["events"]:
            if event["execution_id"] in wanted:
                events[event["execution_id"]] = events.get(event["execution_id"], 0) + 1
                total_micro_usd += event["amount_micro_usd"]
        cursor = page.get("next_before")
        if not cursor:
            break
    missing = wanted - set(events)
    require(not missing, f"{len(missing)} executions escaped the ledger")
    require(all(count == 1 for count in events.values()),
            "an execution settled more than one event")

    evidence = {
        "models": len(models),
        "nodes": len(run["results"]),
        "statements": len(records),
        "wall_seconds": round(wall, 1),
        "total_micro_usd": total_micro_usd,
        "invocation_id": invocation,
    }
    (ARTIFACTS / "scale-run.json").write_text(json.dumps(evidence, indent=2) + "\n")
    print(f"PASS: {len(models)} models and {len(run['results'])} nodes built in "
          f"{wall:.0f}s; {len(records)} statements all terminal, each settling "
          f"exactly one event, {total_micro_usd} micro-USD attributed")


def maintenance_checks():
    """Snapshot listing, expiry, compaction, and cleanup as platform operations.

    A dozen single-row inserts fragment a table into a dozen tiny files and as
    many snapshots; maintenance must measurably shrink both without changing a
    row of data. The operations run through the ordinary execution machinery, so
    history, principal attribution, and metering are observed here rather than
    trusted — and the expiry lever stays in the bootstrap's hands alone.
    """
    import pyarrow.ipc
    admin = Client(URL, TOKEN)
    connection = RemoteConnection(URL, TOKEN, name="maintenance-fixture")
    try:
        sql(connection, "create schema if not exists api_demo")
        sql(connection, "create or replace table api_demo.fragmented (n bigint)")
        for value in range(12):
            sql(connection, f"insert into api_demo.fragmented values ({value})")
        files_before = sql(connection,
                           "select file_count from ducklake_table_info('lake') "
                           "where table_name = 'fragmented'")[0][0]
        require(files_before >= 12,
                f"expected a fragmented table, found {files_before} files")
    finally:
        connection.close()

    def snapshot_count():
        listing = admin.request("GET", "/v1/lake/snapshots")
        return pyarrow.ipc.open_stream(listing).read_all().num_rows

    before = snapshot_count()
    require(before > 12, f"only {before} snapshots before maintenance")

    # A named principal may look; only the bootstrap may expire.
    janitor_credential = json.loads(
        admin.request("POST", "/v1/credentials", {"principal": "janitor"}))
    janitor = Client(URL, janitor_credential["token"])
    pyarrow.ipc.open_stream(janitor.request("GET", "/v1/lake/snapshots")).read_all()
    refused = expect_error(403, lambda: janitor.request(
        "POST", "/v1/lake/maintenance", {"compact": True}))
    require(refused.failure_kind == "forbidden", str(refused.failure_kind))
    admin.request("DELETE", "/v1/credentials/" + janitor_credential["credential_id"])
    # Asking for nothing is the caller's mistake, said plainly.
    expect_error(400, lambda: admin.request("POST", "/v1/lake/maintenance", {}))

    admin.request("POST", "/v1/lake/maintenance",
                  {"expire_older_than_days": 0, "compact": True, "cleanup": True})
    after = snapshot_count()
    require(after < before, f"snapshots did not shrink: {before} -> {after}")

    verify = RemoteConnection(URL, TOKEN, name="maintenance-verify")
    try:
        files_after = sql(verify,
                          "select file_count from ducklake_table_info('lake') "
                          "where table_name = 'fragmented'")[0][0]
        require(files_after < files_before,
                f"compaction did not reduce files: {files_before} -> {files_after}")
        rows = sql(verify, "select count(*), sum(n) from api_demo.fragmented")
        require(rows == [(12, 66)], f"maintenance changed the data: {rows}")
    finally:
        verify.close()

    # In history under the real principal, and metered like any execution.
    history = json.loads(admin.request("GET", "/v1/queries?limit=20"))["queries"]
    swept = [q for q in history
             if q["context"].get("operation") == "lake_maintenance"]
    require(swept, "maintenance left no history record")
    require(swept[0]["status"] == "succeeded", str(swept[0]["status"]))
    require(swept[0]["principal"] == "bootstrap-admin", str(swept[0].get("principal")))
    events = json.loads(admin.request(
        "GET", "/v1/usage?execution_id=" + swept[0]["query_id"]))["events"]
    require(len(events) == 1 and events[0]["amount_micro_usd"] > 0,
            "maintenance escaped the ledger")
    print("PASS: maintenance compacted and expired the lake, metered and in history, "
          "with the expiry lever held by the bootstrap alone")


def dbt_isolated_checks():
    """A dbt model on a session pod of its own, run exactly as a user would run it.

    The adapter must carry `compute: isolated` through resolved node config before
    the first statement; the model's whole lifecycle — hooks included — must stay
    pinned to the one session on the one pod; and the committed table must outlive
    the pod that built it, because a DuckLake commit lives in the catalog and the
    bucket, not in the compute that produced it.
    """
    dbt("isolated", "run", "--select", "isolated_model",
        "--vars", '{"exercise_isolated":true}')
    connection = RemoteConnection(URL, TOKEN)
    try:
        rows = sql(connection,
                   "select count(*), min(hook_value), max(worker_threads) "
                   "from lake.analytics.isolated_model")
        require(rows[0][0] > 0, "the isolated model committed no rows")
        require(rows[0][1] == 7, "the pre-hook's temp table did not reach the model")
        require(rows[0][2] == 1, f"ran with {rows[0][2]} threads, not the requested 1")
        run = json.loads((ARTIFACTS / "isolated/target/run_results.json").read_text())
        invocation = run["metadata"]["invocation_id"]
        history = json.loads(connection.client.request("GET", "/v1/queries"))["queries"]
        queries = [q for q in history
                   if q["context"].get("dbt", {}).get("invocation_id") == invocation
                   and q["context"]["dbt"].get("node_id")
                   == "model.duckfleet_harness.isolated_model"]
        require(queries, "no history for the isolated model")
        require({q["placement"]["compute"] for q in queries} == {"isolated"},
                f"placements: { {q['placement']['compute'] for q in queries} }")
        require(all(q["worker_id"].startswith("duckfleet-session-") for q in queries),
                f"workers: { {q['worker_id'] for q in queries} }")
        require(len({q["session_id"] for q in queries}) == 1,
                "the isolated model crossed sessions")
        require(any("isolated_hook" in q["sql"] for q in queries),
                "the hooks did not reach the pinned session")
    finally:
        connection.close()
    print("PASS: a dbt model ran isolated on its own pod, hooks pinned, table durable")


def credentials_checks():
    """Issued credentials: per-principal, revocable, recorded, and never echoed.

    The environment token is the bootstrap credential: it mints everything else
    and is the only principal allowed to. A token exists in plaintext only in the
    response that minted it; listings carry no secret material, not even digests.
    History rows name the principal the gateway authenticated — server-derived,
    so a caller's own context cannot claim to be someone else.
    """
    admin = Client(URL, TOKEN)
    issued = json.loads(admin.request("POST", "/v1/credentials", {"principal": "analyst"}))
    require(issued["principal"] == "analyst", str(issued.keys()))
    require(issued["token"].startswith("dfk_"), "tokens carry a greppable prefix")

    # The issued credential works like any caller, and its work is attributed.
    connection = RemoteConnection(URL, issued["token"], name="analyst-session")
    try:
        cursor = connection.cursor()
        require(cursor.execute("select 1").fetchall() == [(1,)], "issued token could not query")
        analyst_query = cursor.query_id
    finally:
        connection.close()
    record = json.loads(admin.request("GET", "/v1/queries/" + analyst_query))
    require(record["principal"] == "analyst", f"history named {record.get('principal')!r}")
    bootstrap = RemoteConnection(URL, TOKEN, name="bootstrap-session")
    try:
        cursor = bootstrap.cursor()
        cursor.execute("select 2")
        bootstrap_query = cursor.query_id
    finally:
        bootstrap.close()
    record = json.loads(admin.request("GET", "/v1/queries/" + bootstrap_query))
    require(record["principal"] == "bootstrap-admin", f"history named {record.get('principal')!r}")

    # Listing discloses no secret material: not the token, and not its digest.
    listed = json.loads(admin.request("GET", "/v1/credentials"))["credentials"]
    require(any(c["credential_id"] == issued["credential_id"] for c in listed),
            "the minted credential is missing from the list")
    require(issued["token"] not in json.dumps(listed), "a listing leaked a token")
    require(all("token" not in key and "sha" not in key for c in listed for key in c),
            str(sorted(listed[0])))

    # Revocation bites immediately, even for a credential that was just used.
    require(json.loads(admin.request("DELETE", "/v1/credentials/" + issued["credential_id"]))
            == {"revoked": True}, "revocation did not report")
    refused = expect_error(401, lambda: RemoteConnection(URL, issued["token"]))
    require(refused.failure_kind == "unauthorized", str(refused.failure_kind))
    # Revoking it again says so rather than pretending.
    expect_error(404, lambda: admin.request("DELETE", "/v1/credentials/" + issued["credential_id"]))

    # Expiry: a one-second credential is dead before anyone here can use it.
    brief = json.loads(admin.request("POST", "/v1/credentials",
                                     {"principal": "contractor", "expires_seconds": 1}))
    time.sleep(2)
    expect_error(401, lambda: RemoteConnection(URL, brief["token"]))

    # Wrong, malformed, and missing credentials are refused alike, and the mint
    # payload is policed: no blanks, no spoofing the bootstrap's name, no
    # already-expired credentials.
    expect_error(401, lambda: RemoteConnection(URL, "dfk_" + "0" * 64))
    expect_error(401, lambda: RemoteConnection(URL, "not-even-token-shaped"))
    expect_error(401, lambda: Client(URL, "").request("GET", "/v1/queries?limit=1"))
    expect_error(400, lambda: admin.request("POST", "/v1/credentials", {"principal": "has space"}))
    expect_error(400, lambda: admin.request("POST", "/v1/credentials", {"principal": "bootstrap-admin"}))
    expect_error(400, lambda: admin.request("POST", "/v1/credentials",
                                            {"principal": "x", "expires_seconds": 0}))

    # Management is the bootstrap's alone: a named credential that could mint
    # could quietly become every principal at once.
    service = json.loads(admin.request("POST", "/v1/credentials", {"principal": "etl-service"}))
    client = Client(URL, service["token"])
    for attempt in [
        lambda: client.request("POST", "/v1/credentials", {"principal": "sneaky"}),
        lambda: client.request("GET", "/v1/credentials"),
        lambda: client.request("DELETE", "/v1/credentials/" + service["credential_id"]),
        # The budget ceiling too: a spend control writable by the spenders it
        # constrains would be an accident log, not a control.
        lambda: client.request("PUT", "/v1/budget", {"max_job_micro_usd": 1}),
    ]:
        refused = expect_error(403, attempt)
        require(refused.failure_kind == "forbidden", str(refused.failure_kind))
    # Reading the position stays open: it is how any caller sizes a request.
    require("available_micro_usd" in json.loads(client.request("GET", "/v1/budget")),
            "a named credential could not read the budget position")

    # A session answers only to its owner and the bootstrap. The session ID is in
    # history, which every credential can read, so possession proves nothing.
    analyst = json.loads(admin.request("POST", "/v1/credentials", {"principal": "analyst-2"}))
    owned = RemoteConnection(URL, analyst["token"], name="owned-session")
    try:
        require(owned.cursor().execute("select 3").fetchall() == [(3,)], "owner refused")
        session_path = owned.path  # /v1/sessions/<id>
        for attempt in [
            lambda: client.request("POST", session_path + "/queries",
                                   {"request_id": str(uuid.uuid4()), "sql": "select 1"}),
            lambda: client.request("POST", session_path + "/cancel"),
            lambda: client.request("DELETE", session_path),
        ]:
            refused = expect_error(403, attempt)
            require(refused.failure_kind == "forbidden", str(refused.failure_kind))
        # Still the owner's after all that: the probes neither acted nor touched it.
        require(owned.cursor().execute("select 4").fetchall() == [(4,)],
                "an intruder's refusals broke the owner's session")
        # The bootstrap is the operator and may close anyone's.
        admin.request("DELETE", session_path)
        refused = expect_error(404, lambda: owned.cursor().execute("select 5"))
        require(refused.failure_kind == "unknown_session", str(refused.failure_kind))
    finally:
        try:
            owned.close()
        except Exception:
            pass  # Already closed by the bootstrap above.
    # Removed afterwards, so repeated runs do not accumulate live credentials.
    admin.request("DELETE", "/v1/credentials/" + service["credential_id"])
    admin.request("DELETE", "/v1/credentials/" + analyst["credential_id"])
    print("PASS: credentials are issued, attributed in history, listed without secrets, "
          "revoked at once, expired on time, and managed by the bootstrap alone; "
          "sessions and the budget ceiling answer only to their owners")


def gateway_replaced():
    """What a caller holding a session experiences when the gateway dies without grace.

    The script kills the gateway pod the moment history shows the insert running —
    the ledger is the signal, as with the evictions, because a kind harness Job
    writes artifacts to a claim the host cannot read. Four things must be true
    afterwards: the in-flight request failed *classified* rather than as a raw
    socket error; a plain reopen rode through the replacement window on the client's
    bounded open retry; the lost session refuses further use with guidance rather
    than a bare 404; and the insert's true outcome was knowable from history — the
    worker finishes the statement whether or not the gateway that proxied it
    survives, and the committed rows must match that record.
    """
    setup = RemoteConnection(URL, TOKEN, compute={"cpu": 1, "memory": "1Gi"})
    try:
        sql(setup, "create schema if not exists api_demo")
        sql(setup, "create or replace table api_demo.gateway_replaced (n bigint)")
    finally:
        setup.close()

    connection = RemoteConnection(URL, TOKEN, compute={"cpu": 1, "memory": "1Gi"})
    outcome = {}
    request_id = str(uuid.uuid4())

    def write():
        try:
            connection.client.request("POST", connection.path + "/queries", {
                "request_id": request_id,
                # Sized to outlive the script's 2-second history polling plus the
                # kill's latency on fast hardware — at 200M a local run finished
                # before the kill landed and the stage proved nothing — while
                # staying inside the explicit timeout on the slowest runner seen.
                "sql": "insert into api_demo.gateway_replaced "
                       "select range from range(2000000000) where range % 13 = 5",
                "timeout_seconds": 58,
            })
            outcome["completed"] = True
        except RemoteError as error:
            outcome["classified"] = {"kind": error.failure_kind,
                                     "retry_safe": error.retry_safe}
        except Exception as error:  # noqa: BLE001 - the shape under test
            outcome["unclassified"] = f"{type(error).__name__}: {error}"

    thread = threading.Thread(target=write)
    thread.start()
    # The transport timeout is 75s, so a severed connection that dies silently still
    # returns within this window.
    thread.join(timeout=300)
    require(not thread.is_alive(), "the request never returned after the gateway was killed")
    require("unclassified" not in outcome,
            f"the gateway's death surfaced unclassified: {outcome.get('unclassified')}")
    require("classified" in outcome,
            f"the statement completed, so the kill came after it: {outcome}")
    require(outcome["classified"]["kind"] == "transport_failure", str(outcome))
    require(outcome["classified"]["retry_safe"] is False,
            "a severed request must never be reported retry-safe")
    print("PASS: the in-flight request failed classified, not as a raw socket error")

    # A plain constructor call, no special handling: the bounded open retry is what
    # rides through refused connections until the replacement gateway serves.
    waited = time.monotonic()
    reopened = RemoteConnection(URL, TOKEN, compute={"cpu": 1, "memory": "1Gi"})
    print(f"PASS: a reopen rode through the gateway replacement "
          f"({time.monotonic() - waited:.1f}s)")
    try:
        # The lost session refuses further use, and says what to do instead.
        try:
            sql(connection, "select 1")
            raise AssertionError("a session route survived a gateway replacement")
        except RemoteError as error:
            require(error.failure_kind == "unknown_session", str(error))
            require("Open a new connection" in str(error), str(error))
        # Closing it is tolerated: the session being gone is what closing wanted.
        connection.close()

        # The severed insert's outcome is in history. The worker ran it to the end
        # with no gateway to answer to, and a single-statement insert autocommits,
        # so history must say succeeded and the table must hold every row.
        record = None
        for _ in range(120):
            record = json.loads(
                reopened.client.request("GET", "/v1/queries/" + request_id))
            if record["status"] != "running":
                break
            time.sleep(2)
        require(record["status"] == "succeeded",
                f"the severed insert ended {record['status']}")
        committed = sql(reopened,
                        "select count(*) from api_demo.gateway_replaced")[0][0]
        # Arithmetic rather than a second scan: recomputing the predicate over the
        # range would take as long as the insert and could time out on its own.
        expected = len(range(5, 2_000_000_000, 13))
        require(committed == expected,
                f"{committed} rows committed, {expected} expected — a partial commit")
    finally:
        reopened.close()
    print("PASS: a gateway replaced without grace left classified outcomes "
          "and a knowable history")


def evicted_midquery(expect_classified=False):
    """Reports how a statement ends when its worker pod is deleted underneath it.

    The ticket asks for eviction during an open transaction to be classified
    consistently rather than left to chance. This does not prescribe the outcome: it
    requires the outcome to be *legible* — either the statement completed, or it
    failed with a classification saying whether it is safe to send again. A hang, or a
    transport error with no classification, is the failure this rules out.

    Committed state is checked afterwards either way, because a DuckLake commit is
    atomic: whatever the caller was told, the table must not be half-written.
    """
    marker = ARTIFACTS / "evicted-midquery.started"
    marker.unlink(missing_ok=True)
    # A table per mode, so a graceful run and an abrupt one cannot read each other's
    # rows when deciding whether a commit was partial.
    table = "api_demo.evicted_hard" if expect_classified else "api_demo.evicted"
    setup = RemoteConnection(URL, TOKEN, compute={"cpu": 1, "memory": "1Gi"})
    try:
        sql(setup, "create schema if not exists api_demo")
        sql(setup, f"create or replace table {table} (n bigint)")
    finally:
        setup.close()

    connection = RemoteConnection(URL, TOKEN, compute={"cpu": 1, "memory": "1Gi"})
    outcome = {}

    def write():
        try:
            connection.client.request("POST", connection.path + "/queries", {
                "request_id": str(uuid.uuid4()),
                # Sized to outlive the script's 2-second history polling plus the
                # kill's latency on fast hardware — at 200M a local run finished
                # before the kill landed and the hard mode proved nothing — while
                # staying inside the explicit timeout on the slowest runner seen.
                "sql": f"insert into {table} "
                       "select range from range(2000000000) where range % 13 = 5",
                "timeout_seconds": 58,
            })
            outcome["committed"] = True
        except RemoteError as error:
            outcome["classified"] = {"status": error.status, "kind": error.failure_kind,
                                     "retry_safe": error.retry_safe, "message": str(error)}
        except Exception as error:  # noqa: BLE001
            outcome["unclassified"] = f"{type(error).__name__}: {error}"

    thread = threading.Thread(target=write)
    thread.start()
    time.sleep(5)
    marker.write_text(connection.placement["worker_id"])
    thread.join(timeout=180)
    require(not thread.is_alive(), "the statement never returned after its worker was deleted")
    require("unclassified" not in outcome,
            f"eviction surfaced without a classification: {outcome.get('unclassified')}")
    # With no grace period the pod cannot drain, so completing would mean the statement
    # was never actually interrupted and this mode proved nothing.
    if expect_classified:
        require("classified" in outcome,
                f"the statement completed despite an immediate kill: {outcome}")

    # Either answer is acceptable; an illegible one is not.
    reader = RemoteConnection(URL, TOKEN, compute={"cpu": 1, "memory": "1Gi"})
    try:
        rows = sql(reader, f"select count(*) from {table}")[0][0]
    finally:
        reader.close()
    if outcome.get("committed"):
        require(rows > 0, f"the statement reported success but the table holds {rows} rows")
        verdict = "completed before the pod went away"
    else:
        # Not committed, so nothing may be visible: a partial write would mean a
        # DuckLake commit was not atomic after all.
        require(rows == 0, f"a statement that did not commit left {rows} rows behind")
        verdict = f"failed as {outcome['classified']['kind']}, retry_safe=" \
                  f"{outcome['classified']['retry_safe']}"
    (ARTIFACTS / "evicted-midquery.json").write_text(
        json.dumps({"outcome": outcome, "rows_after": rows}, indent=2) + "\n")
    print(f"PASS: a statement whose worker was deleted mid-run {verdict}, "
          f"and committed state stayed consistent ({rows} rows)")


def spread_checks():
    """Two workers of the same size share the load, so a fleet can scale a class.

    Run with worker-small-b added to DUCKFLEET_WORKER_URLS. Four small sessions,
    routed least-loaded-first, must land on both small workers rather than piling
    onto whichever answered discovery first.
    """
    small = {"cpu": 1, "memory": "1Gi"}
    sessions = [RemoteConnection(URL, TOKEN, compute=small) for _ in range(4)]
    try:
        placements = [s.placement["worker_id"] for s in sessions]
        for session in sessions:
            require(sql(session, "select 1") == [(1,)], "a spread session did not answer")
        require(set(placements) >= {"small", "small-b"},
                f"four small sessions did not spread across both workers: {placements}")
        # Least-loaded routing splits an even count evenly rather than 3/1 or 4/0.
        require(all(placements.count(worker) == 2 for worker in {"small", "small-b"}),
                f"sessions did not balance evenly: {placements}")
    finally:
        for session in sessions:
            session.close()
    print(f"PASS: same-size sessions spread across both small workers ({placements})")


def spread_failover_checks():
    """An unreachable worker fails over to its sibling, so one worker being gone does
    not refuse the fleet.

    Run after worker-small is stopped: the gateway still holds it in its fleet cache
    and tries it first, the connection is refused (nothing sent, so no session was
    created — as safe to skip as a classified capacity refusal), and the open falls
    over to the surviving sibling. Both small sessions must open, both on small-b.
    """
    small = {"cpu": 1, "memory": "1Gi"}
    sessions = []
    try:
        for _ in range(2):
            session = RemoteConnection(URL, TOKEN, compute=small)
            require(sql(session, "select 1") == [(1,)], "a failover session did not answer")
            sessions.append(session)
        placements = [s.placement["worker_id"] for s in sessions]
        require(all(worker == "small-b" for worker in placements),
                f"opens did not fail over to the surviving small worker: {placements}")
    finally:
        for session in sessions:
            session.close()
    print("PASS: opens failed over from a draining small worker to its sibling")


def main():
    ARTIFACTS.mkdir(exist_ok=True)
    import observability
    if "--spread" in sys.argv:
        spread_checks()
        return
    if "--spread-failover" in sys.argv:
        spread_failover_checks()
        return
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
    if "--evicted-midquery" in sys.argv:
        evicted_midquery(expect_classified="--expect-classified" in sys.argv)
        return
    if "--drain-inflight" in sys.argv:
        drain_inflight()
        return
    if "--gateway-drain-inflight" in sys.argv:
        drain_inflight("gateway-drain-inflight.started", "the gateway")
        return
    if "--gateway-replaced" in sys.argv:
        gateway_replaced()
        return
    if "--credentials" in sys.argv:
        credentials_checks()
        return
    if "--dbt-isolated" in sys.argv:
        dbt_isolated_checks()
        return
    if "--cold-start" in sys.argv:
        cold_start_checks()
        return
    if "--maintenance" in sys.argv:
        maintenance_checks()
        return
    if "--scale" in sys.argv:
        scale_checks()
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
        credentials_checks()
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
        # Where this deployment cannot isolate, a model that demands isolation must
        # fail without running SQL — never quietly run on shared compute. Where it
        # can (the kind cluster), the dedicated --dbt-isolated stage proves the
        # positive path instead, so this refusal is only meaningful to assert here.
        if not json.loads(connection.client.request("GET", "/v1/workers"))["isolated_compute"]:
            dbt("unsupported-isolated", "run", "--select", "unsupported_isolated",
                "--vars", '{"exercise_unsupported_isolated":true}', expected_code=1)
            require(sql(connection, "select count(*) from checks.replay") == [(1,)],
                    "an isolated request this deployment cannot serve executed its pre-hook")
        history = json.loads(connection.client.request("GET", "/v1/queries"))["queries"]
        bad_invocation = json.loads((ARTIFACTS / "unsupported-size/target/run_results.json").read_text())["metadata"]["invocation_id"]
        require(not any(q["context"].get("dbt", {}).get("node_id") == "model.duckfleet_harness.unsupported_size" and q["context"]["dbt"]["invocation_id"] == bad_invocation for q in history), "unsupported model ran SQL")
        require(not any(q["context"].get("dbt", {}).get("node_id") == "model.duckfleet_harness.unsupported_isolated" for q in history), "an unavailable isolated mode ran SQL")
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
