"""The whole warehouse workflow with dbt absent: HTTP client and CLI only.

Runs in `docker/cli.Dockerfile`, an image that installs the standalone client and
nothing else. The first assertion is that `import dbt` fails — run anywhere dbt
happens to be installed, everything below would still pass and would prove only that
the API works, not that it works without dbt.
"""
import json
import os
import subprocess
import sys
import uuid

from duckfleet import Client, RemoteConnection, RemoteError

URL = os.environ["DUCKFLEET_URL"]
TOKEN = os.environ["DUCKFLEET_TOKEN"]
SCHEMA = "api_demo"


def require(condition, message):
    if not condition:
        raise AssertionError(message)


def expect_error(status, action):
    try:
        action()
    except RemoteError as error:
        require(error.status == status, f"expected HTTP {status}, got {error.status}: {error}")
        return error
    raise AssertionError(f"expected HTTP {status}")


def dbt_must_be_absent():
    try:
        import dbt  # noqa: F401
    except ModuleNotFoundError:
        print("PASS: dbt is not installed, so what follows is a dbt-free path")
        return
    raise AssertionError(
        "dbt is importable in this image, so this test cannot show anything about "
        "running without it; build docker/cli.Dockerfile instead"
    )


def cli(*arguments, expect_code=0):
    """Runs the installed console script, not the module.

    Through the entry point because that is what a user gets from `pip install
    duckfleet`; importing `cli.main` would skip the packaging this ticket is about.
    """
    result = subprocess.run(["duckfleet", *arguments], capture_output=True, text=True,
                            env=dict(os.environ, DUCKFLEET_URL=URL, DUCKFLEET_TOKEN=TOKEN))
    require(result.returncode == expect_code,
            f"duckfleet {' '.join(arguments)}: exit {result.returncode}\n{result.stderr}")
    return result


def create_and_load():
    """Create a table, load rows, read them back — the minimum a warehouse must do."""
    run_id = str(uuid.uuid4())
    connection = RemoteConnection(URL, TOKEN, name="api-workflow",
                                  context={"api_workflow_run": run_id})
    try:
        cursor = connection.cursor()
        cursor.execute(f"create schema if not exists {SCHEMA}")
        cursor.execute(f"create or replace table {SCHEMA}.events "
                       "(event_id integer, region varchar, amount decimal(10,2))")
        # Bound parameters rather than interpolated SQL, which is what the transport
        # is for: the server receives values, not a composed statement.
        for event_id, region, amount in [(1, "eu", "10.50"), (2, "us", "20.25"),
                                         (3, "eu", "30.00")]:
            cursor.execute(f"insert into {SCHEMA}.events values (?, ?, ?)",
                           [event_id, region, amount])
        rows = connection.cursor().execute(
            f"select region, sum(amount) from {SCHEMA}.events where region = ? group by region",
            ["eu"],
        ).fetchall()
        require(rows == [("eu", 40.50)] or [(r[0], float(r[1])) for r in rows] == [("eu", 40.50)],
                f"aggregate came back as {rows}")
        total = connection.cursor().execute(f"select count(*) from {SCHEMA}.events").fetchall()
        require(total == [(3,)], f"expected three rows, got {total}")
        print("PASS: created a table, loaded rows with bound parameters, and queried them back")
        return run_id
    finally:
        connection.close()


def durable_across_sessions(run_id):
    """A new session sees the committed data, so the table is in the catalog rather
    than in whatever session created it."""
    connection = RemoteConnection(URL, TOKEN, name="api-workflow-reader")
    try:
        rows = connection.cursor().execute(
            f"select count(*) from {SCHEMA}.events").fetchall()
        require(rows == [(3,)], f"a second session saw {rows}")
    finally:
        connection.close()
    # The statements are in history under the context this run supplied, which is how
    # a non-dbt caller attributes work it ran.
    history = json.loads(Client(URL, TOKEN).request("GET", "/v1/queries?limit=200"))["queries"]
    mine = [row for row in history if row["context"].get("api_workflow_run") == run_id]
    require(mine, f"no history rows carried api_workflow_run={run_id}")
    require(any("insert into" in row["sql"] for row in mine), "no load statement in history")
    require(all(row["placement"]["worker_id"] for row in mine), "history lost the placement")
    usage = json.loads(Client(URL, TOKEN).request("GET", "/v1/usage?limit=200"))["events"]
    executions = {row["query_id"] for row in mine}
    require(executions & {event["execution_id"] for event in usage},
            "statements this run executed were not metered")
    print(f"PASS: data survived the session, and {len(mine)} statements are in history and metered")


def compute_selection():
    """Choosing where a statement runs, with no dbt config anywhere."""
    for cpu, memory, expected in [(1, "1Gi", "small"), (2, "2Gi", "medium")]:
        connection = RemoteConnection(URL, TOKEN, compute={"cpu": cpu, "memory": memory})
        try:
            require(connection.placement["worker_id"] == expected,
                    f"cpu={cpu} landed on {connection.placement}")
            threads = connection.cursor().execute(
                "select current_setting('threads')::integer").fetchall()
            require(threads == [(cpu,)], f"engine ran with {threads}, expected {cpu}")
        finally:
            connection.close()
    # A size this deployment does not run is refused, naming what it does run, rather
    # than quietly served by something else.
    absent = expect_error(503, lambda: RemoteConnection(
        URL, TOKEN, compute={"cpu": 32, "memory": "128Gi"}))
    require("no worker provides" in str(absent), str(absent))
    # Isolation is expressible here too. Compose cannot create pods, so this
    # deployment refuses it explicitly; the point is that the caller can ask.
    isolated = expect_error(501, lambda: RemoteConnection(
        URL, TOKEN, compute={"compute": "isolated", "cpu": 1, "memory": "1Gi"}))
    require(isolated.failure_kind == "compute_unavailable", str(isolated))
    print("PASS: compute was selected per request, and unavailable sizes were refused")


def cli_workflow():
    """The same workflow a user would run from a shell."""
    table = f"{SCHEMA}.cli_events"
    cli("sql", f"create or replace table {table} (id integer, label varchar)",
        f"insert into {table} values (1, 'from-the-cli')")
    # Statements given together share one session, which is what makes a load and the
    # query that checks it one unit of work.
    selected = cli("sql", f"select label from {table} where id = ?", "--param", "1",
                   "--format", "json")
    require(json.loads(selected.stdout) == [{"label": "from-the-cli"}], selected.stdout)
    # Compute selection from the command line, or it is only available to people
    # writing their own HTTP calls.
    placed = cli("sql", "select current_setting('threads')::integer as threads",
                 "--cpu", "2", "--memory", "2Gi", "--placement", "--format", "json")
    require(json.loads(placed.stdout) == [{"threads": 2}], placed.stdout)
    require(json.loads(placed.stderr)["worker_id"] == "medium", placed.stderr)
    # A refusal exits non-zero and reports the server's own classification, so a shell
    # script can tell "ask differently" from "ask later".
    refused = cli("sql", "select 1", "--cpu", "32", "--memory", "128Gi", expect_code=1)
    require("unavailable" in refused.stderr, refused.stderr)
    require("retry_safe:" in refused.stderr, refused.stderr)
    # --cpu without --memory is the server's rule, enforced before a session opens so
    # the caller is told rather than having a default chosen for them.
    incomplete = cli("sql", "select 1", "--cpu", "2", expect_code=1).stderr
    require("together" in incomplete, incomplete)
    for command, expected in [(["workers"], "isolated_compute"),
                              (["history", "--limit", "1"], "queries"),
                              (["usage", "--limit", "1"], "events"),
                              (["budget"], "prepaid_micro_usd")]:
        require(expected in cli(*command).stdout, f"duckfleet {command[0]} lost {expected}")
    # A statement can be inspected in full by id, which is how a caller gets the
    # profile for something it ran earlier.
    first = json.loads(cli("history", "--limit", "1").stdout)["queries"][0]
    detail = json.loads(cli("query", first["query_id"]).stdout)
    require(detail["query_id"] == first["query_id"], str(detail))
    print("PASS: the CLI ran the workflow, selected compute, and reported refusals")


def main():
    dbt_must_be_absent()
    run_id = create_and_load()
    durable_across_sessions(run_id)
    compute_selection()
    cli_workflow()
    print("PASS: a complete warehouse workflow ran with no dbt installed")


if __name__ == "__main__":
    sys.exit(main())
