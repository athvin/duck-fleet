"""Command line access to a DuckFleet warehouse, with no dbt involved.

Covers what a warehouse actually needs from a shell: run statements, choose the
compute they run on, and read back what ran and what it cost. Statements given
together share one session, because DDL, a load and a query are usually one unit of
work and a session is what pins them to a single worker.
"""
import argparse
import csv
import json
import os
import sys

from . import RemoteConnection, RemoteError, Client


def compute_from(arguments):
    """Builds the compute request, or `None` to take the deployment's default.

    CPU and memory are sent together or not at all, which is the server's rule too;
    sending one would be refused as a malformed request rather than defaulted.
    """
    compute = {}
    if arguments.isolated:
        compute["compute"] = "isolated"
    if (arguments.cpu is None) != (arguments.memory is None):
        raise SystemExit("--cpu and --memory must be given together")
    if arguments.cpu is not None:
        compute["cpu"] = arguments.cpu
        compute["memory"] = arguments.memory
    return compute or None


def emit(cursor, how):
    if cursor.description is None:
        return
    names = [column[0] for column in cursor.description]
    rows = cursor.fetchall()
    if how == "json":
        print(json.dumps([dict(zip(names, row)) for row in rows], indent=2, default=str))
    elif how == "csv":
        writer = csv.writer(sys.stdout)
        writer.writerow(names)
        writer.writerows(rows)
    else:
        widths = [max(len(name), *(len(str(row[i])) for row in rows)) if rows else len(name)
                  for i, name in enumerate(names)]
        line = "  ".join(name.ljust(width) for name, width in zip(names, widths))
        print(line)
        print("  ".join("-" * width for width in widths))
        for row in rows:
            print("  ".join(str(value).ljust(width) for value, width in zip(row, widths)))
        print(f"({len(rows)} row{'' if len(rows) == 1 else 's'})")


def run_sql(client_arguments, arguments):
    connection = RemoteConnection(*client_arguments, name="cli",
                                  compute=compute_from(arguments))
    if arguments.placement:
        print(json.dumps(connection.placement), file=sys.stderr)
    try:
        for statement in arguments.statement:
            cursor = connection.cursor().execute(statement, arguments.param)
            emit(cursor, arguments.format)
    finally:
        # Closing rolls back anything uncommitted, which is the server's behaviour and
        # the reason a failed load does not leave a half-written table behind.
        connection.close()


def get(client_arguments, path):
    print(json.dumps(json.loads(Client(*client_arguments).request("GET", path)), indent=2))


def main(argv=None):
    parser = argparse.ArgumentParser(prog="duckfleet", description=__doc__.splitlines()[0])
    parser.add_argument("--url", default=os.environ.get("DUCKFLEET_URL"),
                        help="server base URL, or DUCKFLEET_URL")
    parser.add_argument("--token", default=os.environ.get("DUCKFLEET_TOKEN"),
                        help="bearer token, or DUCKFLEET_TOKEN")
    commands = parser.add_subparsers(dest="command", required=True)

    sql = commands.add_parser("sql", help="run statements in one session")
    sql.add_argument("statement", nargs="+")
    sql.add_argument("--param", action="append", default=[],
                     help="bound parameter, repeatable; always sent as a parameter "
                          "rather than pasted into the SQL")
    sql.add_argument("--cpu", type=int)
    sql.add_argument("--memory")
    sql.add_argument("--isolated", action="store_true",
                     help="run on a pod of this session's own; refused where the "
                          "deployment cannot provide it, never downgraded")
    sql.add_argument("--format", choices=["table", "json", "csv"], default="table")
    sql.add_argument("--placement", action="store_true",
                     help="report the worker and cgroup limits on stderr")

    history = commands.add_parser("history", help="statements this warehouse has run")
    history.add_argument("--limit", type=int, default=20)
    history.add_argument("--worker-id")
    commands.add_parser("workers", help="the sizes this deployment runs")
    query = commands.add_parser("query", help="one statement in full, with its profile")
    query.add_argument("id")
    usage = commands.add_parser("usage", help="the usage ledger")
    usage.add_argument("--limit", type=int, default=20)
    commands.add_parser("budget", help="prepaid balance and per-statement cap")

    arguments = parser.parse_args(argv)
    if not arguments.url or not arguments.token:
        raise SystemExit("set --url/--token or DUCKFLEET_URL/DUCKFLEET_TOKEN")
    client_arguments = (arguments.url, arguments.token)
    try:
        if arguments.command == "sql":
            run_sql(client_arguments, arguments)
        elif arguments.command == "history":
            path = f"/v1/queries?limit={arguments.limit}"
            if arguments.worker_id:
                path += f"&worker_id={arguments.worker_id}"
            get(client_arguments, path)
        elif arguments.command == "query":
            get(client_arguments, f"/v1/queries/{arguments.id}")
        elif arguments.command == "usage":
            get(client_arguments, f"/v1/usage?limit={arguments.limit}")
        elif arguments.command == "budget":
            get(client_arguments, "/v1/budget")
        elif arguments.command == "workers":
            get(client_arguments, "/v1/workers")
    except RemoteError as error:
        # The server's own classification, because it is what a caller acts on:
        # whether the statement may be sent again is not this client's judgement.
        print(f"{error.failure_kind or 'error'} (HTTP {error.status}): {error}", file=sys.stderr)
        print(f"retry_safe: {str(error.retry_safe).lower()}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
