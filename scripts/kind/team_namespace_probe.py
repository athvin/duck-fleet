"""Smoke and isolation probe for a freshly provisioned team namespace.

JSON endpoints only, so the host needs nothing beyond the standard library: list
the fleet, mint and revoke a credential, open a session, commit real work into
the namespace's own lake — then the negative test the isolation claim owes:
reach for the FIRST namespace's catalog holding its real password, and require
the attempt to die at the network rather than be answered. The isolation policy
keeps 5432 namespace-local, so credential discipline is not the only wall.
"""
import argparse
import base64
import json
import re
import shlex
import subprocess
import urllib.error
import urllib.request
import uuid


def secret_values(context, namespace):
    document = json.loads(subprocess.check_output(
        ["kubectl", "--context", context, "-n", namespace, "get", "secret",
         "duckfleet-dev", "-o", "json"], text=True))
    return {key: base64.b64decode(value).decode()
            for key, value in document["data"].items()}


def catalog_probe(dsn, namespace):
    """Use the current fixture DSN, with a host valid in either namespace."""
    fields = {}
    for part in shlex.split(dsn):
        key, separator, value = part.partition("=")
        if not separator or key in fields:
            raise ValueError("catalog fixture requires unique keyword DSN fields")
        fields[key] = value
    if set(fields) != {"host", "port", "dbname", "user", "password"}:
        raise ValueError("catalog fixture requires the generated role DSN fields")
    if not re.fullmatch(r"[a-z][a-z0-9-]*", namespace):
        raise ValueError("invalid source namespace")
    host = fields["host"]
    if not re.fullmatch(r"[a-z][a-z0-9.-]*", host):
        raise ValueError("catalog fixture requires a Kubernetes service hostname")
    if "." not in host:
        fields["host"] = f"{host}.{namespace}.svc.cluster.local"
    # This fixture uses database_roles.py's generated DNS/role/hex values, not a
    # general operator DSN. Reject other spellings without including their value.
    # Keeping password=hex unquoted also exercises the existing inline redactor
    # when DuckDB embeds the complete statement in a connection error.
    if not all(re.fullmatch(r"[a-zA-Z0-9_.:-]+", value) for value in fields.values()):
        raise ValueError("catalog fixture DSN contains unsupported field characters")
    # libpq otherwise waits indefinitely while connecting. DuckDB's statement
    # interrupt need not stop that blocking call through dropped policy packets.
    # Both controls use this same DSN and its per-address connection deadline.
    fields["connect_timeout"] = "3"
    literal = "'" + " ".join(key + "=" + value for key, value in fields.items()) + "'"
    return ("ATTACH " + literal + " AS qualification_catalog (TYPE postgres, READ_ONLY); "
            + "SELECT COUNT(*) FROM qualification_catalog.public.ducklake_snapshot")


def call(port, token, method, path, payload=None, timeout=70):
    request = urllib.request.Request(
        f"http://127.0.0.1:{port}{path}",
        data=None if payload is None else json.dumps(payload).encode(),
        method=method,
        headers={"Authorization": f"Bearer {token}",
                 "Content-Type": "application/json"})
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return response.read()


def positive_control(port, token, probe, password):
    opened = json.loads(call(port, token, "POST", "/v1/sessions", {}))
    session = opened["session_id"]
    try:
        try:
            call(port, token, "POST", f"/v1/sessions/{session}/queries",
                 {"request_id": str(uuid.uuid4()), "sql": probe,
                  "timeout_seconds": 25})
        except urllib.error.HTTPError as error:
            body = error.read().decode()
            if password in body:
                raise AssertionError("the positive-control credential leaked") from None
            raise AssertionError(f"positive-control catalog query failed: {body}") from None
    finally:
        call(port, token, "DELETE", f"/v1/sessions/{session}")


def verify_denial(body, password):
    message = body.get("error", "")
    if password in message:
        raise AssertionError("the probe credential leaked into an error message")
    kind = body.get("failure_kind")
    if kind == "timeout":
        return
    # A missing Service, bad credential, missing extension/table, or authorization
    # failure cannot count as proof that NetworkPolicy blocked a live endpoint.
    network_failure = re.search(r"timed out|timeout|network is unreachable|no route to host",
                                message, re.IGNORECASE)
    if kind != "sql_error" or not network_failure:
        raise AssertionError("cross-namespace probe did not fail at the network/deadline")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--source-port", type=int, required=True)
    parser.add_argument("--context", required=True)
    parser.add_argument("--namespace", required=True)
    parser.add_argument("--source-namespace", required=True)
    args = parser.parse_args()
    # Read only the current Secret in each explicit namespace. Secrets are never
    # placed in this process's command line or written to an artifact.
    source = secret_values(args.context, args.source_namespace)
    team = secret_values(args.context, args.namespace)
    token = team["DUCKFLEET_TOKEN"]
    probe = catalog_probe(source["CATALOG_DSN"], args.source_namespace)
    positive_control(args.source_port, source["DUCKFLEET_TOKEN"], probe,
                     source["DATABASE_CATALOG_PASSWORD"])

    fleet = json.loads(call(args.port, token, "GET", "/v1/workers"))
    assert {w["worker_id"] for w in fleet["workers"]} == {"small"}, fleet
    minted = json.loads(call(args.port, token, "POST", "/v1/credentials",
                             {"principal": "team-b-analyst"}))
    call(args.port, token, "DELETE", "/v1/credentials/" + minted["credential_id"])

    opened = json.loads(call(args.port, token, "POST", "/v1/sessions", {}))
    session = opened["session_id"]
    try:
        call(args.port, token, "POST", f"/v1/sessions/{session}/queries",
             {"request_id": str(uuid.uuid4()),
              "sql": "create schema if not exists team_b; "
                     "create or replace table team_b.hello as select 1 as n"})
        try:
            call(args.port, token, "POST", f"/v1/sessions/{session}/queries",
                 {"request_id": str(uuid.uuid4()), "sql": probe,
                  "timeout_seconds": 25}, timeout=70)
            raise AssertionError("the first namespace's catalog answered across the boundary")
        except urllib.error.HTTPError as error:
            body = json.loads(error.read().decode())
            verify_denial(body, source["DATABASE_CATALOG_PASSWORD"])
    finally:
        call(args.port, token, "DELETE", f"/v1/sessions/{session}")
    print("PASS: the team namespace serves its own lake, and the neighbour's "
          "catalog is unreachable even with the right password")


if __name__ == "__main__":
    main()
