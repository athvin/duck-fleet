"""Smoke and isolation probe for a freshly provisioned team namespace.

JSON endpoints only, so the host needs nothing beyond the standard library: list
the fleet, mint and revoke a credential, open a session, commit real work into
the namespace's own lake — then the negative test the isolation claim owes:
reach for the FIRST namespace's catalog holding its real password, and require
the attempt to die at the network rather than be answered. The isolation policy
keeps 5432 namespace-local, so credential discipline is not the only wall.
"""
import argparse
import json
import urllib.error
import urllib.request
import uuid


def call(port, token, method, path, payload=None, timeout=70):
    request = urllib.request.Request(
        f"http://127.0.0.1:{port}{path}",
        data=None if payload is None else json.dumps(payload).encode(),
        method=method,
        headers={"Authorization": f"Bearer {token}",
                 "Content-Type": "application/json"})
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return response.read()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--token", required=True)
    parser.add_argument("--dev-password", required=True)
    args = parser.parse_args()

    fleet = json.loads(call(args.port, args.token, "GET", "/v1/workers"))
    assert {w["worker_id"] for w in fleet["workers"]} == {"small"}, fleet
    minted = json.loads(call(args.port, args.token, "POST", "/v1/credentials",
                             {"principal": "team-b-analyst"}))
    call(args.port, args.token, "DELETE", "/v1/credentials/" + minted["credential_id"])

    opened = json.loads(call(args.port, args.token, "POST", "/v1/sessions", {}))
    session = opened["session_id"]
    try:
        call(args.port, args.token, "POST", f"/v1/sessions/{session}/queries",
             {"request_id": str(uuid.uuid4()),
              "sql": "create schema if not exists team_b; "
                     "create or replace table team_b.hello as select 1 as n"})
        probe = ("SELECT * FROM postgres_scan("
                 "'host=postgres.duckfleet-dev.svc.cluster.local port=5432 "
                 f"dbname=lake user=duckfleet password={args.dev_password}', "
                 "'public', 'ducklake_snapshot')")
        try:
            call(args.port, args.token, "POST", f"/v1/sessions/{session}/queries",
                 {"request_id": str(uuid.uuid4()), "sql": probe,
                  "timeout_seconds": 25}, timeout=70)
            raise AssertionError("the first namespace's catalog answered across the boundary")
        except urllib.error.HTTPError as error:
            body = json.loads(error.read().decode())
            # Denied in transport or stopped at the deadline — never a PostgreSQL
            # authentication answer, which would mean the packet got through.
            assert body["failure_kind"] in ("sql_error", "timeout"), body
            assert args.dev_password not in body["error"], \
                "the probe's credential leaked into an error message"
    finally:
        call(args.port, args.token, "DELETE", f"/v1/sessions/{session}")
    print("PASS: the team namespace serves its own lake, and the neighbour's "
          "catalog is unreachable even with the right password")


if __name__ == "__main__":
    main()
