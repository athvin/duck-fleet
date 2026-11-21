#!/usr/bin/env python3
"""Disposable Compose proof of bounded statements exceeding sixty seconds.

Requires Docker Compose and PyArrow. Never uses an existing deployment.
"""

import http.server, json, os, secrets, subprocess, tempfile, threading, time, urllib.request, urllib.error, uuid
from pathlib import Path
import argparse

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--image", required=True)
parser.add_argument("--output", type=Path, required=True)
args = parser.parse_args()
source = Path(__file__).resolve().parents[1]
output = args.output.resolve()
output.parent.mkdir(parents=True, exist_ok=True)
project = "duckfleet-deadline-" + uuid.uuid4().hex[:10]
token = secrets.token_hex(32)
env = dict(
    os.environ,
    DUCKFLEET_PORT="0",
    DUCKFLEET_TOKEN=token,
    DUCKFLEET_POSTGRES_ADMIN_PASSWORD=secrets.token_hex(32),
    DUCKFLEET_INTERNAL_TOKEN=secrets.token_hex(32),
)


class Slow(http.server.BaseHTTPRequestHandler):
    delayed = False

    def log_message(self, *args):
        pass

    def do_HEAD(self):
        self.send_response(200)
        self.send_header("Content-Length", "5")
        self.end_headers()

    def do_GET(self):
        if not Slow.delayed:
            Slow.delayed = True
            time.sleep(65)
        self.send_response(200)
        self.send_header("Content-Length", "5")
        self.end_headers()
        try:
            self.wfile.write(b"n\n42\n")
        except BrokenPipeError:
            pass


slow = http.server.ThreadingHTTPServer(("0.0.0.0", 0), Slow)
threading.Thread(target=slow.serve_forever, daemon=True).start()
with (
    tempfile.TemporaryDirectory(prefix=project) as scratch,
    output.with_suffix(".log").open("w") as log,
):
    override = Path(scratch) / "compose.json"
    override.write_text(
        json.dumps(
            {
                "services": {
                    name: {"image": args.image, "pull_policy": "never"}
                    for name in ["platform-migrate", "worker-small", "server"]
                }
            }
        )
    )
    command = [
        "docker",
        "compose",
        "-p",
        project,
        "-f",
        str(source / "compose.yaml"),
        "-f",
        str(override),
    ]

    def req(method, path, body=None):
        data = None if body is None else json.dumps(body).encode()
        request = urllib.request.Request(
            url + path,
            data=data,
            method=method,
            headers={
                "Authorization": "Bearer " + token,
                "Content-Type": "application/json",
            },
        )
        with urllib.request.urlopen(request, timeout=160) as response:
            return response.read()

    try:
        subprocess.run(
            command + ["up", "-d", "--wait", "server"],
            env=env,
            stdout=log,
            stderr=log,
            check=True,
        )
        url = (
            "http://"
            + subprocess.check_output(
                command + ["port", "server", "8080"], env=env, text=True
            ).strip()
        )
        session = json.loads(req("POST", "/v1/sessions", {}))["session_id"]
        last_request_id = None

        def query(sql, seconds=30):
            global last_request_id
            last_request_id = str(uuid.uuid4())
            return req(
                "POST",
                "/v1/sessions/" + session + "/queries",
                {"request_id": last_request_id, "sql": sql, "timeout_seconds": seconds},
            )

        try:
            query("SELECT 1", 601)
            raise AssertionError("601 accepted")
        except urllib.error.HTTPError as e:
            assert e.code == 400
        query("SET http_timeout=120")
        query("SET http_retries=0")
        started = time.monotonic()
        result = query(
            "SELECT * FROM read_csv('http://host.docker.internal:%d/slow.csv',header=true,columns={'n':'BIGINT'})"
            % slow.server_port,
            120,
        )
        elapsed = time.monotonic() - started
        assert 60 < elapsed < 150, elapsed
        import pyarrow.ipc as ipc

        assert ipc.open_stream(result).read_all().to_pylist() == [{"n": 42}]
        Slow.delayed = False
        short_started = time.monotonic()
        try:
            query(
                "SELECT * FROM read_csv('http://host.docker.internal:%d/short.csv',header=true,columns={'n':'BIGINT'})"
                % slow.server_port,
                1,
            )
            raise AssertionError("short deadline unexpectedly succeeded")
        except urllib.error.HTTPError as error:
            refusal = json.loads(error.read())
            assert refusal["failure_kind"] in ("timeout", "unavailable"), refusal[
                "failure_kind"
            ]
            # Blocking external I/O may observe the interrupt after the bounded
            # gateway wait. Reconcile the same ID; never replay the statement.
            until = time.monotonic() + 90
            while True:
                outcome = json.loads(req("GET", "/v1/requests/" + last_request_id))
                if outcome.get("status") == "timed_out":
                    break
                assert time.monotonic() < until, outcome.get("status")
                time.sleep(1)
        short_elapsed = time.monotonic() - short_started
        req("DELETE", "/v1/sessions/" + session)
        output.write_text(
            json.dumps(
                {
                    "elapsed_seconds": elapsed,
                    "statement_deadline_seconds": 120,
                    "rows": [{"n": 42}],
                    "out_of_range_601_refused": True,
                    "short_deadline_refused": True,
                    "short_initial_failure": refusal["failure_kind"],
                    "short_final_status": outcome["status"],
                    "short_deadline_elapsed_seconds": short_elapsed,
                    "scope": "actual DuckDB statement through gateway; delayed synthetic HTTP CSV",
                },
                indent=2,
            )
            + "\n"
        )
        print(output.read_text(), flush=True)
    finally:
        subprocess.run(
            command + ["down", "--volumes", "--remove-orphans"],
            env=env,
            stdout=log,
            stderr=log,
        )
        slow.shutdown()
