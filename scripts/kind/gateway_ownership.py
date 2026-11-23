import argparse
import json
import socket
import subprocess
import threading
import time
import urllib.error
import urllib.request
import uuid

from team_namespace_probe import call, secret_values


def kubectl(context, namespace, args, input_text=None, check=True):
    return subprocess.run(
        ["kubectl", "--context", context, "-n", namespace, *args],
        input=input_text, capture_output=True, text=True, check=check,
    )


def free_port():
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return listener.getsockname()[1]


class Gateway:
    def __init__(self, context, namespace, pod, token):
        self.context = context
        self.namespace = namespace
        self.name = pod["metadata"]["name"]
        self.uid = pod["metadata"]["uid"]
        self.token = token
        self.port = free_port()
        self.forward = subprocess.Popen(
            ["kubectl", "--context", context, "-n", namespace, "port-forward",
             f"pod/{self.name}", f"{self.port}:8080"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        self.wait_ready()

    def wait_ready(self):
        for _ in range(120):
            try:
                with urllib.request.urlopen(
                    f"http://127.0.0.1:{self.port}/healthz", timeout=1,
                ) as response:
                    if response.status == 200:
                        return
            except Exception:
                pass
            time.sleep(0.5)
        raise AssertionError(f"gateway {self.name} did not become reachable")

    def stop(self):
        self.forward.terminate()
        try:
            self.forward.wait(timeout=5)
        except subprocess.TimeoutExpired:
            self.forward.kill()

    def call(self, method, path, payload=None, timeout=75):
        return call(self.port, self.token, method, path, payload, timeout)

    def json(self, method, path, payload=None, timeout=75):
        return json.loads(self.call(method, path, payload, timeout))

    def open(self):
        opened = self.json("POST", "/v1/sessions", {"compute": {"cpu": 1, "memory": "768Mi"}})
        return opened["session_id"], opened["placement"]["worker_id"]

    def query(self, session, sql):
        return self.call("POST", f"/v1/sessions/{session}/queries",
                         {"request_id": str(uuid.uuid4()), "sql": sql, "timeout_seconds": 25})


def expect_status(expected, operation):
    try:
        operation()
    except urllib.error.HTTPError as error:
        expected_codes = [expected] if isinstance(expected, int) else expected
        assert error.code in expected_codes, (error.code, error.read().decode())
        return
    raise AssertionError(f"expected HTTP {expected}")


def sql(context, namespace, statement):
    return kubectl(context, namespace,
                   ["exec", "statefulset/duckfleet-postgres", "--", "psql", "-U", "duckfleet",
                    "-d", "platform", "-At", "-c", statement]).stdout.strip()


def pod_exists(context, namespace, name):
    return kubectl(context, namespace, ["get", "pod", name], check=False).returncode == 0


def wait_absent(context, namespace, name):
    for _ in range(120):
        if not pod_exists(context, namespace, name):
            return
        time.sleep(1)
    raise AssertionError(f"pod {name} still exists")


def wait_gateways(context, namespace):
    for _ in range(180):
        pods = json.loads(kubectl(context, namespace, [
            "get", "pods", "-l", "app=duckfleet-gateway", "-o", "json",
        ]).stdout)["items"]
        ready = [pod for pod in pods if all(
            container.get("ready") for container in pod["status"].get("containerStatuses", [])
        )]
        if len(ready) == 2:
            return sorted(ready, key=lambda pod: pod["metadata"]["name"])
        time.sleep(1)
    raise AssertionError("two gateways did not become ready")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--context", required=True)
    parser.add_argument("--namespace", required=True)
    args = parser.parse_args()
    context, namespace = args.context, args.namespace
    token = secret_values(context, namespace)["DUCKFLEET_TOKEN"]
    pods = wait_gateways(context, namespace)
    first = Gateway(context, namespace, pods[0], token)
    second = Gateway(context, namespace, pods[1], token)
    gateways = [first, second]
    try:
        time.sleep(3)
        session, worker = first.open()
        second.query(session, "SELECT 1")
        expect_status(404, lambda: second.call("DELETE", f"/v1/sessions/{session}"))
        assert pod_exists(context, namespace, worker)
        print("PASS: a sibling routed reads but could not close the live owner session")

        kubectl(context, namespace, ["delete", "pod", second.name, "--wait=true"])
        second.stop()
        second = Gateway(context, namespace, wait_gateways(context, namespace)[1], token)
        gateways.append(second)
        first.query(session, "SELECT 1")
        assert pod_exists(context, namespace, worker)
        print("PASS: concurrent startup and non-owner rolling replacement preserved live compute")

        sql(context, namespace,
            f"UPDATE gateway_sessions SET gateway_epoch='stale-epoch' WHERE session_id='{session}'")
        expect_status(404, lambda: first.call("DELETE", f"/v1/sessions/{session}"))
        assert pod_exists(context, namespace, worker)
        sql(context, namespace,
            f"UPDATE gateway_sessions SET lease_until=now()-interval '1 second' WHERE session_id='{session}'")
        current_second = wait_gateways(context, namespace)[1]
        kubectl(context, namespace, ["delete", "pod", current_second["metadata"]["name"], "--wait=true"])
        wait_absent(context, namespace, worker)
        second.stop()
        second = Gateway(context, namespace, wait_gateways(context, namespace)[1], token)
        gateways.append(second)
        print("PASS: a stale epoch was fenced and only an expired lease permitted orphan cleanup")

        first.stop()
        first = Gateway(context, namespace, wait_gateways(context, namespace)[0], token)
        gateways.append(first)
        kubectl(context, namespace, ["scale", "statefulset/duckfleet-postgres", "--replicas=0"])
        time.sleep(2)
        expect_status(503, first.open)
        kubectl(context, namespace, ["scale", "statefulset/duckfleet-postgres", "--replicas=1"])
        kubectl(context, namespace, ["rollout", "status", "statefulset/duckfleet-postgres", "--timeout=240s"])
        recovered, recovered_worker = first.open()
        first.query(recovered, "SELECT 1")
        first.call("DELETE", f"/v1/sessions/{recovered}")
        wait_absent(context, namespace, recovered_worker)
        print("PASS: a database outage refused new ownership and recovered without leaked compute")

        owned, owned_worker = first.open()
        kubectl(context, namespace, ["delete", "pod", first.name, "--wait=true"])
        wait_absent(context, namespace, owned_worker)
        expect_status((404, 410, 503), lambda: second.query(owned, "SELECT 1"))
        print("PASS: losing the owning gateway lost the worker rather than silently recreating a session")

        replacement = wait_gateways(context, namespace)[0]
        owner = Gateway(context, namespace, replacement, token)
        gateways.append(owner)
        time.sleep(3)
        commit_session, commit_worker = owner.open()
        owner.query(commit_session, "CREATE SCHEMA IF NOT EXISTS api_demo")
        owner.query(commit_session, "CREATE OR REPLACE TABLE api_demo.ha_commit (n BIGINT)")
        request_id = str(uuid.uuid4())
        outcome = {"error": None}

        def write():
            try:
                owner.json("POST", f"/v1/sessions/{commit_session}/queries", {
                    "request_id": request_id,
                    "sql": "INSERT INTO api_demo.ha_commit SELECT count(*) FROM range(40000000000) WHERE range % 7 = 3",
                    "timeout_seconds": 58,
                }, timeout=70)
            except Exception as error:
                outcome["error"] = error

        thread = threading.Thread(target=write)
        thread.start()
        for _ in range(100):
            state = sql(context, namespace,
                        f"SELECT status FROM query_history WHERE query_id='{request_id}'")
            if state == "running":
                break
            time.sleep(0.1)
        else:
            raise AssertionError("the write did not reach running before the gateway kill")
        kubectl(context, namespace, ["delete", "pod", owner.name, "--grace-period=0", "--force", "--wait=false"])
        thread.join(timeout=75)
        assert outcome["error"] is not None, "an interrupted commit unexpectedly succeeded"
        wait_absent(context, namespace, commit_worker)
        for _ in range(120):
            try:
                record = json.loads(call(second.port, token, "GET", f"/v1/queries/{request_id}"))
            except (urllib.error.URLError, ConnectionError):
                time.sleep(1)
                continue
            if record.get("status") not in {"running", "queued", "pending"}:
                break
            time.sleep(1)
        attempts = sql(context, namespace,
                       f"SELECT count(*) FROM query_history WHERE query_id='{request_id}'")
        assert attempts == "1", attempts
        second.stop()
        second = Gateway(context, namespace, wait_gateways(context, namespace)[1], token)
        gateways.append(second)
        probe = second.json("POST", "/v1/sessions", {})
        probe_id = probe["session_id"]
        second.query(probe_id, "SELECT CASE WHEN (SELECT count(*) FROM api_demo.ha_commit)=0 "
                               "THEN 0 ELSE error('interrupted write committed') END")
        second.call("DELETE", f"/v1/sessions/{probe_id}")
        print("PASS: an interrupted write stayed classified and was never replayed")
    finally:
        kubectl(context, namespace, ["scale", "statefulset/duckfleet-postgres", "--replicas=1"], check=False)
        for gateway in gateways:
            gateway.stop()


if __name__ == "__main__":
    main()
