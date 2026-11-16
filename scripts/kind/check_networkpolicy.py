"""Proves the release's NetworkPolicy is enforced, not merely applied.

A connection failing proves nothing on its own — it could fail because the address is
wrong, the port is closed, or the image lacks a client. So every denial here is checked
twice: once with the policies in place, and once with them removed. Only a connection
that fails *and then succeeds once the policy is gone* establishes that the policy is
what denied it. That asymmetry is the whole point, because the gap this closes (#30)
was a policy that looked right and did nothing.

The probe pair is the boundary that matters most: a worker runs user SQL and must not
reach the Kubernetes API, while the gateway must, because that is how it creates
isolated session pods. Same destination, opposite outcomes, decided only by the pod's
labels — which is also what shows enforcement is per-pod rather than globally lax.
"""

import argparse
import json
import subprocess
import sys
import time

# curl's own vocabulary for "I never got a connection" versus "I connected and then
# disliked the certificate". The API server presents a CA this probe does not trust,
# so reaching it at all looks like a certificate failure, and that is the signal.
BLOCKED = {7, 28}  # could not connect, timed out
CONNECTED = {0, 35, 60}  # ok, TLS handshake, certificate not trusted


def kubectl(*arguments, context, namespace, check=True, timeout=300):
    return subprocess.run(
        ["kubectl", "--context", context, "--namespace", namespace, *arguments],
        capture_output=True, text=True, check=check, timeout=timeout,
    )


def probe(name, label, address, *, image, context, namespace):
    """Runs one connection attempt from a pod carrying `label` and returns curl's code.

    A pod rather than a `kubectl exec` into an existing one, because the label is the
    independent variable: the policies select on it, so the probe has to be able to
    present either one.
    """
    kubectl("delete", "pod", name, "--ignore-not-found", "--wait=true",
            context=context, namespace=namespace)
    manifest = {
        "apiVersion": "v1", "kind": "Pod",
        "metadata": {"name": name, "labels": dict([label.split("=")])},
        "spec": {
            "restartPolicy": "Never",
            # No token: this is a network probe, and mounting one would let a
            # misconfiguration look like success for the wrong reason.
            "automountServiceAccountToken": False,
            "containers": [{
                "name": "probe",
                "image": image,
                "imagePullPolicy": "Never",
                "command": ["sh", "-c",
                            f"curl -sS --max-time 5 -o /dev/null {address}; echo exit=$?"],
                "resources": {"requests": {"cpu": "10m", "memory": "32Mi"},
                              "limits": {"cpu": "100m", "memory": "64Mi"}},
            }],
        },
    }
    subprocess.run(["kubectl", "--context", context, "--namespace", namespace,
                    "apply", "-f", "-"], input=json.dumps(manifest), text=True,
                   capture_output=True, check=True)
    for _ in range(60):
        phase = kubectl("get", "pod", name, "-o", "jsonpath={.status.phase}",
                        context=context, namespace=namespace, check=False).stdout
        if phase in ("Succeeded", "Failed"):
            break
        time.sleep(2)
    else:
        raise SystemExit(f"probe {name} never finished")
    logs = kubectl("logs", name, context=context, namespace=namespace).stdout
    kubectl("delete", "pod", name, "--ignore-not-found", "--wait=false",
            context=context, namespace=namespace)
    for line in logs.splitlines():
        if line.startswith("exit="):
            return int(line.removeprefix("exit=")), logs.strip()
    raise SystemExit(f"probe {name} produced no exit code: {logs!r}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--context", required=True)
    parser.add_argument("--namespace", required=True)
    parser.add_argument("--image", required=True, help="an image with curl; the worker image has it")
    parser.add_argument("--release", required=True, help="the rendered release, to restore policies from")
    parser.add_argument("--evidence", required=True)
    arguments = parser.parse_args()
    common = {"image": arguments.image, "context": arguments.context,
              "namespace": arguments.namespace}

    # The endpoint, not the Service address: a pod's egress is matched against where
    # the traffic actually goes, and probing the ClusterIP would test a path the
    # policy was never written about.
    address = kubectl("get", "endpoints", "kubernetes", "-o",
                      "jsonpath={.subsets[0].addresses[0].ip}",
                      context=arguments.context, namespace="default").stdout.strip()
    port = kubectl("get", "endpoints", "kubernetes", "-o",
                   "jsonpath={.subsets[0].ports[0].port}",
                   context=arguments.context, namespace="default").stdout.strip()
    target = f"https://{address}:{port}/version"
    evidence = {"api_server": target, "probes": []}

    def record(what, code, logs, expected):
        verdict = "blocked" if code in BLOCKED else (
            "connected" if code in CONNECTED else "inconclusive")
        evidence["probes"].append({"probe": what, "curl_exit": code, "verdict": verdict,
                                   "expected": expected})
        if verdict == "inconclusive":
            raise SystemExit(f"{what}: curl exited {code}, which says neither; {logs}")
        if verdict != expected:
            raise SystemExit(f"{what}: expected {expected}, got {verdict} (curl {code})")
        return verdict

    # 1. The gateway may reach the API server; that is how isolated compute creates
    #    session pods. Checked first, because if this is blocked the policy is wrong
    #    rather than merely strict, and the denial below would be meaningless.
    code, logs = probe("netpol-gateway", "app=duckfleet-gateway", target, **common)
    record("gateway to the API server", code, logs, "connected")

    # 2. A worker may not. It runs user SQL, and the worker policy permits 443 only
    #    outside the private ranges, so the API server's address is excluded.
    code, logs = probe("netpol-worker", "app=duckfleet-worker", target, **common)
    record("worker to the API server", code, logs, "blocked")

    # 3. The same attempt with the policies removed. Without this the step above
    #    proves only that a connection failed, which is the mistake that let #30
    #    through: a policy that denied nothing still produced a passing suite.
    policies = kubectl("get", "networkpolicy", "-o", "name",
                       context=arguments.context, namespace=arguments.namespace).stdout.split()
    evidence["policies_removed"] = policies
    kubectl("delete", *policies, context=arguments.context, namespace=arguments.namespace)
    try:
        code, logs = probe("netpol-worker-unpoliced", "app=duckfleet-worker", target, **common)
        record("worker to the API server, policies removed", code, logs, "connected")
    finally:
        # Restored from the rendered release rather than re-created by hand, so what
        # comes back is what the chart ships.
        subprocess.run(["kubectl", "--context", arguments.context, "--namespace",
                        arguments.namespace, "apply", "-f", arguments.release],
                       capture_output=True, text=True, check=True)

    # 4. And denied again, so the restore is real and the cluster is left as found.
    code, logs = probe("netpol-worker-again", "app=duckfleet-worker", target, **common)
    record("worker to the API server, policies restored", code, logs, "blocked")

    with open(arguments.evidence, "w") as handle:
        json.dump(evidence, handle, indent=2)
        handle.write("\n")
    print("PASS: NetworkPolicy is enforced — the gateway reaches the API server, a "
          "worker cannot, and removing the policy is what lets it through")


if __name__ == "__main__":
    sys.exit(main())
