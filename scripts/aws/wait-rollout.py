"""Waits for the release to converge, failing as soon as a pod cannot be scheduled.

`helm --wait` reports only `UPGRADE FAILED: context deadline exceeded` when a pod
never schedules, and only once the whole timeout has elapsed. Five AWS deploys have
failed that way with nothing in the output about the cause. The cause is carried on
the pod's `PodScheduled` condition, which this reads directly and quotes verbatim.

Unschedulable is deliberately not fatal on first sight. EKS Auto Mode provisions
nodes on demand, so a pod is briefly unschedulable whenever a new node is needed. A
pod has to stay unschedulable for the whole grace window before the deploy is
abandoned, which distinguishes "waiting for a node" from "no node can ever fit this".
"""

import argparse
import json
import subprocess
import sys
import time


def unschedulable_reason(pod):
    """The scheduler's own message, or None when the pod is not held unschedulable."""
    for condition in pod.get("status", {}).get("conditions", []):
        if (
            condition.get("type") == "PodScheduled"
            and condition.get("status") == "False"
            and condition.get("reason") == "Unschedulable"
        ):
            return condition.get("message") or "no scheduler message"
    return None


def deployments_pending(deployments):
    """Deployments whose ready replica count has not reached the desired count."""
    pending = []
    for deployment in deployments:
        desired = deployment["spec"].get("replicas", 1)
        ready = deployment.get("status", {}).get("readyReplicas", 0)
        if ready != desired:
            pending.append(f"{deployment['metadata']['name']} {ready}/{desired} ready")
    return pending


def evaluate(deployments, pods, first_seen, now, grace_seconds):
    """Classifies one observation as ready, still waiting, or not worth waiting for.

    `first_seen` maps a pod UID to the moment it was first seen unschedulable and is
    updated in place, so a pod that becomes schedulable stops counting against the
    grace window.
    """
    for pod in pods:
        uid = pod["metadata"]["uid"]
        reason = unschedulable_reason(pod)
        if reason is None:
            first_seen.pop(uid, None)
            continue
        waited = now - first_seen.setdefault(uid, now)
        if waited >= grace_seconds:
            name = pod["metadata"]["name"]
            return "failed", f"{name} stayed unschedulable for {int(waited)}s: {reason}"
    pending = deployments_pending(deployments)
    if pending:
        return "waiting", "; ".join(pending)
    return "ready", "every deployment reports its desired replicas ready"


def observe(kubectl, kind):
    output = subprocess.run(
        [*kubectl, "get", kind, "-o", "json"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    return json.loads(output)["items"]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--namespace", required=True)
    parser.add_argument("--context", required=True)
    parser.add_argument("--timeout-seconds", type=int, default=600)
    # Long enough for EKS Auto Mode to provision a node, short enough that a class
    # which can never be placed is reported in about a minute rather than fifteen.
    parser.add_argument("--grace-seconds", type=int, default=90)
    parser.add_argument("--poll-seconds", type=int, default=5)
    args = parser.parse_args()

    kubectl = ["kubectl", "--context", args.context, "-n", args.namespace]
    first_seen = {}
    deadline = time.monotonic() + args.timeout_seconds
    while True:
        now = time.monotonic()
        state, message = evaluate(
            observe(kubectl, "deployments"),
            observe(kubectl, "pods"),
            first_seen,
            now,
            args.grace_seconds,
        )
        if state == "ready":
            print(f"PASS: {message}")
            return 0
        if state == "failed":
            print(f"Deploy abandoned: {message}", file=sys.stderr)
            return 1
        if now >= deadline:
            print(
                f"Release did not converge within {args.timeout_seconds}s: {message}",
                file=sys.stderr,
            )
            return 1
        print(f"waiting: {message}", flush=True)
        time.sleep(args.poll_seconds)


if __name__ == "__main__":
    sys.exit(main())
