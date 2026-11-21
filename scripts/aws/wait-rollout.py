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
    """Require the observed rollout, not ready replicas left from an old revision."""
    pending = []
    for deployment in deployments:
        desired = deployment["spec"].get("replicas", 1)
        status = deployment.get("status", {})
        ready = status.get("readyReplicas", 0)
        name = deployment["metadata"]["name"]
        if status.get("observedGeneration", 0) < deployment["metadata"].get("generation", 0):
            pending.append(f"{name} waiting for its current generation")
        elif ready != desired:
            pending.append(f"{name} {ready}/{desired} ready")
        elif status.get("updatedReplicas", 0) != desired or status.get("replicas", 0) != desired:
            pending.append(f"{name} waiting for its current replicas")
    return pending


def required_migrations(deployments):
    return {d["metadata"].get("annotations", {}).get("duckfleet.io/migration-job")
            for d in deployments if d["spec"].get("replicas", 1) > 0} - {None}


def migration_state(deployments, jobs):
    by_name = {job["metadata"]["name"]: job for job in jobs}
    pending = []
    for name in sorted(required_migrations(deployments)):
        job = by_name.get(name)
        if job is None:
            pending.append(f"migration {name} has not been created")
            continue
        conditions = {c.get("type"): c for c in job.get("status", {}).get("conditions", [])
                      if c.get("status") == "True"}
        if "Failed" in conditions:
            failure = conditions["Failed"]
            return "failed", (f"migration {name} failed: {failure.get('reason', '')} "
                              f"{failure.get('message', '')}").strip()
        if "Complete" not in conditions:
            pending.append(f"migration {name} has not completed")
    return ("waiting", "; ".join(pending)) if pending else ("ready", "migrations complete")


def completed_migrations_to_prune(deployments, jobs):
    """Retire old successful runs only once the desired schema run completed."""
    if not required_migrations(deployments) or migration_state(deployments, jobs)[0] != "ready":
        return []
    # Protect references on scaled-down Deployments as well: a suspended worker's
    # desired template is still evidence, even while another Deployment is live.
    current = {d["metadata"].get("annotations", {}).get("duckfleet.io/migration-job")
               for d in deployments} - {None}
    retired = []
    for job in jobs:
        metadata = job["metadata"]
        if (metadata.get("labels", {}).get("app") != "duckfleet-platform-migrate"
                or metadata["name"] in current or metadata.get("deletionTimestamp")):
            continue
        status = job.get("status", {})
        conditions = {c.get("type") for c in status.get("conditions", [])
                      if c.get("status") == "True"}
        if "Complete" in conditions and "Failed" not in conditions and not status.get("active", 0):
            retired.append(metadata["name"])
    return sorted(retired)


def prune_migrations(kubectl, deployments, jobs):
    names = completed_migrations_to_prune(deployments, jobs)
    if not names:
        return
    # One bounded, namespace-scoped operation; garbage collection removes the
    # completed Pods too. Never prune failures or the current restore template.
    result = subprocess.run(
        [*kubectl, "delete", "job", *names, "--ignore-not-found", "--wait=false",
         "--cascade=background"],
        capture_output=True, text=True, timeout=30,
    )
    if result.returncode:
        raise RuntimeError(result.stderr.strip() or "could not prune old migration Jobs")
    print(f"Pruned {len(names)} superseded successful migration Job(s)")


def evaluate(deployments, pods, first_seen, now, grace_seconds, jobs=(), statefulsets=()):
    """Classify the current desired migration and workloads, with bounded scheduling."""
    migration, detail = migration_state(deployments, jobs)
    if migration == "failed":
        return migration, detail
    required = required_migrations(deployments)
    for pod in pods:
        if pod.get("status", {}).get("phase") in ("Succeeded", "Failed"):
            continue
        if pod["metadata"].get("labels", {}).get("app") == "duckfleet-platform-migrate":
            owners = {owner["name"] for owner in pod["metadata"].get("ownerReferences", [])
                      if owner.get("kind") == "Job"}
            if not owners & required:
                continue
        uid = pod["metadata"]["uid"]
        reason = unschedulable_reason(pod)
        if reason is None:
            first_seen.pop(uid, None)
            continue
        waited = now - first_seen.setdefault(uid, now)
        if waited >= grace_seconds:
            name = pod["metadata"]["name"]
            return "failed", f"{name} stayed unschedulable for {int(waited)}s: {reason}"
    if migration == "waiting":
        return migration, detail
    pending = deployments_pending(deployments)
    for statefulset in statefulsets:
        desired = statefulset["spec"].get("replicas", 1)
        status = statefulset.get("status", {})
        ready = status.get("readyReplicas", 0)
        if (ready != desired or status.get("observedGeneration", 0)
                < statefulset["metadata"].get("generation", 0)):
            pending.append(f"{statefulset['metadata']['name']} {ready}/{desired} ready")
    if pending:
        return "waiting", "; ".join(pending)
    return "ready", "the current migration and every desired workload are ready"


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
    parser.add_argument("--prune-migrations", action="store_true",
                        help="after readiness, remove old successful migration Jobs; retain current and failed runs")
    args = parser.parse_args()

    kubectl = ["kubectl", "--context", args.context, "-n", args.namespace]
    first_seen = {}
    deadline = time.monotonic() + args.timeout_seconds
    while True:
        now = time.monotonic()
        deployments = observe(kubectl, "deployments")
        jobs = observe(kubectl, "jobs")
        state, message = evaluate(
            deployments,
            observe(kubectl, "pods"),
            first_seen,
            now,
            args.grace_seconds,
            jobs=jobs,
            statefulsets=observe(kubectl, "statefulsets"),
        )
        if state == "ready":
            if args.prune_migrations:
                try:
                    prune_migrations(kubectl, deployments, jobs)
                except (RuntimeError, subprocess.TimeoutExpired) as error:
                    print(f"Release is ready, but migration cleanup failed: {error}", file=sys.stderr)
                    return 1
            print(f"PASS: {message}")
            return 0
        if state == "failed":
            print(f"Deploy abandoned: {message}", file=sys.stderr)
            for name in sorted(required_migrations(deployments)):
                logs = subprocess.run([*kubectl, "logs", f"job/{name}",
                                       "--all-containers", "--tail=100"],
                                      capture_output=True, text=True)
                print(logs.stdout or logs.stderr, file=sys.stderr)
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
