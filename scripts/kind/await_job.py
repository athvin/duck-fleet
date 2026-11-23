"""Waits for a Job to finish, reporting failure as soon as it fails.

`kubectl wait` takes a single condition, so waiting for `complete` sits through the
whole timeout when a Job has already failed. This watches both terminal conditions
and surfaces the Job's own logs on failure, which is what the deploy lane needs to
be diagnosable.
"""

import argparse
import json
import subprocess
import sys
import time


def kubectl(args, context, namespace, check=True):
    return subprocess.run(
        ["kubectl", "--context", context, "-n", namespace, *args],
        check=check,
        capture_output=True,
        text=True,
    )


def terminal_state(job):
    """Returns 'complete', 'failed', or None while the Job is still running."""
    for condition in job.get("status", {}).get("conditions", []):
        if condition.get("status") != "True":
            continue
        if condition.get("type") == "Complete":
            return "complete"
        if condition.get("type") == "Failed":
            return f"failed: {condition.get('reason')} {condition.get('message', '')}".strip()
    return None


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--context", required=True)
    parser.add_argument("--namespace", required=True)
    parser.add_argument("--job", required=True)
    parser.add_argument("--timeout", type=int, default=1200)
    parser.add_argument("--poll-seconds", type=int, default=5)
    args = parser.parse_args()

    deadline = time.monotonic() + args.timeout
    previous_isolated_logs = ""
    while True:
        result = kubectl(["get", "job", args.job, "-o", "json"], args.context, args.namespace)
        job = json.loads(result.stdout)
        state = terminal_state(job)
        if state == "complete":
            print(f"{args.job} completed")
            return 0
        if state is not None:
            print(f"{args.job} {state}", file=sys.stderr)
            logs = kubectl(["logs", f"job/{args.job}", "--all-containers", "--tail=200"],
                           args.context, args.namespace, check=False)
            print(logs.stdout or logs.stderr, file=sys.stderr)
            isolated = kubectl(
                ["logs", "-l", "duckfleet-compute=isolated", "--all-containers",
                 "--prefix", "--tail=200"],
                args.context, args.namespace, check=False,
            )
            print(isolated.stdout or isolated.stderr, file=sys.stderr)
            return 1
        if time.monotonic() >= deadline:
            status = job.get("status", {})
            print(f"{args.job} did not finish within {args.timeout}s: {status}", file=sys.stderr)
            return 1
        isolated = kubectl(
            ["logs", "-l", "duckfleet-compute=isolated", "--all-containers",
             "--prefix", "--tail=200"],
            args.context, args.namespace, check=False,
        )
        if isolated.stdout and isolated.stdout != previous_isolated_logs:
            print(isolated.stdout, file=sys.stderr)
            previous_isolated_logs = isolated.stdout
        active = job.get("status", {}).get("active", 0)
        print(f"waiting: {args.job} active={active}", flush=True)
        time.sleep(args.poll_seconds)


if __name__ == "__main__":
    sys.exit(main())
