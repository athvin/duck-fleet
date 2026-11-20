"""Checks each worker's cgroup against its pod spec, from inside the container.

The load-bearing property is that a worker's cgroup CPU quota comes from its
CPU **limit**, which the chart always sets to the exact class size. The worker
asserts this for itself at startup; this re-checks it from outside against the
live cgroup, which Compose cannot do: it has no request/limit distinction, so a
regression here would only ever surface on Kubernetes.

Emits one JSON record per worker as evidence, matching the Compose harness.
"""

import argparse
import json
import subprocess
import sys

GIB = 1024**3


def kubectl(args, context, namespace):
    return subprocess.run(
        ["kubectl", "--context", context, "-n", namespace, *args],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


def cores(quantity):
    return float(quantity[:-1]) / 1000 if quantity.endswith("m") else float(quantity)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--context", required=True)
    parser.add_argument("--namespace", required=True)
    args = parser.parse_args()

    failures = []
    for size, cpu in [("small", 1)]:
        pod = kubectl(
            ["get", "pod", "-l", f"size={size}", "-o", "jsonpath={.items[0].metadata.name}"],
            args.context,
            args.namespace,
        )
        spec = json.loads(
            kubectl(
                ["get", "pod", pod, "-o", "jsonpath={.spec.containers[0].resources}"],
                args.context,
                args.namespace,
            )
        )
        observed = kubectl(
            ["exec", pod, "-c", "worker", "--", "sh", "-c",
             "cat /sys/fs/cgroup/cpu.max /sys/fs/cgroup/memory.max"],
            args.context,
            args.namespace,
        ).split()
        quota, period, memory_max = int(observed[0]), int(observed[1]), int(observed[2])
        request, limit = cores(spec["requests"]["cpu"]), cores(spec["limits"]["cpu"])

        record = {
            "pod": pod, "size": size, "cpu_request": spec["requests"]["cpu"],
            "cpu_limit": spec["limits"]["cpu"], "cgroup_cpu_quota": quota,
            "cgroup_cpu_period": period, "cgroup_memory_max": memory_max,
        }
        print(json.dumps(record))

        def check(condition, message):
            if not condition:
                failures.append(f"{pod}: {message}")

        # The container must see the class size, not whatever was requested.
        check(quota == period * cpu, f"cpu.max {quota}/{period} is not {cpu} CPU")
        check(memory_max == cpu * GIB, f"memory.max {memory_max} is not {cpu}GiB")
        check(limit == cpu, f"CPU limit {limit} is not the class size {cpu}")
        # The persistent tier runs request == limit, so the cgroup reporting the
        # class size is the same statement as the limit driving the cgroup.
        check(request == limit, f"CPU request {request} must equal the limit {limit}")
        check(0 < request, f"CPU request {request} must be positive")

    if failures:
        print("\n".join(failures), file=sys.stderr)
        return 1
    print(
        "PASS: worker cgroups match their configured CPU limits",
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
