"""Compare every persistent worker's declared allocation with its pod and cgroup.

The Kind transform lowers CPU requests; the worker must still observe its full CPU
limit. Memory and disk are independent dimensions, including for named pools.
"""
import argparse
import json
from pathlib import Path
import subprocess
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from worker_resources import assert_worker_resources, cores, quantity_bytes  # noqa: E402


def kubectl(args, context, namespace):
    return subprocess.run(
        ["kubectl", "--context", context, "-n", namespace, *args],
        check=True, capture_output=True, text=True,
    ).stdout.strip()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--context", required=True)
    parser.add_argument("--namespace", required=True)
    args = parser.parse_args()
    failures = []
    listed = json.loads(kubectl(["get", "pods", "-l", "app=duckfleet-worker", "-o", "json"],
                               args.context, args.namespace))["items"]
    workers = [pod for pod in listed if "duckfleet-compute" not in pod["metadata"].get("labels", {})
               and not pod["metadata"].get("deletionTimestamp")]
    if not workers:
        failures.append("no persistent workers were found")
    for pod in workers:
        name = pod["metadata"]["name"]
        container = next(item for item in pod["spec"]["containers"] if item["name"] == "worker")
        env = {entry["name"]: entry.get("value") for entry in container["env"]}
        spec = container["resources"]
        cpu = int(env["DUCKFLEET_WORKER_CPU"])
        memory = quantity_bytes(env["DUCKFLEET_WORKER_MEMORY"])
        try:
            assert_worker_resources(spec, cpu, name, memory=env["DUCKFLEET_WORKER_MEMORY"],
                                    disk=env["DUCKFLEET_WORKER_DISK"])
        except AssertionError as error:
            failures.append(str(error))
        observed = kubectl(
            ["exec", name, "-c", "worker", "--", "sh", "-c",
             "cat /sys/fs/cgroup/cpu.max /sys/fs/cgroup/memory.max"],
            args.context, args.namespace,
        ).split()
        quota, period, memory_max = map(int, observed)
        request, limit = cores(spec["requests"]["cpu"]), cores(spec["limits"]["cpu"])
        print(json.dumps({
            "pod": name, "size": pod["metadata"].get("labels", {}).get("size"),
            "pool": env.get("DUCKFLEET_POOL"), "worker_id": env["DUCKFLEET_WORKER_ID"],
            "cpu_request": spec["requests"]["cpu"], "cpu_limit": spec["limits"]["cpu"],
            "memory_limit": spec["limits"]["memory"], "disk": env["DUCKFLEET_WORKER_DISK"],
            "cgroup_cpu_quota": quota, "cgroup_cpu_period": period, "cgroup_memory_max": memory_max,
        }))
        if quota != period * cpu:
            failures.append(f"{name}: cpu.max {quota}/{period} is not {cpu} CPU")
        if memory_max != memory:
            failures.append(f"{name}: memory.max {memory_max} differs from declared {memory} bytes")
        if not 0 < request < limit:
            failures.append(f"{name}: Kind CPU request must be positive and below its limit")
    if failures:
        print("\n".join(failures), file=sys.stderr)
        return 1
    print("PASS: worker cgroups match independent allocations while CPU requests sit lower",
          file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
