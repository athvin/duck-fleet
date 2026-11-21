"""Re-points the chart's harness Job at a chosen command, under a chosen name.

The chart's Job runs the AWS harness, which asserts the S3 prefix is the real
development bucket. That is right on EKS and meaningless locally, so the local run
invokes the shared harness directly instead.

Reusing the Job's own spec — volumes, env, token, security context, the init
container that copies the dbt project — rather than writing a second one keeps the
local run on exactly the pod shape the release defines. Running it more than once
under different names is what makes pod-replacement assertions expressible.
"""

import argparse
import shlex
import sys

import yaml

HOOKS = ["helm.sh/hook", "helm.sh/hook-delete-policy", "helm.sh/hook-weight"]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--name", required=True)
    parser.add_argument("--command", required=True, help="command line for the dbt container")
    parser.add_argument(
        "--artifacts-claim",
        help="persist /artifacts on this claim instead of an emptyDir, so a later "
        "run can read what an earlier one wrote",
    )
    parser.add_argument(
        "--env",
        action="append",
        default=[],
        metavar="NAME=VALUE",
        help="add or replace a plain environment variable on the dbt container",
    )
    args = parser.parse_args()

    documents = [doc for doc in yaml.safe_load_all(sys.stdin.read()) if doc is not None]
    jobs = [doc for doc in documents if doc.get("kind") == "Job"]
    if len(jobs) != 1:
        raise SystemExit(f"expected exactly one Job to re-point, found {len(jobs)}")
    job = jobs[0]

    job["metadata"]["name"] = args.name
    # Applied with kubectl, so Helm's hook annotations would only mislead a reader.
    annotations = job["metadata"].get("annotations", {})
    for hook in HOOKS:
        annotations.pop(hook, None)
    if annotations:
        job["metadata"]["annotations"] = annotations
    else:
        job["metadata"].pop("annotations", None)

    pod = job["spec"]["template"]["spec"]
    dbt = next((c for c in pod["containers"] if c["name"] == "dbt"), None)
    if dbt is None:
        raise SystemExit("the harness Job no longer has a container named 'dbt'")
    dbt["command"] = shlex.split(args.command)

    for item in args.env:
        name, sep, value = item.partition("=")
        if not sep or not name:
            raise SystemExit(f"--env expects NAME=VALUE, got {item!r}")
        env = dbt.setdefault("env", [])
        entry = next((e for e in env if e.get("name") == name), None)
        if entry is None:
            env.append({"name": name, "value": value})
        else:
            entry.pop("valueFrom", None)
            entry["value"] = value

    if args.artifacts_claim:
        # The restart assertions read a query id an earlier run wrote. An emptyDir
        # is scoped to one pod, so a second Job would start with it empty.
        volume = next((v for v in pod.get("volumes", []) if v["name"] == "artifacts"), None)
        if volume is None or "emptyDir" not in volume:
            raise SystemExit("expected an 'artifacts' emptyDir volume to replace")
        volume.pop("emptyDir")
        volume["persistentVolumeClaim"] = {"claimName": args.artifacts_claim}

    yaml.safe_dump_all(documents, sys.stdout, default_flow_style=False)
    print(f"harness job {args.name}: {dbt['command']}", file=sys.stderr)


if __name__ == "__main__":
    main()
