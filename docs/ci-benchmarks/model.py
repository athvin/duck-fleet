#!/usr/bin/env python3
"""Projects the new local-harness critical path from the baseline step history.

Inputs are only measured numbers: per-step means from baseline-steps.csv (six green
runs of the old serial job) and the local build benchmarks in local-build-bench.txt.
Run: python3 docs/ci-benchmarks/model.py
"""
import csv
import pathlib
import statistics

here = pathlib.Path(__file__).parent
steps = {}
for row in csv.DictReader(open(here / "baseline-steps.csv")):
    steps.setdefault(row["step"], []).append(int(row["seconds"]))
mean = {k: statistics.mean(v) for k, v in steps.items()}

# Measured inside the baseline test-local log (run 46080801201): 405 s of the
# 1300 s step was the --scale stage, 133 s was image build plus host ops compile.
scale_share = 405 / 1300
build_share = 133 / 1300

# Fixed per-runner overheads (seconds): checkout + toolchain (baseline logs),
# artifact download (assumed ~550 MB at ~50 MB/s), archive load (measured V1),
# stack start (baseline log: 'up -d --wait' to first PASS).
setup, download, load, up = 15, 11, 14.3, 60
# Image build job: cold V1 compile of the worker/dbt/api/s3 images run
# concurrently by compose (sum of the serial V1 timings is an upper bound),
# archive save (measured), upload (assumed ~550 MB at ~50 MB/s).
images = setup + (23.6 + 64.8 + 21.7 + 30.7) + 10.4 + 11
# The image build now also compiles the dependencies the check stage used to
# compile first; charge the V1 dependency compile once.
images += 66 * 280.6 / 359.7

def shard(targets, own_stack=False):
    body = sum(mean.get(t, 0) for t in targets)
    return images + setup + download + load + (0 if own_stack else up) + body

core = mean["test-local"] * (1 - scale_share - build_share)
jobs = {
    "compose/core": images + setup + download + load + core,
    "compose/scale": images + setup + download + load + up + mean["test-local"] * scale_share,
    "compose/cli": shard(["test-cli", "test-shell", "test-catalog", "test-history", "test-local-io", "test-pgwire-clients"]),
    "compose/data": shard(["test-import", "test-export", "test-snapshots", "test-advise"]),
    "compose/clients": shard(["test-advise-dbt", "test-dbt-packages", "test-adbc", "test-sdk"]),
    "rust-checks": setup + mean["rust-checks"] * 280.6 / 359.7 + mean["test-dev"],
}
# kind: baseline job minus the in-cluster scale stage (388 s), minus overlap of
# cluster boot with image builds (57 s), minus the V1 build and load savings.
k8s_total = statistics.mean([2207, 2158, 2161, 2464, 2161, 2132])
build_saving = (38 + 91.5 + 43.6) - (23.6 + 64.8 + 30.7)
load_saving = 95 * (1 - 548550552 / 838789963)
jobs["kubernetes/main"] = k8s_total - 388 - 57 - build_saving - load_saving
jobs["kubernetes/scale"] = k8s_total - (2142 - 655) + 388 - 57 - build_saving - load_saving

# job-total rows hold both jobs; the harness rows are the ones above the kind lane.
harness = [v for v in steps["job-total"] if v > 2600]
print(f"baseline wall clock (harness job, n={len(harness)}): {statistics.mean(harness)/60:.1f} min")
for name, seconds in sorted(jobs.items(), key=lambda kv: -kv[1]):
    print(f"  {name:18} {seconds/60:5.1f} min")
critical = max(jobs.values())
print(f"projected wall clock: {critical/60:.1f} min ({max(jobs, key=jobs.get)})")
