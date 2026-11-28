# CI timing: the Docker dbt harness

Every pull request and every push to main runs `local-harness.yml`, and on main the
AWS deploy lane waits for it. Before this change, its wall clock was one serial job.

## Result

| | wall clock |
|---|---|
| Before: six green runs, Nov 27–28 (`baseline-steps.csv`) | **63.4 min** mean (49.2–75.8) |
| Projection from the baseline data (`model.py`) | 28.0 min |
| After: run [46098600019](https://github.com/athvin/duck-fleet/actions/runs/46098600019) | **27.0 min** |
| After: run [46100942491](https://github.com/athvin/duck-fleet/actions/runs/46100942491), all green | **27.0 min** |
| Markdown-only change (outside `server/`) | ~10 s (one `changes` job; everything else skipped) |

Run 46098600019 had one failure, the PTY shell test, which was order-dependent (see
below); it is fixed in the same branch and 46100942491 is the run with the fix.

## Where the time went before

Six green runs of the old `harness` job, per-step mean (from the Actions API):

| step | mean | |
|---|---|---|
| `make test` (`test-local.sh`) | 27.7 min | of which `--scale` 6.8 min, image build + host ops compile 2.2 min |
| `make check` (fmt, clippy, workspace tests in Docker) | 5.3 min | cold compile every run |
| 15 further suites, one after another | 30.4 min | each 0.9–5.3 min |
| **job total** | **63.4 min** | |

The `kubernetes` job (36.9 min mean) already ran beside it, so it was never the
bottleneck. Its own breakdown (run 46080801201): 1 min cluster + Calico, then 4 min
of serial image builds, 95 s `kind load`, 29 min of tests (6.5 min of them the
in-cluster scale build).

## What changed, and what each part bought

| change | evidence |
|---|---|
| Compose images built once (`images` job, 3.2 min) and loaded by five suite runners | every suite script creates and drops its own fixtures; archive load 11–14 s per runner |
| `test-local.sh` scale stage moved to its own runner (`test-scale.sh`) | `make test` 27.7 → 12.3–12.6 min; scale alone 6.1–6.7 min, in parallel |
| Kind lane split into `main` and `scale` clusters (`DUCKFLEET_K8S_PART`) | main 26.6–26.7 min (was 36.9); scale cluster 11.1–15.3 min, in parallel |
| Kind image builds and the host `duckfleet-ops` build run while the cluster and Calico boot | cluster setup to first deploy 385 → ~220 s |
| `fmt`/`clippy`/tests and `duckfleet dev` on their own runner | `rust-checks` 8.1–9.7 min, off the critical path |
| Dev profile keeps line tables only (see `server/Cargo.toml`) | table below |
| Markdown-only changes skip the harness (`scripts/ci/changed-code.sh`) | PR #205 (docs only) paid a full 77 min run |
| Dependency caches for the host-side cargo builds, `sdk.yml`, `infra.yml` | keyed by `Cargo.lock`; workspace crates still rebuild, so no stale-binary risk |

### Dev-profile debuginfo, measured locally

Same 4 vCPU / 15 GB machine class as a GitHub `ubuntu-24.04` runner (local cold
`check` 360 s vs 258–350 s on Actions), empty BuildKit cache for each cold run.
Raw numbers: `local-build-bench.txt` (B0 = before, V1 = line tables only).

| | before | line tables only | |
|---|---|---|---|
| cold `check` stage (fmt, clippy, tests) | 360 s | 281 s | −22% |
| cold CI compile set (check + 4 images) | 570 s | 425 s | −25% |
| warm rebuild after a one-line `server/src` change (`check`) | 76 s | 55 s | −28% |
| `duckfleet-server` binary | 567 MB | — | |
| worker image | 1.25 GB | 0.73 GB | −41% |
| dbt image | 1.46 GB | 0.94 GB | −35% |
| 4-image zstd archive | 839 MB | 549 MB | −35% |
| archive load (`zstd -dc \| docker load`) | 21.8 s | 14.3 s | −34% |
| `kind load` on Actions | 95 s | 24 s | |

Workspace crates keep line tables, so panics and backtraces still name file and
line. `CARGO_PROFILE_DEV_DEBUG=true` restores full debuginfo for a debugging session.
This also shrinks the images the AWS lane deploys, which are built from the same
Dockerfiles in the dev profile.

## Considered and not done

- **Persisting the Docker cargo cache between runs** (restoring BuildKit cache mounts
  from `actions/cache`). A warm rebuild is fast (`check` 76 s vs 360 s cold), but the
  target mount is 3.9 GB even with line tables only: restoring and re-injecting it
  costs about what it saves, and it would take 40% of the repository's 10 GB cache.
  The image build is no longer on the critical path in any case.
- **Pinning source mtimes so restored host `target/` stays fresh.** Faster, but a
  cache restored from a different commit can then look newer than changed sources
  and skip a rebuild. Correctness wins; only dependencies are cached.
- **Raising dbt threads in the scale project.** It would likely shorten the scale
  stage, but it changes what that test exercises; the stage now runs in parallel.

## Cost

Fewer runner-minutes, not more: about 100 per run before (63.4 + 36.9 mean), 98.0
and 93.2 in the two runs after (per-job durations in `after-runs.csv`). Each extra
runner pays checkout, image load and stack start (about 1.5 min), but no runner
compiles what another already compiled, the scale build no longer rides on a
37-minute job, and the smaller images are cheaper to build, move and load.

## Order dependence the split exposed

`server/cli/tests/shell_pty.rs` ran `\dt` and expected a header, which only appears
when the lake has tables. In the serial job, `test-local.sh` always ran first and
left tables behind; on a fresh stack the assertion timed out. The test now creates
and drops its own table. Every other suite passed on a fresh stack unchanged.

## Repeating the measurement

- Every harness runner writes `artifacts/ci-timings.csv` (`shard,target,seconds,status`)
  into its evidence artifact.
- `python3 docs/ci-benchmarks/model.py` recomputes the projection from the baseline.
- `local-harness.yml` has `workflow_dispatch`, so a branch can be timed without a PR.
