# DuckDB 2.0.1 release and cutover

## Product decision

**Go for supported development/self-hosted deployments after the linked evidence passes.** This is not a claim that a production cutover occurred. Keep the DuckDB 1.5.4 rollback path available until the operator explicitly ends the rollback window.

The delivered engine is DuckDB `v2.0.1` with `aws` `9ba41b09a0`, `ducklake` `08089b16dc`, `httpfs` `f71cfdb4bf`, `icu` `v2.0.1`, and `postgres_scanner` `8b9d0bbe8d`. Both Linux architectures use the same reviewed manifest. Worker startup checks the native engine, loaded extension versions, forbidden custom repositories, and DuckLake catalog `1.0` before reporting ready. Persistent and isolated workers use the same immutable image digest; a mismatch prevents readiness rather than falling back to another engine.

The dbt binary is the Rust `duckfleet-v0.1.4` release, which includes per-node transaction rollback. Its [adapter fix](https://github.com/athvin/dbt/pull/1) prevents a failed model from committing its pre-hook.

## Evidence and sign-off

- [Engine port and isolated qualification](https://github.com/athvin/duck-fleet/pull/138) established the 1.5.4/2.0.1 lanes.
- [Extension security and live AWS acceptance](https://github.com/athvin/duck-fleet/actions/runs/45062582712) passed deployment, live tests, evidence verification, and suspension on both ordinary and explicit-upgrade paths.
- [SQL, transaction, Arrow, and profiling qualification](https://github.com/athvin/duck-fleet/actions/runs/45073794020) passed all stages, including dbt and the 110-model/40-test scale project: 10 unchanged, 7 accepted, 3 fixed, 0 blocking.
- [Restore and rollback qualification](https://github.com/athvin/duck-fleet/actions/runs/45082138242) passed all stages and proved a 1.5.4 read/write rollback after a 2.0.1 write.
- The delivered cutover run must pass the same immutable candidate through PR Compose and Kind checks and the live AWS lane. Link its run and downloaded `summary.json`, `recovery.json`, and `cutover.json` in the release record before closing the rollout ticket.

Accepted differences are documented in `engine-upgrades.md`: native PEG parsing, disabled single-arrow lambdas, partial regex matching, factorial precedence, typed binary literals, division/modulo-by-zero errors, and the identifier-case setting. Multi-match writes retain native 2.0.1 single-target behavior. Untyped NULL remains null; VARIANT has an explicit unsupported-conversion error; pgwire binary/nested results require an explicit VARCHAR cast. No legacy SQL compatibility bridge is enabled globally.

Timings are recorded as evidence rather than treated as hardware-independent thresholds. The final 1.5.4/2.0.1 comparison showed lower candidate worker memory after the same stages (48.52 MiB versus 74.48 MiB), with gateway memory 14.89 MiB versus 12.4 MiB and all concurrency/spill/artifact checks passing. Product sign-off accepts this evidence; there is no known regression requiring a hold. Re-evaluate if a delivered workload shows a material latency, memory, spill, or concurrency regression.

## Cutover runbook

1. Confirm the approved image digest and extension manifest. Take a named backup of both `lake` and `platform`, retain the Parquet objects it references, and rehearse on a copy when possible.
2. Pause SQL admission by stopping the gateway first. Existing gateway/worker termination handling refuses new sessions while admitted statements finish within the documented grace. A statement that cannot finish receives a truthful timeout/interruption with its durable identity and `retry_safe: false`; it is not automatically replayed. Do not create replacement workers while old writers are draining.
3. Stop workers and isolated pods with `scripts/stop-compute.sh compose` or the explicit-context `kube` form. PostgreSQL and migration Jobs remain. Queued, uncertain, or in-flight writes are never replayed automatically; inspect their durable identities and restore instead when the outcome is uncertain.
4. Start the approved 2.0.1 images and run the platform migration before compute. The startup guard verifies the engine, extensions, repository policy, and catalog version. While compute is stopped, SQL is honestly unavailable; supported control-plane metadata remains available without pretending queries ran.
5. Verify every serving worker reports `v2.0.1`, no 1.5.4 writer remains, and the catalog is still 1.0. Run HTTP, pgwire, dbt, durable-job/artifact, maintenance, RBAC, history, budget, and concurrency checks before resuming normal admission.
6. Record the delivered Compose, Kind, and AWS evidence and the product decision. `scripts/qualify-engine.py` includes the backup/drain/replacement/smoke rehearsal and writes `cutover.json`; it never performs this drill against the only copy of a catalog.

For Compose, the basic commands are:

```sh
scripts/backup-catalog.sh compose artifacts/recovery-point
scripts/stop-compute.sh compose
DUCKFLEET_IMAGE_TAG=<approved-immutable-tag> docker compose up -d --wait server
```

For Kubernetes, set `DUCKFLEET_KUBE_CONTEXT` and `DUCKFLEET_NAMESPACE` explicitly and use the `kube` forms of the backup/stop scripts. Deploy the same immutable worker digest through Helm; do not mix engine writers during the initial cutover.

## Rollback

Keep DuckLake at `1.0`; never attach with `AUTOMATIC_MIGRATION`. A migrated 1.1 catalog is outside this rollback path. Stop every 2.0.1 writer before starting 1.5.4. If the approved recovery point requires restoration, use `DUCKFLEET_CONFIRM_RESTORE=yes scripts/restore-catalog.sh <compose|kube> <directory>`; schema replacement, migrations, and the stopped-compute guarantee are part of that script. Do not replay uncertain writes.

Start only the approved 1.5.4 image set, then verify reads, a new write, and platform history. Missing retained Parquet objects, a migrated catalog, or an incomplete restore/rollback drill is a no-go. See `engine-upgrades.md` for dump-gap, object-retention, and recovery-environment requirements.
