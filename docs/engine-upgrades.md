# Engine qualification

Use the shadow qualification lane before changing DuckFleet's DuckDB engine. It builds immutable worker, dbt, and API image sets from two committed Git refs, starts isolated Compose projects, and compares their behavior. It does not use or attach to an existing deployment.

```bash
python3 scripts/qualify-engine.py \
  --baseline-ref d30084c \
  --candidate-ref HEAD \
  --output artifacts/engine-upgrade/duckdb-2.0.1
```

The command requires Docker and Python 3. It resets only the `duckfleet-qualification-baseline` and `duckfleet-qualification-candidate` Compose projects, including their volumes. Use `--keep` to inspect those projects after the run; otherwise it removes them. Use `--skip-build` with `--baseline-tag` and `--candidate-tag` to compare existing image sets without building. On an ARM host without a compatible dbt binary, `--skip-dbt` permits a local tooling diagnostic and records `release_complete: false`; release qualification must run the default full suite on the x86_64 CI lane.

Each lane runs the API-only workflow, dbt lifecycle, concurrency, durable jobs and downloadable artifacts, the engine query corpus, observability checks, and pgwire. The candidate also proves its worker forbids custom extension repositories and retains the reviewed PostgreSQL pushdown policy. Each lane seeds its own disposable write fixtures. Statements are never replayed from one engine into the other.

The corpus at `server/src/harness/engine_qualification.rs` records SQL text, columns, Arrow types, rows, error classification, profile status, engine placement, the five-extension manifest, and per-query latency. Both lanes use the candidate's dbt-free API image as the corpus client, so a historical baseline does not need to contain the new harness; the worker and gateway under test remain on the lane's own image. The orchestrator also records image IDs, resource limits, container resource usage, service logs, and suite logs.

The lanes have separate Compose networks and PostgreSQL/object-storage volumes. Their data paths are respectively `s3://duckfleet/engine-qualification/baseline/lake/` and `s3://duckfleet/engine-qualification/candidate/lake/`. The isolation check requires disjoint networks and volumes, distinct data paths, and failure to resolve baseline database, storage, and gateway container names from the candidate worker.

`make check` also runs the complete extension bundle with networking disabled, checks ICU calendar/timezone/collation behavior and extension versions, exercises Pod Identity credentials, and proves that a bucket-scoped S3 credential is never sent to a prefix-neighbor bucket.

## Difference review

`examples/engine-qualification-2.0.json` lists intentional differences. `accepted` means an intentional behavior change; `fixed` means a change that corrects prior behavior. Any other row, schema, error, or profile-status difference is blocking. Query timing is reported as evidence, not used as an implicit pass/fail threshold.

The final `summary.json` passes only when both lanes complete all stages, report their expected engine versions, pass isolation, and have no blocking differences. `comparison.json` contains the per-query verdict and relative timings. The GitHub Actions engine-qualification workflow uploads the entire output directory as run evidence.

## DuckDB 2.0.1 contracts

DuckFleet uses native 2.0.1 parser and expression behavior; it does not enable the legacy parser, single-arrow lambdas, full-match regex operators, or old division behavior globally. In particular, single-arrow lambdas fail, `~`/`!~` match substrings, factorial binds before multiplication, `X`/`B` literals produce binary types, and integer division/modulo by zero fail. Multi-match `UPDATE ... FROM` and `MERGE` retain one target row and native 2.0.1 source-selection behavior. Parser/binder failures abort an explicit transaction, and rollback must discard its earlier writes; the dbt failing-model fixture checks the same contract for its pre-hook.

Arrow IPC preserves nulls, decimals, timestamps, nested values, blobs/bitstrings, UUIDs, empty schemas, and chunked artifacts. An untyped DuckDB `NULL` is an Arrow `Null` value and becomes JSON null, never an empty string. `VARIANT` results currently fail explicitly because Arrow variant decoding is unsupported; DuckFleet does not silently coerce them. SQL completion remains distinct from result conversion, storage, and delivery failures.

DuckDB 2.0.1 places profile metrics under `query`, `system`, and `io`. The Rust worker exports only the existing numeric metric names, whether found at the profile root or in those sections. It does not invent values for omitted metrics such as `rows_returned` or latency. The existing pre-execution file deletion proves profile freshness.
