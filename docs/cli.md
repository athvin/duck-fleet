# Native DuckFleet CLI

The `duckfleet` executable speaks the existing HTTP/Arrow API. It does not embed DuckDB and does not require Cargo, Docker, Python, or dbt to install or run.

## Install

With Homebrew on macOS arm64, Linux x86_64, or Linux aarch64:

```sh
brew install athvin/duckfleet/duckfleet
duckfleet version
```

To install a release archive directly, download the asset for your platform from the [DuckFleet releases page](https://github.com/athvin/duck-fleet/releases), then verify it against `SHA256SUMS` before extracting it:

```sh
curl -fLO https://github.com/athvin/duck-fleet/releases/download/duckfleet-v0.6.0/duckfleet-0.6.0-aarch64-apple-darwin.tar.gz
curl -fLO https://github.com/athvin/duck-fleet/releases/download/duckfleet-v0.6.0/SHA256SUMS
shasum -a 256 -c SHA256SUMS --ignore-missing
tar -xzf duckfleet-0.6.0-aarch64-apple-darwin.tar.gz
install -m 0755 duckfleet-0.6.0-aarch64-apple-darwin/duckfleet /usr/local/bin/duckfleet
duckfleet version
```

Use `sha256sum -c SHA256SUMS --ignore-missing` on Linux. Replace the archive and target directory with the names for your platform and installation prefix.

## Initialize a project

From a DuckFleet checkout with Docker Engine and Compose v2 installed, initialize the directory and start the real local platform:

```sh
duckfleet init
duckfleet dev
duckfleet sql 'select 42'
duckfleet history
duckfleet workers
duckfleet pools
```

`duckfleet dev` creates a local development credential when none exists and stores it only in protected local state.

`init` creates two kinds of configuration:

- `duckfleet.json` contains versioned, non-secret project defaults and is safe to commit. The default URL is `http://127.0.0.1:18080`.
- `.duckfleet/config.json` contains local protected state, including the generated Compose project identity. If `DUCKFLEET_TOKEN` or `--token` is present during `init`, the token is stored here rather than in `duckfleet.json`. The directory and file have user-only permissions on macOS and Linux.

The command adds `.duckfleet/` to `.gitignore` exactly once. Existing configuration is not overwritten unless `init --force` is explicit. Without `--force`, an existing local credential file is preserved. You can keep the token in `DUCKFLEET_TOKEN` instead of storing it locally; pass it to `init` only when persistent project-local credentials are wanted.

Run `duckfleet config show` to inspect the resolved URL, configuration location, and value sources. The token is always shown as `<redacted>` and is never included in command output by `init` or `config show`.

Flags override environment variables, which override project defaults:

1. `--url` and `--token`
2. `DUCKFLEET_URL` and `DUCKFLEET_TOKEN`
3. `duckfleet.json` and `.duckfleet/config.json`

Commands discover `duckfleet.json` in the current directory or its nearest ancestor. Malformed JSON fails before any network request. Existing scripts that set `DUCKFLEET_URL` and `DUCKFLEET_TOKEN` continue to work without project files.

## Local development lifecycle

The lifecycle commands orchestrate the same `compose.yaml` used by `make up`; they do not introduce an embedded execution engine. Run them from the initialized project directory or one of its children. The Compose file is located in the current checkout or its ancestors.

- `duckfleet dev` builds and starts PostgreSQL, RustFS, one persistent worker, and the gateway. It is idempotent and waits for both gateway health and an eligible ready worker. The project URL selects the loopback port, so use a distinct `duckfleet init --url http://127.0.0.1:<port>` for a second local project.
- `duckfleet dev --demo` builds the bundled retail dbt project after the base stack is healthy. Successful completion is recorded in `.duckfleet/demo.json`; repeated invocations do not rerun dbt. Reset clears this marker.
- `duckfleet status` prints the selected project and non-secret endpoint. Exit code 0 means ready, 1 stopped, 2 starting, and 3 degraded. Ready requires the gateway and at least one eligible worker; status never prints credentials.
- `duckfleet stop` stops only the selected Compose project's services and retains catalog, history, and object-store volumes. A later `dev` preserves tables and history.
- `duckfleet reset` lists the selected Compose resources and named volumes before deletion. Interactive use requires typing the generated project name; automation requires `reset --yes`. It uses Compose's project namespace and never removes another project's resources. Connection settings are retained for the next `dev`, while durable volumes and the demo marker are removed.

Startup failures name the relevant service and print a credential-free `docker compose ... logs` command. Existing `make up`, `make test`, and `make down` workflows remain available.

## Query history and DuckDB profiles

`duckfleet history` defaults to a compact table that does not print SQL text. Use `--format json` for the API's query records and cursor:

```sh
duckfleet history --status failed --worker-id small
duckfleet history --failure-kind transaction_conflict
duckfleet history --model-id model.retail_demo.fct_orders
duckfleet history --invocation-id <dbt-invocation>
duckfleet history --model-run-id <run-id>
duckfleet history --limit 50 --before <history-id> --format json
```

All filters map to the existing authorized history API. Cursor pages use `--before` with the returned `next_before` history ID and remain stable under concurrent inserts. `duckfleet history --watch --interval 2` polls at a bounded 1–60 second interval, retains only the current page in memory, and stops cleanly on Ctrl-C. JSON watch mode emits each polled page.

Inspect one query with:

```sh
duckfleet profile <query-id>
duckfleet profile <query-id> --metrics
duckfleet profile <query-id> --raw
```

Default profile output includes queue, preparation/provisioning, engine execution, delivery, total observed, and client-observed totals separately. Missing timestamps and delivery/client measurements remain `unknown`; they are never converted to zero. The operator view lists the most expensive reported operators and a bounded tree with individual timings, rows, and estimated cardinality. Overlapping operator/CPU timings are not summed as wall-clock latency.

`--metrics` omits the operator tree. `--raw` prints only the retained profile JSON returned by the authorized API, not the surrounding SQL-bearing history record. The two flags are mutually exclusive. Missing, sampled-out, expired, disabled, malformed, or unauthorized profiles produce classified errors rather than a false empty plan.

Memory, temporary-directory, spill, and I/O metrics are labeled as DuckDB engine scope. They are not complete container RSS or provider cost measurements. Metadata-only principals can render authorized summary fields, but SQL text, raw errors, profile expressions, and raw profiles are never synthesized or exposed through these commands.

## Lake catalog discovery and previews

Use the ordinary authorized execution session to inspect lake objects:

```sh
duckfleet schemas
duckfleet tables [--schema analytics]
duckfleet views [--schema analytics]
duckfleet describe analytics.events
duckfleet preview analytics.events --limit 20 --format table
```

`schemas` includes empty schemas. `tables` and `views` keep object kinds distinct; filtering by an existing empty schema returns zero rows, while a missing schema is a classified error. `describe` reports relation schema/name plus column ordinal, name, type, and nullability where DuckDB exposes them. Unqualified relation names resolve only when exactly one visible relation matches; ambiguous and missing relations have distinct errors.

Relation arguments are parsed as identifiers, never executable SQL fragments. Unquoted components follow DuckDB's lowercase folding. Double quotes preserve mixed case, spaces, and dots, and a doubled quote represents a literal quote:

```sh
duckfleet describe 'catalog_fixture."dot.name"'
duckfleet describe '"Odd.Schema"."space table"'
duckfleet describe 'catalog_fixture."quote""name"'
```

Discovery values use bound parameters. Preview resolves the exact relation first and quotes each identifier component before executing `SELECT *`. The default limit is 20 and the valid range is 1–1000; malformed, zero, negative, and excessive limits fail before opening a session. Output includes the applied limit and preserves typed values through Arrow. Reader and writer principals receive only behavior allowed by their existing catalog/storage credentials; these commands do not expose a writer DSN or privileged platform metadata.

The shell's `\dt` and `\d <relation>` meta-commands call the same implementation and formatting as `tables` and `describe`.

## Interactive SQL shell

Run `duckfleet shell` to open one remote session for the lifetime of the shell. Temporary tables and explicit transactions remain visible across statements; the prompt changes to `duckfleet(tx)>` after a successful `BEGIN`/`START TRANSACTION` until `COMMIT` or `ROLLBACK`. EOF and `\quit` close the session, rolling back uncommitted work.

Finish SQL with a semicolon. Enter on an incomplete statement opens the `...> ` continuation prompt. Quoted semicolons, line/block comments, dollar-quoted strings, blank input, and multiple statements are handled by the shell's SQL splitter. The editor provides persistent history and SQL/meta-command completion.

Meta-commands:

- `\dt` lists lake tables through the caller's ordinary session and catalog authority.
- `\d <schema.table>` shows column names, ordinals, types, and nullability.
- `\timing` toggles total client latency and the server elapsed time when the retained query record provides it; unknown values are not reported as zero.
- `\format table|csv|json|vertical` selects output without dropping rows; vertical output renders nulls as `NULL`.
- `\help` and `\quit` display help and close the session.

Ctrl-C during execution displays the caller-generated request ID and requests cancellation for that request; it never resubmits SQL. Ctrl-C at the prompt clears only the current input. Worker loss, expired credentials, unknown sessions, and uncertain transport outcomes preserve the server's failure classification and request identity. The shell does not automatically reconnect or replay a statement.

History is stored as `.duckfleet/shell_history` beside the selected project, with user-only directory/file permissions. Credential-shaped input such as bearer tokens, `DUCKFLEET_TOKEN`, `--token`, passwords, and `CREATE SECRET` is not recorded. Use `duckfleet shell --no-history` to disable loading and saving history.

## Import local CSV and Parquet files

Use `duckfleet import <file> --table <schema.table>` with exactly one of `--create` or `--append`:

```sh
duckfleet import ./events.parquet --table raw.events --create
duckfleet import ./more-events.csv --table raw.events --append
duckfleet import ./legacy.data --format csv --delimiter '|' --no-header --table raw.legacy --create
```

The format is inferred from a case-insensitive `.csv` or `.parquet` extension; `--format csv|parquet` overrides an unrecognized extension. CSV uses DuckDB's native `auto_detect` type inference. A header is expected by default; `--no-header` and `--delimiter <one ASCII character>` are explicit CSV options and cannot be combined with Parquet. Create mode uses DuckDB's native Parquet schema and inferred CSV columns. Append mode uses the destination's existing column order and DuckDB's checked insertion coercions; mismatches fail without changing existing rows. Existing tables are never replaced, and overwrite/truncate is deliberately not supported.

The CLI hashes and streams one regular local file, up to 1 GiB, through an authenticated gateway endpoint. Directories, globs, arbitrary URLs, and caller-selected storage keys are not accepted. The gateway writes to its private result/staging prefix with an unpredictable request identity and verifies byte count and SHA-256 before execution. Workers use a result-prefix-scoped S3 secret; no local filesystem path is sent to a worker and no credentials or signed URLs are exposed.

Transfer and execution progress are reported separately. After a verified transfer, create/append runs in one ordinary authorized session as an atomic transaction with optional schema creation. Parse errors, schema mismatches, constraints, cancellation, and commit failures leave no partial destination rows. A commit conflict or interrupted execution is never replayed automatically. Staged bytes are deleted after the attempt; failed and abandoned uploads are reclaimed by the existing fenced maintenance pass after 15 minutes. Import SQL is retained in ordinary query history and usage metering under the caller's principal, with the import request ID in its context.

## Export queries and durable jobs

`duckfleet export` writes one local CSV, JSON, Arrow, or Parquet file from a query or a finished durable job:

```sh
duckfleet export 'select * from raw.events' --output events.parquet
duckfleet export 'select id, name from raw.users' --output users.psv --format csv --delimiter '|'
duckfleet export --job 7b4f2f97-6ea7-4b01-886d-2b26eac08bb9 --output result.json
duckfleet export --job 7b4f2f97-6ea7-4b01-886d-2b26eac08bb9 --output result.json --resume
```

The format comes from the extension (`.csv`, `.json`, `.arrow`/`.arrows`/`.ipc`/`.feather`, `.parquet`), or from an explicit `--format`. SQL exports always run as durable jobs, so a result above the interactive 100,000-row / 16 MiB caps exports without changing those caps. Durable-job limits, result retention, and `result.read` authorization apply to every manifest and chunk request. `--param` and the compute flags (`--cpu`, `--memory`, `--disk`, `--mode`, `--pool`, `--timeout`, `--queue-timeout`) behave as they do for `job submit`.

The CLI prints the job's `request_id` before submitting, and reports SQL completion separately from export completion. Verified Arrow chunks download into a sibling `<output>.duckfleet-partial/` directory. Each chunk is length-, SHA-256-, schema-, and row-checked against the manifest, then converted one chunk at a time, so peak memory is bounded by a chunk plus, for Parquet, one row group. The final file is published only after every chunk verifies. Without `--force` the publish is a no-clobber hard link, and an existing destination is refused before any SQL is submitted. With `--force`, the named file is replaced by a rename only after the complete file exists. If the SQL succeeded but the download or conversion failed, the command exits non-zero and keeps the request ID. Rerun with `--job <id> --resume` to reuse the chunks that already verified. Resume compares the stored manifest with the server's current result generation and refuses any mismatch. The SQL is never resubmitted, and a failed delivery never makes a write safe to retry. Ctrl-C while the job runs requests job cancellation and publishes nothing.

Format behavior:

| Format | Types | Nulls | Empty result |
|---|---|---|---|
| Parquet | Arrow schema preserved, including decimals, timestamps, lists, and structs; Snappy-compressed | native | valid file with schema |
| Arrow | Arrow IPC file (`ARROW1`) with the manifest schema | native | valid file with schema |
| JSON | one JSON array of objects. Nested values stay nested, decimals are JSON numbers, and timestamps are ISO-8601 strings | key omitted | `[]` |
| CSV | header row, `,` unless `--delimiter`, RFC 4180 quoting, ISO-8601 timestamps | empty field | header only |

CSV refuses nested (list/struct/map) and dictionary columns before downloading. Cast them to `VARCHAR` in SQL, or choose another format.

## DuckLake snapshots and time travel

Every DuckLake commit creates a snapshot. The snapshot commands read them through an ordinary authorized session, so a reader principal can use them as far as its catalog credentials allow. They never expire snapshots, delete files, or change current data, and they create no snapshots of their own.

```sh
duckfleet snapshots --limit 20                      # newest first; prints "next page: --before <id>"
duckfleet snapshots --limit 20 --before 1052 --format json
duckfleet snapshot 1053                             # time, schema version, changes, author, message
duckfleet snapshot preview raw.events --at 1052 --limit 50
duckfleet snapshot diff raw.events --from 1051 --to 1054 --limit 5
```

`snapshots` pages by snapshot ID in descending order. Each page has at most `--limit` entries (1–500, default 50), and JSON output carries `next_before` for the next page. `snapshot preview` runs `SELECT * FROM <table> AT (VERSION => <id>) LIMIT <n>` (1–500 rows, default 20) and prints the snapshot and row limit it applied. Reading an old snapshot never changes the table's current contents.

`snapshot diff` reports the net change between the table's state at `--from` and its state at `--to`. It reads DuckLake's `ducklake_table_changes` for the snapshots after `--from` up to and including `--to`, then groups the changes by DuckLake row ID:

- **added**: the row did not exist at `--from` and exists at `--to`
- **deleted**: the row existed at `--from` and is gone at `--to`
- **updated**: DuckLake attributed an update to the same row ID, and the row's value at `--to` differs from its value at `--from`

A row inserted and deleted inside the range is not reported. Update attribution comes from DuckLake's own pre- and post-images, so the command never pairs a delete and an insert into an invented update. `--limit` (1–100, default 10) bounds the examples shown per change kind; the counts always cover every change. Images use the table's current schema, so a column added inside the range reads as null in older images. `--from` equal to `--to` is an empty diff.

Refusals happen before any table scan. A missing or expired snapshot is `snapshot_not_found` (expiry removes a snapshot from the listing). `--from` after `--to`, malformed IDs, a relation in another catalog (`cross_catalog`), and views (`not_a_table`) are all refused. Each statement has a 60-second deadline, and Ctrl-C cancels it through the session.

**Retention.** Lake maintenance (`POST /v1/lake/maintenance`, which needs maintenance authority) expires snapshots older than its retention window. After expiry, `snapshot preview` and `snapshot diff` refuse those IDs, and the snapshot commands cannot bring them back. To time-travel to the state just after a query in history, find its finish time and choose the newest snapshot at or before it:

```sh
finished=$(duckfleet history --limit 1 --format json | jq -r '.queries[0].finished_at')
duckfleet sql "SELECT max(snapshot_id) AS id FROM ducklake_snapshots(current_catalog()) WHERE snapshot_time <= '$finished'::TIMESTAMPTZ"
duckfleet snapshot preview raw.events --at <id>
```

## Compute advice from observed evidence

`duckfleet advise` turns retained history and profile metrics into an advisory sizing recommendation. It only makes authorized metadata reads (`GET /v1/queries/<id>` and `GET /v1/queries`). It opens no session, creates no pod, and changes no allocation, pool, grant, or dbt metadata. It keeps working while execution workers are stopped, as long as the evidence is retained.

```sh
duckfleet query <query-id>                                  # history -> one record
duckfleet profile <query-id> --metrics                      # profile -> engine evidence
duckfleet advise query <query-id>                           # advice from this query and repeat runs of it
duckfleet advise model model.retail.fct_orders --limit 50   # aggregate over a dbt model's recent runs
duckfleet advise query <query-id> --thresholds queue_share=0.3,min_samples=5 --format json
duckfleet sql --memory 2Gi --file model.sql                 # the explicit rerun the advice suggests
```

`advise query` uses the named query plus up to `--limit` (default 20, max 100) earlier successful runs with the same query fingerprint. Fingerprint matching needs SQL visibility. `advise model` uses recent successful runs whose dbt `node_id` matches. Samples are grouped by resolved allocation (mode, pool, CPU, memory, disk), and each group is assessed separately, so evidence from different compute is never blended. Every group reports its source query IDs, sample count, median phase times, evidence, a recommendation, the suggested next experiment, and its tradeoff.

The phases are assessed separately:

| Phase | Source | Notes |
|---|---|---|
| queue | `queued_at` → `selected_at` | Waiting for the worker's single execution slot. Time spent opening an interactive session before the request is accepted is not in history. |
| preparation | `selected_at` → `engine_started_at` | Includes isolated pod startup for isolated compute |
| engine | `engine_started_at` → `finished_at` | DuckDB execution |
| delivery | — | Not measured in history, reported as unknown |

Recommendations, checked in this order:

1. `investigate_queueing`: queue wait is at least `queue_share` (0.5) of the observed time. Larger resources would not help, so look at pool capacity and concurrent work.
2. `isolated_startup_dominated`: isolated preparation is at least `startup_share` (0.5) of the time. Startup is reported separately from SQL execution.
3. `insufficient_evidence`: no captured, visible profile (sampled, expired, withheld from a metadata-only caller, or a failed run).
4. `try_more_memory`: the engine spilled, or its peak buffer reached `memory_share` (0.8) of the DuckDB memory limit. The suggestion doubles memory.
5. `try_more_cpu`: engine CPU time per allocated thread-second reached `cpu_share` (0.8) without memory pressure. The suggestion doubles CPU.
6. `keep_current_allocation`: no threshold crossed.

Confidence is `supported`, `qualified`, or `none`. A recommendation is `qualified` when there are fewer than `min_samples` (3) samples, when engine time varies with a coefficient of variation above `max_variation` (0.5), when history mixes allocations, or when memory evidence is not visible. All six thresholds can be changed with `--thresholds name=value,...`.

The evidence is DuckDB engine metrics: peak buffer memory, temp-directory size, bytes spilled, and CPU time. They are not container RSS or provider cost, and the advisor never estimates cost or promises a speedup on other hardware. A metadata-only caller, such as the `operator` role, gets timing-based advice. It never sees SQL text or profile content, and without profile evidence a resource recommendation is never `supported`. The manual sizing contract is unchanged: you apply any change yourself by rerunning with explicit `--cpu`, `--memory`, `--disk`, `--mode`, or `--pool`, or through `meta.platform` in dbt.

## dbt write-conflict warnings

`duckfleet advise dbt` reads a compiled project's `target/manifest.json` and warns about independent DAG branches that may write the same DuckLake table concurrently. Static analysis needs no running warehouse, URL, or token:

```sh
dbt compile                                    # or any dbt command that writes target/manifest.json
duckfleet advise dbt --project-dir .           # human-readable warnings and coverage
duckfleet advise dbt --strict                  # CI gate: exit 1 when any warning exists
duckfleet advise dbt --history --format json   # also correlate recorded transaction conflicts
```

DuckLake checks conflicts when a transaction commits, at **data-file granularity**. Two transactions that write one table conflict unless both only append (see [Concurrent writes and retry safety](../README.md#concurrent-writes-and-retry-safety)). For every model, seed, and snapshot, the advisor derives writes from:

- the materialization: `table`, `view`, `seed`, `snapshot`, and `materialized_view` rewrite their own relation. `incremental` with `append`, or with no strategy and no `unique_key`, appends. `delete+insert`, `merge`, `insert_overwrite`, `microbatch`, or a `unique_key` with no strategy rewrites. `ephemeral` writes nothing.
- literal SQL in `pre-hook`/`post-hook` and in compiled model SQL: `INSERT` (append, or rewrite with `OR REPLACE` or `ON CONFLICT DO UPDATE`), `COPY … FROM` (append), and `DELETE`, `UPDATE`, `MERGE`, `TRUNCATE`, `CREATE`, `DROP`, and `ALTER` (rewrite). Temporary tables are ignored. Names compare case-insensitively, quoted or not. A one-part name resolves to the `main` schema of the node's database and a two-part name to the node's database, the same way DuckFleet sessions resolve them.

A warning names both models, the fully qualified relation, each model's write kind and its sources, why the DAG permits overlap, and a concrete remedy: an explicit `-- depends_on: {{ ref('…') }}` in the later model's file, or separate target tables. Models ordered by an actual dependency (direct or transitive), writers of different tables, pairs of appends, and read-only models do not warn. The command never edits project files, changes the DAG, serializes execution, or retries a model.

Coverage is always reported. Hooks with unrendered Jinja, `EXECUTE`/`PREPARE`/`CALL`, models without compiled SQL, and unsupported materializations or incremental strategies show up as `UNKNOWN` rather than a false all-clear. The summary line gives the warning, analyzed, and unknown counts. Exit codes: `0` for a completed analysis (even with warnings), `1` under `--strict` when warnings exist, and `2` when `manifest.json` is missing or malformed.

A warning is a static possibility, not proof that two writes will overlap at runtime. The writes may never run at the same moment, or they may touch rows in different data files. `--history` reads recorded `transaction_conflict` failures (`GET /v1/queries?failure_kind=transaction_conflict`) and attaches the ones whose dbt node ID matches a warned model, changing its confidence from `static_possibility` to `observed_race`. Model identity comes back only to principals allowed to see it. A metadata-only caller gets the static warnings with no observed conflicts and no SQL text. After a conflict, rerunning the whole model (for example `dbt retry`) replays its transaction. Nothing replays it automatically.

## SQL files and standard input

`duckfleet sql --file <path>` and piped `duckfleet sql` execute all statements on one session. A final semicolon is optional for file/stdin input; input is UTF-8 and bounded to one MiB. Use `--file -` to name stdin explicitly.

```sh
duckfleet sql --file ./load.sql
printf 'create temporary table t (id int); insert into t values (42); select * from t;' | duckfleet sql
```

Execution stops at the first error by default and returns non-zero. `--continue-on-error` explicitly continues after classified SQL errors while retaining a failing exit status; it never continues after an unknown session, transport uncertainty, or other outcome where further execution could hide recovery requirements.

## Admin console

`duckfleet ui` serves a read-only web console for the gateway the CLI is configured for:

```sh
duckfleet ui              # listens on 127.0.0.1:7878
duckfleet ui --port 0     # any free port
duckfleet ui --url https://gateway.example --token "$TOKEN"
```

It prints a one-time launch link; open it in a browser. The first view, **Fleet**, refreshes every two seconds from `/v1/workers` and `/v1/pools`. It shows gateway sessions against their limit, open sessions against capacity for each persistent pool, and every worker's state, resources, sessions, and queue. Polling pauses while the tab is hidden.

The console is a static Next.js bundle (`web/admin`) embedded in the CLI binary. The page calls the gateway's own `/v1/...` paths. The CLI forwards those requests to the gateway with its configured bearer token, so the browser never holds the credential and the gateway authorizes every request exactly as it would a CLI command. Only `GET` requests are forwarded: the console cannot cancel, change budgets, or run SQL.

The local server is meant for one person on their own machine:

- It binds only to 127.0.0.1 and refuses requests whose `Host` header names anything else, which defeats DNS rebinding.
- API requests need the random per-launch key from the printed link. The launch link swaps the key for an `HttpOnly`, `SameSite=Strict` cookie and removes it from the address bar.
- Cross-site and foreign-`Origin` requests are refused.

These checks protect against other web pages in your browser, not against other processes running as your user, which can already read the CLI's local credential.

To work on the console:

```sh
cd web/admin
npm ci
npm run build          # writes web/admin/out
cd ../../server
cargo run -p duckfleet-cli -- ui
```

Debug builds read `web/admin/out` from disk on every request, so after the first build `npm run build` and a browser reload are enough. Release builds embed the bundle. A binary built without `web/admin/out` still works; `duckfleet ui` then explains how to build the bundle. `make ui` does both builds.

## Source-based development

The native crate reuses the command implementation in `server/src/cli.rs` and HTTP/Arrow transport in `server/src/client.rs`; it is not a second client implementation.

```sh
cd server
cargo run -p duckfleet-cli -- version
cargo test -p duckfleet-cli
```

The Docker Compose `api` service and `DUCKFLEET_ROLE=cli duckfleet-server` role remain available for existing development and harness workflows.

## Releases

A tag named `duckfleet-v<version>` runs the native CLI release workflow. Each platform job builds the admin console bundle, then builds and packages the binary, extracts the archive, and runs `version`, `init`, and redaction checks against that extracted executable. The release job verifies and publishes `SHA256SUMS` and generates a Homebrew formula containing the same checksums.
