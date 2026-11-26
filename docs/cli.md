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

## SQL files and standard input

`duckfleet sql --file <path>` and piped `duckfleet sql` execute all statements on one session. A final semicolon is optional for file/stdin input; input is UTF-8 and bounded to one MiB. Use `--file -` to name stdin explicitly.

```sh
duckfleet sql --file ./load.sql
printf 'create temporary table t (id int); insert into t values (42); select * from t;' | duckfleet sql
```

Execution stops at the first error by default and returns non-zero. `--continue-on-error` explicitly continues after classified SQL errors while retaining a failing exit status; it never continues after an unknown session, transport uncertainty, or other outcome where further execution could hide recovery requirements.

## Source-based development

The native crate reuses the command implementation in `server/src/cli.rs` and HTTP/Arrow transport in `server/src/client.rs`; it is not a second client implementation.

```sh
cd server
cargo run -p duckfleet-cli -- version
cargo test -p duckfleet-cli
```

The Docker Compose `api` service and `DUCKFLEET_ROLE=cli duckfleet-server` role remain available for existing development and harness workflows.

## Releases

A tag named `duckfleet-v<version>` runs the native CLI release workflow. Each platform job builds and packages the binary, extracts the archive, and runs `version`, `init`, and redaction checks against that extracted executable. The release job verifies and publishes `SHA256SUMS` and generates a Homebrew formula containing the same checksums.
