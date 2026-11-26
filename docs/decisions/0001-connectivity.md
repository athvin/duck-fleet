# 0001 — Next connectivity path: an ADBC driver, plus targeted pgwire correctness fixes

- Status: accepted (2026-11-26)
- Ticket: #172 (part of #163)
- Evidence: `prototypes/connectivity/run.sh` regenerates `prototypes/connectivity/matrix.json` against the Compose stack.

## Decision

**Primary path: a DuckFleet ADBC driver.** It is built in Rust on the `duckfleet` SDK (#171) and speaks the existing HTTP/Arrow API. Results stay typed Arrow end to end, and the driver reuses the platform's session, request-identity, cancellation, classified-failure, and never-replay contracts instead of re-implementing them in a second protocol.

**Rejected alternative: broad PostgreSQL wire compatibility.** That means extended-protocol binds, binary formats, cursors, COPY, TLS termination in the listener, and per-connection pool selection. It would take a second implementation of session and recovery semantics on a protocol that carries text values. Even after that work, Arrow-native clients would lose type fidelity, and the platform would be on the hook for every driver's protocol quirks. pgwire stays a compatibility layer for `psql` and simple-protocol tools. It receives **targeted correctness fixes** for the defects this study found, split by capability, and no general compatibility project.

- Maintenance owner: the SDK/CLI maintainers. The driver is a thin layer over the SDK's `Client`/`Session`.
- Distribution: per-platform shared libraries (`libadbc_duckfleet.{so,dylib}`, exporting `AdbcDuckfleetInit`) attached to tagged GitHub releases with `SHA256SUMS`, and an ADBC driver manifest so driver managers can find the library by name. Python, R, and other users load it through their existing ADBC driver manager. There is no PyO3 or language-specific binding.
- Security: no new listener. The driver authenticates with the same bearer credential as the SDK (the ADBC `password` option) over the same HTTP endpoint, so TLS and exposure stay with the existing exposure layer. Grants, `result.read`, and metering apply exactly as for any HTTP session.
- Reused contracts: explicit sessions with fixed compute, typed scalar parameters, interactive caps, the cancellation endpoint, `failure_kind`/`retry_safe`/request identity (mapped to ADBC status and SQLSTATE: `40001`, `57014`, `42601`), and verified durable-job chunks for large results (#190).

## Workflows tested

Every run used the Compose stack (DuckDB v2.0.1 workers) with the local bootstrap token. Each capability below is an observed outcome. Nothing is marked compatible because `SELECT 1` works.

| ID | Client | Exact versions | Authentication | Path |
|---|---|---|---|---|
| W1 | Python DB-API over ADBC, results as Arrow | adbc-driver-manager 1.13.0, pyarrow 26.0.1, Python 3.12, prototype driver `prototypes/adbc-duckfleet` | ADBC `uri` + `password` (token) | HTTP/Arrow via the SDK |
| W2 | pandas `read_sql` over the same ADBC connection | pandas 3.1.1 | same | same |
| W3 | `psql` | psql 17.6 (`postgres:17.6-bookworm`) | password = token | pgwire, simple protocol |
| W4 | psycopg | psycopg 3.3.9 (binary) | password = token | pgwire; default server-side binding and `ClientCursor` |

SQL used:

- Parameterized: `SELECT ?::INTEGER + 1, ?::VARCHAR, ?::DECIMAL(10,2), ?::TIMESTAMP` with `(41, 'héllo', '9.99', '2026-01-02 03:04:05.123456')`. Expected Arrow types: int32, utf8, decimal128(10,2), timestamp[us].
- Nested: `SELECT NULL::INTEGER, [1, 2, NULL], {'a': 1, 'b': 'x'}, '\xAA'::BLOB`.
- Large: `range(90000)` within the 100,000-row cap, and `range(150000)` over it.
- Cancellation: `SELECT sum(sin(range)) FROM range(100000000000)`, canceled after 1.5–3 s.
- Conflict: two transactions that both `DELETE` the same rows, both committing.

## Compatibility matrix

| Workflow | Capability | Outcome | Observed |
|---|---|---|---|
| W1 | extended bind: typed parameters and typed Arrow result | works | {'answer': 'int32', 'name': 'string', 'price': 'decimal128(10, 2)', 'at': 'timestamp[us]'} |
| W1 | null and nested types | works | {'n': 'int32', 'l': 'list<l: int32>', 's': 'struct<a: int32, b: string>', 'b': 'binary'} |
| W1 | large results (90k rows, interactive cap is 100k rows / 16 MiB) | works | 90000 rows within the interactive cap |
| W1 | results over the interactive cap (durable-job path not wired into ADBC) | fails | ProgrammingError: INVALID_ARGUMENT: result exceeds local response limit (100000 rows / 16 MiB) (failure_kind: sql_error, retry_safe: false,  |
| W1 | transaction state | works | commit/rollback through ADBC autocommit=false; temp table survived, rolled-back insert did not |
| W1 | native cancellation (AdbcStatementCancel -> session cancel) | works | canceled after 1.6s: CANCELLED: INTERRUPT Error: Interrupted!: Error code 1: Unknown error code (failure_kind: canceled, retry_safe: false,  |
| W1 | classified failure: lost commit race | works | sqlstate=40001: INVALID_STATE: TransactionContext Error: Failed to commit: Failed to commit DuckLake transaction. Transaction conflict - att |
| W1 | unsupported parameter type is explicit | works | refused before sending: NOT_IMPLEMENTED: parameter type Binary cannot be bound exactly; bind it as text and cast in SQL |
| W1 | catalog metadata (AdbcConnectionGetObjects) | fails | NotSupportedError: NOT_IMPLEMENTED: get_objects is not implemented by the DuckFleet ADBC prototype |
| W2 | parameterized read_sql into a DataFrame | fails | NotImplementedError: 'params' is not implemented for ADBC drivers |
| W2 | pyarrow-backed dtypes keep integer nulls | works | {'a': 'int64[pyarrow]', 'b': 'int32[pyarrow]'} |
| W4 | connect and simple query | works | (1,) |
| W4 | extended bind (server-side parameters, psycopg default) | fails | FeatureNotSupported: only unnamed parameterless prepared statements are supported |
| W4 | client-side binding (ClientCursor, simple protocol) | works | (42, "o'k") |
| W4 | COPY TO STDOUT | fails | ProgrammingError: copy() should be used only with COPY ... TO STDOUT or COPY ... FROM STDIN statements, got TUPLES_OK |
| W4 | native cancellation (CancelRequest) | fails | AssertionError: CancelRequest had no effect; the statement ran until its deadline after 63s (QueryCanceled) |
| W4 | TLS (sslmode=require) | fails | OperationalError: connection failed: connection to server at "127.0.0.1", port 15432 failed: server does not support SSL, but SSL was requir |
| W4 | pool selection via startup options | ignored | a startup option naming the offline pool connects, but the backend session always uses the default persistent pool (documented: no per-conne |
| W4 | transaction state reporting (ReadyForQuery) | fails | AssertionError: ReadyForQuery reported IDLE inside a transaction and IDLE after an error; psycopg therefore treats commit() as a no-op |
| W4 | null and nested types | fails | OperationalError: consuming input failed: server closed the connection unexpectedly 	This probably means the server terminated abnormally 	b |
| W4 | large results (90k rows) | works | 90000 rows |
| W4 | classified failure: lost commit race | fails | AssertionError: no conflict: with ReadyForQuery always IDLE, psycopg never sent COMMIT |
| W3 | simple query | works | exit 0 after 6s: simple_ok_42 |
| W3 | extended bind (\bind) | fails | exit 0 after 0s: ERROR:  named portals, parameters, and non-text results are not supported |
| W3 | COPY TO STDOUT (\copy) | fails | no CopyOut reaches the client: the worker executes DuckDB COPY ... TO STDOUT itself, writing the rows to its own stdout (container logs), an |
| W3 | null and nested types | fails | exit 2 after 2s: server closed the connection unexpectedly 	This probably means the server terminated abnormally 	before or while processing |
| W3 | large results (90k rows) | works | exit 0 after 3s: 0 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30 31 32 33 34 35 36 37 38 39 40 41 42 43 4 |
| W3 | aborted-transaction SQLSTATE (25P02) | fails | the engine enforces the aborted transaction, but the error is reported with SQLSTATE 42601 and ReadyForQuery stays I. exit 0 after 3s: ERROR |
| W3 | native cancellation (Ctrl-C sends CancelRequest) | fails | exit 137 after 9s: Cancel request sent |
| W3 | TLS (sslmode=require) | fails | exit 2 after 1s: psql: error: connection to server at "server" (172.18.0.5), port 5432 failed: server does not support SSL, but SSL was requ |

### What the matrix says

- **ADBC (W1/W2)** gets typed Arrow through, including decimals, microsecond timestamps, lists, structs, and binary. It also handles bound parameters, real transactions, sub-second cancellation, `40001` on a lost commit race, and explicit refusals for unsupported parameter types. Its gaps are the expected unimplemented surface: catalog metadata (#189) and results over the interactive cap (#190). pandas' `read_sql` does not forward `params` to ADBC connections, so parameterized reads go through the DB-API cursor. That is a pandas limitation, not a driver one.
- **pgwire (W3/W4)** handles the simple protocol and large text results. Extended binds, CopyOut, CancelRequest, and TLS are refused as documented, and startup options do not select a pool. The study also found defects that are not documented limits:
  - ReadyForQuery always reports `I`, so psycopg's `commit()` is a no-op (#186).
  - Aborted transactions report `42601`, not `25P02` (#186).
  - Nested values close the connection instead of returning an error (#187).
  - CancelRequest is ignored (#188).
  - `COPY ... TO STDOUT` runs on the worker and writes rows to the **worker's stdout and logs** on every protocol (#185).

## Comparison

| | ADBC driver | Broad pgwire compatibility |
|---|---|---|
| Implementation cost | Small: about 500 lines over the SDK proved the core path | Large: extended protocol, binary encodings, COPY, cursors, TLS, cancel keys |
| Maintenance | One client contract (HTTP/Arrow) shared with CLI and SDK | A second protocol with its own transaction, recovery, and type semantics |
| Arrow fidelity | Native: schema and values unchanged | Text format with typed OIDs; nested and binary types need casts |
| Cancellation | Existing session cancel endpoint | Needs BackendKeyData and a CancelRequest listener path |
| Distribution | Shared libraries and a driver manifest | Nothing to ship, but every driver's protocol quirks become ours |
| Security | Same credential and endpoint as HTTP | Listener-level TLS or a terminating proxy, plus cancel-key secrecy |
| Expected coverage | pyarrow, pandas, Polars, DuckDB, R, and other ADBC consumers | BI tools and drivers that need extended binds (JDBC, psycopg default) |

## Follow-up tickets

- #189: ship the ADBC driver (workspace crate, metadata, release artifacts, real Compose suite)
- #190: ADBC partitioned results over durable-job chunks
- #185: refuse `COPY TO STDOUT`/`FROM STDIN` (rows reach worker logs)
- #186: pgwire transaction status and `25P02`
- #187: pgwire unsupported types as classified errors
- #188: pgwire CancelRequest

The pgwire extended protocol, binary formats, cursors, listener TLS, and per-connection pool selection stay out of scope unless a named workflow needs them. If one does, it gets its own capability ticket.

## Reproduce

```sh
make up
bash prototypes/connectivity/run.sh   # needs uv, docker, cargo, jq; writes prototypes/connectivity/matrix.json
```
