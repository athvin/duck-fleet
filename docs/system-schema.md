# Query operational metadata with SQL

DuckFleet exposes three version 1 relations: `duckfleet_system.query_history`,
`duckfleet_system.query_attempts`, and `duckfleet_system.usage`. They read the
authoritative platform PostgreSQL database through the gateway. They need no
execution worker, compute session, lake attachment, or additional service.

Use the CLI, HTTP, or pgwire with the explicit database `duckfleet_system`:

```sh
duckfleet system tables
duckfleet system describe query_history
duckfleet system sql 'SHOW TABLES' --format json
PGPASSWORD="$DUCKFLEET_TOKEN" psql -h localhost -p 5432 -d duckfleet_system
```

`GET /v1/system/schema` returns the authorized column catalog, including types,
nullability, units, descriptions, and whether access is readable, partially
masked, or masked. The checked-in [v1 contract](system-schema-v1.json) defines the
complete column set. `SHOW TABLES`, `DESCRIBE query_history`, and `SHOW COLUMNS
FROM query_history` provide SQL discovery. Discovery requires history or usage
read permission. It does not require query execution or a compute grant.

The ordinary pgwire database continues to execute warehouse SQL. System SQL is
a bounded inspection dialect: it cannot join user lake tables, access arbitrary
PostgreSQL tables, or fall back to a worker. DuckDB's ordinary `information_schema`
continues to describe the user catalog. Generic BI connection probes are not a
compatibility promise; use the documented queries and discovery statements.

## HTTP and result types

POST `/v1/system/query` with the usual bearer credential:

```json
{
  "contract_version": 1,
  "sql": "SELECT status, COUNT(*) AS runs FROM query_history WHERE accepted_at >= $1 AND accepted_at < $2 GROUP BY status",
  "parameters": ["2026-11-20T00:00:00Z", "2026-11-21T00:00:00Z"],
  "format": "json"
}
```

The CLI accepts repeated `--param` string arguments, which work for these UTC
bounds and request IDs. The Rust client's `Client::system_sql` accepts typed JSON
parameters. Pgwire supports simple SQL and parameterless unnamed extended
queries; bound pgwire parameters remain unsupported. Use UTC literals in psql.

JSON returns column descriptors and row arrays, plus `system_contract_version`,
`snapshot_at`, `namespace_id`, and `applied_bounds`. BIGINT and DECIMAL cells are
strings, preserving integers beyond JavaScript's exact range. NULL remains JSON
null; booleans and finite doubles retain their types. JSON-bearing columns are
opaque strings. Request `format: "arrow"` for typed Arrow IPC; its schema metadata
carries the same snapshot and bounds. Pgwire uses BIGINT, exact NUMERIC, and
TIMESTAMPTZ with an explicit offset. `SUM(BIGINT)` is DECIMAL(38,0), and
`AVG(BIGINT)` is DECIMAL(38,6), rounded by PostgreSQL to six decimal places.
An out-of-range result fails rather than silently losing precision.

## Useful queries

Each source requires its own explicit time window or request-ID filter. These
examples use bound parameters over HTTP/CLI. Substitute offset-bearing timestamp
literals in psql.

```sql
SELECT query_fingerprint, query_fingerprint_version,
       COUNT(*) AS runs, AVG(engine_elapsed_ms) AS average_engine_ms,
       AVG(queue_wait_ms) AS average_queue_ms
FROM duckfleet_system.query_history
WHERE accepted_at >= $1 AND accepted_at < $2
  AND query_fingerprint IS NOT NULL
GROUP BY query_fingerprint, query_fingerprint_version
ORDER BY average_engine_ms DESC LIMIT 50;

SELECT failure_kind, outcome, COUNT(*) AS attempts
FROM duckfleet_system.query_attempts
WHERE accepted_at >= $1 AND accepted_at < $2
  AND state = 'terminal' AND status <> 'succeeded'
GROUP BY failure_kind, outcome ORDER BY attempts DESC LIMIT 100;

SELECT owner_principal_name, model_id, rate_card_version,
       SUM(amount_micro_usd) AS amount_micro_usd
FROM duckfleet_system.usage
WHERE observed_at >= $1 AND observed_at < $2
GROUP BY owner_principal_name, model_id, rate_card_version
ORDER BY amount_micro_usd DESC LIMIT 100;

SELECT h.request_id, h.status, h.engine_elapsed_ms,
       u.event_id, u.amount_micro_usd, u.rate_card_version, u.source
FROM duckfleet_system.query_history h
LEFT JOIN duckfleet_system.usage u
  ON h.request_id = u.request_id
 AND u.observed_at >= $3 AND u.observed_at < $4
WHERE h.accepted_at >= $1 AND h.accepted_at < $2
ORDER BY h.history_id DESC LIMIT 100;
```

The LEFT JOIN retains requests without usage. One request can have several usage
events; summing history durations after such a join can double-count them. Missing
usage is not proof that work was free. Model and fingerprint grouping requires
SQL-read permission over all visible rows.

## Permissions, bounds, and freshness

The existing history/usage policy applies before predicates, joins, aggregation,
and limits. Own visibility uses immutable authenticated principal identity within
the configured namespace. Direct SQL/context/error/fingerprint/model projections
are NULL-masked where SQL-read permission is absent. Raw profiles require both
SQL-read and profile-read permission. Filtering, ordering, grouping, or aggregating
on sensitive columns requires permission covering every visible row. Fixed
numeric profile metrics need ordinary metadata visibility. No caller header,
SQL identifier, parameter, or context value selects an authorization identity.

The supported grammar includes SELECT, aliases, comparisons, AND/OR/NOT,
IS NULL, IN, COUNT/SUM/AVG/MIN/MAX, GROUP BY/HAVING/ORDER BY/LIMIT, and INNER/LEFT
joins on request identity across at most three distinct relations. There are no
self joins, CTEs, subqueries, windows, arbitrary functions, casts, arithmetic,
CASE, OFFSET, EXPLAIN, DDL, or writes. Unsupported statements fail explicitly.
GROUP BY and ORDER BY use names or supported expressions, not numeric ordinals.

Every source needs a necessary time-bound conjunct of at most 31 days, or at most
100 explicit request IDs. An OR that can bypass the bound is rejected. A LEFT
JOIN's ON predicate may bound its right side, not its preserved left side. HTTP
may also supply `bounds` keyed by canonical relation name, with paired `from` and
`until`, or `request_ids`; unused envelope entries are rejected. Timestamps need
explicit offsets and at most microsecond precision. LIMIT does not bound source
input. Each source is capped at 50,000 eligible rows, or 100 when raw profiles are
selected, and at most 8 MiB of visible referenced text/JSON payloads. Sources that
emit text share a stricter 7 MiB allowance divided by the number of text output
occurrences; repeating a large column or emitting text from several sources
reduces that allowance before execution. This prevents one PostgreSQL result
row from multiplying a large payload beyond the gateway's buffer budget. Exceeding these limits
rejects the query; aggregate input is never silently truncated.

Results are limited to 10,000 rows and 8 MiB. SQL is at most 64 KiB, with additional
token, nesting, expression, and 32-output-column limits. Request bodies are at
most 256 KiB. Two admitted queries per gateway share no worker execution slot;
their memory permit remains held until the response is delivered or dropped.
Authentication has a separate bounded lane. The read deadline is three seconds
including body collection, authentication, parsing, database work, and encoding.
HTTP result delivery has a separate absolute five-second allowance. If response
bytes remain held past that deadline, the gateway closes the HTTP connection
and releases its buffered result and admission slot. This also closes other
streams sharing that HTTP/2 connection; use a separate connection for system
downloads when that matters. Completed delivery cancels the deadline, and other
API responses retain their existing behavior. Pgwire result writes are likewise
bounded to five seconds; password frames are limited before authentication to
8,192 token bytes.
Pgwire may perform a separate bounded authentication check after a permission
denial to decide whether the connection can remain open. Database spill is
limited to at most 16 MiB per gateway backend by administrator provisioning.
Closing a socket does not acknowledge that PostgreSQL stopped its query. An
abandoned read therefore retains its admission slot until the original deadline
plus the three-second database statement timeout, unless acknowledged COMMIT or
ROLLBACK releases it sooner. A canceled client can temporarily receive `capacity`
on replacement reads. This bounds cancellation churn under ordinary transport
and database timeout behavior; it is not distributed backend fencing.

Each result uses one read-only REPEATABLE READ snapshot for authorization, source
caps, and data. A subsequent statement sees current committed policy and data.
Existing HTTP history/usage cursors remain live keyset pages with their existing
500-row ceiling; they are not a reusable SQL snapshot. There is no result cache
or automatic retry. Reading system metadata creates no warehouse history or
usage record; permission denials produce bounded, best-effort audit events.

## Capture and upgrades

Schema 4 adds nullable observations without rewriting historical rows. Unknown
legacy dimensions remain NULL. Requested compute preserves omitted choices;
selected compute records the allocation actually chosen. Engine version is
captured from the worker. Query-shape fingerprints have their own version and
capture status; unsupported/large SQL can execute normally with a NULL shape.
The immutable request-deduplication fingerprints are unchanged.

Queue/preparation/lifecycle durations use documented database observations.
`engine_elapsed_ms` preserves the worker's monotonic execution and Arrow
serialization interval, excluding later profiling and settlement; usage
accounting start/end values retain their separate billing meaning. Numeric
profile metrics describe the captured profile, which may cover only the final
statement of a multi-statement warehouse request. Missing profiles are not zero
resource use. Usage stores saved pricing and attribution independently of
diagnostic history retention.

Follow the [quiesced upgrade instructions](../infra/README.md). Rerun database
provisioning to install the gateway spill limit, migrate to schema 4, and restart
compute. Runtime roles cannot repair their own grants or raise the spill limit.
System SQL reuses those constrained roles; no owner connection or raw platform
DSN is exposed to SQL users.
