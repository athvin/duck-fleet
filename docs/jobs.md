# Durable query jobs

Jobs submit one SQL statement, return after PostgreSQL records acceptance, and run
in a private, server-owned DuckDB session. They use the same pool admission,
principal limits, prepaid budget, history, and usage ledger as interactive queries.
The gateway connection is not the lifetime of the job. PostgreSQL and the configured
private object store are the only additional services required.

Compose enables the result store. Kubernetes deployments opt in with
`results.enabled=true`; see [deployment configuration](../infra/README.md).
Apply platform schema migration 5 before starting the new gateway and workers.

## Submit and reconnect

The 300 second example requires a matching compute grant with that job runtime
allowance, or administrative authority. Ordinary Analyst grants default to 60 seconds.

```sh
export DUCKFLEET_ROLE=cli
export DUCKFLEET_URL=http://localhost:8080
export DUCKFLEET_TOKEN=your-platform-credential

duckfleet-server job submit 'SELECT * FROM lake.analytics.events' \
  --pool small --timeout 300 --queue-timeout 120 \
  --result-max-bytes 268435456 --result-max-rows 1000000 \
  --result-retention 3600

duckfleet-server job status JOB_UUID --wait
duckfleet-server job cancel JOB_UUID
duckfleet-server job result JOB_UUID --output ./job-result
duckfleet-server job result JOB_UUID --output ./job-result --resume
```

The CLI prints the generated request ID before submitting. Applications should
generate and retain a UUID before `POST /v1/jobs`:

```json
{
  "request_id": "388f7084-1362-42f8-9805-35f9154ac011",
  "sql": "SELECT event_type, count(*) FROM lake.analytics.events GROUP BY 1",
  "compute": {"mode": "persistent", "pool": "small"},
  "timeout_seconds": 300,
  "queue_timeout_seconds": 120,
  "result": {
    "format": "arrow_ipc_chunks",
    "max_bytes": 268435456,
    "max_rows": 1000000,
    "retention_seconds": 3600
  }
}
```

A successful submission returns 202 with `request_id`, `acceptance: "accepted"`,
`status_url`, `cancel_url`, and `result_url`. Acceptance means durable ownership of
the attempt, not that SQL parsed or ran successfully. A queue-capacity refusal can
also be durably recorded; inspect its `acceptance` field. A lost response has an
unknown acceptance outcome: reconcile the same ID rather than inventing another.

Repeating the same ID and payload returns 409 with the retained attempt. It never
re-executes completed or uncertain work. Retained options and resolved limits make
duplicates stable across changes to operator defaults. Changing SQL, parameters,
context, options, or retry lineage conflicts. Omitted options and explicit defaults
may be distinct intents. A new attempt with `retry_of` is allowed only where the
existing lifecycle proves that an explicit retry is safe. The service never
automatically retries SQL.

## State and result access

| Endpoint | Meaning |
| --- | --- |
| `GET /v1/jobs/{id}` | Durable attempt, SQL completion, result state, accepted limits |
| `POST /v1/jobs/{id}/cancel` | Request cooperative cancellation, including queued work |
| `GET /v1/jobs/{id}/result` | Complete manifest, or a classified availability response |
| `GET /v1/jobs/{id}/result/chunks/{number}` | One authorized, checksum-verified Arrow IPC stream |

SQL outcome and result delivery are separate. `sql_completion: "succeeded"` remains
true if a completed write subsequently cannot publish its result. In that case
`result_state` is `failed`, and `result_failure_kind` identifies the delivery stage.
The write is not safe to replay merely because its output is unavailable.

Result discovery returns 200 when ready, 202 while pending, 409 when unavailable,
410 after expiration, or 503 when a complete object cannot currently be retrieved
and verified. Each manifest and chunk request resolves current authorization and
checks expiration before accessing the object store. No public bucket, caller
supplied storage key, or signed URL is required.

Result reading has its own `result.read` permission with own/all scope. Analysts
can read their own results; Admin can read all results. Viewer and Operator roles
do not implicitly receive result access. History, SQL-text, and profile access do
not grant result-data access. A replacement credential may inspect authorized
retained work, but cannot replace the immutable credential that authorized a
queued execution. Revocation or a reduced policy is checked before execution.

## Bounds and session semantics

Each job is a fresh autocommit session supporting one engine-parsed SELECT, INSERT,
UPDATE, DELETE, CREATE, ALTER, or DROP statement. Scripts, explicit transactions,
caller sessions, and MERGE are rejected before execution. A job cannot share a
temporary table or transaction with another job. DuckDB 2.0.1 is checked before
worker readiness, and the vendored Rust adapter adds checked streaming and strict
single-statement preparation.

Hard ceilings are 3600 seconds execution, 1 GiB encoded result data, 10 million rows,
4096 chunks, and 24 hours retention. Queue waiting defaults to 120 seconds with a
3600 second hard ceiling, including provisioning time. Operator, pool, and one
matching compute grant may lower execution/result ceilings. An omitted option
is resolved once and clipped to the authorized envelope; an explicit value above
it is refused. Existing compute grants without job limits retain a 60 second job
runtime allowance. Longer work requires an explicit grant or administrative policy.

All results use independent Arrow IPC chunks, including an empty result with its
schema. The manifest contains the schema, complete flag, generation, expiration,
total rows/bytes, and each numeric chunk's size and SHA256. Retention starts at
verified SQL completion; result publication does not extend that deadline. It becomes visible only
after every chunk is acknowledged and final accounting commits. The CLI downloads
one chunk at a time and checks length, checksum, schema, and row count before
publishing the local file; `--resume` checks the same authorized manifest generation.

The application bounds imported Arrow batches at 8 MiB and encoded chunks at 16 MiB.
The gateway admits two concurrent downloads and holds their memory permits through
HTTP delivery, with a 30 second absolute delivery deadline. An oversized native
value or a materialized DuckDB operator can allocate before the application checks
its size, so engine and container memory limits remain necessary. DML, RETURNING,
and CTAS may materialize inside DuckDB. This is not a constant-memory engine claim.

## Failure and maintenance behavior

Gateway replacement does not replay accepted work. Job pods use their durable
binding and operator deadline rather than a gateway pod owner reference, so a
gateway rollout does not garbage-collect their engine. Queued work can be rebound
only when the old ownership is conclusively ended and no execution grant or budget
reservation was issued. Interrupted executing work is terminal with an unknown
outcome unless a durable SQL completion milestone proves more. Killing a worker
does not preserve its transaction. Cooperative cancellation may wait for an engine
or extension IO boundary.

Isolated pod lifetimes and deployment shutdown grace remain operator limits. The
service refuses a job before creating its pod if the execution allowance plus
30 seconds completion allowance exceeds that pod's configured lifetime. Provisioning
and queue delays also consume pod lifetime; rollout or pod loss can interrupt a
long job. The existing 615 second application drain and 630 second container grace
are unchanged. Jobs do not add multiple-gateway failover guarantees.

Expiry is enforced on reads even if the maintainer is temporarily down. The leased
maintainer removes expired or abandoned chunk/manifest objects in bounded passes,
while retaining request identity, terminal outcome, and usage. Durable deletion
tombstones permit repeat cleanup after an uncertain PUT completes late. Configure
a private-prefix storage lifecycle as an additional leak guard. Lake maintenance
must never own the result prefix.

SQL remains a trusted workload boundary in this release. API result permissions
do not turn an engine process carrying storage credentials into a hostile-SQL
sandbox. Credentials, result prefixes, and actual storage/OS authority should be
scoped accordingly.
