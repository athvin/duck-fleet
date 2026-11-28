# 0002 — How DuckDB's memory and thread limits are set on each worker

- Status: accepted (2026-11-28)
- Code: `EngineSettings::resolve` in `server/src/compute.rs`, applied at worker startup in `server/src/worker.rs`
- Configuration: `persistentPools.<name>.engine` and `isolatedCompute.engineOverride` in `infra/helm/duckfleet/values.yaml`

## Context

Every worker runs one DuckDB database and sets `memory_limit`, `threads`, and `max_temp_directory_size` on it before it serves anything. Until now both worker kinds used the same rule: half the container's memory, one thread per CPU, and spill sized from the disk.

These settings are **database-wide** in DuckDB, not per connection. An isolated pod serves one session, so the limits belong to that session. A persistent worker serves up to `maxSessionsPerWorker` sessions, and each one is a clone of the same connection. They all share one buffer pool and one thread pool.

## Decisions

### 1. Isolated pods size the engine from the request automatically

When a caller asks for `cpu`/`memory`/`disk`, the pod is created at that size, and the worker sets:

| Setting | Value |
| --- | --- |
| `memory_limit` | 50% of the requested memory |
| `threads` | the requested CPU |
| `max_temp_directory_size` | the requested disk minus 512 MiB |

A caller who asks for more gets a bigger engine, with no extra knob to turn. The worker also checks its cgroup against the declared size and refuses to start on a mismatch, so these values always match what was provisioned.

The 50% leaves room outside DuckDB's buffer pool for extensions, HTTP/S3 clients and Arrow result batches, which DuckDB does not count against `memory_limit`.

### 2. Isolated pods can be overridden, but only deliberately and visibly

`isolatedCompute.engineOverride` exists for corner cases, for example memory-bound statistics passes with narrow results that could safely use 70% of the pod. It is deliberately constrained:

- **It applies to the whole deployment.** It is not a per-request field, so a caller cannot change it.
- **Memory is a percentage only** (`memoryPercent`, 10–90). Isolated sizes change per request, so an absolute value would be wrong for most of them. The worker refuses an absolute `DUCKFLEET_ENGINE_MEMORY` on an isolated pod.
- **Threads is a ceiling** (`maxThreads`). Each pod gets `min(maxThreads, requested cpu)`, so a single value works for every size.
- **It is recorded.** Every isolated placement carries `engine_limits: "override"`, and the actual values are in `duckdb_memory_limit` and `duckdb_threads`. History therefore shows that a query ran under the override.

The values file marks the block `MANUAL OVERRIDE — leave empty unless you have a specific reason`.

### 3. Persistent workers default lower, and each pool can configure them

With nothing configured, a persistent worker uses:

| Setting | Value |
| --- | --- |
| `memory_limit` | 40% of the container's memory |
| `threads` | the container's CPU |
| `max_temp_directory_size` | the disk minus 512 MiB (unchanged) |

Memory is below the isolated 50% because every concurrent session on the worker adds its own result buffers and Arrow batches **outside** DuckDB's pool. A shared worker needs a wider margin to avoid being OOM-killed, and an OOM kill takes down every session on it, not just one.

Threads stay at the CPU count. Fewer threads than CPUs only leaves paid-for CPU idle unless several heavy scans run at once. Pools that see that can lower it.

A pool sets its own values with `engine`:

```yaml
persistentPools:
  interactive:
    cpu: 4
    memory: 8Gi
    disk: 8Gi
    engine: {memory: 3Gi, threads: 4}   # or memoryPercent: 35
```

- `memory` (a quantity) or `memoryPercent` (10–90), not both.
- `memory` may be at most 90% of the container. Higher would leave no room outside the engine.
- `threads` may not exceed the pool's `cpu`.

The chart rejects violations when it renders, and the worker rejects them again at startup, so a hand-edited manifest fails just as loudly. The placement records `engine_limits: "configured"`.

### 4. Lowering the engine limit is not tenant isolation

Lowering `memory_limit` on a shared worker shrinks the pool that all of its sessions share. It does **not** give each session a fair share: one large aggregation can still use nearly all of it. Keeping one tenant from crowding out the rest takes admission limits (`maxRunningQueries`, `maxRunningPerPrincipal`). A future per-session memory cap, if DuckDB's per-connection settings make one practical, would also help. A tenant that needs guaranteed resources should request isolated compute.

## Configuration reference

| Environment variable (worker) | Set by | Meaning |
| --- | --- | --- |
| `DUCKFLEET_ENGINE_MEMORY` | `persistentPools.<name>.engine.memory` | absolute `memory_limit`; persistent only |
| `DUCKFLEET_ENGINE_MEMORY_PERCENT` | `…engine.memoryPercent`, `isolatedCompute.engineOverride.memoryPercent` | share of the container's memory |
| `DUCKFLEET_ENGINE_THREADS` | `…engine.threads`, `isolatedCompute.engineOverride.maxThreads` | exact on persistent workers; a ceiling on isolated pods |

Placement fields reported by every worker: `engine_limits` (`automatic` \| `configured` \| `override`), `duckdb_memory_limit`, `duckdb_threads`.

## Consequences

- The default persistent worker (1 CPU / 1 GiB) now starts DuckDB with about 409 MiB instead of 512 MiB. Statements that only just fit before may now spill, or hit the limit sooner. Set `engine.memoryPercent: 50` on the pool to keep the old behaviour.
- Isolated behaviour is unchanged unless the override is set.
- `duckfleet advise` reads the effective limits from the placement, so its advice follows whatever was in force.
