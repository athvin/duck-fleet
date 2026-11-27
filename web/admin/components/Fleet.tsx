"use client";

import { useCallback, useState } from "react";
import { api, ApiError, type ConsoleMeta, type Pool, type PoolsResponse, type WorkerReport, type WorkersResponse } from "@/lib/api";
import { ago, bytes, count, duration, resources } from "@/lib/format";
import { useNow, usePolling } from "@/lib/usePolling";
import { Meter } from "./Meter";
import { StatusBadge, StatusIcon, workerStatus } from "./Status";

const POLL_MS = 2000;

type Snapshot = { workers: WorkersResponse; pools: PoolsResponse };

export function Fleet() {
  const [live, setLive] = useState(true);
  const now = useNow();
  const load = useCallback(async (signal: AbortSignal): Promise<Snapshot> => {
    const [workers, pools] = await Promise.all([api.workers(signal), api.pools(signal)]);
    return { workers, pools };
  }, []);
  const fleet = usePolling(load, POLL_MS, live);
  const meta = usePolling(api.meta, 60_000, true);

  return (
    <div className="shell">
      <Header meta={meta.data} live={live} onToggle={() => setLive((v) => !v)} onRefresh={fleet.refresh} updatedAt={fleet.updatedAt} now={now} />
      <main className="content">
        {fleet.error && <ErrorBanner error={fleet.error} stale={fleet.data !== undefined} updatedAt={fleet.updatedAt} now={now} />}
        {fleet.data ? (
          <>
            <Kpis snapshot={fleet.data} />
            <Pools pools={fleet.data.pools} />
            <Workers workers={fleet.data.workers} now={now} />
          </>
        ) : (
          !fleet.error && <p className="muted loading">Loading the fleet…</p>
        )}
      </main>
    </div>
  );
}

function Header(props: { meta?: ConsoleMeta; live: boolean; onToggle: () => void; onRefresh: () => void; updatedAt?: number; now: number }) {
  return (
    <header className="topbar">
      <div className="brand">
        <span className="brand-name">DuckFleet</span>
        <span className="brand-sub">Fleet</span>
      </div>
      <div className="topbar-meta">
        {props.meta && (
          <span className="pill" title="The gateway this console reads, through the duckfleet CLI's credential">
            {props.meta.gateway}
            {props.meta.read_only && <span className="pill-tag">read-only</span>}
          </span>
        )}
        <span className="muted small" aria-live="polite">
          {props.updatedAt ? `Updated ${ago(props.updatedAt, props.now)}` : ""}
        </span>
        <button type="button" className="button" onClick={props.onRefresh}>
          Refresh
        </button>
        <button type="button" className="button" aria-pressed={props.live} onClick={props.onToggle}>
          {props.live ? "Pause" : "Resume"}
        </button>
      </div>
    </header>
  );
}

function ErrorBanner({ error, stale, updatedAt, now }: { error: Error; stale: boolean; updatedAt?: number; now: number }) {
  const session = error instanceof ApiError && error.failureKind === "ui_session_missing";
  return (
    <div className="banner" role="alert">
      <StatusIcon severity="critical" />
      <div>
        <strong>{session ? "Not signed in to this console" : "Could not refresh the fleet"}</strong>
        <p>
          {error.message}
          {error instanceof ApiError && error.status === 401 && !session && " The gateway rejected the CLI's credential; check it with `duckfleet config show`."}
        </p>
        {stale && <p className="muted small">Showing data from {ago(updatedAt, now)}.</p>}
      </div>
    </div>
  );
}

function Kpis({ snapshot }: { snapshot: Snapshot }) {
  const { pools, workers } = snapshot;
  const reports = workers.workers;
  const ready = reports.filter((w) => w.ready).length;
  const active = sum(reports, "active_queries");
  const queued = sum(reports, "queued_queries");
  const unreachable = workers.unreachable.length;
  return (
    <section className="kpis" aria-label="Fleet summary">
      <div className="tile">
        <span className="tile-label">Gateway sessions</span>
        <span className="tile-value">
          {count(pools.gateway_open_sessions)}
          <span className="tile-of"> of {count(pools.gateway_max_sessions)}</span>
        </span>
        <Meter value={pools.gateway_open_sessions} limit={pools.gateway_max_sessions} label="Gateway sessions" />
        {pools.gateway_closing_sessions > 0 && <span className="tile-foot">{count(pools.gateway_closing_sessions)} closing</span>}
      </div>
      <div className="tile">
        <span className="tile-label">Workers ready</span>
        <span className="tile-value">
          {count(ready)}
          <span className="tile-of"> of {count(reports.length)}</span>
        </span>
        {unreachable > 0 ? (
          <StatusBadge severity="critical" label={`${unreachable} unreachable`} />
        ) : (
          <span className="tile-foot">all reporting</span>
        )}
      </div>
      <div className="tile">
        <span className="tile-label">Running queries</span>
        <span className="tile-value">{count(active)}</span>
        <span className="tile-foot">preparing or executing</span>
      </div>
      <div className="tile">
        <span className="tile-label">Queued queries</span>
        <span className="tile-value">{count(queued)}</span>
        <span className="tile-foot">{bytes(sum(reports, "queued_bytes"))} waiting</span>
      </div>
      <div className="tile">
        <span className="tile-label">Isolated compute</span>
        <span className="tile-value tile-value-text">{pools.isolated_compute ? "Enabled" : "Off"}</span>
        <span className="tile-foot">observed {duration(workers.observation_age_ms)} ago</span>
      </div>
    </section>
  );
}

function Pools({ pools }: { pools: PoolsResponse }) {
  return (
    <section className="panel" aria-labelledby="pools-title">
      <div className="panel-head">
        <h2 id="pools-title">Persistent pools</h2>
        <span className="muted small">
          Open sessions against each pool&apos;s capacity · registry revision {pools.revision ?? "—"}
        </span>
      </div>
      {pools.pools.length === 0 ? (
        <p className="muted empty">No pools are configured.</p>
      ) : (
        <ul className="meter-list">
          {pools.pools.map((pool) => (
            <PoolRow key={pool.name} pool={pool} isDefault={pool.name === pools.default_pool} />
          ))}
        </ul>
      )}
    </section>
  );
}

function PoolRow({ pool, isDefault }: { pool: Pool; isDefault: boolean }) {
  const capacity = pool.max_sessions_per_worker == null ? null : pool.configured_workers * pool.max_sessions_per_worker;
  return (
    <li className="meter-row">
      <div className="meter-name">
        <span className="pool-name">
          {pool.name}
          {isDefault && <span className="pill-tag">default</span>}
        </span>
        <span className="muted small">
          {resources(pool.resources)} · {pool.eligible_workers} of {pool.configured_workers} workers eligible
          {pool.unreachable_workers > 0 && ` · ${pool.unreachable_workers} unreachable`}
        </span>
      </div>
      <div className="meter-bar">
        {capacity != null && capacity > 0 ? (
          <Meter value={pool.sampled_open_sessions} limit={capacity} label={`${pool.name} sessions`} />
        ) : (
          <span className="muted small">No per-worker session limit</span>
        )}
      </div>
      <div className="meter-figure">
        {count(pool.sampled_open_sessions)}
        {capacity != null && <span className="muted"> / {count(capacity)}</span>}
      </div>
    </li>
  );
}

function Workers({ workers, now }: { workers: WorkersResponse; now: number }) {
  const rows = [...workers.workers].sort((a, b) => (a.pool ?? "").localeCompare(b.pool ?? "") || a.endpoint.localeCompare(b.endpoint));
  return (
    <section className="panel" aria-labelledby="workers-title">
      <div className="panel-head">
        <h2 id="workers-title">Workers</h2>
        <span className="muted small">{rows.length} reporting endpoints</span>
      </div>
      {rows.length === 0 ? (
        <p className="muted empty">No workers are registered with this gateway.</p>
      ) : (
        <div className="table-scroll">
          <table className="table">
            <thead>
              <tr>
                <th scope="col">Endpoint</th>
                <th scope="col">Pool</th>
                <th scope="col">State</th>
                <th scope="col">Resources</th>
                <th scope="col" className="num">
                  Sessions
                </th>
                <th scope="col" className="num">
                  Running
                </th>
                <th scope="col" className="num">
                  Queued
                </th>
                <th scope="col">Last report</th>
              </tr>
            </thead>
            <tbody>
              {rows.map((worker) => (
                <WorkerRow key={worker.endpoint} worker={worker} now={now} />
              ))}
            </tbody>
          </table>
        </div>
      )}
    </section>
  );
}

function WorkerRow({ worker, now }: { worker: WorkerReport; now: number }) {
  const status = workerStatus(worker.state);
  return (
    <tr>
      <td className="mono">
        {worker.endpoint}
        {worker.worker_id && <div className="muted small">{worker.worker_id}</div>}
      </td>
      <td>{worker.pool ?? <span className="muted">—</span>}</td>
      <td>
        <StatusBadge severity={status.severity} label={status.label} />
      </td>
      <td>{resources(worker)}</td>
      <td className="num">
        {count(worker.open_sessions)}
        {worker.max_sessions != null && <span className="muted"> / {count(worker.max_sessions)}</span>}
      </td>
      <td className="num">{count(worker.active_queries)}</td>
      <td className="num">
        {count(worker.queued_queries)}
        {worker.queued_bytes ? <div className="muted small">{bytes(worker.queued_bytes)}</div> : null}
      </td>
      <td className="muted">{ago(worker.last_success_at, now)}</td>
    </tr>
  );
}

function sum(workers: WorkerReport[], key: "active_queries" | "queued_queries" | "queued_bytes"): number {
  return workers.reduce((total, worker) => total + (worker[key] ?? 0), 0);
}
