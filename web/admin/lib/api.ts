// Types for the gateway endpoints the console reads. They mirror the JSON built
// in server/src/gateway.rs (`workers`, `pool_discovery`) and the worker's own
// status snapshot (server/src/worker_status.rs). Every field the gateway can
// omit is optional here, so a partial report renders instead of throwing.

export type Resources = {
  cpu?: number;
  memory_bytes?: number;
  disk_bytes?: number;
};

export type WorkerState = "starting" | "ready" | "busy" | "draining" | "unavailable" | "unreachable";

export type WorkerReport = Resources & {
  endpoint: string;
  state?: WorkerState | string;
  ready?: boolean;
  draining?: boolean;
  worker_id?: string;
  pool?: string | null;
  max_sessions?: number | null;
  open_sessions?: number;
  active_queries?: number;
  preparing_queries?: number;
  executing_queries?: number;
  queued_queries?: number;
  queued_bytes?: number;
  /** Milliseconds since the Unix epoch, stamped by the gateway. */
  last_success_at?: number | null;
};

export type WorkersResponse = {
  workers: WorkerReport[];
  unreachable: string[];
  registry_revision?: number | string;
  isolated_compute: boolean;
  observation_age_ms: number;
};

export type AdmissionLimits = {
  max_queued_queries?: number;
  max_running_queries?: number;
  max_queued_per_principal?: number;
  max_running_per_principal?: number;
};

export type Pool = {
  name: string;
  resources: Resources;
  configured_workers: number;
  eligible_workers: number;
  sampled_workers: number;
  sampled_open_sessions: number;
  unreachable_workers: number;
  max_sessions_per_worker: number | null;
  endpoints: string[];
  admission?: AdmissionLimits;
};

export type PoolsResponse = {
  default_pool: string;
  revision?: number | string;
  pools: Pool[];
  gateway_max_sessions: number;
  gateway_open_sessions: number;
  gateway_closing_sessions: number;
  isolated_compute: boolean;
  observation_age_ms: number;
};

/** What `duckfleet ui` reports about the connection it is proxying. */
export type ConsoleMeta = {
  gateway: string;
  cli_version: string;
  proxy: string;
  read_only: boolean;
};

/** The gateway's classified error shape, plus the console's own failures. */
export class ApiError extends Error {
  constructor(
    message: string,
    readonly status: number,
    readonly failureKind?: string,
  ) {
    super(message);
  }
}

async function getJson<T>(path: string, signal?: AbortSignal): Promise<T> {
  let response: Response;
  try {
    response = await fetch(path, {
      headers: { Accept: "application/json" },
      credentials: "same-origin",
      cache: "no-store",
      signal,
    });
  } catch (error) {
    if (error instanceof DOMException && error.name === "AbortError") throw error;
    throw new ApiError("The console server is not reachable. Is `duckfleet ui` still running?", 0);
  }
  const text = await response.text();
  let body: unknown = undefined;
  try {
    body = text ? JSON.parse(text) : undefined;
  } catch {
    // Non-JSON bodies are reported by status below.
  }
  if (!response.ok) {
    const detail = body as { error?: string; failure_kind?: string } | undefined;
    throw new ApiError(detail?.error ?? `${path} returned HTTP ${response.status}`, response.status, detail?.failure_kind);
  }
  return body as T;
}

export const api = {
  meta: (signal?: AbortSignal) => getJson<ConsoleMeta>("/_duckfleet/meta", signal),
  workers: (signal?: AbortSignal) => getJson<WorkersResponse>("/v1/workers", signal),
  pools: (signal?: AbortSignal) => getJson<PoolsResponse>("/v1/pools", signal),
};
