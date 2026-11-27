"use client";

import { useCallback, useEffect, useRef, useState } from "react";

export type Polled<T> = {
  data: T | undefined;
  error: Error | undefined;
  /** When `data` was last fetched successfully, in epoch ms. */
  updatedAt: number | undefined;
  loading: boolean;
  refresh: () => void;
};

/**
 * Fetches on an interval while `active`, and pauses while the tab is hidden so
 * an idle console costs the gateway nothing. A failed fetch keeps the last good
 * data, so the page shows stale numbers with a warning instead of going blank.
 */
export function usePolling<T>(load: (signal: AbortSignal) => Promise<T>, intervalMs: number, active: boolean): Polled<T> {
  const [data, setData] = useState<T>();
  const [error, setError] = useState<Error>();
  const [updatedAt, setUpdatedAt] = useState<number>();
  const [loading, setLoading] = useState(true);
  const loadRef = useRef(load);
  loadRef.current = load;
  const inflight = useRef<AbortController | null>(null);

  const refresh = useCallback(() => {
    inflight.current?.abort();
    const controller = new AbortController();
    inflight.current = controller;
    setLoading(true);
    loadRef
      .current(controller.signal)
      .then((value) => {
        if (controller.signal.aborted) return;
        setData(value);
        setError(undefined);
        setUpdatedAt(Date.now());
      })
      .catch((reason: unknown) => {
        if (controller.signal.aborted) return;
        setError(reason instanceof Error ? reason : new Error(String(reason)));
      })
      .finally(() => {
        if (!controller.signal.aborted) setLoading(false);
      });
  }, []);

  useEffect(() => {
    if (!active) return;
    let timer: ReturnType<typeof setInterval> | undefined;
    const start = () => {
      if (timer || document.visibilityState === "hidden") return;
      refresh();
      timer = setInterval(refresh, intervalMs);
    };
    const stop = () => {
      if (timer) clearInterval(timer);
      timer = undefined;
    };
    const onVisibility = () => (document.visibilityState === "hidden" ? stop() : start());
    start();
    document.addEventListener("visibilitychange", onVisibility);
    return () => {
      stop();
      document.removeEventListener("visibilitychange", onVisibility);
      inflight.current?.abort();
    };
  }, [active, intervalMs, refresh]);

  return { data, error, updatedAt, loading, refresh };
}

/** A clock that ticks once a second, for "updated 3 s ago" labels. */
export function useNow(): number {
  const [now, setNow] = useState(() => Date.now());
  useEffect(() => {
    const timer = setInterval(() => setNow(Date.now()), 1000);
    return () => clearInterval(timer);
  }, []);
  return now;
}
