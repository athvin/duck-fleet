const compact = new Intl.NumberFormat(undefined, { notation: "compact", maximumFractionDigits: 1 });
const whole = new Intl.NumberFormat(undefined, { maximumFractionDigits: 0 });

export function count(value: number | null | undefined): string {
  if (value == null || Number.isNaN(value)) return "—";
  return value >= 10_000 ? compact.format(value) : whole.format(value);
}

/** Binary units, matching how Kubernetes and the gateway size workers. */
export function bytes(value: number | null | undefined): string {
  if (value == null) return "—";
  const units = ["B", "KiB", "MiB", "GiB", "TiB"];
  let scaled = value;
  let unit = 0;
  while (scaled >= 1024 && unit < units.length - 1) {
    scaled /= 1024;
    unit += 1;
  }
  const digits = scaled >= 100 || unit === 0 ? 0 : 1;
  return `${scaled.toFixed(digits).replace(/\.0$/, "")} ${units[unit]}`;
}

export function duration(ms: number | null | undefined): string {
  if (ms == null) return "—";
  if (ms < 1000) return `${Math.round(ms)} ms`;
  const seconds = ms / 1000;
  if (seconds < 60) return `${seconds.toFixed(seconds < 10 ? 1 : 0)} s`;
  const minutes = seconds / 60;
  if (minutes < 60) return `${Math.round(minutes)} min`;
  return `${(minutes / 60).toFixed(1)} h`;
}

export function ago(epochMs: number | null | undefined, now: number): string {
  if (epochMs == null) return "never";
  const elapsed = Math.max(0, now - epochMs);
  return elapsed < 1500 ? "just now" : `${duration(elapsed)} ago`;
}

export function resources(r: { cpu?: number; memory_bytes?: number; disk_bytes?: number }): string {
  const parts = [];
  if (r.cpu != null) parts.push(`${r.cpu} CPU`);
  if (r.memory_bytes != null) parts.push(bytes(r.memory_bytes));
  if (r.disk_bytes != null) parts.push(`${bytes(r.disk_bytes)} disk`);
  return parts.length ? parts.join(" · ") : "—";
}
