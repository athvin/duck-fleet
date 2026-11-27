// Status is always icon + label, never color alone.

export type Severity = "good" | "info" | "neutral" | "warning" | "serious" | "critical";

const ICONS: Record<Severity, React.ReactNode> = {
  good: <path d="M3.5 8.5 6.5 11.5 12.5 4.5" fill="none" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" />,
  info: <circle cx="8" cy="8" r="3.5" />,
  neutral: <circle cx="8" cy="8" r="3.5" fill="none" strokeWidth="2" />,
  warning: <path d="M8 2.5 14 13H2Z" strokeLinejoin="round" />,
  serious: <path d="M8 1.5 14.5 8 8 14.5 1.5 8Z" />,
  critical: <path d="M4 4 12 12M12 4 4 12" fill="none" strokeWidth="2.25" strokeLinecap="round" />,
};

export function StatusIcon({ severity }: { severity: Severity }) {
  return (
    <svg className={`status-icon sev-${severity}`} viewBox="0 0 16 16" width="14" height="14" aria-hidden="true">
      {ICONS[severity]}
    </svg>
  );
}

export function StatusBadge({ severity, label }: { severity: Severity; label: string }) {
  return (
    <span className={`badge badge-${severity}`}>
      <StatusIcon severity={severity} />
      {label}
    </span>
  );
}

/** Maps a worker's reported state to a severity and a human label. */
export function workerStatus(state: string | undefined): { severity: Severity; label: string } {
  switch (state) {
    case "ready":
      return { severity: "good", label: "Ready" };
    case "busy":
      return { severity: "info", label: "Busy" };
    case "starting":
      return { severity: "neutral", label: "Starting" };
    case "draining":
      return { severity: "warning", label: "Draining" };
    case "unavailable":
      return { severity: "serious", label: "Unavailable" };
    case "unreachable":
      return { severity: "critical", label: "Unreachable" };
    default:
      return { severity: "neutral", label: state ?? "Unknown" };
  }
}
