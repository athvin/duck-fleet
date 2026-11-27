import { StatusIcon, type Severity } from "./Status";

/** Usage at or above these shares of the limit changes the fill's severity. */
export const WARN_AT = 0.8;
export const CRITICAL_AT = 0.95;

export function meterSeverity(share: number): Severity {
  if (share >= CRITICAL_AT) return "critical";
  if (share >= WARN_AT) return "warning";
  return "info";
}

/**
 * One ratio against a limit. The track is a lighter step of the fill's ramp so
 * the state reads across the whole bar; near the limit the fill moves to the
 * status colors and an icon with a label appears beside the figure.
 */
export function Meter({ value, limit, label }: { value: number; limit: number; label: string }) {
  const share = limit > 0 ? Math.min(1, value / limit) : 0;
  const severity = meterSeverity(share);
  const percent = Math.round(share * 100);
  return (
    <div className="meter" title={`${label}: ${value} of ${limit} (${percent}%)`}>
      <div
        className={`meter-track track-${severity}`}
        role="meter"
        aria-label={label}
        aria-valuemin={0}
        aria-valuemax={limit}
        aria-valuenow={value}
      >
        <div className={`meter-fill fill-${severity}`} style={{ width: `${share * 100}%` }} />
      </div>
      {severity !== "info" && (
        <span className="meter-note">
          <StatusIcon severity={severity} />
          {severity === "critical" ? "At limit" : "Near limit"}
        </span>
      )}
    </div>
  );
}
