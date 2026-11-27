//! `duckfleet advise`: advisory compute sizing from retained evidence.
//!
//! Only authorized metadata reads (`/v1/queries/<id>` and `/v1/queries`) are
//! made: no session is opened, no pod is created, and no allocation, pool,
//! grant, or dbt file is changed. Every recommendation names its source query
//! IDs and the observed metrics it relies on, and falls back to
//! `insufficient_evidence` rather than inventing precision.

use crate::client::{Client, RemoteError};
use serde_json::{Map, Value, json};
use std::collections::BTreeMap;
use std::time::Duration;

#[derive(Clone, Copy, Debug)]
pub struct Thresholds {
    /// Queue time share of the observed total above which queueing dominates.
    pub queue_share: f64,
    /// Isolated preparation share above which cold start dominates.
    pub startup_share: f64,
    /// Engine peak buffer memory as a share of the DuckDB limit that counts as pressure.
    pub memory_share: f64,
    /// Engine CPU time per allocated thread-second that counts as saturation.
    pub cpu_share: f64,
    /// Samples needed before a recommendation is unqualified.
    pub min_samples: usize,
    /// Coefficient of variation of engine time above which evidence is too noisy.
    pub max_variation: f64,
}

impl Default for Thresholds {
    fn default() -> Self {
        Thresholds {
            queue_share: 0.5,
            startup_share: 0.5,
            memory_share: 0.8,
            cpu_share: 0.8,
            min_samples: 3,
            max_variation: 0.5,
        }
    }
}

impl Thresholds {
    /// Parses `name=value,...` overrides.
    pub fn parse(text: Option<&str>) -> Result<Self, String> {
        let mut thresholds = Thresholds::default();
        for pair in text
            .into_iter()
            .flat_map(|t| t.split(','))
            .filter(|p| !p.is_empty())
        {
            let (name, value) = pair
                .split_once('=')
                .ok_or_else(|| format!("threshold {pair:?} must be name=value"))?;
            let share = || {
                value
                    .parse::<f64>()
                    .ok()
                    .filter(|v| *v > 0.0 && *v <= 1.0)
                    .ok_or_else(|| format!("threshold {name} must be a number in (0, 1]"))
            };
            match name {
                "queue_share" => thresholds.queue_share = share()?,
                "startup_share" => thresholds.startup_share = share()?,
                "memory_share" => thresholds.memory_share = share()?,
                "cpu_share" => thresholds.cpu_share = share()?,
                "max_variation" => {
                    thresholds.max_variation = value
                        .parse::<f64>()
                        .ok()
                        .filter(|v| *v > 0.0 && *v <= 10.0)
                        .ok_or("threshold max_variation must be in (0, 10]")?
                }
                "min_samples" => {
                    thresholds.min_samples = value
                        .parse::<usize>()
                        .ok()
                        .filter(|v| (1..=100).contains(v))
                        .ok_or("threshold min_samples must be 1-100")?
                }
                other => return Err(format!("unknown threshold {other:?}")),
            }
        }
        Ok(thresholds)
    }
}

pub enum Subject {
    Query { id: uuid::Uuid, samples: u32 },
    Model { id: String, samples: u32 },
}

pub async fn run(
    url: &str,
    token: &str,
    subject: Subject,
    thresholds: Thresholds,
    format: &str,
) -> Result<(), RemoteError> {
    let client = Client::new(url, token)?;
    let (subject_json, records) = match subject {
        Subject::Query { id, samples } => {
            let detail = get(&client, &format!("/v1/queries/{id}")).await?;
            let mut records = vec![detail.clone()];
            if let Some(fingerprint) = detail["query_fingerprint"].as_str() {
                let peers = get(
                    &client,
                    &format!(
                        "/v1/queries?status=succeeded&limit={samples}&fingerprint={fingerprint}"
                    ),
                )
                .await?;
                for peer in peers["queries"].as_array().into_iter().flatten() {
                    if peer["query_id"] != detail["query_id"] {
                        records.push(detailed(&client, peer).await?);
                    }
                }
            }
            (
                json!({"query_id": id, "query_fingerprint": detail["query_fingerprint"]}),
                records,
            )
        }
        Subject::Model { id, samples } => {
            let path = format!(
                "/v1/queries?status=succeeded&limit={samples}&model_id={}",
                encode(&id)
            );
            let page = get(&client, &path).await?;
            let mut records = Vec::new();
            for record in page["queries"].as_array().into_iter().flatten() {
                records.push(detailed(&client, record).await?);
            }
            (json!({"model_id": id}), records)
        }
    };
    let report = advise(subject_json, &records, &thresholds);
    if format == "json" {
        println!(
            "{}",
            serde_json::to_string_pretty(&report).unwrap_or_default()
        );
    } else {
        print!("{}", render(&report));
    }
    Ok(())
}

/// Builds the advisory report. Records are grouped by resolved allocation so
/// evidence from different compute is never blended.
pub fn advise(subject: Value, records: &[Value], thresholds: &Thresholds) -> Value {
    let mut groups: BTreeMap<String, Vec<Observation>> = BTreeMap::new();
    let mut skipped = Vec::new();
    for record in records {
        match Observation::from_record(record) {
            Ok(observation) => groups
                .entry(observation.allocation_key())
                .or_default()
                .push(observation),
            Err(reason) => skipped.push(json!({"query_id": record["query_id"], "reason": reason})),
        }
    }
    let mixed = groups.len() > 1;
    let mut assessed = groups
        .into_values()
        .map(|group| assess(&group, thresholds, mixed))
        .collect::<Vec<_>>();
    if assessed.is_empty() {
        assessed.push(json!({
            "allocation": Value::Null,
            "sample_count": 0,
            "source_query_ids": [],
            "recommendation": {
                "kind": "insufficient_evidence",
                "confidence": "none",
                "summary": "no retained record has complete timing and allocation evidence",
                "next_experiment": "run the workload again with profiling enabled and retained history",
                "tradeoff": "none; nothing was changed",
            }
        }));
    }
    json!({
        "subject": subject,
        "groups": assessed,
        "skipped": skipped,
        "mixed_allocations": mixed,
        "thresholds": {
            "queue_share": thresholds.queue_share,
            "startup_share": thresholds.startup_share,
            "memory_share": thresholds.memory_share,
            "cpu_share": thresholds.cpu_share,
            "min_samples": thresholds.min_samples,
            "max_variation": thresholds.max_variation,
        },
        "assumptions": [
            "engine memory and spill are DuckDB engine metrics, not container RSS",
            "nothing here estimates provider cost or promises a speedup on other hardware",
            "result delivery to the client is not measured in history and is reported as unknown",
            "advice is read-only: no compute, pool, grant, or dbt metadata was changed",
        ],
    })
}

#[derive(Clone, Debug)]
struct Observation {
    query_id: String,
    mode: String,
    pool: Option<String>,
    cpu: u64,
    memory_bytes: u64,
    disk_bytes: u64,
    threads: u64,
    engine_memory_limit: Option<u64>,
    queue_ms: f64,
    preparation_ms: f64,
    engine_ms: f64,
    profile: Option<Profile>,
}

#[derive(Clone, Debug)]
struct Profile {
    cpu_seconds: f64,
    latency_seconds: Option<f64>,
    peak_buffer_bytes: Option<u64>,
    peak_temp_bytes: Option<u64>,
}

impl Observation {
    fn from_record(record: &Value) -> Result<Self, String> {
        if record["status"] != "succeeded" {
            return Err(format!(
                "status {} is not a completed run",
                record["status"]
            ));
        }
        let time = |name: &str| {
            record[name]
                .as_str()
                .and_then(parse_time)
                .ok_or_else(|| format!("{name} is unknown"))
        };
        let queued = time("queued_at").or_else(|_| time("accepted_at"))?;
        let selected = time("selected_at")?;
        let engine = time("engine_started_at")?;
        let finished = time("finished_at")?;
        let span = |from: f64, to: f64| (to >= from).then_some(to - from);
        let resolved = &record["placement"]["resolved_compute"];
        let resources = &resolved["resources"];
        let number = |value: &Value| value.as_u64();
        let mode = resolved["mode"]
            .as_str()
            .or_else(|| record["selected_mode"].as_str())
            .ok_or("resolved allocation is unknown")?
            .to_owned();
        let profile = (record["profile_status"] == "captured")
            .then(|| record["profile_cpu_seconds"].as_f64())
            .flatten()
            .map(|cpu_seconds| {
                let system = &record["profile"]["system"];
                Profile {
                    cpu_seconds,
                    latency_seconds: record["profile_latency_seconds"]
                        .as_f64()
                        .or_else(|| record["profile"]["query"]["total_time"].as_f64())
                        .or_else(|| record["metrics"]["latency"].as_f64()),
                    peak_buffer_bytes: number(&record["peak_buffer_memory_bytes"])
                        .or_else(|| number(&system["peak_buffer_memory"])),
                    peak_temp_bytes: number(&record["peak_temp_dir_bytes"])
                        .or_else(|| number(&system["peak_temp_dir_size"]))
                        .max(number(&system["total_bytes_spilled"])),
                }
            });
        Ok(Observation {
            query_id: record["query_id"].as_str().unwrap_or_default().to_owned(),
            pool: resolved["pool"].as_str().map(str::to_owned),
            cpu: number(&resources["cpu"])
                .or_else(|| number(&record["selected_cpu"]))
                .ok_or("allocated cpu is unknown")?,
            memory_bytes: number(&resources["memory_bytes"])
                .or_else(|| number(&record["selected_memory_bytes"]))
                .ok_or("allocated memory is unknown")?,
            disk_bytes: number(&resources["disk_bytes"])
                .or_else(|| number(&record["selected_disk_bytes"]))
                .unwrap_or(0),
            threads: number(&record["placement"]["duckdb_threads"])
                .unwrap_or(1)
                .max(1),
            engine_memory_limit: record["placement"]["duckdb_memory_limit"]
                .as_str()
                .and_then(parse_size),
            queue_ms: span(queued, selected).ok_or("queue timestamps are out of order")?,
            preparation_ms: span(selected, engine)
                .ok_or("preparation timestamps are out of order")?,
            engine_ms: span(engine, finished).ok_or("engine timestamps are out of order")?,
            mode,
            profile,
        })
    }

    fn allocation_key(&self) -> String {
        format!(
            "{}/{}/{}/{}/{}",
            self.mode,
            self.pool.as_deref().unwrap_or("-"),
            self.cpu,
            self.memory_bytes,
            self.disk_bytes
        )
    }
}

fn assess(group: &[Observation], thresholds: &Thresholds, mixed: bool) -> Value {
    let first = &group[0];
    let median = |values: Vec<f64>| -> f64 {
        let mut values = values;
        values.sort_by(f64::total_cmp);
        let middle = values.len() / 2;
        if values.len().is_multiple_of(2) {
            (values[middle - 1] + values[middle]) / 2.0
        } else {
            values[middle]
        }
    };
    let queue = median(group.iter().map(|o| o.queue_ms).collect());
    let preparation = median(group.iter().map(|o| o.preparation_ms).collect());
    let engine = median(group.iter().map(|o| o.engine_ms).collect());
    let total = queue + preparation + engine;
    let variation =
        coefficient_of_variation(&group.iter().map(|o| o.engine_ms).collect::<Vec<_>>());
    let profiled = group
        .iter()
        .filter_map(|o| o.profile.as_ref().map(|p| (o, p)))
        .collect::<Vec<_>>();
    let cpu_utilization = (!profiled.is_empty()).then(|| {
        median(
            profiled
                .iter()
                .map(|(o, p)| {
                    let seconds = p.latency_seconds.unwrap_or(o.engine_ms / 1000.0).max(0.001);
                    p.cpu_seconds / (seconds * o.threads as f64)
                })
                .collect(),
        )
    });
    let peak_memory = profiled
        .iter()
        .filter_map(|(_, p)| p.peak_buffer_bytes)
        .max();
    let peak_spill = profiled.iter().filter_map(|(_, p)| p.peak_temp_bytes).max();
    let memory_share = peak_memory
        .zip(first.engine_memory_limit)
        .map(|(peak, limit)| peak as f64 / limit.max(1) as f64);
    let few = group.len() < thresholds.min_samples;
    let noisy = variation.is_some_and(|cv| cv > thresholds.max_variation);
    let mut qualifications = Vec::new();
    if few {
        qualifications.push(format!(
            "only {} sample(s); {} are needed for an unqualified recommendation",
            group.len(),
            thresholds.min_samples
        ));
    }
    if noisy {
        qualifications.push(format!(
            "engine time varies widely (coefficient of variation {:.2})",
            variation.unwrap_or_default()
        ));
    }
    if mixed {
        qualifications
            .push("history mixes allocations; this group covers one allocation only".into());
    }
    if profiled.len() < group.len() {
        qualifications.push(format!(
            "{} of {} sample(s) have no captured profile (sampled, expired, or not visible)",
            group.len() - profiled.len(),
            group.len()
        ));
    }
    let memory_unknown = !profiled.is_empty() && peak_memory.is_none() && peak_spill.is_none();
    if memory_unknown {
        qualifications.push(
            "engine memory and spill are not visible (profile detail withheld or not captured)"
                .into(),
        );
    }
    let share = |part: f64| if total > 0.0 { part / total } else { 0.0 };
    let gib = |bytes: u64| format!("{:.1}Gi", bytes as f64 / (1u64 << 30) as f64);
    let (kind, summary, next, tradeoff) = if share(queue) >= thresholds.queue_share {
        (
            "investigate_queueing",
            format!(
                "queue wait is {:.0}% of observed time ({queue:.0} ms of {total:.0} ms); larger DuckDB resources would not shorten it",
                share(queue) * 100.0
            ),
            "inspect pool capacity and concurrent work (`duckfleet pools`, `duckfleet history`), or run at a quieter time, before changing resources".to_owned(),
            "adding pool capacity uses more standing compute; resizing this query would not help".to_owned(),
        )
    } else if first.mode == "isolated" && share(preparation) >= thresholds.startup_share {
        (
            "isolated_startup_dominated",
            format!(
                "isolated startup is {:.0}% of observed time ({preparation:.0} ms) while SQL execution took {engine:.0} ms",
                share(preparation) * 100.0
            ),
            "for repeated short runs, try a persistent pool explicitly and compare total time"
                .to_owned(),
            "a persistent pool avoids cold start but keeps capacity running between runs"
                .to_owned(),
        )
    } else if profiled.is_empty() {
        (
            "insufficient_evidence",
            "no captured engine profile, so CPU and memory pressure cannot be judged".to_owned(),
            "rerun with profiling enabled and keep its history retained".to_owned(),
            "none; nothing was changed".to_owned(),
        )
    } else if peak_spill.is_some_and(|bytes| bytes > 0)
        || memory_share.is_some_and(|share| share >= thresholds.memory_share)
    {
        let next_memory = first.memory_bytes.saturating_mul(2);
        (
            "try_more_memory",
            format!(
                "engine memory pressure: peak buffer {} of a {} DuckDB limit{}",
                peak_memory.map(gib).unwrap_or_else(|| "unknown".into()),
                first
                    .engine_memory_limit
                    .map(gib)
                    .unwrap_or_else(|| "unknown".into()),
                peak_spill
                    .filter(|bytes| *bytes > 0)
                    .map(|bytes| format!(", spilled {} to disk", gib(bytes)))
                    .unwrap_or_default()
            ),
            format!(
                "rerun explicitly with --memory {} (currently {}) and compare engine time and spill",
                gib(next_memory),
                gib(first.memory_bytes)
            ),
            "more memory reserves a larger allocation and may need isolated compute (cold start)"
                .to_owned(),
        )
    } else if cpu_utilization.is_some_and(|u| u >= thresholds.cpu_share) {
        (
            "try_more_cpu",
            format!(
                "engine CPU is saturated ({:.0}% of {} allocated thread(s)) without memory pressure",
                cpu_utilization.unwrap_or_default() * 100.0,
                first.threads
            ),
            format!(
                "rerun explicitly with --cpu {} (currently {}) and compare engine time",
                first.cpu * 2,
                first.cpu
            ),
            "more CPU reserves a larger allocation; parallel speedup depends on the plan"
                .to_owned(),
        )
    } else {
        (
            "keep_current_allocation",
            format!(
                "no queue, startup, memory, or CPU pressure crossed its threshold (engine {engine:.0} ms)"
            ),
            "keep the current allocation; revisit if data volume grows".to_owned(),
            "none".to_owned(),
        )
    };
    let confidence = if kind == "insufficient_evidence" {
        "none"
    } else if few || noisy || mixed || (memory_unknown && kind == "try_more_cpu") {
        "qualified"
    } else {
        "supported"
    };
    let mut allocation = Map::new();
    allocation.insert("mode".into(), json!(first.mode));
    allocation.insert("pool".into(), json!(first.pool));
    allocation.insert("cpu".into(), json!(first.cpu));
    allocation.insert("memory_bytes".into(), json!(first.memory_bytes));
    allocation.insert("disk_bytes".into(), json!(first.disk_bytes));
    allocation.insert("duckdb_threads".into(), json!(first.threads));
    allocation.insert(
        "duckdb_memory_limit_bytes".into(),
        json!(first.engine_memory_limit),
    );
    json!({
        "allocation": allocation,
        "sample_count": group.len(),
        "profiled_samples": profiled.len(),
        "source_query_ids": group.iter().map(|o| o.query_id.clone()).collect::<Vec<_>>(),
        "phases_ms": {
            "queue": queue,
            "preparation": preparation,
            "preparation_includes_isolated_startup": first.mode == "isolated",
            "engine": engine,
            "delivery": Value::Null,
        },
        "evidence": {
            "engine_ms_coefficient_of_variation": variation,
            "cpu_utilization": cpu_utilization,
            "peak_buffer_memory_bytes": peak_memory,
            "peak_buffer_memory_share": memory_share,
            "peak_temp_dir_bytes": peak_spill,
        },
        "recommendation": {
            "kind": kind,
            "confidence": confidence,
            "summary": summary,
            "qualifications": qualifications,
            "next_experiment": next,
            "tradeoff": tradeoff,
        },
    })
}

fn coefficient_of_variation(values: &[f64]) -> Option<f64> {
    if values.len() < 2 {
        return None;
    }
    let mean = values.iter().sum::<f64>() / values.len() as f64;
    if mean <= 0.0 {
        return None;
    }
    let variance =
        values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / (values.len() - 1) as f64;
    Some(variance.sqrt() / mean)
}

fn render(report: &Value) -> String {
    let mut out = String::new();
    for group in report["groups"].as_array().into_iter().flatten() {
        let recommendation = &group["recommendation"];
        out.push_str(&format!(
            "recommendation: {} ({})\n  {}\n",
            recommendation["kind"].as_str().unwrap_or_default(),
            recommendation["confidence"].as_str().unwrap_or_default(),
            recommendation["summary"].as_str().unwrap_or_default()
        ));
        for qualification in recommendation["qualifications"]
            .as_array()
            .into_iter()
            .flatten()
        {
            out.push_str(&format!(
                "  qualified: {}\n",
                qualification.as_str().unwrap_or_default()
            ));
        }
        out.push_str(&format!(
            "  next experiment: {}\n  tradeoff: {}\n",
            recommendation["next_experiment"]
                .as_str()
                .unwrap_or_default(),
            recommendation["tradeoff"].as_str().unwrap_or_default()
        ));
        if !group["allocation"].is_null() {
            let a = &group["allocation"];
            out.push_str(&format!(
                "  allocation: mode={} pool={} cpu={} memory_bytes={} duckdb_threads={}\n",
                a["mode"].as_str().unwrap_or("-"),
                a["pool"].as_str().unwrap_or("-"),
                a["cpu"],
                a["memory_bytes"],
                a["duckdb_threads"]
            ));
            let p = &group["phases_ms"];
            out.push_str(&format!(
                "  phases_ms: queue={:.0} preparation={:.0} engine={:.0} delivery=unknown\n",
                p["queue"].as_f64().unwrap_or_default(),
                p["preparation"].as_f64().unwrap_or_default(),
                p["engine"].as_f64().unwrap_or_default()
            ));
            out.push_str(&format!("  evidence: {}\n", group["evidence"]));
        }
        out.push_str(&format!(
            "  samples: {} ({} profiled); source query IDs: {}\n",
            group["sample_count"],
            group["profiled_samples"].as_u64().unwrap_or(0),
            group["source_query_ids"]
                .as_array()
                .map(|ids| ids
                    .iter()
                    .filter_map(Value::as_str)
                    .collect::<Vec<_>>()
                    .join(", "))
                .unwrap_or_default()
        ));
    }
    for skipped in report["skipped"].as_array().into_iter().flatten() {
        out.push_str(&format!(
            "skipped {}: {}\n",
            skipped["query_id"].as_str().unwrap_or("?"),
            skipped["reason"].as_str().unwrap_or_default()
        ));
    }
    out.push_str("scope: DuckDB engine metrics only; not container RSS or provider cost; nothing was changed\n");
    out
}

/// Engine memory and spill live in the retained profile, which only the
/// per-query detail returns, and only to callers allowed to see it.
async fn detailed(client: &Client, record: &Value) -> Result<Value, RemoteError> {
    match record["query_id"].as_str() {
        Some(id) => get(client, &format!("/v1/queries/{id}")).await,
        None => Ok(record.clone()),
    }
}

async fn get(client: &Client, path: &str) -> Result<Value, RemoteError> {
    let bytes = client
        .request(reqwest::Method::GET, path, None, Duration::from_secs(15))
        .await?;
    serde_json::from_slice(&bytes).map_err(|_| RemoteError {
        status: None,
        failure_kind: Some("invalid_response".into()),
        retry_safe: true,
        message: "history API returned invalid JSON".into(),
        request_id: None,
        acceptance: None,
        status_url: None,
    })
}

fn encode(value: &str) -> String {
    value
        .bytes()
        .map(|b| {
            if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'~') {
                (b as char).to_string()
            } else {
                format!("%{b:02X}")
            }
        })
        .collect()
}

fn parse_time(text: &str) -> Option<f64> {
    let normalized = if text.contains('T') {
        text.to_owned()
    } else {
        text.replacen(' ', "T", 1)
    };
    let normalized = if normalized.ends_with("+00") {
        format!("{normalized}:00")
    } else {
        normalized
    };
    chrono::DateTime::parse_from_rfc3339(&normalized)
        .ok()
        .map(|time| time.timestamp_micros() as f64 / 1000.0)
}

fn parse_size(text: &str) -> Option<u64> {
    let (number, unit) = text.trim().split_once(' ')?;
    let number = number.parse::<f64>().ok()?;
    let scale = match unit {
        "bytes" | "B" => 1.0,
        "KiB" => 1024.0,
        "MiB" => 1024.0 * 1024.0,
        "GiB" => 1024.0 * 1024.0 * 1024.0,
        "TiB" => 1024.0_f64.powi(4),
        _ => return None,
    };
    Some((number * scale) as u64)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(
        mode: &str,
        queue: i64,
        preparation: i64,
        engine: i64,
        cpu_seconds: Option<f64>,
    ) -> Value {
        let base = chrono::DateTime::parse_from_rfc3339("2026-01-01T00:00:00+00:00").unwrap();
        let at = |ms: i64| (base + chrono::Duration::milliseconds(ms)).to_rfc3339();
        json!({
            "query_id": uuid::Uuid::new_v4().to_string(),
            "status": "succeeded",
            "queued_at": at(0),
            "selected_at": at(queue),
            "engine_started_at": at(queue + preparation),
            "finished_at": at(queue + preparation + engine),
            "profile_status": if cpu_seconds.is_some() { "captured" } else { "sampled_out" },
            "profile_cpu_seconds": cpu_seconds,
            "peak_buffer_memory_bytes": 10_000_000,
            "peak_temp_dir_bytes": 0,
            "placement": {
                "duckdb_threads": 1,
                "duckdb_memory_limit": "512.0 MiB",
                "resolved_compute": {"mode": mode, "pool": "small",
                    "resources": {"cpu": 1, "memory_bytes": 1073741824_u64, "disk_bytes": 2147483648_u64}}
            }
        })
    }

    fn kind(report: &Value) -> (&str, &str) {
        let r = &report["groups"][0]["recommendation"];
        (
            r["kind"].as_str().unwrap(),
            r["confidence"].as_str().unwrap(),
        )
    }

    #[test]
    fn queue_dominated_runs_point_at_capacity_not_resources() {
        let records = vec![record("persistent", 9000, 5, 500, Some(0.45)); 3];
        let report = advise(json!({}), &records, &Thresholds::default());
        assert_eq!(kind(&report), ("investigate_queueing", "supported"));
    }

    #[test]
    fn isolated_cold_start_is_reported_separately_from_sql() {
        let records = vec![record("isolated", 10, 40_000, 800, Some(0.7)); 3];
        let report = advise(json!({}), &records, &Thresholds::default());
        assert_eq!(kind(&report), ("isolated_startup_dominated", "supported"));
        assert_eq!(
            report["groups"][0]["phases_ms"]["preparation_includes_isolated_startup"],
            true
        );
    }

    #[test]
    fn memory_and_cpu_pressure_get_distinct_bounded_recommendations() {
        let mut spilled = record("persistent", 5, 5, 10_000, Some(4.0));
        spilled["peak_temp_dir_bytes"] = json!(2_000_000_000_u64);
        let report = advise(json!({}), &vec![spilled; 3], &Thresholds::default());
        assert_eq!(kind(&report).0, "try_more_memory");
        assert!(
            report["groups"][0]["recommendation"]["next_experiment"]
                .as_str()
                .unwrap()
                .contains("--memory 2.0Gi")
        );
        let cpu = record("persistent", 5, 5, 10_000, Some(9.5));
        let report = advise(json!({}), &vec![cpu; 3], &Thresholds::default());
        assert_eq!(kind(&report).0, "try_more_cpu");
        let idle = record("persistent", 5, 5, 10_000, Some(1.0));
        let report = advise(json!({}), &vec![idle; 3], &Thresholds::default());
        assert_eq!(kind(&report), ("keep_current_allocation", "supported"));
    }

    #[test]
    fn weak_evidence_is_qualified_or_insufficient_never_precise() {
        let one = vec![record("persistent", 5, 5, 10_000, Some(9.5))];
        assert_eq!(
            kind(&advise(json!({}), &one, &Thresholds::default())),
            ("try_more_cpu", "qualified")
        );
        let unprofiled = vec![record("persistent", 5, 5, 10_000, None); 3];
        assert_eq!(
            kind(&advise(json!({}), &unprofiled, &Thresholds::default())).0,
            "insufficient_evidence"
        );
        let noisy = vec![
            record("persistent", 5, 5, 1_000, Some(0.1)),
            record("persistent", 5, 5, 20_000, Some(1.0)),
            record("persistent", 5, 5, 3_000, Some(0.2)),
        ];
        assert_eq!(
            kind(&advise(json!({}), &noisy, &Thresholds::default())).1,
            "qualified"
        );
        let mut unknown = record("persistent", 5, 5, 10, Some(0.1));
        unknown["engine_started_at"] = Value::Null;
        let report = advise(json!({}), &[unknown], &Thresholds::default());
        assert_eq!(kind(&report).0, "insufficient_evidence");
        assert_eq!(report["skipped"].as_array().unwrap().len(), 1);
    }

    #[test]
    fn mixed_allocations_are_grouped_not_blended() {
        let mut other = record("persistent", 5, 5, 10_000, Some(1.0));
        other["placement"]["resolved_compute"]["resources"]["cpu"] = json!(4);
        let records = vec![record("persistent", 5, 5, 10_000, Some(9.5)), other];
        let report = advise(json!({}), &records, &Thresholds::default());
        assert_eq!(report["mixed_allocations"], true);
        assert_eq!(report["groups"].as_array().unwrap().len(), 2);
        assert!(
            report["groups"]
                .as_array()
                .unwrap()
                .iter()
                .all(|g| g["recommendation"]["confidence"] != "supported")
        );
    }

    #[test]
    fn thresholds_parse_and_reject_out_of_range_values() {
        let parsed = Thresholds::parse(Some("queue_share=0.3,min_samples=5")).unwrap();
        assert_eq!((parsed.queue_share, parsed.min_samples), (0.3, 5));
        assert!(Thresholds::parse(Some("queue_share=2")).is_err());
        assert!(Thresholds::parse(Some("bogus=1")).is_err());
        assert_eq!(parse_size("512.0 MiB"), Some(512 * 1024 * 1024));
        assert!(parse_time("2026-11-26 19:30:23.762972+00").is_some());
    }
}
