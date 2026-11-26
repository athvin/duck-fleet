use crate::client::{Client, RemoteError};
use serde_json::Value;
use std::collections::BTreeSet;
use std::time::Duration;

pub struct HistoryOptions {
    pub filters: Vec<(String, String)>,
    pub limit: u32,
    pub before: Option<i64>,
    pub format: String,
    pub watch: bool,
    pub interval: u64,
}

pub async fn history(url: &str, token: &str, options: HistoryOptions) -> Result<(), RemoteError> {
    let client = Client::new(url, token)?;
    let interrupt = tokio::signal::ctrl_c();
    tokio::pin!(interrupt);
    let mut previous = BTreeSet::new();
    loop {
        let path = history_path(&options);
        let value = tokio::select! {
            result = get_value(&client, &path) => result?,
            _ = &mut interrupt => {
                eprintln!("history watch stopped");
                return Ok(());
            }
        };
        if options.format == "json" {
            println!(
                "{}",
                serde_json::to_string_pretty(&value).unwrap_or_default()
            );
        } else {
            let rows = value["queries"].as_array().cloned().unwrap_or_default();
            let current: BTreeSet<i64> = rows
                .iter()
                .filter_map(|row| row["history_id"].as_i64())
                .collect();
            let visible: Vec<Value> = if options.watch {
                rows.into_iter()
                    .filter(|row| {
                        row["history_id"]
                            .as_i64()
                            .is_some_and(|id| !previous.contains(&id))
                    })
                    .collect()
            } else {
                rows
            };
            if !visible.is_empty() || !options.watch {
                print_history_table(&visible, value["next_before"].as_i64());
            }
            previous = current;
        }
        if !options.watch {
            return Ok(());
        }
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_secs(options.interval)) => {}
            _ = &mut interrupt => {
                eprintln!("history watch stopped");
                return Ok(());
            }
        }
    }
}

fn history_path(options: &HistoryOptions) -> String {
    let mut url = reqwest::Url::parse("http://duckfleet.local/v1/queries").expect("static URL");
    {
        let mut query = url.query_pairs_mut();
        query.append_pair("limit", &options.limit.to_string());
        if let Some(before) = options.before {
            query.append_pair("before", &before.to_string());
        }
        for (name, value) in &options.filters {
            query.append_pair(name, value);
        }
    }
    match url.query() {
        Some(query) => format!("/v1/queries?{query}"),
        None => "/v1/queries".into(),
    }
}

fn print_history_table(rows: &[Value], next: Option<i64>) {
    println!("query_id  status  worker  pool  elapsed_ms  rows  profile  principal  failure_kind");
    for row in rows {
        let text = |name: &str| row[name].as_str().unwrap_or("unknown");
        let number = |name: &str| {
            row[name]
                .as_i64()
                .map(|value| value.to_string())
                .unwrap_or_else(|| "unknown".into())
        };
        let pool = row["placement"]["pool"].as_str().unwrap_or("unknown");
        println!(
            "{}  {}  {}  {}  {}  {}  {}  {}  {}",
            text("query_id"),
            text("status"),
            text("worker_id"),
            pool,
            number("elapsed_ms"),
            number("result_rows"),
            text("profile_status"),
            text("principal"),
            text("failure_kind")
        );
    }
    match next {
        Some(next) => println!("next_before: {next}"),
        None => println!("next_before: none"),
    }
}

pub struct ProfileOptions {
    pub id: String,
    pub raw: bool,
    pub metrics: bool,
}

pub async fn profile(url: &str, token: &str, options: ProfileOptions) -> Result<(), RemoteError> {
    let client = Client::new(url, token)?;
    let detail = get_value(&client, &format!("/v1/queries/{}", options.id)).await?;
    if options.raw {
        let profile = detail["profile"].clone();
        if profile.is_null() {
            return Err(profile_unavailable(&detail));
        }
        println!(
            "{}",
            serde_json::to_string_pretty(&profile).unwrap_or_default()
        );
        return Ok(());
    }
    let rendered = if options.metrics {
        render_summary(&detail) + &render_metrics(&detail)
    } else {
        if detail["profile"].is_null() {
            return Err(profile_unavailable(&detail));
        }
        render_summary(&detail) + &render_metrics(&detail) + &render_operators(&detail["profile"])?
    };
    print!("{rendered}");
    Ok(())
}

async fn get_value(client: &Client, path: &str) -> Result<Value, RemoteError> {
    let bytes = client
        .request(reqwest::Method::GET, path, None, Duration::from_secs(15))
        .await?;
    serde_json::from_slice(&bytes).map_err(|_| RemoteError {
        status: None,
        failure_kind: Some("invalid_response".into()),
        retry_safe: false,
        message: "history API returned invalid JSON".into(),
        request_id: None,
        acceptance: None,
        status_url: None,
    })
}

fn profile_unavailable(detail: &Value) -> RemoteError {
    RemoteError {
        status: None,
        failure_kind: Some("profile_unavailable".into()),
        retry_safe: true,
        message: format!(
            "profile was not returned by the authorized API (profile_status: {})",
            detail["profile_status"].as_str().unwrap_or("unknown")
        ),
        request_id: detail["query_id"].as_str().map(Into::into),
        acceptance: None,
        status_url: None,
    }
}

fn render_summary(detail: &Value) -> String {
    let mut output = String::new();
    output.push_str(&format!("query_id: {}\n", text(detail, "query_id")));
    output.push_str(&format!("status: {}\n", text(detail, "status")));
    output.push_str(&format!(
        "profile_status: {}\n",
        text(detail, "profile_status")
    ));
    output.push_str(&format!("worker_id: {}\n", text(detail, "worker_id")));
    let accepted = timestamp(detail, "accepted_at");
    let queued = timestamp(detail, "queued_at").or(accepted);
    let selected = timestamp(detail, "selected_at");
    let granted = timestamp(detail, "execution_granted_at");
    let engine = timestamp(detail, "engine_started_at");
    let finished = timestamp(detail, "finished_at");
    output.push_str(&format!("queue_ms: {}\n", milliseconds(queued, selected)));
    let preparation_start = selected.or(granted);
    output.push_str(&format!(
        "preparation_ms: {}\n",
        milliseconds(preparation_start, engine)
    ));
    output.push_str(&format!(
        "engine_execution_ms: {}\n",
        milliseconds(engine, finished)
    ));
    let total = accepted
        .zip(finished)
        .map(|(start, end)| (end - start).num_milliseconds())
        .filter(|value| *value >= 0)
        .map(|value| value.to_string())
        .or_else(|| detail["elapsed_ms"].as_i64().map(|value| value.to_string()))
        .unwrap_or_else(|| "unknown".into());
    output.push_str(&format!("total_observed_ms: {total}\n"));
    output.push_str("delivery_ms: unknown\n");
    output.push_str("client_observed_total_ms: unknown\n");
    output
}

fn render_metrics(detail: &Value) -> String {
    let metrics = &detail["metrics"];
    let profile = &detail["profile"];
    let query = &profile["query"];
    let system = &profile["system"];
    let io = &profile["io"];
    let mut output = String::from("metrics:\n");
    let paired = |left: &Value, left_name: &str, right: &Value, right_name: &str| {
        optional_number(left, left_name)
            .or_else(|| optional_number(right, right_name))
            .unwrap_or_else(|| "unknown".into())
    };
    output.push_str(&format!(
        "  engine_cpu_seconds: {}\n",
        paired(metrics, "cpu_time", query, "cpu_time")
    ));
    output.push_str(&format!(
        "  engine_latency_seconds: {}\n",
        paired(metrics, "latency", query, "total_time")
    ));
    output.push_str(&format!(
        "  engine_peak_buffer_memory_bytes: {}\n",
        paired(
            metrics,
            "system_peak_buffer_memory",
            system,
            "peak_buffer_memory"
        )
    ));
    output.push_str(&format!(
        "  engine_peak_temp_dir_bytes: {}\n",
        paired(
            metrics,
            "system_peak_temp_dir_size",
            system,
            "peak_temp_dir_size"
        )
    ));
    output.push_str(&format!(
        "  engine_total_bytes_spilled: {}\n",
        number(system, "total_bytes_spilled")
    ));
    output.push_str(&format!(
        "  rows_returned: {}\n",
        number(metrics, "rows_returned")
    ));
    output.push_str(&format!(
        "  bytes_read: {}\n",
        paired(metrics, "total_bytes_read", io, "total_bytes_read")
    ));
    output.push_str(&format!(
        "  bytes_written: {}\n",
        paired(metrics, "total_bytes_written", io, "total_bytes_written")
    ));
    output.push_str("  scope: DuckDB engine only; not container RSS or provider cost\n");
    output
}

fn render_operators(profile: &Value) -> Result<String, RemoteError> {
    let roots = profile["operator"].as_array().ok_or_else(|| RemoteError {
        status: None,
        failure_kind: Some("invalid_profile".into()),
        retry_safe: true,
        message: "retained profile has no valid operator array".into(),
        request_id: None,
        acceptance: None,
        status_url: None,
    })?;
    let mut nodes = Vec::new();
    for root in roots {
        flatten(root, "", 0, &mut nodes);
    }
    if nodes.is_empty() {
        return Ok("operators: not_reported\n".into());
    }
    let mut output = String::from("operators (individual timings; do not sum as wall clock):\n");
    let mut ranked = nodes.clone();
    ranked.sort_by(|left, right| right.timing.total_cmp(&left.timing));
    output.push_str("  most_expensive:\n");
    for node in ranked.iter().take(5) {
        output.push_str(&format!(
            "    {} timing_ms={:.3} rows={} cardinality={}\n",
            node.path,
            node.timing * 1000.0,
            node.rows,
            node.cardinality
        ));
    }
    output.push_str("  tree:\n");
    for node in nodes.iter().take(200) {
        output.push_str(&format!(
            "    {}{} timing_ms={:.3} rows={} cardinality={}\n",
            "  ".repeat(node.depth.min(12)),
            node.name,
            node.timing * 1000.0,
            node.rows,
            node.cardinality
        ));
    }
    if nodes.len() > 200 {
        output.push_str(&format!(
            "    ... truncated {} additional operators\n",
            nodes.len() - 200
        ));
    }
    if nodes.iter().any(|node| node.depth == 12) {
        output.push_str("    ... deeper operators truncated\n");
    }
    Ok(output)
}

#[derive(Clone)]
struct Operator {
    path: String,
    name: String,
    depth: usize,
    timing: f64,
    rows: String,
    cardinality: String,
}

fn flatten(value: &Value, parent: &str, depth: usize, output: &mut Vec<Operator>) {
    if output.len() >= 1000 || depth > 12 {
        return;
    }
    let name = value["name"]
        .as_str()
        .or_else(|| value["type"].as_str())
        .unwrap_or("UNKNOWN");
    let path = if parent.is_empty() {
        name.to_owned()
    } else {
        format!("{parent}/{name}")
    };
    output.push(Operator {
        path: path.clone(),
        name: name.to_owned(),
        depth,
        timing: value["timing"].as_f64().unwrap_or(0.0),
        rows: display_number(&value["intermediate_rows"]),
        cardinality: value["extra_info"]["Estimated Cardinality"]
            .as_str()
            .map(str::to_owned)
            .unwrap_or_else(|| "unknown".into()),
    });
    if let Some(children) = value["children"].as_array() {
        for child in children {
            flatten(child, &path, depth + 1, output);
        }
    }
}

fn timestamp(detail: &Value, name: &str) -> Option<chrono::DateTime<chrono::Utc>> {
    let value = detail[name].as_str()?;
    let mut normalized = value.replace(' ', "T");
    if normalized.ends_with("+00") {
        normalized.push_str(":00");
    }
    chrono::DateTime::parse_from_rfc3339(&normalized)
        .ok()
        .map(|value| value.with_timezone(&chrono::Utc))
}

fn milliseconds(
    start: Option<chrono::DateTime<chrono::Utc>>,
    end: Option<chrono::DateTime<chrono::Utc>>,
) -> String {
    start
        .zip(end)
        .map(|(start, end)| (end - start).num_milliseconds())
        .filter(|value| *value >= 0)
        .map(|value| value.to_string())
        .unwrap_or_else(|| "unknown".into())
}

fn text<'a>(detail: &'a Value, name: &str) -> &'a str {
    detail[name].as_str().unwrap_or("unknown")
}

fn number(value: &Value, name: &str) -> String {
    optional_number(value, name).unwrap_or_else(|| "unknown".into())
}

fn optional_number(value: &Value, name: &str) -> Option<String> {
    match &value[name] {
        Value::Number(number) => Some(number.to_string()),
        Value::String(text) => Some(text.clone()),
        _ => None,
    }
}

fn display_number(value: &Value) -> String {
    match value {
        Value::Number(number) => number.to_string(),
        Value::String(text) => text.clone(),
        _ => "unknown".into(),
    }
}

pub fn validate_interval(value: Option<&str>) -> Result<u64, String> {
    value.map_or(Ok(2), |value| {
        value
            .parse::<u64>()
            .ok()
            .filter(|interval| (1..=60).contains(interval))
            .ok_or_else(|| "--interval must be a whole number from 1 to 60 seconds".into())
    })
}

#[cfg(test)]
pub fn fixture_output(detail: &Value) -> Result<String, RemoteError> {
    Ok(render_summary(detail) + &render_metrics(detail) + &render_operators(&detail["profile"])?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn real_profile_fixture() -> Value {
        json!({
            "query_id":"48f80f72-de78-470d-839e-546650b42be2",
            "status":"succeeded","profile_status":"captured","worker_id":"small",
            "accepted_at":"2026-11-26 10:53:34.924578+00",
            "queued_at":"2026-11-26T10:53:34.931595+00:00",
            "selected_at":"2026-11-26T10:53:34.936331+00:00",
            "execution_granted_at":"2026-11-26T10:53:34.939556+00:00",
            "engine_started_at":"2026-11-26T10:53:34.940884+00:00",
            "finished_at":"2026-11-26 10:53:34.943668+00",
            "elapsed_ms":1,"metrics":{"cpu_time":0.000011751,"total_bytes_read":0,"total_bytes_written":0},
            "profile":{
                "query":{"cpu_time":0.000011751,"total_time":0.000066458,"sql":"restricted SQL is not rendered"},
                "system":{"peak_buffer_memory":989332,"peak_temp_dir_size":0,"total_bytes_spilled":0},
                "io":{"total_bytes_read":0,"total_bytes_written":0},
                "operator":[{"name":"RESULT_COLLECTOR","timing":0.000007917,"intermediate_rows":0,"children":[{"name":"PROJECTION","timing":0.000003459,"intermediate_rows":1,"extra_info":{"Estimated Cardinality":"1"},"children":[{"name":"DUMMY_SCAN","timing":0.000000375,"intermediate_rows":1}]}]}]
            }
        })
    }

    #[test]
    fn default_output_is_stable_and_does_not_dump_raw_sql() {
        let detail = real_profile_fixture();
        let rendered = fixture_output(&detail).unwrap();
        let expected = "query_id: 48f80f72-de78-470d-839e-546650b42be2\nstatus: succeeded\nprofile_status: captured\nworker_id: small\nqueue_ms: 4\npreparation_ms: 4\nengine_execution_ms: 2\ntotal_observed_ms: 19\ndelivery_ms: unknown\nclient_observed_total_ms: unknown\nmetrics:\n  engine_cpu_seconds: 0.000011751\n  engine_latency_seconds: 0.000066458\n  engine_peak_buffer_memory_bytes: 989332\n  engine_peak_temp_dir_bytes: 0\n  engine_total_bytes_spilled: 0\n  rows_returned: unknown\n  bytes_read: 0\n  bytes_written: 0\n  scope: DuckDB engine only; not container RSS or provider cost\noperators (individual timings; do not sum as wall clock):\n  most_expensive:\n    RESULT_COLLECTOR timing_ms=0.008 rows=0 cardinality=unknown\n    RESULT_COLLECTOR/PROJECTION timing_ms=0.003 rows=1 cardinality=1\n    RESULT_COLLECTOR/PROJECTION/DUMMY_SCAN timing_ms=0.000 rows=1 cardinality=unknown\n  tree:\n    RESULT_COLLECTOR timing_ms=0.008 rows=0 cardinality=unknown\n      PROJECTION timing_ms=0.003 rows=1 cardinality=1\n        DUMMY_SCAN timing_ms=0.000 rows=1 cardinality=unknown\n";
        assert_eq!(rendered, expected);
        assert!(!rendered.contains("restricted SQL"));
    }

    #[test]
    fn missing_timestamps_and_unavailable_profiles_stay_explicit() {
        let mut detail = real_profile_fixture();
        for field in [
            "queued_at",
            "selected_at",
            "engine_started_at",
            "finished_at",
        ] {
            detail[field] = Value::Null;
        }
        let summary = render_summary(&detail);
        assert!(summary.contains("queue_ms: unknown"));
        assert!(summary.contains("preparation_ms: unknown"));
        assert!(summary.contains("engine_execution_ms: unknown"));
        assert!(summary.contains("delivery_ms: unknown"));
        detail["profile"] = Value::Null;
        detail["profile_status"] = json!("sampled_out");
        let error = profile_unavailable(&detail);
        assert!(error.to_string().contains("sampled_out"));
        assert!(!error.to_string().contains("restricted SQL"));
    }

    #[test]
    fn deep_and_wide_profiles_are_bounded() {
        let mut node = json!({"name":"LEAF","timing":1.0});
        for depth in 0..100 {
            node = json!({"name":format!("N{depth}"),"timing":1.0,"children":[node]});
        }
        let profile = json!({"operator":[node]});
        let rendered = render_operators(&profile).unwrap();
        assert!(rendered.lines().count() < 250);
        assert!(rendered.contains("truncated") || !rendered.contains("N99"));
        assert!(validate_interval(Some("0")).is_err());
        assert!(validate_interval(Some("61")).is_err());
        assert_eq!(validate_interval(None).unwrap(), 2);
    }
}
