//! Fresh immutable diagnostic dimensions, independent of idempotency payloads.
use crate::{
    ApiError,
    compute::{ComputeRequest, Placement},
    query_shape::QueryShape,
};
use postgres::Transaction;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SessionDimensions {
    pub requested_mode: Option<String>,
    pub requested_pool: Option<String>,
    pub requested_cpu: Option<i64>,
    pub requested_memory_bytes: Option<i64>,
    pub requested_disk_bytes: Option<i64>,
    pub selected_mode: Option<String>,
    pub selected_pool: Option<String>,
    pub selected_cpu: Option<i64>,
    pub selected_memory_bytes: Option<i64>,
    pub selected_disk_bytes: Option<i64>,
    pub engine_version: Option<String>,
    pub registry_revision: Option<String>,
}
pub(crate) fn capture_session(
    original: &ComputeRequest,
    selected: &Placement,
    engine_version: &str,
    revision: Option<&str>,
) -> Result<SessionDimensions, ApiError> {
    let requested = original.canonical_intent()?;
    Ok(SessionDimensions {
        requested_mode: original.mode.map(|m| m.name().to_owned()),
        requested_pool: original.pool.clone(),
        requested_cpu: original.cpu.map(i64::from),
        requested_memory_bytes: requested["memory_bytes"]
            .as_u64()
            .and_then(|v| i64::try_from(v).ok()),
        requested_disk_bytes: requested["disk_bytes"]
            .as_u64()
            .and_then(|v| i64::try_from(v).ok()),
        selected_mode: Some(selected.mode.name().to_owned()),
        selected_pool: selected.pool.clone(),
        selected_cpu: Some(i64::from(selected.resources.cpu)),
        selected_memory_bytes: i64::try_from(selected.resources.memory_bytes).ok(),
        selected_disk_bytes: i64::try_from(selected.resources.disk_bytes).ok(),
        engine_version: (!engine_version.is_empty()).then(|| engine_version.to_owned()),
        registry_revision: revision.map(str::to_owned),
    })
}
pub(crate) fn migrate_system(tx: &mut Transaction<'_>) -> anyhow::Result<()> {
    tx.batch_execute("ALTER TABLE queue_sessions ADD COLUMN system_dimensions TEXT;
 ALTER TABLE requests ADD COLUMN open_span_id TEXT,ADD COLUMN execution_timeout_seconds BIGINT,
 ADD COLUMN requested_mode TEXT,ADD COLUMN requested_pool TEXT,ADD COLUMN requested_cpu BIGINT,
 ADD COLUMN requested_memory_bytes BIGINT,ADD COLUMN requested_disk_bytes BIGINT,
 ADD COLUMN selected_mode TEXT,ADD COLUMN selected_pool TEXT,ADD COLUMN selected_cpu BIGINT,
 ADD COLUMN selected_memory_bytes BIGINT,ADD COLUMN selected_disk_bytes BIGINT,
 ADD COLUMN engine_version TEXT,ADD COLUMN registry_revision TEXT,
 ADD COLUMN query_fingerprint TEXT,ADD COLUMN query_fingerprint_version BIGINT,ADD COLUMN normalization_status TEXT,
 ADD COLUMN model_id TEXT,ADD COLUMN invocation_id TEXT,ADD COLUMN model_run_id TEXT;
 ALTER TABLE query_history ADD COLUMN profile_capture_version BIGINT,
 ADD COLUMN profile_latency_seconds DOUBLE PRECISION,ADD COLUMN profile_cpu_seconds DOUBLE PRECISION,
 ADD COLUMN profile_rows_returned BIGINT,ADD COLUMN profile_rows_scanned BIGINT,
 ADD COLUMN peak_buffer_memory_bytes BIGINT,ADD COLUMN peak_temp_dir_bytes BIGINT,
 ADD COLUMN bytes_read BIGINT,ADD COLUMN bytes_written BIGINT;
 CREATE INDEX history_system_window ON query_history(namespace_id,accepted_at,history_id);
 CREATE INDEX history_system_own_window ON query_history(namespace_id,owner_principal_id,accepted_at,history_id);
 CREATE INDEX requests_system_window ON requests(namespace_id,accepted_at,request_id);
 CREATE INDEX usage_system_window ON usage_events(namespace_id,observed_at,ledger_id);
 CREATE INDEX usage_system_own_window ON usage_events(namespace_id,owner_principal_id,observed_at,ledger_id);")?;
    for table in ["reservations", "usage_events"] {
        tx.batch_execute(&format!("ALTER TABLE {table} ADD COLUMN selected_mode TEXT,ADD COLUMN selected_pool TEXT,
   ADD COLUMN owner_principal_name TEXT,ADD COLUMN model_id TEXT,ADD COLUMN invocation_id TEXT,ADD COLUMN model_run_id TEXT"))?;
    }
    tx.batch_execute("ALTER TABLE usage_events ADD COLUMN rate_micro_usd_per_unit_hour BIGINT")?;
    Ok(())
}
fn label(value: Option<&Value>) -> Option<&str> {
    value
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty() && s.len() <= 512)
}
pub(crate) fn record_dimensions(
    tx: &mut Transaction<'_>,
    id: &str,
    d: &SessionDimensions,
    shape: &QueryShape,
    context: &Value,
    timeout: u64,
    open_span: &str,
) -> anyhow::Result<()> {
    let model = label(context.get("dbt").and_then(|v| v.get("node_id")));
    let invocation = label(context.get("dbt").and_then(|v| v.get("invocation_id")));
    let run = label(context.get("model_run_id"));
    tx.execute("UPDATE requests SET requested_mode=$2,requested_pool=$3,requested_cpu=$4,requested_memory_bytes=$5,requested_disk_bytes=$6,
 selected_mode=$7,selected_pool=$8,selected_cpu=$9,selected_memory_bytes=$10,selected_disk_bytes=$11,
 engine_version=$12,registry_revision=$13,query_fingerprint=$14,query_fingerprint_version=$15,normalization_status=$16,
 model_id=$17,invocation_id=$18,model_run_id=$19,execution_timeout_seconds=$20,open_span_id=$21 WHERE request_id=$1",
 &[&id,&d.requested_mode,&d.requested_pool,&d.requested_cpu,&d.requested_memory_bytes,&d.requested_disk_bytes,
 &d.selected_mode,&d.selected_pool,&d.selected_cpu,&d.selected_memory_bytes,&d.selected_disk_bytes,&d.engine_version,&d.registry_revision,
 &shape.fingerprint,&i64::from(shape.version),&shape.status.as_str(),&model,&invocation,&run,&i64::try_from(timeout)?,&open_span])?;
    Ok(())
}
#[derive(Debug, Default, PartialEq)]
pub(crate) struct ProfileMetrics {
    pub latency: Option<f64>,
    pub cpu: Option<f64>,
    pub rows_returned: Option<i64>,
    pub rows_scanned: Option<i64>,
    pub buffer_bytes: Option<i64>,
    pub temp_bytes: Option<i64>,
    pub bytes_read: Option<i64>,
    pub bytes_written: Option<i64>,
}
pub(crate) fn capture_metrics(value: &Value) -> ProfileMetrics {
    let float = |name| {
        value
            .get(name)
            .and_then(Value::as_f64)
            .filter(|v| v.is_finite() && *v >= 0.0)
    };
    let integer = |name| {
        value.get(name).and_then(|v| {
            v.as_i64().filter(|n| *n >= 0).or_else(|| {
                v.as_f64()
                    .filter(|n| {
                        n.is_finite()
                            && *n >= 0.0
                            && *n < 9_223_372_036_854_775_808.0
                            && n.fract() == 0.0
                    })
                    .map(|n| n as i64)
            })
        })
    };
    ProfileMetrics {
        latency: float("latency"),
        cpu: float("cpu_time"),
        rows_returned: integer("rows_returned"),
        rows_scanned: integer("cumulative_rows_scanned"),
        buffer_bytes: integer("system_peak_buffer_memory"),
        temp_bytes: integer("system_peak_temp_dir_size"),
        bytes_read: integer("total_bytes_read"),
        bytes_written: integer("total_bytes_written"),
    }
}
pub(crate) fn record_metrics(
    tx: &mut Transaction<'_>,
    id: &str,
    value: &Value,
    captured: bool,
) -> anyhow::Result<()> {
    let m = capture_metrics(value);
    tx.execute("UPDATE query_history SET profile_capture_version=$2,profile_latency_seconds=$3,profile_cpu_seconds=$4,
 profile_rows_returned=$5,profile_rows_scanned=$6,peak_buffer_memory_bytes=$7,peak_temp_dir_bytes=$8,bytes_read=$9,bytes_written=$10 WHERE query_id=$1",
 &[&id,&captured.then_some(1_i64),&m.latency,&m.cpu,&m.rows_returned,&m.rows_scanned,&m.buffer_bytes,&m.temp_bytes,&m.bytes_read,&m.bytes_written])?;
    Ok(())
}
#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    #[test]
    fn metric_boundaries_do_not_turn_invalid_values_into_measurements() {
        let m = capture_metrics(
            &json!({"latency":0.5,"cpu_time":-1,"rows_returned":2.5,"total_bytes_read":u64::MAX,"total_bytes_written":0}),
        );
        assert_eq!(m.latency, Some(0.5));
        assert_eq!(m.cpu, None);
        assert_eq!(m.rows_returned, None);
        assert_eq!(m.bytes_read, None);
        assert_eq!(m.bytes_written, Some(0));
    }
    #[test]
    fn original_omissions_remain_distinct_from_selected_allocation() {
        let original: ComputeRequest =
            serde_json::from_value(json!({"pool":"interactive","cpu":2})).unwrap();
        let selected = Placement {
            mode: crate::compute::Mode::Persistent,
            pool: Some("interactive".into()),
            resources: crate::compute::Resources {
                cpu: 4,
                memory_bytes: 4 << 30,
                disk_bytes: 8 << 30,
            },
        };
        let d = capture_session(&original, &selected, "v1.5.4", Some("registry-1")).unwrap();
        assert_eq!(d.requested_mode, None);
        assert_eq!(d.requested_cpu, Some(2));
        assert_eq!(d.requested_memory_bytes, None);
        assert_eq!(d.selected_cpu, Some(4));
        assert_eq!(d.selected_memory_bytes, Some(4 << 30));
    }
}
