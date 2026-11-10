//! Best-effort per-statement DuckDB profiles, bounded before durable storage.
use anyhow::Result;
use duckdb::Connection;
use serde_json::{Value, json};
use std::{fs, io::Read, path::PathBuf};
use uuid::Uuid;

const MAX_PROFILE_BYTES: u64 = 1024 * 1024;

pub(crate) struct Profile {
    path: PathBuf,
    enabled: bool,
}
pub(crate) struct Capture {
    pub status: &'static str,
    pub profile: Option<String>,
    pub metrics: String,
}
impl Capture {
    fn missing(status: &'static str) -> Self {
        Self {
            status,
            profile: None,
            metrics: "{}".into(),
        }
    }
}
impl Profile {
    pub fn new(id: Uuid, enabled: bool, conn: &Connection) -> Result<Self> {
        let directory = std::env::temp_dir().join("duckfleet-profiles");
        fs::create_dir_all(&directory)?;
        let path = directory.join(format!("{id}.json"));
        if enabled {
            let escaped = path.to_string_lossy().replace('\'', "''");
            conn.execute_batch(&format!(
                "SET enable_profiling='json'; SET profiling_output='{escaped}'; SET profiling_mode='standard'; SET profiling_coverage='ALL';"
            ))?;
        }
        Ok(Self { path, enabled })
    }

    // Always remove the previous output before executing. Some statements and
    // failures emit no profile; they must never inherit the previous query's.
    pub fn clear(&self) -> bool {
        match fs::remove_file(&self.path) {
            Ok(()) => true,
            Err(error) => error.kind() == std::io::ErrorKind::NotFound,
        }
    }

    pub fn capture(&self, cleared: bool) -> Capture {
        if !self.enabled {
            return Capture::missing("disabled");
        }
        if !cleared {
            return Capture::missing("io_error");
        }
        let file = match fs::File::open(&self.path) {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Capture::missing("not_emitted");
            }
            Err(_) => return Capture::missing("io_error"),
        };
        let mut bytes = Vec::new();
        if file
            .take(MAX_PROFILE_BYTES + 1)
            .read_to_end(&mut bytes)
            .is_err()
        {
            return Capture::missing("io_error");
        }
        if bytes.len() as u64 > MAX_PROFILE_BYTES {
            return Capture::missing("too_large");
        }
        let value: Value = match serde_json::from_slice(&bytes) {
            Ok(Value::Object(value)) => Value::Object(value),
            _ => return Capture::missing("invalid"),
        };
        let mut metrics = json!({});
        for key in [
            "latency",
            "cpu_time",
            "rows_returned",
            "cumulative_rows_scanned",
            "system_peak_buffer_memory",
            "system_peak_temp_dir_size",
            "total_bytes_read",
            "total_bytes_written",
        ] {
            if let Some(number) = value.get(key).filter(|v| v.is_number()) {
                metrics[key] = number.clone();
            }
        }
        Capture {
            status: "captured",
            profile: Some(value.to_string()),
            metrics: metrics.to_string(),
        }
    }
}
impl Drop for Profile {
    fn drop(&mut self) {
        self.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn rejects_oversized_or_malformed_profiles() -> Result<()> {
        let conn = Connection::open_in_memory()?;
        let profile = Profile::new(Uuid::new_v4(), true, &conn)?;
        fs::write(&profile.path, vec![b' '; MAX_PROFILE_BYTES as usize + 1])?;
        assert_eq!(profile.capture(true).status, "too_large");
        fs::write(&profile.path, b"not json")?;
        assert_eq!(profile.capture(true).status, "invalid");
        assert_eq!(profile.capture(false).status, "io_error");
        assert!(profile.clear());
        assert_eq!(profile.capture(true).status, "not_emitted");
        Ok(())
    }
}
