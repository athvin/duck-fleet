//! ADBC partitioned results over DuckFleet durable jobs.
//!
//! `execute_partitions` submits the statement as a durable job and returns one
//! opaque descriptor per verified result chunk: the job ID, result generation,
//! chunk ID, length, and checksum. No storage key or URL. `read_partition`
//! re-reads the manifest under the caller's own `result.read` authority, so
//! another principal's descriptor, an expired result, or a regenerated result
//! is refused, then streams that one chunk and checks its length and SHA-256
//! before decoding it. Memory is bounded by a single chunk.

use crate::{convert, runtime};
use adbc_core::PartitionedResult;
use adbc_core::error::{Error, Result, Status};
use arrow_array::{RecordBatchIterator, RecordBatchReader};
use duckfleet::Param;
use serde_json::{Value, json};
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// The durable job a statement is waiting on, so cancellation can reach it.
#[derive(Clone, Default)]
pub(crate) struct RunningJob(Arc<Mutex<Option<uuid::Uuid>>>);

impl RunningJob {
    pub(crate) fn current(&self) -> Option<uuid::Uuid> {
        *self.0.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn set(&self, id: Option<uuid::Uuid>) {
        *self.0.lock().unwrap_or_else(|e| e.into_inner()) = id;
    }
}

const DESCRIPTOR_VERSION: u64 = 1;

fn invalid(message: impl Into<String>) -> Error {
    Error::with_message_and_status(message, Status::InvalidState)
}

fn manifest(client: &duckfleet::Client, id: uuid::Uuid) -> Result<duckfleet::Manifest> {
    let value = runtime().block_on(client.job_result(id)).map_err(convert)?;
    if value["complete"] != true {
        return Err(invalid(format!(
            "job {id} has no complete result (state: {})",
            value["state"]
        )));
    }
    let manifest: duckfleet::Manifest =
        serde_json::from_value(value).map_err(|_| invalid("the job result manifest is invalid"))?;
    manifest.encode().map_err(|message| {
        invalid(format!(
            "the job result manifest failed validation: {message}"
        ))
    })?;
    if manifest.request_id != id {
        return Err(invalid("the job result manifest names a different job"));
    }
    Ok(manifest)
}

pub(crate) fn execute(
    session: &duckfleet::Session,
    sql: &str,
    params: Vec<Param>,
    compute: &duckfleet::ComputeRequest,
    running: &RunningJob,
) -> Result<PartitionedResult> {
    let client = session.client();
    let parameters = params
        .iter()
        .map(Param::to_wire)
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(convert)?;
    let request: duckfleet::JobRequest = serde_json::from_value(json!({
        "request_id": uuid::Uuid::new_v4(),
        "sql": sql,
        "parameters": parameters,
        "context": {"connection_name": "adbc-partitions"},
        "compute": compute,
    }))
    .map_err(|e| Error::with_message_and_status(e.to_string(), Status::InvalidArguments))?;
    let id = request.request_id;
    running.set(Some(id));
    let outcome = (|| {
        runtime()
            .block_on(client.submit_job(&request))
            .map_err(convert)?;
        let status = runtime()
            .block_on(client.wait_job(id, Duration::from_millis(250), Duration::from_secs(3600)))
            .map_err(convert)?;
        if status["status"] != "succeeded" {
            let kind = status["failure_kind"]
                .as_str()
                .unwrap_or("unknown")
                .to_owned();
            return Err(convert(duckfleet::Error {
                status: None,
                failure_kind: Some(kind),
                retry_safe: false,
                message: format!(
                    "partitioned execution job {id} finished with status {}",
                    status["status"]
                ),
                request_id: Some(id.to_string().into()),
                acceptance: Some("accepted".into()),
                status_url: Some(format!("/v1/jobs/{id}").into()),
            }));
        }
        manifest(client, id)
    })();
    running.set(None);
    let manifest = outcome?;
    let partitions = manifest
        .chunks
        .iter()
        .map(|chunk| {
            serde_json::to_vec(&json!({
                "v": DESCRIPTOR_VERSION,
                "job": id,
                "generation": manifest.generation,
                "chunk": chunk.chunk_id,
                "bytes": chunk.bytes,
                "rows": chunk.rows,
                "sha256": chunk.sha256,
            }))
            .map_err(|e| invalid(e.to_string()))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(PartitionedResult {
        partitions,
        schema: manifest.schema.clone(),
        rows_affected: i64::try_from(manifest.rows).unwrap_or(-1),
    })
}

pub(crate) fn read(
    session: &duckfleet::Session,
    descriptor: &[u8],
) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
    let bad = || {
        Error::with_message_and_status(
            "not a DuckFleet partition descriptor",
            Status::InvalidArguments,
        )
    };
    if descriptor.len() > 4096 {
        return Err(bad());
    }
    let value: Value = serde_json::from_slice(descriptor).map_err(|_| bad())?;
    if value["v"].as_u64() != Some(DESCRIPTOR_VERSION) {
        return Err(bad());
    }
    let id = value["job"]
        .as_str()
        .and_then(|s| uuid::Uuid::parse_str(s).ok())
        .ok_or_else(bad)?;
    let chunk_id = value["chunk"].as_u64().ok_or_else(bad)?;
    let client = session.client();
    let manifest = manifest(client, id)?;
    if value["generation"].as_str() != Some(manifest.generation.to_string().as_str()) {
        return Err(invalid(
            "the job result was regenerated since this partition was issued; execute the partitions again",
        ));
    }
    let chunk = manifest
        .chunks
        .iter()
        .find(|chunk| u64::from(chunk.chunk_id) == chunk_id)
        .ok_or_else(bad)?;
    if value["bytes"].as_u64() != Some(chunk.bytes)
        || value["sha256"].as_str() != Some(chunk.sha256.as_str())
    {
        return Err(invalid(
            "the partition descriptor does not match the authorized manifest",
        ));
    }
    let mut bytes = Vec::with_capacity(usize::try_from(chunk.bytes).unwrap_or(0));
    runtime()
        .block_on(client.download_job_chunk(
            id,
            &chunk.chunk_id.to_string(),
            chunk.bytes,
            &chunk.sha256,
            &mut bytes,
        ))
        .map_err(convert)?;
    let reader = arrow_ipc::reader::StreamReader::try_new(std::io::Cursor::new(bytes), None)
        .map_err(|e| invalid(format!("the result chunk is not valid Arrow IPC: {e}")))?;
    if *reader.schema() != manifest.schema {
        return Err(invalid("the result chunk schema differs from its manifest"));
    }
    let schema = reader.schema();
    let batches = reader
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| invalid(format!("the result chunk could not be decoded: {e}")))?;
    let rows: usize = batches.iter().map(|batch| batch.num_rows()).sum();
    if rows as u64 != chunk.rows {
        return Err(invalid(
            "the result chunk row count differs from its manifest",
        ));
    }
    Ok(Box::new(RecordBatchIterator::new(
        batches.into_iter().map(Ok),
        schema,
    )))
}
