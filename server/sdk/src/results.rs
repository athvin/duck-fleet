//! Verified, resumable download of durable-job results.

use crate::client::{Client, RemoteError};
use crate::protocol::{JobLimits, MAX_CHUNK_BYTES, MAX_MANIFEST_BYTES, Manifest, ManifestChunk};
use serde_json::Value;
use std::path::Path;

impl Client {
    /// Downloads a complete durable-job result into `directory`: `manifest.json`
    /// plus one `NNNNNN.arrow` IPC stream per chunk. Every chunk is streamed to
    /// a `.part` file, checked for length, SHA-256, schema, and row count, and
    /// only then published without replacing an existing file. With `resume`,
    /// the directory's stored manifest must equal the server's current result
    /// generation, chunks that already verify are kept, and the rest download.
    /// Each manifest and chunk request re-checks `result.read` and expiry.
    pub async fn download_job_result(
        &self,
        id: uuid::Uuid,
        directory: &Path,
        resume: bool,
    ) -> Result<Manifest, RemoteError> {
        let client = self;
        use tokio::io::AsyncWriteExt;
        let value = client.job_result(id).await?;
        if value["complete"] != true {
            return Err(job_local_error(
                Some(id),
                "job result is not complete; inspect status before downloading",
            ));
        }
        let manifest: Manifest = serde_json::from_value(value.clone())
            .map_err(|_| job_local_error(Some(id), "invalid job result manifest"))?;
        if manifest.request_id != id
            || manifest.bytes > JobLimits::HARD.result_bytes
            || manifest.rows > JobLimits::HARD.result_rows
        {
            return Err(job_local_error(
                Some(id),
                "job manifest identity or result bounds are invalid",
            ));
        }
        let encoded_manifest = manifest
            .encode()
            .map_err(|_| job_local_error(Some(id), "job result manifest failed validation"))?;
        let path = directory;
        if resume {
            let meta = tokio::fs::symlink_metadata(path)
                .await
                .map_err(|_| job_local_error(Some(id), "cannot read result directory"))?;
            if !meta.is_dir() {
                return Err(job_local_error(
                    Some(id),
                    "result directory must be a real directory",
                ));
            }
            let manifest_path = path.join("manifest.json");
            let meta = tokio::fs::symlink_metadata(&manifest_path)
                .await
                .map_err(|_| {
                    job_local_error(Some(id), "resume needs the matching manifest.json")
                })?;
            if !meta.is_file() || meta.len() > MAX_MANIFEST_BYTES as u64 {
                return Err(job_local_error(
                    Some(id),
                    "resume manifest must be a bounded regular file",
                ));
            }
            let stored = tokio::fs::read(manifest_path).await.map_err(|_| {
                job_local_error(Some(id), "resume needs the matching manifest.json")
            })?;
            if stored.len() > MAX_MANIFEST_BYTES
                || serde_json::from_slice::<Value>(&stored).ok().as_ref() != Some(&value)
            {
                return Err(job_local_error(
                    Some(id),
                    "resume manifest differs from the authorized result generation",
                ));
            }
        } else {
            tokio::fs::create_dir(path).await.map_err(|_| {
                job_local_error(
                    Some(id),
                    "cannot create result directory; existing directories require --resume",
                )
            })?;
            let mut file = tokio::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(path.join("manifest.json"))
                .await
                .map_err(|_| job_local_error(Some(id), "cannot create manifest.json"))?;
            file.write_all(&encoded_manifest)
                .await
                .map_err(|_| job_local_error(Some(id), "cannot write result manifest"))?;
            file.flush()
                .await
                .map_err(|_| job_local_error(Some(id), "cannot flush result manifest"))?;
        }
        for chunk in &manifest.chunks {
            let final_path = path.join(format!("{:06}.arrow", chunk.chunk_id));
            if resume && tokio::fs::symlink_metadata(&final_path).await.is_ok() {
                verify_local_chunk(id, &final_path, chunk, &manifest.schema).await?;
                continue;
            }
            let temporary = path.join(format!("{:06}.arrow.part", chunk.chunk_id));
            if resume && let Ok(meta) = tokio::fs::symlink_metadata(&temporary).await {
                if !meta.is_file() {
                    return Err(job_local_error(
                        Some(id),
                        "partial chunk must be a regular file",
                    ));
                }
                tokio::fs::remove_file(&temporary)
                    .await
                    .map_err(|_| job_local_error(Some(id), "cannot replace partial chunk"))?;
            }
            let mut output = tokio::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&temporary)
                .await
                .map_err(|_| job_local_error(Some(id), "cannot create partial chunk"))?;
            client
                .download_job_chunk(
                    id,
                    &chunk.chunk_id.to_string(),
                    chunk.bytes,
                    &chunk.sha256,
                    &mut output,
                )
                .await?;
            drop(output);
            verify_local_chunk(id, &temporary, chunk, &manifest.schema).await?;
            // Publish without overwriting a path created concurrently or left by a
            // different download. Both names are in the same output directory.
            tokio::fs::hard_link(&temporary, &final_path)
                .await
                .map_err(|_| {
                    job_local_error(
                        Some(id),
                        "cannot publish chunk without replacing an existing file",
                    )
                })?;
            tokio::fs::remove_file(&temporary)
                .await
                .map_err(|_| job_local_error(Some(id), "cannot remove completed partial chunk"))?;
        }
        Ok(manifest)
    }
}

pub(crate) async fn verify_local_chunk(
    id: uuid::Uuid,
    path: &std::path::Path,
    chunk: &ManifestChunk,
    schema: &arrow_schema::Schema,
) -> Result<(), RemoteError> {
    let path = path.to_path_buf();
    let expected_bytes = chunk.bytes;
    let expected_rows = chunk.rows;
    let checksum = chunk.sha256.clone();
    let schema = schema.clone();
    tokio::task::spawn_blocking(move || {
        use std::io::{Read, Seek};
        let bad = || {
            job_local_error(
                Some(id),
                "local chunk failed length, checksum, schema, or row-count validation",
            )
        };
        let meta = std::fs::symlink_metadata(&path).map_err(|_| bad())?;
        if !meta.is_file()
            || meta.len() != expected_bytes
            || expected_bytes > MAX_CHUNK_BYTES as u64
        {
            return Err(bad());
        }
        let mut file = std::fs::File::open(path).map_err(|_| bad())?;
        let mut digest = ring::digest::Context::new(&ring::digest::SHA256);
        let mut buffer = [0_u8; 64 * 1024];
        loop {
            let n = file.read(&mut buffer).map_err(|_| bad())?;
            if n == 0 {
                break;
            }
            digest.update(&buffer[..n]);
        }
        let actual = digest
            .finish()
            .as_ref()
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<String>();
        if actual != checksum {
            return Err(bad());
        }
        file.rewind().map_err(|_| bad())?;
        let reader = arrow_ipc::reader::StreamReader::try_new(file, None).map_err(|_| bad())?;
        if *reader.schema() != schema {
            return Err(bad());
        }
        let mut rows = 0u64;
        for batch in reader {
            rows = rows
                .checked_add(batch.map_err(|_| bad())?.num_rows() as u64)
                .ok_or_else(bad)?;
        }
        if rows != expected_rows {
            return Err(bad());
        }
        Ok(())
    })
    .await
    .map_err(|_| job_local_error(Some(id), "chunk verification task failed"))?
}

fn job_local_error(id: Option<uuid::Uuid>, message: impl Into<String>) -> RemoteError {
    RemoteError {
        status: None,
        failure_kind: Some("job_client_error".into()),
        retry_safe: false,
        message: message.into(),
        request_id: id.map(|id| id.to_string().into()),
        acceptance: None,
        status_url: id.map(|id| format!("/v1/jobs/{id}").into()),
    }
}
