use super::*;
use crate::{artifacts::ArtifactError, imports};
use axum::body::Body;
use futures_util::StreamExt;
use serde::Deserialize;
use std::sync::atomic::AtomicBool;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct UploadQuery {
    destination: String,
    mode: String,
    format: String,
    bytes: u64,
    sha256: String,
    options: Option<String>,
}

pub(super) async fn upload(
    State(app): State<Gateway>,
    Extension(identity): Extension<Identity>,
    Path(id): Path<Uuid>,
    Query(query): Query<UploadQuery>,
    body: Body,
) -> Result<Json<Value>, ApiError> {
    let destination = crate::catalog::destination(&query.destination)
        .map_err(ApiError::bad)?
        .normalized;
    let request = imports::RequestMetadata {
        id,
        destination,
        mode: query.mode,
        format: query.format,
        bytes: query.bytes,
        sha256: query.sha256,
        options: query.options.unwrap_or_else(|| "{}".into()),
    };
    let store = app.artifact_store.clone().ok_or_else(|| {
        ApiError::new(
            failure::COMPUTE_UNAVAILABLE,
            "private import staging is unavailable; no destination was mutated",
        )
    })?;
    let staged = {
        let request = request.clone();
        let identity = identity.clone();
        app.control_db(move |db| imports::begin(db, &identity, &request))
            .await?
    };
    let relative_key = staged.relative_key.clone();
    let stream = body
        .into_data_stream()
        .map(|chunk| chunk.map_err(|_| ArtifactError::Corrupt));
    let canceled = AtomicBool::new(false);
    let result = store
        .put_stream(
            crate::artifacts::StreamPut {
                relative: &relative_key,
                bytes: request.bytes,
                sha256: &request.sha256,
                maximum_bytes: imports::MAX_IMPORT_BYTES,
            },
            stream,
            tokio::time::Instant::now() + Duration::from_secs(600),
            &canceled,
        )
        .await;
    if let Err(error) = result {
        let _ = app.control_db(move |db| imports::mark_failed(db, id)).await;
        return Err(match error {
            ArtifactError::TooLarge | ArtifactError::Corrupt => ApiError::bad(
                "import transfer failed length, chunk, or checksum validation; cleanup is scheduled",
            ),
            _ => ApiError::new(
                failure::UNAVAILABLE,
                "import transfer failed before ingestion; cleanup is scheduled and no destination was mutated",
            ),
        });
    }
    {
        let identity = identity.clone();
        app.control_db(move |db| imports::mark_staged(db, &identity, id))
            .await?;
    }
    let staging_url = store
        .staging_url(&relative_key)
        .map_err(|_| ApiError::internal())?;
    Ok(Json(json!({
        "request_id":id,
        "state":"staged",
        "destination":request.destination,
        "mode":request.mode,
        "format":request.format,
        "bytes":request.bytes,
        "sha256":request.sha256,
        "staging_url":staging_url,
        "expires_at":staged.expires_at,
    })))
}

pub(super) async fn detail(
    State(app): State<Gateway>,
    Extension(identity): Extension<Identity>,
    Path(id): Path<Uuid>,
) -> Result<Json<Value>, ApiError> {
    app.control_db(move |db| imports::detail(db, &identity, id))
        .await
        .map(Json)
}

pub(super) async fn cleanup(
    State(app): State<Gateway>,
    Extension(identity): Extension<Identity>,
    Path(id): Path<Uuid>,
) -> Result<Json<Value>, ApiError> {
    let key = {
        let identity = identity.clone();
        app.control_db(move |db| imports::mark_cleanup(db, &identity, id))
            .await?
    };
    if let Some(key) = key {
        let store = app.artifact_store.as_ref().ok_or_else(ApiError::internal)?;
        store
            .delete(&key, tokio::time::Instant::now() + Duration::from_secs(30))
            .await
            .map_err(|_| {
                ApiError::new(
                    failure::UNAVAILABLE,
                    "staged object deletion failed; durable cleanup remains scheduled",
                )
            })?;
        app.control_db(move |db| imports::ack_delete_direct(db, id))
            .await?;
    }
    Ok(Json(json!({"request_id":id,"deleted":true})))
}
