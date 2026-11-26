use crate::client::{Client, RemoteConnection, RemoteError};
use serde_json::{Value, json};
use std::io::Read;
use std::path::Path;
use std::time::Duration;
use uuid::Uuid;

const MAX_IMPORT_BYTES: u64 = 1024 * 1024 * 1024;

pub enum Mode {
    Create,
    Append,
}

pub struct Options {
    pub path: String,
    pub destination: String,
    pub format: String,
    pub mode: Mode,
    pub delimiter: String,
    pub header: bool,
}

pub async fn run(url: &str, token: &str, options: Options) -> Result<(), RemoteError> {
    let destination = crate::catalog::destination(&options.destination).map_err(invalid)?;
    let path = Path::new(&options.path);
    let metadata = std::fs::metadata(path).map_err(|_| invalid("cannot read import file"))?;
    if !metadata.is_file() || metadata.len() > MAX_IMPORT_BYTES {
        return Err(invalid(
            "import input must be a regular file of at most 1 GiB; directories and globs are unsupported",
        ));
    }
    if options.format == "csv"
        && (options.delimiter.len() != 1
            || !options.delimiter.is_ascii()
            || matches!(options.delimiter.as_bytes()[0], b'\r' | b'\n' | 0))
    {
        return Err(invalid("CSV delimiter must be one ASCII character"));
    }
    let id = Uuid::new_v4();
    let digest = file_digest(path).await?;
    let client = Client::new(url, token)?;
    eprintln!(
        "transfer: request_id {id}; uploading {} bytes",
        metadata.len()
    );
    let upload_path = upload_path(
        &options,
        id,
        &destination.normalized,
        metadata.len(),
        &digest,
    )?;
    let file = tokio::fs::File::open(path)
        .await
        .map_err(|_| invalid("cannot reopen import file"))?;
    let stream = tokio_util::io::ReaderStream::with_capacity(file, 1024 * 1024);
    let body = reqwest::Body::wrap_stream(stream);
    let uploaded = client
        .upload(&upload_path, body, Duration::from_secs(600))
        .await;
    let value = match uploaded {
        Ok(bytes) => serde_json::from_slice::<Value>(&bytes)
            .map_err(|_| local_error("invalid_response", "import upload returned invalid JSON"))?,
        Err(error) => {
            let _ = cleanup(&client, id).await;
            return Err(error);
        }
    };
    let staging_url = value["staging_url"]
        .as_str()
        .ok_or_else(|| local_error("invalid_response", "import upload returned no staging URL"))?
        .to_owned();
    eprintln!(
        "execution: transfer verified; running atomic {} on request_id {id}",
        match options.mode {
            Mode::Create => "create",
            Mode::Append => "append",
        }
    );
    let execution = execute(url, token, id, &destination, &staging_url, &options).await;
    let cleanup_result = cleanup(&client, id).await;
    match (execution, cleanup_result) {
        (Ok(()), Ok(())) => {
            println!(
                "imported {} bytes into {} (request_id: {id})",
                metadata.len(),
                destination.normalized
            );
            Ok(())
        }
        (Ok(()), Err(error)) => Err(local_error(
            "import_cleanup_failed",
            format!(
                "SQL succeeded and the destination committed, but staged cleanup failed; maintenance will retry using request_id {id}: {error}"
            ),
        )),
        (Err(error), Ok(())) => Err(error),
        (Err(error), Err(cleanup)) => Err(local_error(
            "import_cleanup_failed",
            format!(
                "ingestion failed: {error}; staged cleanup also failed and maintenance will retry using request_id {id}: {cleanup}"
            ),
        )),
    }
}

fn upload_path(
    options: &Options,
    id: Uuid,
    destination: &str,
    bytes: u64,
    sha256: &str,
) -> Result<String, RemoteError> {
    let mode = match options.mode {
        Mode::Create => "create",
        Mode::Append => "append",
    };
    let options_json = json!({"delimiter":options.delimiter,"header":options.header}).to_string();
    let mut url = reqwest::Url::parse(&format!("http://duckfleet.local/v1/imports/{id}/upload"))
        .expect("static import URL");
    url.query_pairs_mut()
        .append_pair("destination", destination)
        .append_pair("mode", mode)
        .append_pair("format", &options.format)
        .append_pair("bytes", &bytes.to_string())
        .append_pair("sha256", sha256)
        .append_pair("options", &options_json);
    Ok(format!(
        "/v1/imports/{id}/upload?{}",
        url.query().expect("import query")
    ))
}

async fn execute(
    url: &str,
    token: &str,
    id: Uuid,
    destination: &crate::catalog::Destination,
    staging_url: &str,
    options: &Options,
) -> Result<(), RemoteError> {
    let mut connection = RemoteConnection::open(
        url,
        token,
        "import",
        None,
        Some(json!({"import_id":id})),
        90,
    )
    .await?;
    let source = match options.format.as_str() {
        "parquet" => format!("read_parquet('{}')", sql_literal(staging_url)),
        "csv" => format!(
            "read_csv('{}', auto_detect=true, header={}, delim='{}')",
            sql_literal(staging_url),
            options.header,
            sql_literal(&options.delimiter)
        ),
        _ => return Err(invalid("unsupported import format")),
    };
    let mutation = match options.mode {
        Mode::Create => format!(
            "CREATE TABLE {} AS SELECT * FROM {source}",
            destination.qualified
        ),
        Mode::Append => format!(
            "INSERT INTO {} SELECT * FROM {source}",
            destination.qualified
        ),
    };
    let mut statements = vec!["BEGIN".to_owned()];
    if matches!(options.mode, Mode::Create) {
        statements.push(format!(
            "CREATE SCHEMA IF NOT EXISTS {}",
            quote_identifier(&destination.schema)
        ));
    }
    statements.push(mutation);
    statements.push("COMMIT".into());
    let mut outcome = Ok(());
    for statement in statements {
        if let Err(error) = execute_statement(&connection, &statement).await {
            outcome = Err(error);
            break;
        }
    }
    if let Err(error) = &outcome
        && error.acceptance.as_deref() != Some("unknown")
        && error.failure_kind.as_deref() != Some("transport_failure")
        && error.failure_kind.as_deref() != Some("unknown_session")
    {
        let _ = connection.rollback().await;
    }
    if !outcome
        .as_ref()
        .is_err_and(|error| error.acceptance.as_deref() == Some("unknown"))
    {
        let _ = connection.close().await;
    }
    outcome
}

async fn execute_statement(connection: &RemoteConnection, sql: &str) -> Result<(), RemoteError> {
    let request_id = Uuid::new_v4().to_string();
    let execution =
        connection.execute_with_timeouts_and_id(sql, Vec::new(), 600, 120, request_id.clone());
    tokio::pin!(execution);
    tokio::select! {
        result = &mut execution => result.map(|_| ()),
        _ = tokio::signal::ctrl_c() => {
            eprintln!("canceling import statement request_id: {request_id}");
            connection.cancel().await?;
            let terminal = crate::shell::wait_for_terminal(connection, &request_id).await?;
            if terminal["status"] == "succeeded" {
                return Ok(());
            }
            Err(local_error("import_canceled", format!("import was canceled before commit; staged cleanup was requested and SQL was not replayed (request_id: {request_id})")))
        }
    }
}

async fn cleanup(client: &Client, id: Uuid) -> Result<(), RemoteError> {
    client
        .request(
            reqwest::Method::DELETE,
            &format!("/v1/imports/{id}"),
            None,
            Duration::from_secs(30),
        )
        .await
        .map(|_| ())
}

async fn file_digest(path: &Path) -> Result<String, RemoteError> {
    let path = path.to_path_buf();
    tokio::task::spawn_blocking(move || {
        let mut file = std::fs::File::open(path).map_err(|_| invalid("cannot read import file"))?;
        let mut digest = ring::digest::Context::new(&ring::digest::SHA256);
        let mut buffer = [0_u8; 1024 * 1024];
        loop {
            let read = file
                .read(&mut buffer)
                .map_err(|_| invalid("cannot read import file"))?;
            if read == 0 {
                break;
            }
            digest.update(&buffer[..read]);
        }
        Ok::<_, RemoteError>(
            digest
                .finish()
                .as_ref()
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>(),
        )
    })
    .await
    .map_err(|_| local_error("import_client_error", "file hashing task failed"))?
}

fn quote_identifier(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

fn sql_literal(value: &str) -> String {
    value.replace('\'', "''")
}

fn invalid(message: impl Into<String>) -> RemoteError {
    local_error("invalid_request", message)
}

fn local_error(kind: &str, message: impl Into<String>) -> RemoteError {
    RemoteError {
        status: None,
        failure_kind: Some(kind.into()),
        retry_safe: false,
        message: message.into(),
        request_id: None,
        acceptance: Some("not_accepted".into()),
        status_url: None,
    }
}

pub fn infer_format(path: &str, explicit: Option<&str>) -> Result<String, String> {
    if let Some(format) = explicit {
        if matches!(format, "csv" | "parquet") {
            return Ok(format.into());
        }
        return Err("--format must be csv or parquet".into());
    }
    let path = path.to_ascii_lowercase();
    if path.ends_with(".csv") {
        Ok("csv".into())
    } else if path.ends_with(".parquet") {
        Ok("parquet".into())
    } else {
        Err("cannot infer import format; use --format csv or parquet".into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn format_inference_and_destination_validation_are_pre_transfer() {
        assert_eq!(infer_format("events.csv", None).unwrap(), "csv");
        assert_eq!(infer_format("EVENTS.PARQUET", None).unwrap(), "parquet");
        assert_eq!(infer_format("events.bin", Some("csv")).unwrap(), "csv");
        assert!(infer_format("events.bin", None).is_err());
        assert!(infer_format("events.csv", Some("json")).is_err());
        assert!(crate::catalog::destination("analytics.events").is_ok());
        assert!(crate::catalog::destination("events").is_err());
    }
}
