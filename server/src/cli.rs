//! Command-line access to a DuckFleet warehouse, with no dbt involved — the
//! `cli` role of the one binary. It covers what a warehouse needs from a shell:
//! run statements, choose the compute they run on, and read back what ran and
//! what it cost. Statements given together share one session, because a DDL, a
//! load and a query are usually one unit of work and a session is what pins them
//! to a single worker.
//!
//! Resource flags alone preserve the dedicated-pod behavior. Explicit `--mode`
//! and `--pool` select a persistent pool or isolated compute; placement stays
//! fixed for all statements passed to one invocation.

use serde_json::{Map, Value, json};
use std::io::IsTerminal;

use crate::client::{Client, QueryResult, RemoteConnection, RemoteError};

/// Entry point for the `cli` role. `args` is the argv after the binary name
/// (i.e. the subcommand and its flags). Returns a process exit code.
pub async fn run(args: Vec<String>) -> i32 {
    let parsed = match Parsed::from_args(&args) {
        Ok(parsed) => parsed,
        Err(message) => {
            eprintln!("{message}");
            return 2;
        }
    };
    if parsed.command == "version" {
        println!(
            "duckfleet {} ({}-{})",
            env!("CARGO_PKG_VERSION"),
            std::env::consts::OS,
            std::env::consts::ARCH
        );
        if let Some(release) = option_env!("DUCKFLEET_RELEASE") {
            println!("release: {release}");
        }
        return 0;
    }
    if parsed.command == "init" {
        match crate::cli_config::init(
            parsed.value("url"),
            parsed.value("token"),
            parsed.flags.iter().any(|flag| flag == "force"),
        ) {
            Ok(message) => {
                println!("{message}");
                return 0;
            }
            Err(message) => {
                eprintln!("{message}");
                return 2;
            }
        }
    }
    let resolved = match crate::cli_config::resolve(parsed.value("url"), parsed.value("token")) {
        Ok(resolved) => resolved,
        Err(message) => {
            eprintln!("{message}");
            return 2;
        }
    };
    if parsed.command == "config" {
        println!(
            "{}",
            serde_json::to_string_pretty(&resolved.redacted()).unwrap_or_default()
        );
        return 0;
    }
    if matches!(parsed.command.as_str(), "dev" | "status" | "stop" | "reset") {
        let settings = match crate::cli_config::development(&resolved) {
            Ok(settings) => settings,
            Err(message) => {
                eprintln!("{message}");
                return 2;
            }
        };
        return crate::dev::run(
            &parsed.command,
            settings,
            parsed.flags.iter().any(|flag| flag == "demo"),
            parsed.flags.iter().any(|flag| flag == "yes"),
        )
        .await;
    }
    let (Some(url), Some(token)) = (resolved.url, resolved.token) else {
        eprintln!(
            "missing DuckFleet URL or token; run duckfleet init, or set --url/--token or DUCKFLEET_URL/DUCKFLEET_TOKEN"
        );
        return 2;
    };

    if parsed.command == "shell" {
        return crate::shell::run(crate::shell::ShellOptions {
            url,
            token,
            compute: parsed.compute().expect("validated compute"),
            queue_timeout: parsed.queue_timeout().expect("validated queue timeout"),
            format: parsed.value("format").unwrap_or("table").to_owned(),
            history: !parsed.flags.iter().any(|flag| flag == "no-history"),
        })
        .await;
    }

    let result = match parsed.command.as_str() {
        "sql" => run_sql(&url, &token, &parsed).await,
        "schemas" | "tables" | "views" | "describe" | "preview" => {
            run_catalog(&url, &token, &parsed).await
        }
        "system" => run_system(&url, &token, &parsed).await,
        "job" => run_job(&url, &token, &parsed).await,
        "history" => {
            let mut path = format!("/v1/queries?limit={}", parsed.limit(20));
            if let Some(worker) = parsed.value("worker-id") {
                path.push_str(&format!("&worker_id={worker}"));
            }
            get(&url, &token, &path).await
        }
        "query" => match parsed.positionals.first() {
            Some(id) => get(&url, &token, &format!("/v1/queries/{id}")).await,
            None => {
                eprintln!("query needs an id");
                return 2;
            }
        },
        "request" => match parsed.positionals.first() {
            Some(id) => get(&url, &token, &format!("/v1/requests/{id}")).await,
            None => {
                eprintln!("request needs an id");
                return 2;
            }
        },
        "cancel" => match parsed.positionals.first() {
            Some(id) => {
                request(
                    &url,
                    &token,
                    reqwest::Method::POST,
                    &format!("/v1/requests/{id}/cancel"),
                )
                .await
            }
            None => {
                eprintln!("cancel needs a request id");
                return 2;
            }
        },
        "usage" => {
            get(
                &url,
                &token,
                &format!("/v1/usage?limit={}", parsed.limit(20)),
            )
            .await
        }
        "budget" => get(&url, &token, "/v1/budget").await,
        "workers" => get(&url, &token, "/v1/workers").await,
        "pools" => get(&url, &token, "/v1/pools").await,
        "principals" | "credentials" | "audit" => {
            let mut query = format!("/v1/{}?limit={}", parsed.command, parsed.limit(100));
            let cursor_flag = if parsed.command == "audit" {
                "before"
            } else {
                "after"
            };
            if let Some(cursor) = parsed.value(cursor_flag) {
                query.push_str(&format!("&{cursor_flag}={cursor}"));
            }
            get(&url, &token, &query).await
        }
        "policy" => run_policy(&url, &token, &parsed).await,
        other => {
            eprintln!(
                "unknown command: {other} (init|config|version|dev|status|stop|reset|shell|sql|schemas|tables|views|describe|preview|system|job|history|query|request|cancel|usage|budget|workers|pools|principals|credentials|policy|audit)"
            );
            return 2;
        }
    };

    match result {
        Ok(()) => 0,
        Err(error) => {
            print_remote_error(&error);
            1
        }
    }
}

pub(crate) fn print_remote_error(error: &RemoteError) {
    let status = error
        .status
        .map(|code| code.to_string())
        .unwrap_or_else(|| "-".to_owned());
    let kind = error.failure_kind.as_deref().unwrap_or("error");
    eprintln!("{kind} (HTTP {status}): {error}");
    eprintln!("retry_safe: {}", error.retry_safe);
    if error.failure_kind.as_deref() == Some("transport_failure") {
        eprintln!("next: run duckfleet config show and confirm the configured server is running");
    }
    if let Some(id) = &error.request_id {
        eprintln!(
            "request_id: {id}; acceptance: {}",
            error.acceptance.as_deref().unwrap_or("unknown")
        );
        if let Some(url) = &error.status_url {
            eprintln!("status: {url}");
        }
    }
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
fn print_json(value: &Value) {
    println!(
        "{}",
        serde_json::to_string_pretty(value).unwrap_or_default()
    );
}

async fn run_job(url: &str, token: &str, parsed: &Parsed) -> Result<(), RemoteError> {
    let client = Client::new(url, token)?;
    if parsed.is_job_submit() {
        let request = parsed
            .job_request(uuid::Uuid::new_v4())
            .map_err(|e| job_local_error(None, e))?;
        // Emit the stable reconciliation handle before any request can commit.
        eprintln!(
            "request_id: {}; status: /v1/jobs/{}",
            request.request_id, request.request_id
        );
        print_json(&client.submit_job(&request).await?);
        return Ok(());
    }
    let id = uuid::Uuid::parse_str(&parsed.positionals[1]).expect("validated job ID");
    match parsed.positionals[0].as_str() {
        "cancel" => print_json(&client.cancel_job(id).await?),
        "status" => {
            let waiting = parsed.flags.iter().any(|f| f == "wait");
            loop {
                let status = client.job(id).await?;
                let terminal =
                    status["state"] == "terminal" || status["request"]["state"] == "terminal";
                print_json(&status);
                if !waiting || terminal {
                    break;
                }
                tokio::select! {
                    _=tokio::time::sleep(std::time::Duration::from_secs(2))=>{},
                    _=tokio::signal::ctrl_c()=>break,
                }
            }
        }
        "result" => {
            download_job_result(
                &client,
                id,
                parsed.value("output").expect("validated output"),
                parsed.flags.iter().any(|f| f == "resume"),
            )
            .await?
        }
        _ => unreachable!("validated job command"),
    }
    Ok(())
}

async fn download_job_result(
    client: &Client,
    id: uuid::Uuid,
    directory: &str,
    resume: bool,
) -> Result<(), RemoteError> {
    use tokio::io::AsyncWriteExt;
    let value = client.job_result(id).await?;
    if value["complete"] != true {
        print_json(&value);
        return Err(job_local_error(
            Some(id),
            "job result is not complete; inspect status before downloading",
        ));
    }
    let manifest: crate::artifacts::Manifest = serde_json::from_value(value.clone())
        .map_err(|_| job_local_error(Some(id), "invalid job result manifest"))?;
    if manifest.request_id != id
        || manifest.bytes > crate::job_policy::JobLimits::HARD.result_bytes
        || manifest.rows > crate::job_policy::JobLimits::HARD.result_rows
    {
        return Err(job_local_error(
            Some(id),
            "job manifest identity or result bounds are invalid",
        ));
    }
    let encoded_manifest = manifest
        .encode()
        .map_err(|_| job_local_error(Some(id), "job result manifest failed validation"))?;
    let path = std::path::Path::new(directory);
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
            .map_err(|_| job_local_error(Some(id), "resume needs the matching manifest.json"))?;
        if !meta.is_file() || meta.len() > crate::artifacts::MAX_MANIFEST_BYTES as u64 {
            return Err(job_local_error(
                Some(id),
                "resume manifest must be a bounded regular file",
            ));
        }
        let stored = tokio::fs::read(manifest_path)
            .await
            .map_err(|_| job_local_error(Some(id), "resume needs the matching manifest.json"))?;
        if stored.len() > crate::artifacts::MAX_MANIFEST_BYTES
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
    eprintln!(
        "downloaded {} rows in {} chunks to {}",
        manifest.rows,
        manifest.chunks.len(),
        directory
    );
    Ok(())
}

async fn verify_local_chunk(
    id: uuid::Uuid,
    path: &std::path::Path,
    chunk: &crate::artifacts::ManifestChunk,
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
            || expected_bytes > crate::artifacts::MAX_CHUNK_BYTES as u64
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

async fn run_catalog(url: &str, token: &str, parsed: &Parsed) -> Result<(), RemoteError> {
    let request = match parsed.command.as_str() {
        "schemas" => crate::catalog::Request::Schemas,
        "tables" => crate::catalog::Request::Relations {
            kind: crate::catalog::RelationKind::Table,
            schema: parsed.value("schema").map(str::to_owned),
        },
        "views" => crate::catalog::Request::Relations {
            kind: crate::catalog::RelationKind::View,
            schema: parsed.value("schema").map(str::to_owned),
        },
        "describe" => crate::catalog::Request::Describe(parsed.positionals[0].clone()),
        "preview" => crate::catalog::Request::Preview(
            parsed.positionals[0].clone(),
            crate::catalog::validate_limit(parsed.value("limit")).map_err(|message| {
                RemoteError {
                    status: None,
                    failure_kind: Some("invalid_request".into()),
                    retry_safe: true,
                    message,
                    request_id: None,
                    acceptance: Some("not_accepted".into()),
                    status_url: None,
                }
            })?,
        ),
        _ => unreachable!("validated catalog command"),
    };
    crate::catalog::run(
        url,
        token,
        request,
        parsed.value("format").unwrap_or("table"),
        crate::query::DEFAULT_QUEUE_TIMEOUT,
    )
    .await
}

async fn run_system(url: &str, token: &str, parsed: &Parsed) -> Result<(), RemoteError> {
    let sql = match parsed.positionals[0].as_str() {
        "tables" => "SHOW TABLES FROM duckfleet_system".into(),
        "describe" => format!("DESCRIBE duckfleet_system.{}", parsed.positionals[1]),
        "sql" => parsed.positionals[1].clone(),
        _ => unreachable!("validated system command"),
    };
    let parameters = parsed
        .params
        .iter()
        .cloned()
        .map(Value::String)
        .collect::<Vec<_>>();
    let result = Client::new(url, token)?
        .system_sql(&sql, &parameters, None)
        .await?;
    if parsed.value("format") == Some("json") {
        println!(
            "{}",
            serde_json::to_string_pretty(&result).unwrap_or_default()
        );
    } else {
        if let Some(columns) = result["columns"].as_array() {
            println!(
                "{}",
                columns
                    .iter()
                    .map(|c| c["name"].as_str().unwrap_or("?"))
                    .collect::<Vec<_>>()
                    .join("\t")
            );
        }
        if let Some(rows) = result["rows"].as_array() {
            for row in rows.iter().filter_map(Value::as_array) {
                println!(
                    "{}",
                    row.iter()
                        .map(|v| match v {
                            Value::Null => "NULL".into(),
                            Value::String(s) => s.clone(),
                            _ => v.to_string(),
                        })
                        .collect::<Vec<_>>()
                        .join("\t")
                );
            }
        }
    }
    Ok(())
}

async fn run_policy(url: &str, token: &str, parsed: &Parsed) -> Result<(), RemoteError> {
    let principal = &parsed.positionals[0]; // validated before making a connection
    let path = format!("/v1/principals/{principal}/policy");
    match parsed.value("file") {
        None => get(url, token, &path).await,
        Some(path_to_policy) => {
            let invalid = |message: String| RemoteError {
                status: None,
                failure_kind: Some("invalid_request".into()),
                retry_safe: true,
                message,
                request_id: None,
                acceptance: None,
                status_url: None,
            };
            let metadata = std::fs::metadata(path_to_policy)
                .map_err(|_| invalid("cannot read policy file".into()))?;
            if metadata.len() > 65_536 {
                return Err(invalid("policy file must not exceed 64 KiB".into()));
            }
            let contents = std::fs::read(path_to_policy)
                .map_err(|_| invalid("cannot read policy file".into()))?;
            let policy: Value = serde_json::from_slice(&contents)
                .map_err(|_| invalid("policy file must contain a JSON object".into()))?;
            if !policy.is_object() {
                return Err(invalid("policy file must contain a JSON object".into()));
            }
            let client = Client::new(url, token)?;
            let bytes = client
                .request(
                    reqwest::Method::PUT,
                    &path,
                    Some(&policy),
                    std::time::Duration::from_secs(75),
                )
                .await?;
            let value: Value = serde_json::from_slice(&bytes)
                .map_err(|_| invalid("server returned an invalid policy response".into()))?;
            println!(
                "{}",
                serde_json::to_string_pretty(&value).unwrap_or_default()
            );
            Ok(())
        }
    }
}

async fn run_sql(url: &str, token: &str, parsed: &Parsed) -> Result<(), RemoteError> {
    let invalid = |message: String| RemoteError {
        request_id: None,
        acceptance: None,
        status_url: None,
        status: None,
        failure_kind: Some("invalid_request".into()),
        retry_safe: true,
        message,
    };
    let compute = parsed.compute().map_err(invalid)?;
    let statements = if let Some(path) = parsed.value("file") {
        let text = if path == "-" {
            read_stdin(&invalid)?
        } else {
            let metadata =
                std::fs::metadata(path).map_err(|_| invalid("cannot read SQL file".into()))?;
            if !metadata.is_file() || metadata.len() > 1024 * 1024 {
                return Err(invalid(
                    "SQL file must be a regular file of at most one MiB".into(),
                ));
            }
            std::fs::read_to_string(path)
                .map_err(|_| invalid("cannot read SQL file as UTF-8".into()))?
        };
        crate::shell::read_statements(&text).map_err(invalid)?
    } else if parsed.positionals.is_empty() {
        if std::io::stdin().is_terminal() {
            return Err(invalid(
                "sql needs a statement, --file <path>, or piped stdin; use duckfleet shell for interactive input"
                    .into(),
            ));
        }
        let text = read_stdin(&invalid)?;
        crate::shell::read_statements(&text).map_err(invalid)?
    } else {
        parsed.positionals.clone()
    };
    if statements.is_empty() {
        return Ok(());
    }
    let mut connection = RemoteConnection::open(url, token, "cli", compute, None, 90).await?;
    if parsed.flags.contains(&"placement".to_owned()) {
        eprintln!("{}", connection.placement);
    }
    let parameters: Vec<Value> = parsed
        .params
        .iter()
        .map(|p| Value::String(p.clone()))
        .collect();
    let format = parsed.value("format").unwrap_or("table");
    let continue_on_error = parsed.flags.iter().any(|flag| flag == "continue-on-error");
    let mut outcome = Ok(());
    for statement in &statements {
        match connection
            .execute_with_queue_timeout(
                statement,
                parameters.clone(),
                parsed.queue_timeout().expect("validated arguments"),
            )
            .await
        {
            Ok((_, result)) => emit(&result, format),
            Err(error) => {
                let unknown = error.acceptance.as_deref() == Some("unknown")
                    || error.failure_kind.as_deref() == Some("transport_failure")
                    || error.failure_kind.as_deref() == Some("unknown_session");
                if outcome.is_ok() {
                    outcome = Err(error);
                }
                if unknown || !continue_on_error {
                    break;
                }
            }
        }
    }
    // An unknown transport outcome must not turn into an implicit queued cancel
    // through close. The worker expires this connection after its work ends.
    if !outcome
        .as_ref()
        .is_err_and(|error| error.acceptance.as_deref() == Some("unknown"))
    {
        let _ = connection.close().await;
    }
    outcome
}

fn read_stdin(invalid: &impl Fn(String) -> RemoteError) -> Result<String, RemoteError> {
    use std::io::Read;
    let mut bytes = Vec::new();
    std::io::stdin()
        .take(1024 * 1024 + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| invalid("cannot read SQL from stdin".into()))?;
    if bytes.len() > 1024 * 1024 {
        return Err(invalid("stdin SQL must not exceed one MiB".into()));
    }
    String::from_utf8(bytes).map_err(|_| invalid("stdin SQL must be UTF-8".into()))
}

async fn get(url: &str, token: &str, path: &str) -> Result<(), RemoteError> {
    request(url, token, reqwest::Method::GET, path).await
}

async fn request(
    url: &str,
    token: &str,
    method: reqwest::Method,
    path: &str,
) -> Result<(), RemoteError> {
    let client = Client::new(url, token)?;
    let bytes = client
        .request(method, path, None, std::time::Duration::from_secs(75))
        .await?;
    let value: Value = serde_json::from_slice(&bytes)
        .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(&bytes).into_owned()));
    println!(
        "{}",
        serde_json::to_string_pretty(&value).unwrap_or_default()
    );
    Ok(())
}

pub(crate) fn emit(result: &QueryResult, format: &str) {
    if result.columns.is_empty() {
        return;
    }
    match format {
        "json" => emit_json(result),
        "csv" => emit_csv(result),
        "vertical" => print!("{}", crate::shell::result_to_vertical(result)),
        _ => emit_table(result),
    }
}

fn emit_json(result: &QueryResult) {
    let rows: Vec<Value> = result
        .rows
        .iter()
        .map(|row| {
            let mut object = Map::new();
            for (index, name) in result.columns.iter().enumerate() {
                let cell = row.get(index).and_then(Option::as_deref);
                let typed = cell
                    .map(|text| json_cell(text, &result.column_types[index]))
                    .unwrap_or(Value::Null);
                object.insert(name.clone(), typed);
            }
            Value::Object(object)
        })
        .collect();
    println!(
        "{}",
        serde_json::to_string_pretty(&Value::Array(rows)).unwrap_or_default()
    );
}

/// Renders a stringified cell as a JSON value, using the Arrow column type to
/// decide whether it is a number or boolean rather than always a string.
fn json_cell(text: &str, column_type: &str) -> Value {
    let lower = column_type.to_ascii_lowercase();
    if (lower.contains("int") || lower.contains("float") || lower.contains("decimal"))
        && let Ok(number) = serde_json::from_str::<serde_json::Number>(text)
    {
        return Value::Number(number);
    }
    if lower == "boolean"
        && let Ok(flag) = text.parse::<bool>()
    {
        return Value::Bool(flag);
    }
    Value::String(text.to_owned())
}

fn emit_table(result: &QueryResult) {
    let cell = |value: &Option<String>| value.clone().unwrap_or_default();
    let widths: Vec<usize> = result
        .columns
        .iter()
        .enumerate()
        .map(|(index, name)| {
            let widest = result
                .rows
                .iter()
                .map(|row| cell(&row[index]).chars().count())
                .max()
                .unwrap_or(0);
            widest.max(name.chars().count())
        })
        .collect();
    let header = result
        .columns
        .iter()
        .zip(&widths)
        .map(|(name, width)| format!("{name:<width$}"))
        .collect::<Vec<_>>()
        .join("  ");
    println!("{header}");
    println!(
        "{}",
        widths
            .iter()
            .map(|width| "-".repeat(*width))
            .collect::<Vec<_>>()
            .join("  ")
    );
    for row in &result.rows {
        let line = row
            .iter()
            .zip(&widths)
            .map(|(value, width)| format!("{:<width$}", cell(value)))
            .collect::<Vec<_>>()
            .join("  ");
        println!("{line}");
    }
    let count = result.rows.len();
    println!("({count} row{})", if count == 1 { "" } else { "s" });
}

fn emit_csv(result: &QueryResult) {
    println!(
        "{}",
        result
            .columns
            .iter()
            .map(|name| csv_field(name))
            .collect::<Vec<_>>()
            .join(",")
    );
    for row in &result.rows {
        println!(
            "{}",
            row.iter()
                .map(|value| csv_field(value.as_deref().unwrap_or("")))
                .collect::<Vec<_>>()
                .join(",")
        );
    }
}

fn csv_field(value: &str) -> String {
    if value.contains([',', '"', '\n', '\r']) {
        format!("\"{}\"", value.replace('"', "\"\""))
    } else {
        value.to_owned()
    }
}

/// A tiny argv parser, in the project's minimal-dependency style (no clap). The
/// first bare word is the subcommand; the rest are its positionals, with a fixed
/// set of value flags and one boolean flag recognised everywhere.
struct Parsed {
    command: String,
    positionals: Vec<String>,
    values: Vec<(String, String)>,
    params: Vec<String>,
    flags: Vec<String>,
}

impl Parsed {
    const VALUE_FLAGS: [&'static str; 22] = [
        "url",
        "token",
        "cpu",
        "memory",
        "disk",
        "mode",
        "pool",
        "format",
        "schema",
        "limit",
        "queue-timeout",
        "file",
        "after",
        "before",
        "request-id",
        "retry-of",
        "timeout",
        "result-retention",
        "result-max-bytes",
        "result-max-rows",
        "output",
        "sql",
    ];
    const COMPUTE_FLAGS: [&'static str; 5] = ["cpu", "memory", "disk", "mode", "pool"];

    fn from_args(args: &[String]) -> Result<Self, String> {
        let mut command: Option<String> = None;
        let mut positionals = Vec::new();
        let mut values = Vec::new();
        let mut params = Vec::new();
        let mut flags = Vec::new();
        let mut index = 0;
        while index < args.len() {
            let arg = &args[index];
            if let Some(name) = arg.strip_prefix("--") {
                if [
                    "placement",
                    "wait",
                    "resume",
                    "force",
                    "demo",
                    "yes",
                    "no-history",
                    "continue-on-error",
                ]
                .contains(&name)
                {
                    flags.push(name.to_owned());
                    index += 1;
                } else if name == "param"
                    || name == "worker-id"
                    || Self::VALUE_FLAGS.contains(&name)
                {
                    let value = args
                        .get(index + 1)
                        .ok_or_else(|| format!("--{name} needs a value"))?;
                    if name == "param" {
                        params.push(value.clone());
                    } else {
                        if values.iter().any(|(key, _)| key == name) {
                            return Err(format!("--{name} must be supplied only once"));
                        }
                        values.push((name.to_owned(), value.clone()));
                    }
                    index += 2;
                } else {
                    return Err(format!("unknown flag: --{name}"));
                }
            } else if command.is_none() {
                command = Some(arg.clone());
                index += 1;
            } else {
                positionals.push(arg.clone());
                index += 1;
            }
        }
        let parsed = Parsed {
            command: command.ok_or(
                "usage: <init|config|version|dev|status|stop|reset|shell|sql|schemas|tables|views|describe|preview|system|job|history|query|request|cancel|usage|budget|workers|pools|principals|credentials|policy|audit> ...",
            )?,
            positionals,
            values,
            params,
            flags,
        };
        if parsed.command == "init" && !parsed.positionals.is_empty() {
            return Err("init takes no positional arguments".into());
        }
        if parsed.command == "config" && parsed.positionals != ["show"] {
            return Err("config needs the show subcommand".into());
        }
        if parsed.command == "version" && !parsed.positionals.is_empty() {
            return Err("version takes no positional arguments".into());
        }
        if matches!(parsed.command.as_str(), "dev" | "status" | "stop" | "reset")
            && !parsed.positionals.is_empty()
        {
            return Err(format!("{} takes no positional arguments", parsed.command));
        }
        if parsed.flags.iter().any(|flag| flag == "demo") && parsed.command != "dev" {
            return Err("--demo applies only to dev".into());
        }
        if parsed.flags.iter().any(|flag| flag == "yes") && parsed.command != "reset" {
            return Err("--yes applies only to reset".into());
        }
        if parsed.command == "shell" && !parsed.positionals.is_empty() {
            return Err("shell takes no positional arguments".into());
        }
        if parsed.command == "shell"
            && parsed
                .value("format")
                .is_some_and(|format| !["table", "csv", "json", "vertical"].contains(&format))
        {
            return Err("shell --format must be table, csv, json, or vertical".into());
        }
        if parsed.flags.iter().any(|flag| flag == "no-history") && parsed.command != "shell" {
            return Err("--no-history applies only to shell".into());
        }
        if parsed.flags.iter().any(|flag| flag == "continue-on-error") && parsed.command != "sql" {
            return Err("--continue-on-error applies only to sql".into());
        }
        if parsed.flags.iter().any(|flag| flag == "force") && parsed.command != "init" {
            return Err("--force applies only to init".into());
        }
        match parsed.command.as_str() {
            "schemas" if parsed.positionals.is_empty() => {}
            "tables" | "views" if parsed.positionals.is_empty() => {}
            "describe" | "preview" if parsed.positionals.len() == 1 => {
                crate::catalog::validate_relation(&parsed.positionals[0])?;
            }
            "schemas" | "tables" | "views" | "describe" | "preview" => {
                return Err("catalog command has invalid positional arguments".into());
            }
            _ => {}
        }
        if parsed.value("schema").is_some()
            && !matches!(parsed.command.as_str(), "tables" | "views")
        {
            return Err("--schema applies only to tables and views".into());
        }
        if matches!(
            parsed.command.as_str(),
            "schemas" | "tables" | "views" | "describe" | "preview"
        ) && parsed
            .value("format")
            .is_some_and(|format| !["table", "csv", "json", "vertical"].contains(&format))
        {
            return Err("catalog --format must be table, csv, json, or vertical".into());
        }
        if parsed.command == "preview" {
            crate::catalog::validate_limit(parsed.value("limit"))?;
        }
        if !matches!(parsed.command.as_str(), "sql" | "shell")
            && !parsed.is_job_submit()
            && parsed
                .values
                .iter()
                .any(|(name, _)| Self::COMPUTE_FLAGS.contains(&name.as_str()))
        {
            return Err("compute selection flags apply only to sql or job submit".into());
        }
        if parsed.command == "system" {
            let valid = match parsed.positionals.first().map(String::as_str) {
                Some("tables") => parsed.positionals.len() == 1 && parsed.params.is_empty(),
                Some("describe") => {
                    parsed.positionals.len() == 2
                        && parsed.params.is_empty()
                        && crate::metadata::find_relation(&parsed.positionals[1]).is_some()
                }
                Some("sql") => parsed.positionals.len() == 2,
                _ => false,
            };
            if !valid {
                return Err("system needs tables, describe <query_history|query_attempts|usage>, or sql <one SQL statement>".into());
            }
            if parsed
                .value("format")
                .is_some_and(|v| !["table", "json"].contains(&v))
            {
                return Err("system --format must be table or json".into());
            }
        }
        if !matches!(parsed.command.as_str(), "sql" | "shell")
            && !parsed.is_job_submit()
            && parsed.value("queue-timeout").is_some()
        {
            return Err("--queue-timeout applies only to sql or job submit".into());
        }
        if parsed.value("file").is_some() && !matches!(parsed.command.as_str(), "policy" | "sql") {
            return Err("--file applies only to the policy and sql commands".into());
        }
        if parsed.command == "sql"
            && parsed.value("file").is_some()
            && !parsed.positionals.is_empty()
        {
            return Err("sql --file cannot be combined with positional SQL statements".into());
        }
        if parsed.command == "policy"
            && (parsed.positionals.len() != 1
                || !crate::credentials::valid_principal(&parsed.positionals[0]))
        {
            return Err(
                "policy needs one valid principal name; use --file to replace its entire policy"
                    .into(),
            );
        }
        for cursor in ["after", "before"] {
            if let Some(value) = parsed.value(cursor) {
                let allowed = if cursor == "before" {
                    parsed.command == "audit"
                } else {
                    matches!(parsed.command.as_str(), "principals" | "credentials")
                };
                if !allowed || uuid::Uuid::parse_str(value).is_err() {
                    return Err(format!(
                        "--{cursor} needs a valid cursor from the corresponding listing"
                    ));
                }
            }
        }
        if parsed.command != "preview"
            && let Some(value) = parsed.value("limit")
            && value
                .parse::<u32>()
                .ok()
                .is_none_or(|n| !(1..=500).contains(&n))
        {
            return Err("--limit must be a whole number from 1 to 500".into());
        }
        for flag in [
            "request-id",
            "retry-of",
            "timeout",
            "result-retention",
            "result-max-bytes",
            "result-max-rows",
            "sql",
        ] {
            if parsed.value(flag).is_some() && !parsed.is_job_submit() {
                return Err(format!("--{flag} applies only to job submit"));
            }
        }
        if parsed.value("output").is_some()
            && !(parsed.command == "job"
                && parsed.positionals.first().is_some_and(|s| s == "result"))
        {
            return Err("--output applies only to job result".into());
        }
        for (flag, subcommand) in [("wait", "status"), ("resume", "result")] {
            if parsed.flags.iter().any(|f| f == flag)
                && !(parsed.command == "job"
                    && parsed.positionals.first().is_some_and(|s| s == subcommand))
            {
                return Err(format!("--{flag} applies only to job {subcommand}"));
            }
        }
        if parsed.command == "job" {
            match parsed.positionals.first().map(String::as_str) {
                Some("submit") => { parsed.job_request(uuid::Uuid::nil())?; }
                Some("status"|"cancel"|"result") if parsed.positionals.len()==2 && uuid::Uuid::parse_str(&parsed.positionals[1]).is_ok() => {
                    if !parsed.params.is_empty() || parsed.value("format").is_some() { return Err("job reads and cancellation do not accept parameters or formats".into()); }
                    if parsed.positionals[0]=="result" && parsed.value("output").is_none() { return Err("job result requires --output <directory>".into()); }
                }
                _ => return Err("job needs submit --sql <statement>, status <id>, cancel <id>, or result <id> --output <directory>".into()),
            }
        } else {
            parsed.queue_timeout()?;
        }
        parsed.compute()?;
        Ok(parsed)
    }

    fn is_job_submit(&self) -> bool {
        self.command == "job" && self.positionals.first().is_some_and(|s| s == "submit")
    }

    fn job_request(
        &self,
        generated_id: uuid::Uuid,
    ) -> Result<crate::job_policy::JobRequest, String> {
        let sql = match (self.value("sql"), self.positionals.as_slice()) {
            (Some(sql), [subcommand]) if subcommand == "submit" => sql.to_owned(),
            (None, [subcommand, sql]) if subcommand == "submit" => sql.clone(),
            _ => {
                return Err(
                    "job submit needs exactly one SQL argument or --sql <statement>".into(),
                );
            }
        };
        let id = |name: &str| {
            self.value(name)
                .map(|v| uuid::Uuid::parse_str(v).map_err(|_| format!("--{name} needs a UUID")))
                .transpose()
        };
        let number = |name: &str| {
            self.value(name)
                .map(|v| {
                    v.parse::<u64>()
                        .map_err(|_| format!("--{name} needs a positive whole number"))
                })
                .transpose()
        };
        let format = self
            .value("format")
            .map(|s| {
                if s == "arrow_ipc_chunks" {
                    Ok(crate::job_policy::ResultFormat::ArrowIpcChunks)
                } else {
                    Err("job format must be arrow_ipc_chunks".to_owned())
                }
            })
            .transpose()?;
        let result = crate::job_policy::JobResultRequest {
            format,
            retention_seconds: number("result-retention")?,
            max_bytes: number("result-max-bytes")?,
            max_rows: number("result-max-rows")?,
        };
        let request = crate::job_policy::JobRequest {
            request_id: id("request-id")?.unwrap_or(generated_id),
            retry_of: id("retry-of")?,
            sql,
            parameters: self.params.iter().cloned().map(Value::String).collect(),
            context: json!({}),
            compute: serde_json::from_value(self.compute()?.unwrap_or_else(|| json!({})))
                .map_err(|_| "invalid compute selection".to_owned())?,
            timeout_seconds: number("timeout")?,
            queue_timeout_seconds: number("queue-timeout")?,
            result: (result != crate::job_policy::JobResultRequest::default()).then_some(result),
        };
        request.clone().validate().map_err(|e| e.1)?;
        Ok(request)
    }

    fn value(&self, name: &str) -> Option<&str> {
        self.values
            .iter()
            .find(|(key, _)| key == name)
            .map(|(_, value)| value.as_str())
    }

    fn queue_timeout(&self) -> Result<u64, String> {
        self.value("queue-timeout")
            .map_or(Ok(crate::query::DEFAULT_QUEUE_TIMEOUT), |text| {
                text.parse::<u64>()
                    .ok()
                    .filter(|n| (1..=120).contains(n))
                    .ok_or_else(|| {
                        "--queue-timeout must be a whole number from 1 to 120 seconds".into()
                    })
            })
    }

    fn limit(&self, default: u32) -> u32 {
        self.value("limit")
            .and_then(|text| text.parse().ok())
            .unwrap_or(default)
    }

    /// Validate before opening a connection: malformed flags must never silently
    /// become an empty request and execute on the default pool.
    fn compute(&self) -> Result<Option<Value>, String> {
        let mut map = Map::new();
        if let Some(text) = self.value("cpu") {
            let cpu = text
                .parse::<u16>()
                .ok()
                .filter(|cpu| *cpu > 0)
                .ok_or("--cpu must be a positive whole number from 1 to 65535")?;
            map.insert("cpu".to_owned(), json!(cpu));
        }
        for field in ["memory", "disk", "mode", "pool"] {
            if let Some(value) = self.value(field) {
                map.insert(field.to_owned(), json!(value));
            }
        }
        if map.is_empty() {
            Ok(None)
        } else {
            let value = Value::Object(map);
            let request: crate::compute::ComputeRequest = serde_json::from_value(value.clone())
                .map_err(|error| format!("invalid compute selection: {error}"))?;
            request.validate().map_err(|error| error.1)?;
            Ok(Some(value))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(args: &[&str]) -> Result<Parsed, String> {
        Parsed::from_args(&args.iter().map(|arg| (*arg).to_owned()).collect::<Vec<_>>())
    }

    #[test]
    fn job_arguments_keep_original_omission_and_separate_queue_allowance() {
        let parsed = parse(&["job", "submit", "--sql", "SELECT 1"]).unwrap();
        let id = uuid::Uuid::new_v4();
        let request = parsed.job_request(id).unwrap();
        assert_eq!(request.request_id, id);
        assert_eq!(request.timeout_seconds, None);
        assert_eq!(request.queue_timeout_seconds, None);
        assert!(request.result.is_none());
        let parsed = parse(&[
            "job",
            "submit",
            "SELECT 1",
            "--timeout",
            "3600",
            "--queue-timeout",
            "3600",
            "--pool",
            "batch",
            "--result-retention",
            "86400",
        ])
        .unwrap();
        let request = parsed.job_request(id).unwrap();
        assert_eq!(request.timeout_seconds, Some(3600));
        assert_eq!(request.queue_timeout_seconds, Some(3600));
        assert_eq!(request.compute.pool.as_deref(), Some("batch"));
        assert_eq!(request.result.unwrap().retention_seconds, Some(86400));
        assert!(parse(&["sql", "SELECT 1", "--queue-timeout", "3600"]).is_err());
        for args in [
            vec!["job", "submit", "--sql", "SELECT 1", "SELECT 2"],
            vec!["job", "submit", "SELECT 1", "--timeout", "3601"],
            vec!["job", "submit", "SELECT 1", "--queue-timeout", "0"],
            vec!["job", "submit", "SELECT 1", "--format", "parquet"],
            vec!["job", "submit", "SELECT 1", "--request-id", "bad"],
            vec!["workers", "--timeout", "60"],
            vec!["job", "submit", "SELECT 1", "--resume"],
        ] {
            assert!(parse(&args).is_err(), "accepted {args:?}");
        }
        let text = id.to_string();
        assert!(parse(&["job", "status", &text, "--wait"]).is_ok());
        assert!(parse(&["job", "result", &text, "--output", "results", "--resume"]).is_ok());
        assert!(parse(&["job", "result", &text]).is_err());
        assert!(parse(&["job", "cancel", &text, "--pool", "batch"]).is_err());
    }

    #[test]
    fn policy_and_pagination_arguments_are_validated_before_network_access() {
        assert!(parse(&["policy", "analyst", "--file", "policy.json"]).is_ok());
        for args in [
            vec!["policy"],
            vec!["policy", "../admin"],
            vec!["policy", "one", "two"],
            vec!["sql", "SELECT 1", "--file", "policy.json"],
            vec!["principals", "--after", "forged&limit=500"],
            vec!["audit", "--limit", "501"],
            vec!["audit", "--limit", "0"],
        ] {
            assert!(parse(&args).is_err(), "accepted invalid arguments {args:?}");
        }
        let cursor = uuid::Uuid::new_v4().to_string();
        assert!(parse(&["principals", "--after", &cursor]).is_ok());
        assert!(parse(&["audit", "--before", &cursor]).is_ok());
        assert!(parse(&["audit", "--after", &cursor]).is_err());
    }

    #[test]
    fn queue_allowance_is_validated_before_opening_sql() {
        assert_eq!(
            parse(&["sql", "SELECT 1"])
                .unwrap()
                .queue_timeout()
                .unwrap(),
            10
        );
        assert_eq!(
            parse(&["sql", "--queue-timeout", "120", "SELECT 1"])
                .unwrap()
                .queue_timeout()
                .unwrap(),
            120
        );
        for value in ["0", "121", "1.5", "many"] {
            assert!(parse(&["sql", "--queue-timeout", value, "SELECT 1"]).is_err());
        }
        assert!(parse(&["request", "id", "--queue-timeout", "5"]).is_err());
        assert_eq!(parse(&["cancel", "id"]).unwrap().command, "cancel");
    }

    #[test]
    fn project_commands_validate_before_resolving_configuration() {
        assert!(parse(&["init"]).is_ok());
        assert!(parse(&["init", "--force", "--url", "http://localhost:18080"]).is_ok());
        assert!(parse(&["config", "show"]).is_ok());
        assert!(parse(&["version"]).is_ok());
        assert!(parse(&["dev"]).is_ok());
        assert!(parse(&["dev", "--demo"]).is_ok());
        assert!(parse(&["status"]).is_ok());
        assert!(parse(&["stop"]).is_ok());
        assert!(parse(&["reset", "--yes"]).is_ok());
        assert!(parse(&["shell", "--no-history", "--format", "vertical"]).is_ok());
        assert!(parse(&["sql", "--file", "load.sql", "--continue-on-error"]).is_ok());
        assert!(parse(&["schemas"]).is_ok());
        assert!(parse(&["tables", "--schema", "analytics"]).is_ok());
        assert!(parse(&["views", "--format", "json"]).is_ok());
        assert!(parse(&["describe", "analytics.events"]).is_ok());
        assert!(parse(&["preview", "analytics.events", "--limit", "1000"]).is_ok());
        for args in [
            vec!["init", "unexpected"],
            vec!["config"],
            vec!["config", "edit"],
            vec!["version", "unexpected"],
            vec!["sql", "SELECT 1", "--force"],
            vec!["dev", "unexpected"],
            vec!["status", "--demo"],
            vec!["stop", "--yes"],
            vec!["reset", "--demo"],
            vec!["shell", "unexpected"],
            vec!["shell", "--format", "yaml"],
            vec!["sql", "SELECT 1", "--no-history"],
            vec!["workers", "--continue-on-error"],
            vec!["describe"],
            vec!["schemas", "--schema", "analytics"],
            vec!["preview", "analytics.events", "--limit", "0"],
            vec!["preview", "analytics.events", "--limit", "1001"],
            vec!["tables", "--format", "yaml"],
        ] {
            assert!(parse(&args).is_err(), "accepted {args:?}");
        }
    }

    #[test]
    fn invalid_selection_never_becomes_default_compute() {
        for cpu in ["zero", "0", "-1", "1.5", "65536", ""] {
            assert!(parse(&["sql", "--cpu", cpu, "SELECT 1"]).is_err(), "{cpu}");
        }
        for args in [
            vec!["sql", "--mode", "automatic", "SELECT 1"],
            vec!["sql", "--mode", "isolated", "--pool", "etl", "SELECT 1"],
            vec!["sql", "--pool", "../bad", "SELECT 1"],
            vec!["sql", "--cpu", "1", "--cpu", "2", "SELECT 1"],
            vec!["workers", "--pool", "etl"],
            vec!["sql", "--memory", "bogus", "SELECT 1"],
        ] {
            assert!(parse(&args).is_err(), "{args:?}");
        }
    }

    #[test]
    fn compute_flags_preserve_legacy_and_explicit_selection() {
        assert!(
            parse(&["sql", "SELECT 1"])
                .unwrap()
                .compute()
                .unwrap()
                .is_none()
        );
        assert_eq!(
            parse(&["sql", "--cpu", "2", "SELECT 1"])
                .unwrap()
                .compute()
                .unwrap(),
            Some(json!({"cpu":2}))
        );
        assert_eq!(
            parse(&[
                "sql",
                "--mode",
                "persistent",
                "--pool",
                "etl",
                "--memory",
                "4Gi",
                "SELECT 1"
            ])
            .unwrap()
            .compute()
            .unwrap(),
            Some(json!({"mode":"persistent","pool":"etl","memory":"4Gi"}))
        );
        assert_eq!(parse(&["pools"]).unwrap().command, "pools");
    }
}
