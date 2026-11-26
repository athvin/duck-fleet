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
    if parsed.command == "advise" && parsed.positionals.first().is_some_and(|s| s == "dbt") {
        return run_advise_dbt(&parsed, resolved.url.as_deref(), resolved.token.as_deref()).await;
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
        "advise" => {
            let (subject, thresholds) = parsed.advise_command().expect("validated advise command");
            crate::advise_cli::run(
                &url,
                &token,
                subject,
                thresholds,
                parsed.value("format").unwrap_or("table"),
            )
            .await
        }
        "snapshots" | "snapshot" => {
            crate::snapshot_cli::run(
                &url,
                &token,
                parsed
                    .snapshot_command()
                    .expect("validated snapshot command"),
                parsed.value("format").unwrap_or("table"),
            )
            .await
        }
        "export" => {
            let output = parsed.value("output").expect("validated output").to_owned();
            let source = match parsed.value("job") {
                Some(job) => crate::export_cli::Source::Job(
                    uuid::Uuid::parse_str(job).expect("validated job ID"),
                ),
                None => crate::export_cli::Source::Sql(Box::new(
                    parsed
                        .job_request_for(parsed.positionals[0].clone(), uuid::Uuid::new_v4())
                        .expect("validated export request"),
                )),
            };
            crate::export_cli::run(
                &url,
                &token,
                crate::export_cli::Options {
                    source,
                    format: crate::export_cli::infer_format(&output, parsed.value("format"))
                        .expect("validated export format"),
                    output,
                    force: parsed.flags.iter().any(|flag| flag == "force"),
                    resume: parsed.flags.iter().any(|flag| flag == "resume"),
                    delimiter: parsed.value("delimiter").unwrap_or(",").as_bytes()[0],
                },
            )
            .await
        }
        "import" => {
            crate::import_cli::run(
                &url,
                &token,
                crate::import_cli::Options {
                    path: parsed.positionals[0].clone(),
                    destination: parsed.value("table").expect("validated table").to_owned(),
                    format: crate::import_cli::infer_format(
                        &parsed.positionals[0],
                        parsed.value("format"),
                    )
                    .expect("validated import format"),
                    mode: if parsed.flags.iter().any(|flag| flag == "create") {
                        crate::import_cli::Mode::Create
                    } else {
                        crate::import_cli::Mode::Append
                    },
                    delimiter: parsed.value("delimiter").unwrap_or(",").to_owned(),
                    header: !parsed.flags.iter().any(|flag| flag == "no-header"),
                },
            )
            .await
        }
        "schemas" | "tables" | "views" | "describe" | "preview" => {
            run_catalog(&url, &token, &parsed).await
        }
        "system" => run_system(&url, &token, &parsed).await,
        "job" => run_job(&url, &token, &parsed).await,
        "history" => {
            crate::history_cli::history(
                &url,
                &token,
                crate::history_cli::HistoryOptions {
                    filters: [
                        "status",
                        "failure-kind",
                        "worker-id",
                        "model-id",
                        "invocation-id",
                        "model-run-id",
                    ]
                    .into_iter()
                    .filter_map(|name| {
                        parsed
                            .value(name)
                            .map(|value| (name.replace('-', "_"), value.to_owned()))
                    })
                    .collect(),
                    limit: parsed.limit(20),
                    before: parsed
                        .value("before")
                        .map(|value| value.parse::<i64>().expect("validated history cursor")),
                    format: parsed.value("format").unwrap_or("table").to_owned(),
                    watch: parsed.flags.iter().any(|flag| flag == "watch"),
                    interval: crate::history_cli::validate_interval(parsed.value("interval"))
                        .expect("validated watch interval"),
                },
            )
            .await
        }
        "profile" => {
            crate::history_cli::profile(
                &url,
                &token,
                crate::history_cli::ProfileOptions {
                    id: parsed.positionals[0].clone(),
                    raw: parsed.flags.iter().any(|flag| flag == "raw"),
                    metrics: parsed.flags.iter().any(|flag| flag == "metrics"),
                },
            )
            .await
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
                "unknown command: {other} (init|config|version|dev|status|stop|reset|shell|sql|import|export|snapshots|snapshot|advise|schemas|tables|views|describe|preview|system|job|history|profile|query|request|cancel|usage|budget|workers|pools|principals|credentials|policy|audit)"
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
        "result" => download_job_result(
            &client,
            id,
            parsed.value("output").expect("validated output"),
            parsed.flags.iter().any(|f| f == "resume"),
        )
        .await
        .map(|_| ())?,
        _ => unreachable!("validated job command"),
    }
    Ok(())
}

/// Static dbt analysis needs no warehouse; `--history` adds an authorized,
/// read-only lookup of recorded transaction conflicts. Exit 0 for a completed
/// analysis, 1 for warnings under `--strict`, 2 for unusable artifacts.
async fn run_advise_dbt(parsed: &Parsed, url: Option<&str>, token: Option<&str>) -> i32 {
    let directory = std::path::PathBuf::from(parsed.value("project-dir").unwrap_or("."));
    let mut report = match crate::advise_dbt::load(&directory)
        .and_then(|manifest| crate::advise_dbt::analyze(&manifest))
    {
        Ok(report) => report,
        Err(message) => {
            eprintln!("invalid_dbt_artifacts: {message}");
            return 2;
        }
    };
    if parsed.flags.iter().any(|flag| flag == "history") {
        let (Some(url), Some(token)) = (url, token) else {
            eprintln!(
                "--history needs a DuckFleet URL and token; the static analysis needs neither"
            );
            return 2;
        };
        let history = match Client::new(url, token) {
            Ok(client) => client
                .request(
                    reqwest::Method::GET,
                    "/v1/queries?failure_kind=transaction_conflict&limit=500",
                    None,
                    std::time::Duration::from_secs(15),
                )
                .await
                .ok()
                .and_then(|bytes| serde_json::from_slice::<Value>(&bytes).ok()),
            Err(_) => None,
        };
        match history {
            Some(history) => {
                crate::advise_dbt::correlate(&mut report, &history);
                report["history_note"] = json!(
                    "correlated with recorded transaction_conflict history by dbt node ID; model identity is shown only where this principal may see it"
                );
            }
            None => {
                report["history_note"] = json!(
                    "history correlation unavailable for this principal; static warnings are unaffected"
                );
            }
        }
    }
    if parsed.value("format") == Some("json") {
        println!(
            "{}",
            serde_json::to_string_pretty(&report).unwrap_or_default()
        );
    } else {
        print!("{}", crate::advise_dbt::render(&report));
    }
    let warnings = report["warnings"].as_array().map_or(0, Vec::len);
    if parsed.flags.iter().any(|flag| flag == "strict") && warnings > 0 {
        return 1;
    }
    0
}

pub(crate) async fn download_job_result(
    client: &Client,
    id: uuid::Uuid,
    directory: &str,
    resume: bool,
) -> Result<duckfleet::protocol::Manifest, RemoteError> {
    let manifest = client
        .download_job_result(id, std::path::Path::new(directory), resume)
        .await?;
    eprintln!(
        "downloaded {} rows in {} chunks to {}",
        manifest.rows,
        manifest.chunks.len(),
        directory
    );
    Ok(manifest)
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
    println!(
        "{}",
        serde_json::to_string_pretty(&rows_json(result)).unwrap_or_default()
    );
}

pub(crate) fn rows_json(result: &QueryResult) -> Value {
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
    Value::Array(rows)
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
    const VALUE_FLAGS: [&'static str; 36] = [
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
        "failure-kind",
        "model-id",
        "invocation-id",
        "model-run-id",
        "interval",
        "status",
        "table",
        "delimiter",
        "job",
        "at",
        "from",
        "to",
        "thresholds",
        "project-dir",
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
                    "watch",
                    "raw",
                    "metrics",
                    "create",
                    "append",
                    "no-header",
                    "strict",
                    "history",
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
                "usage: <init|config|version|dev|status|stop|reset|shell|sql|import|export|snapshots|snapshot|advise|schemas|tables|views|describe|preview|system|job|history|profile|query|request|cancel|usage|budget|workers|pools|principals|credentials|policy|audit> ...",
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
        if parsed.flags.iter().any(|flag| flag == "force")
            && !matches!(parsed.command.as_str(), "init" | "export")
        {
            return Err("--force applies only to init and export".into());
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
        if parsed.command == "import" {
            if parsed.positionals.len() != 1 || parsed.value("table").is_none() {
                return Err("import needs one file and --table <schema.table>".into());
            }
            let create = parsed.flags.iter().any(|flag| flag == "create");
            let append = parsed.flags.iter().any(|flag| flag == "append");
            if create == append {
                return Err("import requires exactly one of --create or --append".into());
            }
            crate::catalog::destination(parsed.value("table").expect("checked table"))?;
            let format =
                crate::import_cli::infer_format(&parsed.positionals[0], parsed.value("format"))?;
            if format == "parquet"
                && (parsed.value("delimiter").is_some()
                    || parsed.flags.iter().any(|flag| flag == "no-header"))
            {
                return Err("CSV delimiter/header options do not apply to Parquet".into());
            }
        }
        for flag in ["create", "append", "no-header"] {
            if parsed.flags.iter().any(|value| value == flag) && parsed.command != "import" {
                return Err(format!("--{flag} applies only to import"));
            }
        }
        if parsed.value("table").is_some() && parsed.command != "import" {
            return Err("--table applies only to import".into());
        }
        if parsed.value("delimiter").is_some()
            && !matches!(parsed.command.as_str(), "import" | "export")
        {
            return Err("--delimiter applies only to import and export".into());
        }
        if matches!(parsed.command.as_str(), "snapshots" | "snapshot") {
            parsed.snapshot_command()?;
            if parsed
                .value("format")
                .is_some_and(|format| !["table", "json", "csv", "vertical"].contains(&format))
            {
                return Err("snapshot --format must be table, json, csv, or vertical".into());
            }
        }
        let advise_dbt =
            parsed.command == "advise" && parsed.positionals.first().is_some_and(|s| s == "dbt");
        if advise_dbt {
            if parsed.positionals.len() != 1
                || parsed.value("thresholds").is_some()
                || parsed.value("limit").is_some()
                || parsed
                    .value("format")
                    .is_some_and(|format| !["table", "json"].contains(&format))
            {
                return Err("usage: advise dbt [--project-dir <path>] [--strict] [--history] [--format table|json]".into());
            }
        } else if parsed.command == "advise" {
            parsed.advise_command()?;
        } else if parsed.value("thresholds").is_some() {
            return Err("--thresholds applies only to advise".into());
        }
        for flag in ["strict", "history"] {
            if parsed.flags.iter().any(|value| value == flag) && !advise_dbt {
                return Err(format!("--{flag} applies only to advise dbt"));
            }
        }
        if parsed.value("project-dir").is_some() && !advise_dbt {
            return Err("--project-dir applies only to advise dbt".into());
        }
        for flag in ["at", "from", "to"] {
            if parsed.value(flag).is_some() && parsed.command != "snapshot" {
                return Err(format!(
                    "--{flag} applies only to snapshot preview and diff"
                ));
            }
        }
        if parsed.command == "export" {
            let output = parsed
                .value("output")
                .ok_or("export needs --output <file>")?;
            let format = crate::export_cli::infer_format(output, parsed.value("format"))?;
            if let Some(delimiter) = parsed.value("delimiter")
                && (format != crate::export_cli::Format::Csv
                    || delimiter.len() != 1
                    || matches!(delimiter.as_bytes()[0], b'\r' | b'\n' | b'"' | 0)
                    || !delimiter.is_ascii())
            {
                return Err(
                    "--delimiter needs one ASCII character and applies only to CSV exports".into(),
                );
            }
            match (parsed.value("job"), parsed.positionals.as_slice()) {
                (Some(job), []) => {
                    uuid::Uuid::parse_str(job).map_err(|_| "--job needs a UUID".to_owned())?;
                    if !parsed.params.is_empty() || parsed.compute()?.is_some() {
                        return Err("export --job reads a finished result and takes no SQL parameters or compute".into());
                    }
                    for flag in ["timeout", "queue-timeout"] {
                        if parsed.value(flag).is_some() {
                            return Err(format!("--{flag} does not apply to export --job"));
                        }
                    }
                }
                (None, [sql]) => {
                    if parsed.flags.iter().any(|flag| flag == "resume") {
                        return Err(
                            "--resume needs --job <request-id>; SQL is never resubmitted to resume"
                                .into(),
                        );
                    }
                    parsed.job_request_for(sql.clone(), uuid::Uuid::nil())?;
                }
                _ => return Err("export needs one SQL argument or --job <request-id>".into()),
            }
        } else if parsed.value("job").is_some() {
            return Err("--job applies only to export".into());
        }
        if parsed.command == "history" && !parsed.positionals.is_empty() {
            return Err("history takes no positional arguments".into());
        }
        if parsed.command == "history"
            && parsed
                .value("format")
                .is_some_and(|format| !["table", "json"].contains(&format))
        {
            return Err("history --format must be table or json".into());
        }
        if parsed.command == "profile"
            && (parsed.positionals.len() != 1
                || uuid::Uuid::parse_str(&parsed.positionals[0]).is_err())
        {
            return Err("profile needs one valid query UUID".into());
        }
        if parsed.flags.iter().any(|flag| flag == "watch") && parsed.command != "history" {
            return Err("--watch applies only to history".into());
        }
        if parsed.value("interval").is_some() && parsed.command != "history" {
            return Err("--interval applies only to history".into());
        }
        if parsed.command == "history" {
            crate::history_cli::validate_interval(parsed.value("interval"))?;
        }
        for flag in ["raw", "metrics"] {
            if parsed.flags.iter().any(|value| value == flag) && parsed.command != "profile" {
                return Err(format!("--{flag} applies only to profile"));
            }
        }
        if parsed.command == "profile"
            && parsed.flags.iter().any(|flag| flag == "raw")
            && parsed.flags.iter().any(|flag| flag == "metrics")
        {
            return Err("profile --raw and --metrics are mutually exclusive".into());
        }
        for flag in [
            "status",
            "failure-kind",
            "worker-id",
            "model-id",
            "invocation-id",
            "model-run-id",
        ] {
            if parsed.value(flag).is_some() && parsed.command != "history" {
                return Err(format!("--{flag} applies only to history"));
            }
        }
        if !matches!(parsed.command.as_str(), "sql" | "shell")
            && !(parsed.command == "export" && parsed.value("job").is_none())
            && !parsed.is_job_submit()
            && parsed
                .values
                .iter()
                .any(|(name, _)| Self::COMPUTE_FLAGS.contains(&name.as_str()))
        {
            return Err(
                "compute selection flags apply only to sql, job submit, or SQL export".into(),
            );
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
            && !(parsed.command == "export" && parsed.value("job").is_none())
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
                if cursor == "before" && parsed.command == "snapshots" {
                    crate::snapshot_cli::parse_id("--before", value)?;
                    continue;
                }
                if cursor == "before" && parsed.command == "history" {
                    if value.parse::<i64>().ok().is_none_or(|before| before <= 0) {
                        return Err("--before needs a positive history cursor".into());
                    }
                    continue;
                }
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
            if parsed.value(flag).is_some()
                && !parsed.is_job_submit()
                && !(parsed.command == "export" && flag == "timeout")
            {
                return Err(format!("--{flag} applies only to job submit"));
            }
        }
        if parsed.value("output").is_some()
            && parsed.command != "export"
            && !(parsed.command == "job"
                && parsed.positionals.first().is_some_and(|s| s == "result"))
        {
            return Err("--output applies only to job result and export".into());
        }
        for (flag, subcommand) in [("wait", "status"), ("resume", "result")] {
            if parsed.flags.iter().any(|f| f == flag)
                && !(flag == "resume" && parsed.command == "export")
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

    fn advise_command(
        &self,
    ) -> Result<(crate::advise_cli::Subject, crate::advise_cli::Thresholds), String> {
        let thresholds = crate::advise_cli::Thresholds::parse(self.value("thresholds"))?;
        let samples = crate::snapshot_cli::bounded("--limit", self.value("limit"), 20, 100)?;
        if self
            .value("format")
            .is_some_and(|format| !["table", "json"].contains(&format))
        {
            return Err("advise --format must be table or json".into());
        }
        let positionals: Vec<&str> = self.positionals.iter().map(String::as_str).collect();
        let subject = match positionals.as_slice() {
            ["query", id] => crate::advise_cli::Subject::Query {
                id: uuid::Uuid::parse_str(id).map_err(|_| "advise query needs a query ID")?,
                samples,
            },
            ["model", id] if !id.is_empty() && id.len() <= 512 => {
                crate::advise_cli::Subject::Model {
                    id: (*id).to_owned(),
                    samples,
                }
            }
            _ => {
                return Err(
                    "usage: advise query <query-id> | advise model <dbt-node-id> [--limit N] [--thresholds name=value,...] | advise dbt [--project-dir <path>] [--strict] [--history]".into(),
                );
            }
        };
        Ok((subject, thresholds))
    }

    fn snapshot_command(&self) -> Result<crate::snapshot_cli::Command, String> {
        use crate::snapshot_cli::{
            Command, DEFAULT_EXAMPLES, DEFAULT_PAGE, MAX_EXAMPLES, MAX_PAGE, bounded, parse_id,
        };
        let id = |flag: &str| {
            self.value(flag)
                .ok_or_else(|| format!("snapshot needs --{flag} <snapshot-id>"))
                .and_then(|value| parse_id(&format!("--{flag}"), value))
        };
        let only = |allowed: &[&str]| {
            for flag in ["at", "from", "to", "before", "limit"] {
                if self.value(flag).is_some() && !allowed.contains(&flag) {
                    return Err(format!("--{flag} does not apply to this snapshot command"));
                }
            }
            Ok(())
        };
        let positionals: Vec<&str> = self.positionals.iter().map(String::as_str).collect();
        match (self.command.as_str(), positionals.as_slice()) {
            ("snapshots", []) => {
                only(&["before", "limit"])?;
                Ok(Command::List {
                    limit: bounded("--limit", self.value("limit"), DEFAULT_PAGE, MAX_PAGE)?,
                    before: self.value("before").map(|v| parse_id("--before", v)).transpose()?,
                })
            }
            ("snapshot", ["preview", relation]) => {
                only(&["at", "limit"])?;
                crate::catalog::validate_relation(relation)?;
                Ok(Command::Preview {
                    relation: (*relation).to_owned(),
                    at: id("at")?,
                    limit: crate::catalog::validate_limit(self.value("limit"))?,
                })
            }
            ("snapshot", ["diff", relation]) => {
                only(&["from", "to", "limit"])?;
                crate::catalog::validate_relation(relation)?;
                let (from, to) = (id("from")?, id("to")?);
                if from > to {
                    return Err(format!("--from {from} is after --to {to}"));
                }
                Ok(Command::Diff {
                    relation: (*relation).to_owned(),
                    from,
                    to,
                    examples: bounded("--limit", self.value("limit"), DEFAULT_EXAMPLES, MAX_EXAMPLES)?,
                })
            }
            ("snapshot", [snapshot]) if !matches!(*snapshot, "preview" | "diff") => {
                only(&[])?;
                Ok(Command::Show { id: parse_id("snapshot", snapshot)? })
            }
            _ => Err("usage: snapshots [--limit N] [--before ID] | snapshot <id> | snapshot preview <schema.table> --at <id> [--limit N] | snapshot diff <schema.table> --from <id> --to <id> [--limit N]".into()),
        }
    }

    fn is_job_submit(&self) -> bool {
        self.command == "job" && self.positionals.first().is_some_and(|s| s == "submit")
    }

    fn job_request(
        &self,
        generated_id: uuid::Uuid,
    ) -> Result<duckfleet::protocol::JobRequest, String> {
        let sql = match (self.value("sql"), self.positionals.as_slice()) {
            (Some(sql), [subcommand]) if subcommand == "submit" => sql.to_owned(),
            (None, [subcommand, sql]) if subcommand == "submit" => sql.clone(),
            _ => {
                return Err(
                    "job submit needs exactly one SQL argument or --sql <statement>".into(),
                );
            }
        };
        self.job_request_for(sql, generated_id)
    }

    /// Export submits the same durable job contract as `job submit`.
    fn job_request_for(
        &self,
        sql: String,
        generated_id: uuid::Uuid,
    ) -> Result<duckfleet::protocol::JobRequest, String> {
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
        // Export's --format names the local file format, not the job result.
        let format = self
            .value("format")
            .filter(|_| self.command != "export")
            .map(|s| {
                if s == "arrow_ipc_chunks" {
                    Ok(duckfleet::protocol::ResultFormat::ArrowIpcChunks)
                } else {
                    Err("job format must be arrow_ipc_chunks".to_owned())
                }
            })
            .transpose()?;
        let result = duckfleet::protocol::JobResultRequest {
            format,
            retention_seconds: number("result-retention")?,
            max_bytes: number("result-max-bytes")?,
            max_rows: number("result-max-rows")?,
        };
        let request = duckfleet::protocol::JobRequest {
            request_id: id("request-id")?.unwrap_or(generated_id),
            retry_of: id("retry-of")?,
            sql,
            parameters: self.params.iter().cloned().map(Value::String).collect(),
            context: json!({}),
            compute: serde_json::from_value(self.compute()?.unwrap_or_else(|| json!({})))
                .map_err(|_| "invalid compute selection".to_owned())?,
            timeout_seconds: number("timeout")?,
            queue_timeout_seconds: number("queue-timeout")?,
            result: (result != duckfleet::protocol::JobResultRequest::default()).then_some(result),
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
            let request: duckfleet::protocol::ComputeRequest =
                serde_json::from_value(value.clone())
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
        assert!(
            parse(&[
                "history",
                "--status",
                "failed",
                "--format",
                "json",
                "--watch",
                "--interval",
                "1",
                "--before",
                "10"
            ])
            .is_ok()
        );
        let query_id = uuid::Uuid::new_v4().to_string();
        assert!(parse(&["profile", &query_id, "--raw"]).is_ok());
        assert!(parse(&["profile", &query_id, "--metrics"]).is_ok());
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
            vec!["history", "unexpected"],
            vec!["history", "--format", "yaml"],
            vec!["history", "--before", "0"],
            vec!["profile", "not-a-uuid"],
            vec!["profile", &query_id, "--raw", "--metrics"],
            vec!["workers", "--watch"],
            vec!["history", "--raw"],
            vec!["workers", "--status", "failed"],
        ] {
            assert!(parse(&args).is_err(), "accepted {args:?}");
        }
    }

    #[test]
    fn advise_dbt_arguments_are_offline_and_specific() {
        assert!(parse(&["advise", "dbt"]).is_ok());
        assert!(
            parse(&[
                "advise",
                "dbt",
                "--project-dir",
                "p",
                "--strict",
                "--history",
                "--format",
                "json"
            ])
            .is_ok()
        );
        for args in [
            vec!["advise", "dbt", "extra"],
            vec!["advise", "dbt", "--limit", "5"],
            vec!["advise", "dbt", "--format", "csv"],
            vec!["advise", "dbt", "--thresholds", "cpu_share=0.5"],
            vec!["sql", "SELECT 1", "--strict"],
            vec!["history", "--history"],
            vec!["sql", "SELECT 1", "--project-dir", "p"],
        ] {
            assert!(
                parse(&args).is_err(),
                "accepted invalid advise dbt arguments {args:?}"
            );
        }
    }

    #[test]
    fn advise_arguments_are_read_only_and_bounded() {
        let id = "7b4f2f97-6ea7-4b01-886d-2b26eac08bb9";
        assert!(parse(&["advise", "query", id]).is_ok());
        assert!(parse(&["advise", "model", "model.retail.orders", "--limit", "100"]).is_ok());
        assert!(
            parse(&[
                "advise",
                "query",
                id,
                "--thresholds",
                "cpu_share=0.9",
                "--format",
                "json"
            ])
            .is_ok()
        );
        for args in [
            vec!["advise"],
            vec!["advise", "query", "not-a-uuid"],
            vec!["advise", "query", id, "--limit", "101"],
            vec!["advise", "query", id, "--thresholds", "cpu_share=3"],
            vec!["advise", "query", id, "--format", "csv"],
            vec!["advise", "query", id, "--pool", "small"],
            vec!["advise", "pool", "small"],
            vec!["sql", "SELECT 1", "--thresholds", "cpu_share=0.5"],
        ] {
            assert!(
                parse(&args).is_err(),
                "accepted invalid advise arguments {args:?}"
            );
        }
    }

    #[test]
    fn snapshot_arguments_are_bounded_and_validated_before_any_read() {
        assert!(parse(&["snapshots"]).is_ok());
        assert!(parse(&["snapshots", "--limit", "500", "--before", "10"]).is_ok());
        assert!(parse(&["snapshot", "42", "--format", "json"]).is_ok());
        assert!(parse(&["snapshot", "preview", "raw.t", "--at", "4", "--limit", "5"]).is_ok());
        assert!(parse(&["snapshot", "diff", "raw.t", "--from", "1", "--to", "1"]).is_ok());
        for args in [
            vec!["snapshots", "extra"],
            vec!["snapshots", "--limit", "501"],
            vec!["snapshots", "--before", "abc"],
            vec!["snapshots", "--at", "1"],
            vec!["snapshot"],
            vec!["snapshot", "abc"],
            vec!["snapshot", "1", "--limit", "5"],
            vec!["snapshot", "preview", "raw.t"],
            vec!["snapshot", "preview", "raw.t", "--at", "-1"],
            vec!["snapshot", "preview", "raw.t", "--at", "1", "--from", "1"],
            vec!["snapshot", "diff", "raw.t", "--from", "5", "--to", "2"],
            vec!["snapshot", "diff", "raw.t", "--from", "1"],
            vec![
                "snapshot", "diff", "raw.t", "--from", "1", "--to", "2", "--limit", "101",
            ],
            vec![
                "snapshot", "diff", "raw.t", "--from", "1", "--to", "2", "--format", "xml",
            ],
            vec!["sql", "SELECT 1", "--at", "1"],
        ] {
            assert!(
                parse(&args).is_err(),
                "accepted invalid snapshot arguments {args:?}"
            );
        }
    }

    #[test]
    fn export_arguments_never_resubmit_sql_and_validate_formats_before_running() {
        let id = "7b4f2f97-6ea7-4b01-886d-2b26eac08bb9";
        assert!(parse(&["export", "SELECT 1", "--output", "out.parquet"]).is_ok());
        assert!(
            parse(&[
                "export", "SELECT 1", "--output", "o.bin", "--format", "json"
            ])
            .is_ok()
        );
        assert!(
            parse(&[
                "export",
                "SELECT 1",
                "--output",
                "o.csv",
                "--delimiter",
                "|",
                "--force"
            ])
            .is_ok()
        );
        assert!(parse(&["export", "SELECT 1", "--output", "o.csv", "--pool", "small"]).is_ok());
        assert!(parse(&["export", "--job", id, "--output", "o.csv", "--resume"]).is_ok());
        let request = parse(&["export", "SELECT 1", "--output", "o.csv", "--format", "csv"])
            .unwrap()
            .job_request_for("SELECT 1".into(), uuid::Uuid::nil())
            .unwrap();
        assert!(
            request.result.is_none(),
            "export --format is not a job result format"
        );
        for args in [
            vec!["export", "SELECT 1"],
            vec!["export", "SELECT 1", "--output", "o.xlsx"],
            vec!["export", "SELECT 1", "--output", "o.csv", "--resume"],
            vec!["export", "--job", "nope", "--output", "o.csv"],
            vec!["export", "SELECT 1", "--job", id, "--output", "o.csv"],
            vec![
                "export", "--job", id, "--output", "o.csv", "--pool", "small",
            ],
            vec![
                "export",
                "SELECT 1",
                "--output",
                "o.json",
                "--delimiter",
                "|",
            ],
            vec![
                "export",
                "SELECT 1",
                "--output",
                "o.csv",
                "--delimiter",
                "||",
            ],
            vec![
                "export",
                "SELECT 1",
                "--output",
                "o.csv",
                "--request-id",
                id,
            ],
            vec!["sql", "SELECT 1", "--job", id],
        ] {
            assert!(
                parse(&args).is_err(),
                "accepted invalid export arguments {args:?}"
            );
        }
    }

    #[test]
    fn import_arguments_are_specific_and_validated_before_transfer() {
        assert!(parse(&["import", "events.csv", "--table", "raw.events", "--create"]).is_ok());
        assert!(
            parse(&[
                "import",
                "events.data",
                "--table",
                "raw.events",
                "--append",
                "--format",
                "csv",
                "--delimiter",
                "|",
                "--no-header"
            ])
            .is_ok()
        );
        assert!(
            parse(&[
                "import",
                "events.PARQUET",
                "--table",
                "raw.events",
                "--append"
            ])
            .is_ok()
        );
        for args in [
            vec!["import", "events.csv", "--table", "raw.events"],
            vec![
                "import",
                "events.csv",
                "--table",
                "raw.events",
                "--create",
                "--append",
            ],
            vec![
                "import",
                "events.unknown",
                "--table",
                "raw.events",
                "--create",
            ],
            vec!["import", "events.csv", "--table", "events", "--create"],
            vec![
                "import",
                "events.parquet",
                "--table",
                "raw.events",
                "--create",
                "--delimiter",
                "|",
            ],
            vec![
                "import",
                "events.parquet",
                "--table",
                "raw.events",
                "--create",
                "--no-header",
            ],
            vec!["sql", "SELECT 1", "--create"],
            vec!["import", "events.csv", "--create"],
        ] {
            assert!(
                parse(&args).is_err(),
                "accepted invalid import arguments {args:?}"
            );
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
