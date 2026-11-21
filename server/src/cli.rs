//! Command-line access to a DuckFleet warehouse, with no dbt involved — the
//! `cli` role of the one binary. It covers what a warehouse needs from a shell:
//! run statements, choose the compute they run on, and read back what ran and
//! what it cost. Statements given together share one session, because a DDL, a
//! load and a query are usually one unit of work and a session is what pins them
//! to a single worker.
//!
//! Naming any of cpu, memory, or disk asks for a dedicated pod of that size; the
//! server fills the dimensions left unset from the default. Naming none runs on
//! the shared persistent tier. There is no mode flag — asking for a size is the
//! request.

use serde_json::{Map, Value, json};

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
    let url = parsed
        .value("url")
        .map(str::to_owned)
        .or_else(|| std::env::var("DUCKFLEET_URL").ok());
    let token = parsed
        .value("token")
        .map(str::to_owned)
        .or_else(|| std::env::var("DUCKFLEET_TOKEN").ok());
    let (Some(url), Some(token)) = (url, token) else {
        eprintln!("set --url/--token or DUCKFLEET_URL/DUCKFLEET_TOKEN");
        return 2;
    };

    let result = match parsed.command.as_str() {
        "sql" => run_sql(&url, &token, &parsed).await,
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
        other => {
            eprintln!("unknown command: {other} (sql|history|query|request|usage|budget|workers)");
            return 2;
        }
    };

    match result {
        Ok(()) => 0,
        Err(error) => {
            // The server's own classification, because it is what a caller acts
            // on: whether the statement may be sent again is not this client's
            // judgement.
            let status = error
                .status
                .map(|code| code.to_string())
                .unwrap_or_else(|| "-".to_owned());
            let kind = error.failure_kind.as_deref().unwrap_or("error");
            eprintln!("{kind} (HTTP {status}): {error}");
            eprintln!("retry_safe: {}", error.retry_safe);
            1
        }
    }
}

async fn run_sql(url: &str, token: &str, parsed: &Parsed) -> Result<(), RemoteError> {
    let compute = parsed.compute();
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
    // Closing rolls back anything uncommitted, which is the server's behaviour
    // and the reason a failed load does not leave a half-written table behind —
    // so close in both the success and the error path.
    let mut outcome = Ok(());
    for statement in &parsed.positionals {
        match connection.execute(statement, parameters.clone()).await {
            Ok(result) => emit(&result, format),
            Err(error) => {
                outcome = Err(error);
                break;
            }
        }
    }
    let _ = connection.close().await;
    outcome
}

async fn get(url: &str, token: &str, path: &str) -> Result<(), RemoteError> {
    let client = Client::new(url, token)?;
    let bytes = client
        .request(
            reqwest::Method::GET,
            path,
            None,
            std::time::Duration::from_secs(75),
        )
        .await?;
    let value: Value = serde_json::from_slice(&bytes)
        .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(&bytes).into_owned()));
    println!(
        "{}",
        serde_json::to_string_pretty(&value).unwrap_or_default()
    );
    Ok(())
}

fn emit(result: &QueryResult, format: &str) {
    if result.columns.is_empty() {
        return;
    }
    match format {
        "json" => emit_json(result),
        "csv" => emit_csv(result),
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
    const VALUE_FLAGS: [&'static str; 7] =
        ["url", "token", "cpu", "memory", "disk", "format", "limit"];

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
                if name == "placement" {
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
        Ok(Parsed {
            command: command.ok_or("usage: <sql|history|query|usage|budget|workers> ...")?,
            positionals,
            values,
            params,
            flags,
        })
    }

    fn value(&self, name: &str) -> Option<&str> {
        self.values
            .iter()
            .find(|(key, _)| key == name)
            .map(|(_, value)| value.as_str())
    }

    fn limit(&self, default: u32) -> u32 {
        self.value("limit")
            .and_then(|text| text.parse().ok())
            .unwrap_or(default)
    }

    /// The compute request, or `None` to take the shared default: naming any of
    /// cpu/memory/disk asks for a dedicated pod; naming none runs shared.
    fn compute(&self) -> Option<Value> {
        let mut map = Map::new();
        if let Some(cpu) = self.value("cpu").and_then(|text| text.parse::<u16>().ok()) {
            map.insert("cpu".to_owned(), json!(cpu));
        }
        if let Some(memory) = self.value("memory") {
            map.insert("memory".to_owned(), json!(memory));
        }
        if let Some(disk) = self.value("disk") {
            map.insert("disk".to_owned(), json!(disk));
        }
        if map.is_empty() {
            None
        } else {
            Some(Value::Object(map))
        }
    }
}
