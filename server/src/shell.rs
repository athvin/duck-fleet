use crate::client::{QueryResult, RemoteConnection};
use rustyline::completion::{Completer, Pair};
use rustyline::error::ReadlineError;
use rustyline::highlight::Highlighter;
use rustyline::hint::Hinter;
use rustyline::history::FileHistory;
use rustyline::validate::Validator;
use rustyline::{Config, Context, Editor, Helper};
use serde_json::Value;
use std::path::Path;
use std::time::Instant;

pub struct ShellOptions {
    pub url: String,
    pub token: String,
    pub compute: Option<Value>,
    pub queue_timeout: u64,
    pub format: String,
    pub history: bool,
}

#[derive(Default)]
struct ShellHelper;

impl Completer for ShellHelper {
    type Candidate = Pair;

    fn complete(
        &self,
        line: &str,
        position: usize,
        _context: &Context<'_>,
    ) -> rustyline::Result<(usize, Vec<Pair>)> {
        let start = line[..position]
            .rfind(|character: char| {
                !(character.is_alphanumeric() || matches!(character, '_' | '\\'))
            })
            .map_or(0, |index| index + 1);
        let word = &line[start..position];
        let lower = word.to_ascii_lowercase();
        let candidates = KEYWORDS
            .iter()
            .filter(|keyword| keyword.to_ascii_lowercase().starts_with(&lower))
            .map(|keyword| Pair {
                display: (*keyword).to_owned(),
                replacement: (*keyword).to_owned(),
            })
            .collect();
        Ok((start, candidates))
    }
}
impl Hinter for ShellHelper {
    type Hint = String;
}
impl Highlighter for ShellHelper {}
impl Validator for ShellHelper {}
impl Helper for ShellHelper {}

const KEYWORDS: &[&str] = &[
    "SELECT", "FROM", "WHERE", "GROUP", "ORDER", "LIMIT", "INSERT", "UPDATE", "DELETE", "CREATE",
    "TABLE", "VIEW", "SCHEMA", "BEGIN", "COMMIT", "ROLLBACK", "WITH", "JOIN", "\\dt", "\\d",
    "\\timing", "\\format", "\\help", "\\quit",
];

pub async fn run(options: ShellOptions) -> i32 {
    let mut connection = match RemoteConnection::open(
        &options.url,
        &options.token,
        "shell",
        options.compute,
        None,
        90,
    )
    .await
    {
        Ok(connection) => connection,
        Err(error) => {
            crate::cli::print_remote_error(&error);
            return 1;
        }
    };
    let mut editor = match Editor::<ShellHelper, FileHistory>::with_config(Config::default()) {
        Ok(editor) => editor,
        Err(error) => {
            eprintln!("cannot initialize the interactive editor: {error}");
            let _ = connection.close().await;
            return 1;
        }
    };
    editor.set_helper(Some(ShellHelper));
    let history_path = crate::cli_config::shell_history_path().ok();
    if options.history
        && let Some(path) = &history_path
        && let Err(error) = prepare_history(path)
    {
        eprintln!("cannot prepare shell history: {error}");
        let _ = connection.close().await;
        return 1;
    }
    if options.history
        && let Some(path) = &history_path
    {
        let _ = editor.load_history(path);
    }
    let mut format = options.format;
    let mut timing = false;
    let mut transaction_open = false;
    let mut fatal = false;
    let mut pending = String::new();
    println!("DuckFleet shell; \\help for commands, \\quit to close");
    loop {
        let prompt = if pending.is_empty() {
            if transaction_open {
                "duckfleet(tx)> "
            } else {
                "duckfleet> "
            }
        } else {
            "...> "
        };
        let line = match editor.readline(prompt) {
            Ok(line) => line,
            Err(ReadlineError::Interrupted) => {
                pending.clear();
                println!("input canceled");
                continue;
            }
            Err(ReadlineError::Eof) => break,
            Err(error) => {
                eprintln!("input error: {error}");
                fatal = true;
                break;
            }
        };
        if pending.is_empty() && line.trim_start().starts_with('\\') {
            let command = line.trim();
            if options.history && !sensitive_history(command) {
                let _ = editor.add_history_entry(command);
            }
            match command {
                "\\quit" => break,
                "\\help" => print_help(),
                "\\timing" => {
                    timing = !timing;
                    println!("timing {}", if timing { "on" } else { "off" });
                }
                "\\dt" => {
                    let rows = crate::catalog::tables(&connection, options.queue_timeout).await;
                    if finish_catalog(rows, &format) == Outcome::Fatal {
                        fatal = true;
                        break;
                    }
                }
                _ if command.starts_with("\\d ") => {
                    let rows = crate::catalog::describe(
                        &connection,
                        command[3..].trim(),
                        options.queue_timeout,
                    )
                    .await;
                    if finish_catalog(rows, &format) == Outcome::Fatal {
                        fatal = true;
                        break;
                    }
                }
                _ if command.starts_with("\\format ") => {
                    let requested = command[8..].trim();
                    if ["table", "csv", "json", "vertical"].contains(&requested) {
                        format = requested.to_owned();
                        println!("format {format}");
                    } else {
                        eprintln!("format must be table, csv, json, or vertical");
                    }
                }
                _ => eprintln!("unknown meta-command; use \\help"),
            }
            continue;
        }
        pending.push_str(&line);
        pending.push('\n');
        let Some(statements) = split_statements(&pending, false) else {
            continue;
        };
        if options.history && !statements.is_empty() && !sensitive_history(&pending) {
            let _ = editor.add_history_entry(pending.trim());
        }
        pending.clear();
        for statement in statements {
            match execute(
                &connection,
                &statement,
                Vec::new(),
                options.queue_timeout,
                &format,
                timing,
            )
            .await
            {
                Outcome::Success => update_transaction(&statement, &mut transaction_open),
                Outcome::Failed | Outcome::Canceled => {}
                Outcome::Fatal => {
                    fatal = true;
                    break;
                }
            }
        }
        if fatal {
            break;
        }
    }
    if options.history
        && let Some(path) = &history_path
        && let Err(error) = editor.save_history(path)
    {
        eprintln!("cannot save shell history: {error}");
    }
    if options.history
        && let Some(path) = &history_path
    {
        let _ = restrict_file(path);
    }
    if !fatal && let Err(error) = connection.close().await {
        crate::cli::print_remote_error(&error);
        return 1;
    }
    if fatal { 1 } else { 0 }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Outcome {
    Success,
    Failed,
    Canceled,
    Fatal,
}

fn finish_catalog(
    result: Result<QueryResult, crate::client::RemoteError>,
    format: &str,
) -> Outcome {
    match result {
        Ok(rows) => {
            crate::cli::emit(&rows, format);
            Outcome::Success
        }
        Err(error) => {
            let fatal = error.failure_kind.as_deref() == Some("unknown_session")
                || error.failure_kind.as_deref() == Some("transport_failure")
                || error.acceptance.as_deref() == Some("unknown");
            crate::cli::print_remote_error(&error);
            if fatal {
                Outcome::Fatal
            } else {
                Outcome::Failed
            }
        }
    }
}

async fn execute(
    connection: &RemoteConnection,
    sql: &str,
    parameters: Vec<Value>,
    queue_timeout: u64,
    format: &str,
    timing: bool,
) -> Outcome {
    let request_id = uuid::Uuid::new_v4().to_string();
    eprintln!("request_id: {request_id}");
    let started = Instant::now();
    let execution = connection.execute_with_timeouts_and_id(
        sql,
        parameters,
        30,
        queue_timeout,
        request_id.clone(),
    );
    tokio::pin!(execution);
    tokio::select! {
        result = &mut execution => {
            match result {
                Ok((id, rows)) => {
                    crate::cli::emit(&rows, format);
                    if timing {
                        print_timing(connection, &id, started).await;
                    }
                    Outcome::Success
                }
                Err(error) => {
                    let fatal = error.failure_kind.as_deref() == Some("unknown_session")
                        || error.failure_kind.as_deref() == Some("transport_failure")
                        || error.acceptance.as_deref() == Some("unknown");
                    crate::cli::print_remote_error(&error);
                    if timing {
                        println!("client_total_ms: {}", started.elapsed().as_millis());
                        println!("server_elapsed_ms: unknown");
                    }
                    if fatal { Outcome::Fatal } else { Outcome::Failed }
                }
            }
        }
        _ = tokio::signal::ctrl_c() => {
            eprintln!("canceling request_id: {request_id}");
            match connection.cancel().await {
                Ok(_) => {
                    println!("cancellation requested for {request_id}");
                    if timing {
                        println!("client_total_ms: {}", started.elapsed().as_millis());
                        println!("server_elapsed_ms: unknown");
                    }
                    Outcome::Canceled
                }
                Err(error) => {
                    crate::cli::print_remote_error(&error);
                    Outcome::Fatal
                }
            }
        }
    }
}

async fn print_timing(connection: &RemoteConnection, request_id: &str, started: Instant) {
    println!("client_total_ms: {}", started.elapsed().as_millis());
    let elapsed = connection
        .client()
        .request(
            reqwest::Method::GET,
            &format!("/v1/queries/{request_id}"),
            None,
            std::time::Duration::from_secs(2),
        )
        .await
        .ok()
        .and_then(|bytes| serde_json::from_slice::<Value>(&bytes).ok())
        .and_then(|value| value["elapsed_ms"].as_u64());
    match elapsed {
        Some(elapsed) => println!("server_elapsed_ms: {elapsed}"),
        None => println!("server_elapsed_ms: unknown"),
    }
}

fn update_transaction(statement: &str, open: &mut bool) {
    let keyword = statement
        .split_whitespace()
        .next()
        .unwrap_or_default()
        .to_ascii_uppercase();
    match keyword.as_str() {
        "BEGIN" | "START" => *open = true,
        "COMMIT" | "ROLLBACK" => *open = false,
        _ => {}
    }
}

fn print_help() {
    println!("\\dt                    list lake tables");
    println!("\\d <schema.table>      describe columns");
    println!("\\timing                toggle client/server timing");
    println!("\\format table|csv|json|vertical");
    println!("\\help                  show this help");
    println!("\\quit                  close the session");
    println!(
        "Finish SQL with ;. Ctrl-C at the prompt clears input; during SQL it requests cancellation."
    );
}

fn sensitive_history(input: &str) -> bool {
    let lower = input.to_ascii_lowercase();
    [
        "duckfleet_token",
        "--token",
        "bearer ",
        "create secret",
        "password",
        "credential",
    ]
    .iter()
    .any(|needle| lower.contains(needle))
}

fn prepare_history(path: &Path) -> Result<(), String> {
    let parent = path.parent().ok_or("history path has no parent")?;
    std::fs::create_dir_all(parent).map_err(|_| "cannot create .duckfleet".to_owned())?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
        std::fs::set_permissions(parent, std::fs::Permissions::from_mode(0o700))
            .map_err(|_| "cannot restrict history directory permissions".to_owned())?;
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .mode(0o600)
            .open(path)
            .map_err(|_| "cannot create shell history".to_owned())?;
        file.set_permissions(std::fs::Permissions::from_mode(0o600))
            .map_err(|_| "cannot restrict shell history permissions".to_owned())?;
    }
    #[cfg(not(unix))]
    {
        std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .map_err(|_| "cannot create shell history".to_owned())?;
    }
    Ok(())
}

fn restrict_file(path: &Path) -> Result<(), String> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
            .map_err(|_| "cannot restrict shell history permissions".to_owned())?;
    }
    Ok(())
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum LexState {
    Normal,
    Single,
    Double,
    LineComment,
    BlockComment,
    Dollar,
}

pub fn split_statements(input: &str, allow_final_without_semicolon: bool) -> Option<Vec<String>> {
    let bytes = input.as_bytes();
    let mut state = LexState::Normal;
    let mut block_depth = 0_u32;
    let mut dollar = String::new();
    let mut statements = Vec::new();
    let mut start = 0_usize;
    let mut index = 0_usize;
    let mut content = false;
    while index < bytes.len() {
        match state {
            LexState::Normal => {
                if bytes[index].is_ascii_whitespace() {
                    index += 1;
                } else if input[index..].starts_with("--") {
                    state = LexState::LineComment;
                    index += 2;
                } else if input[index..].starts_with("/*") {
                    state = LexState::BlockComment;
                    block_depth = 1;
                    index += 2;
                } else if bytes[index] == b'\'' {
                    content = true;
                    state = LexState::Single;
                    index += 1;
                } else if bytes[index] == b'"' {
                    content = true;
                    state = LexState::Double;
                    index += 1;
                } else if bytes[index] == b'$'
                    && let Some(end) = input[index + 1..].find('$')
                {
                    let tag = &input[index + 1..index + 1 + end];
                    if tag.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
                        dollar = format!("${tag}$");
                        content = true;
                        state = LexState::Dollar;
                        index += dollar.len();
                    } else {
                        content = true;
                        index += 1;
                    }
                } else if bytes[index] == b';' {
                    let statement = input[start..index].trim();
                    if !statement.is_empty() {
                        statements.push(statement.to_owned());
                    }
                    content = false;
                    index += 1;
                    start = index;
                } else {
                    content = true;
                    index += input[index..].chars().next().unwrap().len_utf8();
                }
            }
            LexState::Single => {
                if bytes[index] == b'\'' {
                    if bytes.get(index + 1) == Some(&b'\'') {
                        index += 2;
                    } else {
                        state = LexState::Normal;
                        index += 1;
                    }
                } else {
                    index += 1;
                }
            }
            LexState::Double => {
                if bytes[index] == b'"' {
                    if bytes.get(index + 1) == Some(&b'"') {
                        index += 2;
                    } else {
                        state = LexState::Normal;
                        index += 1;
                    }
                } else {
                    index += 1;
                }
            }
            LexState::LineComment => {
                if bytes[index] == b'\n' {
                    state = LexState::Normal;
                }
                index += 1;
            }
            LexState::BlockComment => {
                if input[index..].starts_with("/*") {
                    block_depth += 1;
                    index += 2;
                } else if input[index..].starts_with("*/") {
                    block_depth -= 1;
                    index += 2;
                    if block_depth == 0 {
                        state = LexState::Normal;
                    }
                } else {
                    index += input[index..].chars().next().unwrap().len_utf8();
                }
            }
            LexState::Dollar => {
                if input[index..].starts_with(&dollar) {
                    index += dollar.len();
                    state = LexState::Normal;
                } else {
                    index += input[index..].chars().next().unwrap().len_utf8();
                }
            }
        }
    }
    if matches!(
        state,
        LexState::Single | LexState::Double | LexState::BlockComment | LexState::Dollar
    ) {
        return None;
    }
    if content {
        if !allow_final_without_semicolon {
            return None;
        }
        let statement = input[start..].trim();
        if !statement.is_empty() {
            statements.push(statement.to_owned());
        }
    }
    Some(statements)
}

pub fn read_statements(text: &str) -> Result<Vec<String>, String> {
    split_statements(text, true)
        .ok_or_else(|| "incomplete SQL: unterminated quote, dollar quote, or block comment".into())
}

pub fn result_to_vertical(result: &QueryResult) -> String {
    let mut output = String::new();
    for (row_index, row) in result.rows.iter().enumerate() {
        output.push_str(&format!("-[ RECORD {} ]-\n", row_index + 1));
        for (index, name) in result.columns.iter().enumerate() {
            output.push_str(&format!(
                "{} | {}\n",
                name,
                row.get(index).and_then(Option::as_deref).unwrap_or("NULL")
            ));
        }
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn multiline_sql_respects_quotes_comments_and_multiple_statements() {
        assert_eq!(split_statements("select 1", false), None);
        assert_eq!(split_statements("select '", false), None);
        assert_eq!(split_statements("select 'semi;colon'", false), None);
        assert_eq!(
            split_statements("select 'semi;colon'; select 2;", false).unwrap(),
            vec!["select 'semi;colon'", "select 2"]
        );
        assert_eq!(
            split_statements("select 1; -- done\nselect 2;", false).unwrap(),
            vec!["select 1", "-- done\nselect 2"]
        );
        assert_eq!(
            split_statements("-- comment only", false).unwrap(),
            Vec::<String>::new()
        );
        assert_eq!(
            split_statements("/* comment */", false).unwrap(),
            Vec::<String>::new()
        );
        assert_eq!(
            split_statements("select $$a;b$$; select 3", true).unwrap(),
            vec!["select $$a;b$$", "select 3"]
        );
        assert!(read_statements("select 'unterminated").is_err());
        assert_eq!(
            split_statements("select 'é;漢字'; select '🦆'", true).unwrap(),
            vec!["select 'é;漢字'", "select '🦆'"]
        );
    }

    #[test]
    fn history_filters_credentials_and_vertical_keeps_nulls() {
        for sensitive in [
            "DUCKFLEET_TOKEN=abc",
            "create secret s (type s3)",
            "select 'password'",
            "Bearer abc",
        ] {
            assert!(sensitive_history(sensitive));
        }
        assert!(!sensitive_history("select 1"));
        let result = QueryResult {
            columns: vec!["id".into(), "value".into()],
            column_types: vec!["Int32".into(), "Utf8".into()],
            rows: vec![vec![Some("1".into()), None]],
        };
        let rendered = result_to_vertical(&result);
        assert!(rendered.contains("RECORD 1"));
        assert!(rendered.contains("value | NULL"));
    }
}
