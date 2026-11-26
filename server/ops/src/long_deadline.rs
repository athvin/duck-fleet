use crate::util;
use anyhow::{Context, Result, bail, ensure};
use arrow_array::{Array, Int64Array};
use axum::{Router, extract::State, routing::any};
use reqwest::{Method, StatusCode};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    io::Write,
    path::PathBuf,
    process::{Command, Output},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

fn logged(
    base: &[String],
    environment: &BTreeMap<String, String>,
    arguments: &[String],
    log: &PathBuf,
    check: bool,
) -> Result<Output> {
    let output = Command::new(&base[0])
        .args(&base[1..])
        .args(arguments)
        .envs(environment)
        .current_dir(util::root())
        .output()?;
    let mut log = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(log)?;
    log.write_all(&output.stdout)?;
    log.write_all(&output.stderr)?;
    if check && !output.status.success() {
        bail!("command failed: {}", arguments.join(" "));
    }
    Ok(output)
}

struct Cleanup {
    base: Vec<String>,
    environment: BTreeMap<String, String>,
    log: PathBuf,
}

impl Drop for Cleanup {
    fn drop(&mut self) {
        let _ = logged(
            &self.base,
            &self.environment,
            &["down".into(), "--volumes".into(), "--remove-orphans".into()],
            &self.log,
            false,
        );
    }
}

async fn request(
    client: &reqwest::Client,
    url: &str,
    token: &str,
    method: Method,
    path: &str,
    payload: Option<Value>,
) -> Result<(StatusCode, Vec<u8>)> {
    let mut request = client
        .request(method, format!("{url}{path}"))
        .header("Authorization", format!("Bearer {token}"));
    if let Some(payload) = payload {
        request = request.json(&payload);
    }
    let response = request.send().await?;
    let status = response.status();
    let body = response.bytes().await?.to_vec();
    Ok((status, body))
}

async fn query(
    client: &reqwest::Client,
    url: &str,
    token: &str,
    session: &str,
    sql: &str,
    seconds: u64,
) -> Result<(String, StatusCode, Vec<u8>)> {
    let id = uuid::Uuid::new_v4().to_string();
    let (status, body) = request(
        client,
        url,
        token,
        Method::POST,
        &format!("/v1/sessions/{session}/queries"),
        Some(json!({"request_id":id,"sql":sql,"timeout_seconds":seconds})),
    )
    .await?;
    Ok((id, status, body))
}

#[derive(Clone)]
struct Slow {
    delayed: Arc<AtomicBool>,
}

async fn slow_csv(State(state): State<Slow>) -> &'static [u8] {
    if !state.delayed.swap(true, Ordering::SeqCst) {
        tokio::time::sleep(Duration::from_secs(65)).await;
    }
    b"n\n42\n"
}

pub async fn run(args: Vec<String>) -> Result<()> {
    let mut args = args;
    let image = util::option(&mut args, "--image").context("--image is required")?;
    let output = util::option(&mut args, "--output").context("--output is required")?;
    util::reject_extra(&args, 0)?;
    let output = std::path::PathBuf::from(output);
    if let Some(parent) = output.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let output = if output.is_absolute() {
        output
    } else {
        util::root().join(output)
    };
    let log = output.with_extension("log");
    let project = format!(
        "duckfleet-deadline-{}",
        &uuid::Uuid::new_v4().simple().to_string()[..10]
    );
    let token = util::random_hex(32)?;
    let mut environment: BTreeMap<String, String> = std::env::vars().collect();
    environment.extend([
        ("DUCKFLEET_PORT".into(), "0".into()),
        ("DUCKFLEET_TOKEN".into(), token.clone()),
        (
            "DUCKFLEET_POSTGRES_ADMIN_PASSWORD".into(),
            util::random_hex(32)?,
        ),
        ("DUCKFLEET_INTERNAL_TOKEN".into(), util::random_hex(32)?),
    ]);
    let scratch = tempfile::tempdir()?;
    let override_path = scratch.path().join("compose.json");
    util::write_json(
        &override_path,
        &json!({"services":ComposeServices::new(image)}),
    )?;
    let base = vec![
        "docker".into(),
        "compose".into(),
        "-p".into(),
        project,
        "-f".into(),
        util::root()
            .join("compose.yaml")
            .to_string_lossy()
            .into_owned(),
        "-f".into(),
        override_path.to_string_lossy().into_owned(),
    ];
    let cleanup = Cleanup {
        base: base.clone(),
        environment: environment.clone(),
        log: log.clone(),
    };
    let delayed = Arc::new(AtomicBool::new(false));
    let router = Router::new()
        .route("/{*path}", any(slow_csv))
        .with_state(Slow {
            delayed: delayed.clone(),
        });
    let listener = tokio::net::TcpListener::bind("0.0.0.0:0").await?;
    let slow_port = listener.local_addr()?.port();
    let server = tokio::spawn(async move {
        let _ = axum::serve(listener, router).await;
    });
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(160))
        .build()?;
    let result = async {
        logged(&base, &environment, &["up".into(), "-d".into(), "--wait".into(), "server".into()], &log, true)?;
        let endpoint = logged(&base, &environment, &["port".into(), "server".into(), "8080".into()], &log, true)?;
        let url = format!("http://{}", util::stdout_text(&endpoint).trim());
        let (status, body) = request(&client, &url, &token, Method::POST, "/v1/sessions", Some(json!({}))).await?;
        ensure!(status.is_success(), "session open failed");
        let opened: Value = serde_json::from_slice(&body)?;
        let session = util::text(opened.get("session_id")).to_owned();
        let (_, status, _) = query(&client, &url, &token, &session, "SELECT 1", 601).await?;
        ensure!(status == StatusCode::BAD_REQUEST, "601-second statement was accepted");
        for setting in ["SET http_timeout=120", "SET http_retries=0"] {
            let (_, status, body) = query(&client, &url, &token, &session, setting, 30).await?;
            ensure!(status.is_success(), "setting failed: {}", String::from_utf8_lossy(&body));
        }
        let sql = format!("SELECT * FROM read_csv('http://host.docker.internal:{slow_port}/slow.csv',header=true,columns={{'n':'BIGINT'}})");
        let started = std::time::Instant::now();
        let (_, status, body) = query(&client, &url, &token, &session, &sql, 120).await?;
        let elapsed = started.elapsed().as_secs_f64();
        ensure!(status.is_success(), "long statement failed: {}", String::from_utf8_lossy(&body));
        ensure!(elapsed > 60.0 && elapsed < 150.0, "unexpected statement time: {elapsed}");
        let mut reader = arrow_ipc::reader::StreamReader::try_new(std::io::Cursor::new(body), None)?;
        let batch = reader.next().context("empty Arrow result")??;
        let values = batch.column(0).as_any().downcast_ref::<Int64Array>().context("result is not BIGINT")?;
        ensure!(batch.num_rows() == 1 && values.value(0) == 42, "wrong Arrow result");
        ensure!(reader.next().is_none(), "multiple Arrow batches were unexpected");

        delayed.store(false, Ordering::SeqCst);
        let sql = format!("SELECT * FROM read_csv('http://host.docker.internal:{slow_port}/short.csv',header=true,columns={{'n':'BIGINT'}})");
        let short_started = std::time::Instant::now();
        let (last_request_id, status, body) = query(&client, &url, &token, &session, &sql, 1).await?;
        ensure!(!status.is_success(), "short deadline unexpectedly succeeded");
        let refusal: Value = serde_json::from_slice(&body)?;
        ensure!(matches!(refusal["failure_kind"].as_str(), Some("timeout" | "unavailable")), "unexpected short failure: {refusal}");
        let until = std::time::Instant::now() + Duration::from_secs(90);
        let outcome = loop {
            let (_, body) = request(&client, &url, &token, Method::GET, &format!("/v1/requests/{last_request_id}"), None).await?;
            let outcome: Value = serde_json::from_slice(&body)?;
            if outcome["status"] == "timed_out" { break outcome; }
            ensure!(std::time::Instant::now() < until, "short query did not settle: {outcome}");
            tokio::time::sleep(Duration::from_secs(1)).await;
        };
        let short_elapsed = short_started.elapsed().as_secs_f64();
        let (status, _) = request(&client, &url, &token, Method::DELETE, &format!("/v1/sessions/{session}"), None).await?;
        ensure!(status.is_success(), "session close failed");
        let report = json!({
            "elapsed_seconds":elapsed,
            "statement_deadline_seconds":120,
            "rows":[{"n":42}],
            "out_of_range_601_refused":true,
            "short_deadline_refused":true,
            "short_initial_failure":refusal["failure_kind"],
            "short_final_status":outcome["status"],
            "short_deadline_elapsed_seconds":short_elapsed,
            "scope":"actual DuckDB statement through gateway; delayed synthetic HTTP CSV",
        });
        util::write_json(&output, &report)?;
        println!("{}", serde_json::to_string_pretty(&report)?);
        Ok(())
    }
    .await;
    server.abort();
    drop(cleanup);
    result
}

#[derive(serde::Serialize)]
struct ComposeServices {
    #[serde(rename = "platform-migrate")]
    migrate: Image,
    #[serde(rename = "worker-small")]
    worker: Image,
    server: Image,
}

impl ComposeServices {
    fn new(image: String) -> Self {
        Self {
            migrate: Image::new(&image),
            worker: Image::new(&image),
            server: Image::new(&image),
        }
    }
}

#[derive(serde::Serialize)]
struct Image {
    image: String,
    pull_policy: &'static str,
}

impl Image {
    fn new(image: &str) -> Self {
        Self {
            image: image.into(),
            pull_policy: "never",
        }
    }
}
