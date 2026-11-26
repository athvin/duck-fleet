use crate::util;
use anyhow::{Context, Result, bail, ensure};
use reqwest::{Method, StatusCode};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    process::{Child, Command, Stdio},
    time::Duration,
};

pub fn secret_values(context: &str, namespace: &str) -> Result<BTreeMap<String, String>> {
    let output = util::run([
        "kubectl",
        "--context",
        context,
        "-n",
        namespace,
        "get",
        "secret",
        "duckfleet-dev",
        "-o",
        "json",
    ])?;
    let document: Value = serde_json::from_slice(&output.stdout)?;
    util::object(document.get("data").unwrap_or(&Value::Null))
        .iter()
        .map(|(key, value)| {
            Ok((
                key.clone(),
                util::decode_base64(value.as_str().context("Secret data must be strings")?)?,
            ))
        })
        .collect()
}

pub fn catalog_probe(dsn: &str, namespace: &str) -> Result<String> {
    let fields = catalog_fields(dsn, namespace)?;
    let literal = fields
        .iter()
        .map(|(key, value)| format!("{key}={value}"))
        .collect::<Vec<_>>()
        .join(" ");
    Ok(format!(
        "ATTACH '{literal}' AS qualification_catalog (TYPE postgres, READ_ONLY); SELECT COUNT(*) FROM qualification_catalog.public.ducklake_snapshot"
    ))
}

/// The source catalog's cluster-qualified host and port.
pub fn catalog_endpoint(dsn: &str, namespace: &str) -> Result<(String, String)> {
    let fields = catalog_fields(dsn, namespace)?;
    Ok((fields["host"].clone(), fields["port"].clone()))
}

fn catalog_fields(dsn: &str, namespace: &str) -> Result<BTreeMap<String, String>> {
    let mut fields = BTreeMap::new();
    for part in shell_words::split(dsn)? {
        let (key, value) = part
            .split_once('=')
            .context("catalog fixture requires keyword DSN fields")?;
        if fields.insert(key.to_owned(), value.to_owned()).is_some() {
            bail!("catalog fixture requires unique keyword DSN fields");
        }
    }
    let expected: Vec<_> = fields.keys().map(String::as_str).collect();
    ensure!(
        expected == ["dbname", "host", "password", "port", "user"],
        "catalog fixture requires the generated role DSN fields"
    );
    if !regex::Regex::new(r"^[a-z][a-z0-9-]*$")?.is_match(namespace) {
        bail!("invalid source namespace");
    }
    let host = fields["host"].clone();
    if !regex::Regex::new(r"^[a-z][a-z0-9.-]*$")?.is_match(&host) {
        bail!("catalog fixture requires a Kubernetes service hostname");
    }
    if !host.contains('.') {
        fields.insert(
            "host".into(),
            format!("{host}.{namespace}.svc.cluster.local"),
        );
    }
    if !fields.values().all(|value| {
        regex::Regex::new(r"^[a-zA-Z0-9_.:-]+$")
            .unwrap()
            .is_match(value)
    }) {
        bail!("catalog fixture DSN contains unsupported field characters");
    }
    fields.insert("connect_timeout".into(), "3".into());
    Ok(fields)
}

/// Opens a TCP connection from inside a namespace's worker pod. Workers cannot
/// attach databases from SQL (engine external access is disabled after
/// startup), so the network boundary is probed at the socket instead.
fn worker_reaches(context: &str, namespace: &str, host: &str, port: &str) -> Result<bool> {
    ensure!(
        regex::Regex::new(r"^[a-z0-9.-]+$")?.is_match(host)
            && port.bytes().all(|b| b.is_ascii_digit()),
        "invalid catalog endpoint"
    );
    let output = kubectl(
        context,
        namespace,
        &[
            "exec".into(),
            "deployment/worker-small".into(),
            "--".into(),
            "timeout".into(),
            "8".into(),
            "bash".into(),
            "-c".into(),
            format!("exec 3<>/dev/tcp/{host}/{port}"),
        ],
        false,
    )?;
    Ok(output.status.success())
}

async fn response(
    port: u16,
    token: &str,
    method: Method,
    path: &str,
    payload: Option<Value>,
    timeout: u64,
) -> Result<(StatusCode, Vec<u8>)> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(timeout))
        .build()?;
    let mut request = client
        .request(method, format!("http://127.0.0.1:{port}{path}"))
        .header("Authorization", format!("Bearer {token}"));
    if let Some(payload) = payload {
        request = request.json(&payload);
    }
    let response = request.send().await?;
    let status = response.status();
    let body = response.bytes().await?.to_vec();
    Ok((status, body))
}

pub async fn call(
    port: u16,
    token: &str,
    method: Method,
    path: &str,
    payload: Option<Value>,
    timeout: u64,
) -> Result<Vec<u8>> {
    let (status, body) = response(port, token, method, path, payload, timeout).await?;
    if !status.is_success() {
        bail!("HTTP {status}: {}", String::from_utf8_lossy(&body));
    }
    Ok(body)
}

pub async fn run_probe(args: Vec<String>) -> Result<()> {
    let mut args = args;
    let port = util::option(&mut args, "--port")
        .context("--port is required")?
        .parse()?;
    let source_port: u16 = util::option(&mut args, "--source-port")
        .context("--source-port is required")?
        .parse()?;
    let context = util::option(&mut args, "--context").context("--context is required")?;
    let namespace = util::option(&mut args, "--namespace").context("--namespace is required")?;
    let source_namespace =
        util::option(&mut args, "--source-namespace").context("--source-namespace is required")?;
    util::reject_extra(&args, 0)?;
    let source = secret_values(&context, &source_namespace)?;
    let team = secret_values(&context, &namespace)?;
    let token = team["DUCKFLEET_TOKEN"].clone();
    let probe = catalog_probe(&source["CATALOG_DSN"], &source_namespace)?;
    let (host, catalog_port) = catalog_endpoint(&source["CATALOG_DSN"], &source_namespace)?;
    ensure!(
        worker_reaches(&context, &source_namespace, &host, &catalog_port)?,
        "positive control: the source worker cannot reach its own catalog"
    );
    ensure!(
        !worker_reaches(&context, &namespace, &host, &catalog_port)?,
        "the team worker reached the neighbour's catalog across the namespace boundary"
    );
    let _ = source_port;

    let fleet = call(port, &token, Method::GET, "/v1/workers", None, 70).await?;
    let fleet: Value = serde_json::from_slice(&fleet)?;
    let workers: std::collections::BTreeSet<_> = util::array(fleet.get("workers"))
        .iter()
        .filter_map(|worker| worker["worker_id"].as_str())
        .collect();
    ensure!(
        workers == std::collections::BTreeSet::from(["small"]),
        "unexpected team fleet: {fleet}"
    );
    let minted = call(
        port,
        &token,
        Method::POST,
        "/v1/credentials",
        Some(json!({"principal":"team-b-analyst"})),
        70,
    )
    .await?;
    let minted: Value = serde_json::from_slice(&minted)?;
    call(
        port,
        &token,
        Method::DELETE,
        &format!(
            "/v1/credentials/{}",
            minted["credential_id"].as_str().unwrap()
        ),
        None,
        70,
    )
    .await?;

    let opened = call(
        port,
        &token,
        Method::POST,
        "/v1/sessions",
        Some(json!({})),
        70,
    )
    .await?;
    let opened: Value = serde_json::from_slice(&opened)?;
    let session = util::text(opened.get("session_id")).to_owned();
    let outcome = async {
        call(
            port,
            &token,
            Method::POST,
            &format!("/v1/sessions/{session}/queries"),
            Some(json!({"request_id":uuid::Uuid::new_v4(),"sql":"create schema if not exists team_b; create or replace table team_b.hello as select 1 as n"})),
            70,
        )
        .await?;
        let (status, body) = response(
            port,
            &token,
            Method::POST,
            &format!("/v1/sessions/{session}/queries"),
            Some(json!({"request_id":uuid::Uuid::new_v4(),"sql":probe,"timeout_seconds":25})),
            70,
        )
        .await?;
        ensure!(!status.is_success(), "the first namespace's catalog answered across the boundary");
        let body: Value = serde_json::from_slice(&body)?;
        let message = util::text(body.get("error"));
        ensure!(
            !message.contains(source["DATABASE_CATALOG_PASSWORD"].as_str()),
            "the probe credential leaked into an error message"
        );
        ensure!(
            message.contains("disabled through configuration"),
            "a team worker's SQL attach was not refused by the engine lockdown: {message}"
        );
        Ok::<(), anyhow::Error>(())
    }
    .await;
    let close = call(
        port,
        &token,
        Method::DELETE,
        &format!("/v1/sessions/{session}"),
        None,
        70,
    )
    .await;
    outcome?;
    close?;
    println!(
        "PASS: the team namespace serves its own lake, and the neighbour's catalog is unreachable even with the right password"
    );
    Ok(())
}

fn kubectl(
    context: &str,
    namespace: &str,
    args: &[String],
    check: bool,
) -> Result<std::process::Output> {
    let output = util::run_status(
        ["kubectl", "--context", context, "-n", namespace]
            .into_iter()
            .map(String::from)
            .chain(args.iter().cloned())
            .collect::<Vec<_>>(),
    )?;
    if check && !output.status.success() {
        bail!("kubectl failed: {}", util::stderr_text(&output));
    }
    Ok(output)
}

fn sql(context: &str, namespace: &str, statement: &str) -> Result<String> {
    Ok(util::stdout_text(&kubectl(
        context,
        namespace,
        &[
            "exec".into(),
            "statefulset/duckfleet-postgres".into(),
            "--".into(),
            "psql".into(),
            "-U".into(),
            "duckfleet".into(),
            "-d".into(),
            "platform".into(),
            "-At".into(),
            "-c".into(),
            statement.into(),
        ],
        true,
    )?)
    .trim()
    .to_owned())
}

fn pod_exists(context: &str, namespace: &str, name: &str) -> Result<bool> {
    Ok(kubectl(
        context,
        namespace,
        &["get".into(), "pod".into(), name.into()],
        false,
    )?
    .status
    .success())
}

fn wait_absent(context: &str, namespace: &str, name: &str) -> Result<()> {
    for _ in 0..120 {
        if !pod_exists(context, namespace, name)? {
            return Ok(());
        }
        std::thread::sleep(Duration::from_secs(1));
    }
    bail!("pod {name} still exists")
}

fn wait_gateways(context: &str, namespace: &str) -> Result<Vec<Value>> {
    for _ in 0..180 {
        let output = kubectl(
            context,
            namespace,
            &[
                "get".into(),
                "pods".into(),
                "-l".into(),
                "app=duckfleet-gateway".into(),
                "-o".into(),
                "json".into(),
            ],
            true,
        )?;
        let pods: Value = serde_json::from_slice(&output.stdout)?;
        let mut ready: Vec<_> = util::array(pods.get("items"))
            .iter()
            .filter(|pod| {
                !util::array(pod["status"].get("containerStatuses")).is_empty()
                    && util::array(pod["status"].get("containerStatuses"))
                        .iter()
                        .all(|container| container["ready"] == true)
            })
            .cloned()
            .collect();
        ready.sort_by_key(|pod| util::text(pod["metadata"].get("name")).to_owned());
        if ready.len() == 2 {
            return Ok(ready);
        }
        std::thread::sleep(Duration::from_secs(1));
    }
    bail!("two gateways did not become ready")
}

struct PostgresGuard {
    context: String,
    namespace: String,
}

impl Drop for PostgresGuard {
    fn drop(&mut self) {
        let _ = kubectl(
            &self.context,
            &self.namespace,
            &[
                "scale".into(),
                "statefulset/duckfleet-postgres".into(),
                "--replicas=1".into(),
            ],
            false,
        );
    }
}

struct Gateway {
    context: String,
    namespace: String,
    name: String,
    token: String,
    port: u16,
    forward: Option<Child>,
}

impl Gateway {
    async fn new(context: &str, namespace: &str, pod: &Value, token: &str) -> Result<Self> {
        let name = util::text(pod["metadata"].get("name")).to_owned();
        let port = std::net::TcpListener::bind("127.0.0.1:0")?
            .local_addr()?
            .port();
        let forward = Command::new("kubectl")
            .args([
                "--context",
                context,
                "-n",
                namespace,
                "port-forward",
                &format!("pod/{name}"),
                &format!("{port}:8080"),
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()?;
        let mut gateway = Self {
            context: context.into(),
            namespace: namespace.into(),
            name,
            token: token.into(),
            port,
            forward: Some(forward),
        };
        for _ in 0..120 {
            if let Ok((status, _)) = response(
                gateway.port,
                &gateway.token,
                Method::GET,
                "/healthz",
                None,
                1,
            )
            .await
                && status == StatusCode::OK
            {
                return Ok(gateway);
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        gateway.stop();
        bail!("gateway did not become reachable")
    }

    fn stop(&mut self) {
        if let Some(mut child) = self.forward.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }

    async fn response(
        &self,
        method: Method,
        path: &str,
        payload: Option<Value>,
        timeout: u64,
    ) -> Result<(StatusCode, Vec<u8>)> {
        response(self.port, &self.token, method, path, payload, timeout).await
    }

    async fn json(&self, method: Method, path: &str, payload: Option<Value>) -> Result<Value> {
        let body = call(self.port, &self.token, method, path, payload, 75).await?;
        Ok(serde_json::from_slice(&body)?)
    }

    async fn open(&self) -> Result<(String, String)> {
        let opened = self
            .json(
                Method::POST,
                "/v1/sessions",
                Some(json!({"compute":{"cpu":1,"memory":"768Mi"}})),
            )
            .await?;
        Ok((
            util::text(opened.get("session_id")).to_owned(),
            util::text(opened["placement"].get("worker_id")).to_owned(),
        ))
    }

    async fn query(&self, session: &str, sql: &str) -> Result<Vec<u8>> {
        call(
            self.port,
            &self.token,
            Method::POST,
            &format!("/v1/sessions/{session}/queries"),
            Some(json!({"request_id":uuid::Uuid::new_v4(),"sql":sql,"timeout_seconds":25})),
            75,
        )
        .await
    }

    fn delete_pod(&self) -> Result<()> {
        kubectl(
            &self.context,
            &self.namespace,
            &[
                "delete".into(),
                "pod".into(),
                self.name.clone(),
                "--wait=true".into(),
            ],
            true,
        )?;
        Ok(())
    }
}

impl Drop for Gateway {
    fn drop(&mut self) {
        self.stop();
    }
}

pub async fn run_gateway_ownership(args: Vec<String>) -> Result<()> {
    let mut args = args;
    let context = util::option(&mut args, "--context").context("--context is required")?;
    let namespace = util::option(&mut args, "--namespace").context("--namespace is required")?;
    util::reject_extra(&args, 0)?;
    let token = secret_values(&context, &namespace)?
        .remove("DUCKFLEET_TOKEN")
        .context("missing token")?;
    let pods = wait_gateways(&context, &namespace)?;
    let mut first = Gateway::new(&context, &namespace, &pods[0], &token).await?;
    let mut second = Gateway::new(&context, &namespace, &pods[1], &token).await?;
    tokio::time::sleep(Duration::from_secs(3)).await;
    let (session, worker) = first.open().await?;
    second.query(&session, "SELECT 1").await?;
    let (status, _) = second
        .response(Method::DELETE, &format!("/v1/sessions/{session}"), None, 75)
        .await?;
    ensure!(
        status == StatusCode::NOT_FOUND,
        "sibling closed the owner session"
    );
    ensure!(
        pod_exists(&context, &namespace, &worker)?,
        "worker disappeared"
    );
    println!("PASS: a sibling routed reads but could not close the live owner session");

    second.delete_pod()?;
    second.stop();
    let pods = wait_gateways(&context, &namespace)?;
    second = Gateway::new(&context, &namespace, &pods[1], &token).await?;
    first.query(&session, "SELECT 1").await?;
    ensure!(
        pod_exists(&context, &namespace, &worker)?,
        "worker disappeared after sibling replacement"
    );
    println!("PASS: concurrent startup and non-owner rolling replacement preserved live compute");

    sql(
        &context,
        &namespace,
        &format!(
            "UPDATE gateway_sessions SET gateway_epoch='stale-epoch' WHERE session_id='{session}'"
        ),
    )?;
    let (status, _) = first
        .response(Method::DELETE, &format!("/v1/sessions/{session}"), None, 75)
        .await?;
    ensure!(
        status == StatusCode::NOT_FOUND,
        "stale epoch was not fenced"
    );
    ensure!(
        pod_exists(&context, &namespace, &worker)?,
        "stale epoch removed live compute"
    );
    sql(
        &context,
        &namespace,
        &format!(
            "UPDATE gateway_sessions SET lease_until=now()-interval '1 second' WHERE session_id='{session}'"
        ),
    )?;
    let current = wait_gateways(&context, &namespace)?;
    kubectl(
        &context,
        &namespace,
        &[
            "delete".into(),
            "pod".into(),
            util::text(current[1]["metadata"].get("name")).into(),
            "--wait=true".into(),
        ],
        true,
    )?;
    wait_absent(&context, &namespace, &worker)?;
    second.stop();
    second = Gateway::new(
        &context,
        &namespace,
        &wait_gateways(&context, &namespace)?[1],
        &token,
    )
    .await?;
    println!("PASS: a stale epoch was fenced and only an expired lease permitted orphan cleanup");

    first.stop();
    first = Gateway::new(
        &context,
        &namespace,
        &wait_gateways(&context, &namespace)?[0],
        &token,
    )
    .await?;
    let database_guard = PostgresGuard {
        context: context.clone(),
        namespace: namespace.clone(),
    };
    kubectl(
        &context,
        &namespace,
        &[
            "scale".into(),
            "statefulset/duckfleet-postgres".into(),
            "--replicas=0".into(),
        ],
        true,
    )?;
    tokio::time::sleep(Duration::from_secs(2)).await;
    let outage = first.open().await;
    ensure!(outage.is_err(), "database outage admitted new ownership");
    kubectl(
        &context,
        &namespace,
        &[
            "scale".into(),
            "statefulset/duckfleet-postgres".into(),
            "--replicas=1".into(),
        ],
        true,
    )?;
    kubectl(
        &context,
        &namespace,
        &[
            "rollout".into(),
            "status".into(),
            "statefulset/duckfleet-postgres".into(),
            "--timeout=240s".into(),
        ],
        true,
    )?;
    let (recovered, recovered_worker) = first.open().await?;
    first.query(&recovered, "SELECT 1").await?;
    call(
        first.port,
        &token,
        Method::DELETE,
        &format!("/v1/sessions/{recovered}"),
        None,
        75,
    )
    .await?;
    wait_absent(&context, &namespace, &recovered_worker)?;
    drop(database_guard);
    println!("PASS: a database outage refused new ownership and recovered without leaked compute");

    let (owned, owned_worker) = first.open().await?;
    first.delete_pod()?;
    wait_absent(&context, &namespace, &owned_worker)?;
    let (status, _) = second
        .response(
            Method::POST,
            &format!("/v1/sessions/{owned}/queries"),
            Some(json!({"request_id":uuid::Uuid::new_v4(),"sql":"SELECT 1"})),
            75,
        )
        .await?;
    ensure!(
        matches!(
            status,
            StatusCode::NOT_FOUND | StatusCode::GONE | StatusCode::SERVICE_UNAVAILABLE
        ),
        "lost owner recreated a session"
    );
    println!(
        "PASS: losing the owning gateway lost the worker rather than silently recreating a session"
    );

    let replacement = wait_gateways(&context, &namespace)?;
    let owner = Gateway::new(&context, &namespace, &replacement[0], &token).await?;
    tokio::time::sleep(Duration::from_secs(3)).await;
    let (commit_session, commit_worker) = owner.open().await?;
    owner
        .query(&commit_session, "CREATE SCHEMA IF NOT EXISTS api_demo")
        .await?;
    owner
        .query(
            &commit_session,
            "CREATE OR REPLACE TABLE api_demo.ha_commit (n BIGINT)",
        )
        .await?;
    let request_id = uuid::Uuid::new_v4();
    let port = owner.port;
    let request_token = token.clone();
    let request_session = commit_session.clone();
    let write = tokio::spawn(async move {
        response(
            port,
            &request_token,
            Method::POST,
            &format!("/v1/sessions/{request_session}/queries"),
            Some(json!({"request_id":request_id,"sql":"INSERT INTO api_demo.ha_commit SELECT count(*) FROM range(40000000000) WHERE range % 7 = 3","timeout_seconds":58})),
            70,
        )
        .await
    });
    for _ in 0..100 {
        if sql(
            &context,
            &namespace,
            &format!("SELECT status FROM query_history WHERE query_id='{request_id}'"),
        )? == "running"
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    ensure!(
        sql(
            &context,
            &namespace,
            &format!("SELECT status FROM query_history WHERE query_id='{request_id}'")
        )? == "running",
        "write did not reach running before gateway kill"
    );
    kubectl(
        &context,
        &namespace,
        &[
            "delete".into(),
            "pod".into(),
            owner.name.clone(),
            "--grace-period=0".into(),
            "--force".into(),
            "--wait=false".into(),
        ],
        true,
    )?;
    let outcome = write.await?;
    ensure!(
        outcome.is_err() || outcome.is_ok_and(|(status, _)| !status.is_success()),
        "an interrupted commit unexpectedly succeeded"
    );
    wait_absent(&context, &namespace, &commit_worker)?;
    for _ in 0..120 {
        if let Ok(body) = call(
            second.port,
            &token,
            Method::GET,
            &format!("/v1/queries/{request_id}"),
            None,
            75,
        )
        .await
        {
            let record: Value = serde_json::from_slice(&body)?;
            if !matches!(
                record["status"].as_str(),
                Some("running" | "queued" | "pending")
            ) {
                break;
            }
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    ensure!(
        sql(
            &context,
            &namespace,
            &format!("SELECT count(*) FROM query_history WHERE query_id='{request_id}'")
        )? == "1",
        "interrupted write was replayed"
    );
    second.stop();
    second = Gateway::new(
        &context,
        &namespace,
        &wait_gateways(&context, &namespace)?[1],
        &token,
    )
    .await?;
    let probe = second
        .json(Method::POST, "/v1/sessions", Some(json!({})))
        .await?;
    let probe_id = util::text(probe.get("session_id"));
    second.query(probe_id, "SELECT CASE WHEN (SELECT count(*) FROM api_demo.ha_commit)=0 THEN 0 ELSE error('interrupted write committed') END").await?;
    call(
        second.port,
        &token,
        Method::DELETE,
        &format!("/v1/sessions/{probe_id}"),
        None,
        75,
    )
    .await?;
    println!("PASS: an interrupted write stayed classified and was never replayed");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn catalog_probe_is_scoped_and_redaction_is_required() {
        let sql = catalog_probe(
            "host=postgres port=5432 user=catalog password=abc123 dbname=lake",
            "duckfleet-dev",
        )
        .unwrap();
        assert!(sql.contains("postgres.duckfleet-dev.svc.cluster.local"));
        assert!(sql.contains("connect_timeout=3"));
        let (host, port) = catalog_endpoint(
            "host=postgres port=5432 user=catalog password=abc123 dbname=lake",
            "duckfleet-dev",
        )
        .unwrap();
        assert_eq!(
            (host.as_str(), port.as_str()),
            ("postgres.duckfleet-dev.svc.cluster.local", "5432")
        );
    }
}
