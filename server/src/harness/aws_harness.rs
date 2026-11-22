//! The EKS harness entrypoint: the full lifecycle, scale and cold-start checks,
//! extension policy and secret-scope probes, then bounded S3-backed evidence.

use std::sync::{Arc, Mutex};

use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, UdpSocket};
use tokio::task::JoinHandle;

use super::{Ctx, require, sql};

pub(super) async fn main(ctx: &Ctx) -> anyhow::Result<()> {
    super::main_lifecycle(ctx, &[]).await?;
    super::scale_checks(ctx).await?;
    super::cold_start_checks(ctx).await?;
    super::engine_qualification::security(ctx).await?;
    let secret_scope = s3_secret_scope_check(ctx).await?;
    s3_glob_check(ctx, secret_scope).await?;
    println!(
        "PASS: the EKS harness ran the lifecycle, scale, cold-start, extension security and S3 checks"
    );
    Ok(())
}

#[derive(Debug)]
struct RecordedRequest {
    path: String,
    authorization: Option<String>,
}

struct MockS3 {
    address: String,
    requests: Arc<Mutex<Vec<RecordedRequest>>>,
    task: JoinHandle<()>,
}

impl MockS3 {
    async fn start() -> anyhow::Result<Self> {
        let listener = TcpListener::bind("0.0.0.0:5432").await?;
        let route = UdpSocket::bind("0.0.0.0:0").await?;
        route.connect("server:8080").await?;
        let address = format!(
            "{}:{}",
            route.local_addr()?.ip(),
            listener.local_addr()?.port()
        );
        let requests = Arc::new(Mutex::new(Vec::new()));
        let recorded = Arc::clone(&requests);
        let task = tokio::spawn(async move {
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    break;
                };
                let mut bytes = Vec::new();
                let mut chunk = [0_u8; 2048];
                while !bytes.windows(4).any(|window| window == b"\r\n\r\n") && bytes.len() < 16384 {
                    match socket.read(&mut chunk).await {
                        Ok(0) | Err(_) => break,
                        Ok(count) => bytes.extend_from_slice(&chunk[..count]),
                    }
                }
                let request = String::from_utf8_lossy(&bytes);
                let mut lines = request.lines();
                let path = lines
                    .next()
                    .and_then(|line| line.split_whitespace().nth(1))
                    .unwrap_or_default()
                    .to_owned();
                let authorization = lines.find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    name.eq_ignore_ascii_case("authorization")
                        .then(|| value.trim().to_owned())
                });
                recorded.lock().unwrap().push(RecordedRequest {
                    path,
                    authorization,
                });
                let _ = socket
                    .write_all(
                        b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                    )
                    .await;
            }
        });
        Ok(Self {
            address,
            requests,
            task,
        })
    }

    fn take(&self) -> Vec<RecordedRequest> {
        std::mem::take(&mut *self.requests.lock().unwrap())
    }
}

impl Drop for MockS3 {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn s3_secret_scope_check(ctx: &Ctx) -> anyhow::Result<Value> {
    let server = MockS3::start().await?;
    let mut connection = ctx.open_named("aws-secret-scope", None).await?;
    sql(
        &connection,
        &format!(
            "CREATE SECRET live_scope_probe (TYPE S3, KEY_ID 'TESTLOCALONLY', SECRET 'test-local-secret', ENDPOINT '{}', URL_STYLE 'path', USE_SSL false, REGION 'us-east-2', SCOPE 's3://duckfleet-scope-fixture/lake/')",
            server.address
        ),
    )
    .await?;
    let _ = connection
        .execute(
            "SELECT * FROM read_csv('s3://duckfleet-scope-fixture/lake/fixture.csv')",
            Vec::new(),
        )
        .await;
    let in_scope = server.take();
    let signed_in_scope = in_scope
        .iter()
        .filter(|request| {
            request.path.starts_with("/duckfleet-scope-fixture/lake/")
                && request
                    .authorization
                    .as_deref()
                    .is_some_and(|value| value.contains("Credential=TESTLOCALONLY/"))
        })
        .count();
    require(
        signed_in_scope > 0,
        "the in-scope S3 fixture received no signed request",
    )?;

    // Give the neighbor the same endpoint without credentials so its request is
    // observed rather than sent to the real bucket or omitted entirely.
    sql(
        &connection,
        &format!(
            "CREATE SECRET live_scope_neighbor (TYPE S3, KEY_ID '', SECRET '', ENDPOINT '{}', URL_STYLE 'path', USE_SSL false, REGION 'us-east-2', SCOPE 's3://duckfleet-scope-fixture-other/lake/')",
            server.address
        ),
    )
    .await?;
    let _ = connection
        .execute(
            "SELECT * FROM read_csv('s3://duckfleet-scope-fixture-other/lake/fixture.csv')",
            Vec::new(),
        )
        .await;
    let neighbor = server.take();
    connection.close().await?;
    require(
        !neighbor.is_empty(),
        "the prefix-neighbor probe made no S3 request",
    )?;
    require(
        neighbor
            .iter()
            .all(|request| request.authorization.is_none()),
        "a prefix-neighbor bucket received scoped credentials",
    )?;
    require(
        neighbor
            .iter()
            .all(|request| !request.path.starts_with("/duckfleet-scope-fixture/")),
        "a prefix-neighbor probe reused the in-scope bucket path",
    )?;
    let evidence = json!({
        "signed_in_scope_requests": signed_in_scope,
        "unsigned_neighbor_requests": neighbor.len(),
        "neighbor_credentials_sent": false,
    });
    super::write_artifact(
        "aws-secret-scope.json",
        &serde_json::to_string_pretty(&evidence)?,
    );
    println!("PASS: live worker scoped credentials were not sent to a prefix-neighbor bucket");
    Ok(evidence)
}

async fn s3_glob_check(ctx: &Ctx, secret_scope: Value) -> anyhow::Result<()> {
    let prefix = std::env::var("EXPECT_S3_PREFIX")?;
    let account = prefix
        .strip_prefix("s3://duckfleet-dev-")
        .and_then(|value| value.strip_suffix("/lake/"));
    require(
        account.is_some_and(|value| {
            !value.is_empty()
                && value.chars().all(|character| {
                    character.is_ascii_lowercase() || character.is_ascii_digit() || character == '-'
                })
        }),
        "AWS harness must target the DuckFleet development lake",
    )?;
    let connection = ctx.open().await?;
    let rows = sql(
        &connection,
        "select count(*) from lake.analytics.fct_orders",
    )
    .await?;
    let count = rows
        .first()
        .and_then(|row| row[0].as_deref())
        .unwrap_or("0");
    require(
        count != "0",
        "the lake read returned no rows from S3-backed storage",
    )?;
    let files = sql(
        &connection,
        &format!("SELECT file FROM glob('{prefix}**/*.parquet') LIMIT 5"),
    )
    .await?;
    require(
        !files.is_empty()
            && files.iter().all(|row| {
                row[0]
                    .as_deref()
                    .is_some_and(|path| path.starts_with(&prefix))
            }),
        "no Parquet in the AWS lake",
    )?;
    let mut connection = connection;
    connection.close().await?;

    let workers: Value = serde_json::from_str(&std::fs::read_to_string(
        "/artifacts/worker-resources.json",
    )?)?;
    let model_placement: Value =
        serde_json::from_str(&std::fs::read_to_string("/artifacts/model-placement.json")?)?;
    let evidence = json!({
        "workers": workers,
        "model_placement": model_placement,
        "s3_files": files.into_iter().map(|row| row[0].clone()).collect::<Vec<_>>(),
        "secret_scope": secret_scope,
    });
    println!(
        "DUCKFLEET_AWS_EVIDENCE={}",
        serde_json::to_string(&evidence)?
    );
    println!("PASS: committed lake data is readable from the object store ({count} rows)");
    Ok(())
}
