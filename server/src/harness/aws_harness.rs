//! The EKS harness entrypoint: the full lifecycle, scale and cold-start checks,
//! extension policy and secret-scope probes, then bounded S3-backed evidence.

use std::sync::{Arc, Mutex};

use reqwest::Method;
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

/// SQL may reach the lake prefix and nothing else (#196): a secret scoped to any
/// other bucket, pointed at an endpoint we observe, must be refused by the engine
/// before a single request leaves the worker. That is strictly stronger than the
/// property this probe once checked (credentials signed only within their scope),
/// so a refusal plus an empty request log is the pass, and any request reaching
/// the fixture, signed or not, is a failure.
async fn s3_secret_scope_check(ctx: &Ctx) -> anyhow::Result<Value> {
    let server = MockS3::start().await?;
    let mut connection = ctx.open_named("aws-secret-scope", None).await?;
    let mut refusals = 0;
    for (name, key, bucket) in [
        (
            "live_scope_probe",
            "TESTLOCALONLY",
            "duckfleet-scope-fixture",
        ),
        ("live_scope_neighbor", "", "duckfleet-scope-fixture-other"),
    ] {
        sql(
            &connection,
            &format!(
                "CREATE SECRET {name} (TYPE S3, KEY_ID '{key}', SECRET 'test-local-secret', ENDPOINT '{}', URL_STYLE 'path', USE_SSL false, REGION 'us-east-2', SCOPE 's3://{bucket}/lake/')",
                server.address
            ),
        )
        .await?;
        let outcome = connection
            .execute(
                &format!("SELECT * FROM read_csv('s3://{bucket}/lake/fixture.csv')"),
                Vec::new(),
            )
            .await;
        let refused = outcome.as_ref().err().is_some_and(|error| {
            format!("{error:?}").contains("file system operations are disabled by configuration")
        });
        require(
            refused,
            format!(
                "a read of s3://{bucket}/lake/ outside the lake was not refused by the engine file lockdown"
            ),
        )?;
        refusals += 1;
    }
    let requests = server.take();
    connection.close().await?;
    // Each recorded request names its path and whether it carried credentials.
    require(
        requests.is_empty(),
        format!(
            "S3 requests left the worker for buckets outside the lake (path, signed): {:?}",
            requests
                .iter()
                .map(|request| (&request.path, request.authorization.is_some()))
                .collect::<Vec<_>>()
        ),
    )?;
    let evidence = json!({
        "out_of_lake_reads_refused": refusals,
        "requests_leaving_worker": requests.len(),
        "credentials_sent_outside_lake": false,
    });
    super::write_artifact(
        "aws-secret-scope.json",
        &serde_json::to_string_pretty(&evidence)?,
    );
    println!(
        "PASS: the engine lockdown refused out-of-lake S3 reads before any request left the worker"
    );
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
    let model_placement = connection.placement.clone();
    let mut connection = connection;
    connection.close().await?;

    let workers =
        super::request_json(&ctx.client()?, Method::GET, "/v1/workers", None).await?["workers"]
            .clone();
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
