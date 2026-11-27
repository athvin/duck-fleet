use axum::{
    Router,
    extract::{Request, State},
    http::StatusCode,
    routing::any,
};
use serde_json::json;
use std::sync::{Arc, Mutex};

fn extensions() -> duckdb::Result<duckdb::Connection> {
    let connection = duckdb::Connection::open_in_memory()?;
    connection.execute_batch(
        "SET extension_directory='/opt/duckdb/extensions';
         LOAD icu; LOAD httpfs; LOAD postgres; LOAD ducklake; LOAD aws;",
    )?;
    Ok(connection)
}

fn extension_bundle() -> anyhow::Result<()> {
    let connection = extensions()?;
    let mut statement = connection.prepare(
        "SELECT extension_name, loaded::VARCHAR, installed::VARCHAR, install_path, extension_version
         FROM duckdb_extensions() WHERE extension_name IN ('httpfs', 'postgres_scanner', 'ducklake', 'aws', 'icu')
         ORDER BY extension_name",
    )?;
    let rows = statement
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, String>(4)?,
            ))
        })?
        .collect::<duckdb::Result<Vec<_>>>()?;
    let required =
        std::collections::BTreeSet::from(["aws", "ducklake", "httpfs", "icu", "postgres_scanner"]);
    let actual: std::collections::BTreeSet<_> = rows.iter().map(|row| row.0.as_str()).collect();
    anyhow::ensure!(actual == required, "extension manifest mismatch: {rows:?}");
    for (name, loaded, installed, path, version) in rows {
        anyhow::ensure!(
            loaded == "true" && installed == "true",
            "extension {name} is not loaded/installed"
        );
        anyhow::ensure!(
            path == "(BUILT-IN)" || path.contains("/v2.0.1/"),
            "extension {name} has unexpected path {path}"
        );
        anyhow::ensure!(!version.is_empty(), "extension {name} has no version");
        println!("{name} {version} {path}");
    }
    let calendar = connection.query_row(
        "SELECT timezone('America/New_York', TIMESTAMPTZ '2026-07-01 12:00:00+00')::VARCHAR,
                date_part('isodow', DATE '2026-11-21')::BIGINT,
                'a' COLLATE NOCASE = 'A', 'a' COLLATE NOACCENT = 'á'",
        [],
        |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, bool>(2)?,
                row.get::<_, bool>(3)?,
            ))
        },
    )?;
    anyhow::ensure!(
        calendar == ("2026-07-01 08:00:00".into(), 6, true, true),
        "ICU behavior changed: {calendar:?}"
    );
    connection.execute_batch("SET allow_extension_repositories='forbidden'")?;
    let error = connection
        .execute_batch("CREATE EXTENSION REPOSITORY example.org")
        .expect_err("custom extension repository was allowed");
    let message = error.to_string().to_ascii_lowercase();
    anyhow::ensure!(
        message.contains("extension") && message.contains("repositories"),
        "unexpected repository refusal: {message}"
    );
    println!(
        "PASS: offline 2.0.1 extensions, ICU calendar/collation behavior, and repository lockdown"
    );
    Ok(())
}

type SecurityResponseFuture =
    std::pin::Pin<Box<dyn std::future::Future<Output = (StatusCode, String)> + Send>>;
type SecurityHandler<T> = fn(State<T>, Request) -> SecurityResponseFuture;
type RequestLog = Arc<Mutex<Vec<(String, Option<String>)>>>;

async fn spawn_server<T>(
    state: T,
    handler: SecurityHandler<T>,
) -> anyhow::Result<(u16, tokio::task::JoinHandle<()>)>
where
    T: Clone + Send + Sync + 'static,
{
    let router = Router::new()
        .route(
            "/{*path}",
            any(move |state, request| async move { handler(state, request).await }),
        )
        .with_state(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let port = listener.local_addr()?.port();
    let task = tokio::spawn(async move {
        let _ = axum::serve(listener, router).await;
    });
    Ok((port, task))
}

#[derive(Clone, Default)]
struct CredentialState {
    requests: Arc<Mutex<Vec<Option<String>>>>,
    keys: Arc<Mutex<Vec<String>>>,
}

async fn credentials_handler(
    State(state): State<CredentialState>,
    request: Request,
) -> (StatusCode, String) {
    let authorization = request
        .headers()
        .get("authorization")
        .map(|value| value.to_str().unwrap_or_default().to_owned());
    state.requests.lock().unwrap().push(authorization.clone());
    if authorization.as_deref() != Some("test-pod-identity-token") {
        return (StatusCode::FORBIDDEN, "denied".into());
    }
    let key = if state.keys.lock().unwrap().is_empty() {
        "TESTOLDLOCALONLY"
    } else {
        "TESTNEWLOCALONLY"
    };
    state.keys.lock().unwrap().push(key.into());
    (
        StatusCode::OK,
        json!({"AccessKeyId":key,"SecretAccessKey":"test-local-secret","Token":"test-local-session","Expiration":"2030-01-01T00:00:00Z"}).to_string(),
    )
}

#[derive(Clone, Default)]
struct S3State {
    requests: RequestLog,
}

async fn s3_refresh_handler(
    State(state): State<S3State>,
    request: Request,
) -> (StatusCode, String) {
    let authorization = request
        .headers()
        .get("authorization")
        .map(|value| value.to_str().unwrap_or_default().to_owned())
        .unwrap_or_default();
    state
        .requests
        .lock()
        .unwrap()
        .push((request.uri().path().to_owned(), Some(authorization.clone())));
    if authorization.contains("Credential=TESTNEWLOCALONLY/") {
        (StatusCode::OK, "value\n42\n".into())
    } else {
        (StatusCode::FORBIDDEN, "denied".into())
    }
}

/// The worker's Pod Identity path (`pod_credentials`), against the real engine:
/// credentials come from the agent using the token file, the engine is locked
/// down exactly as a worker locks it, and a secret replaced with fresh
/// credentials after the lockdown is the one S3 then sees. DuckDB's own
/// credential_chain cannot do that last step once external access is off, which
/// is what broke long-running persistent workers.
async fn aws_credentials() -> anyhow::Result<()> {
    let credentials = CredentialState::default();
    let storage = S3State::default();
    let (credential_port, credential_task) = spawn_server(credentials.clone(), |state, headers| {
        Box::pin(credentials_handler(state, headers))
    })
    .await?;
    let (storage_port, storage_task) = spawn_server(storage.clone(), |state, headers| {
        Box::pin(s3_refresh_handler(state, headers))
    })
    .await?;
    let token_path =
        std::env::temp_dir().join(format!("duckfleet-test-token-{}", uuid::Uuid::new_v4()));
    std::fs::write(&token_path, "test-pod-identity-token")?;
    // pod_credentials reads process environment. These values are fixture-only and
    // no other code mutates this process's environment while set.
    unsafe {
        std::env::set_var(
            "AWS_CONTAINER_CREDENTIALS_FULL_URI",
            format!("http://127.0.0.1:{credential_port}/v1/credentials"),
        );
        std::env::set_var("AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE", &token_path);
        std::env::set_var("AWS_EC2_METADATA_DISABLED", "true");
    }
    let runtime = tokio::runtime::Handle::current();
    let result = tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
        use crate::pod_credentials::{Secret, fetch, secret_sql_at};
        let connection = extensions()?;
        let secrets = [Secret {
            name: "aws_s3",
            scope: "s3://test/lake/".into(),
        }];
        let endpoint = format!("127.0.0.1:{storage_port}");
        let read = |connection: &duckdb::Connection| {
            connection.query_row(
                "SELECT * FROM read_csv('s3://test/lake/fixture.csv')",
                [],
                |row| row.get::<_, i64>(0),
            )
        };
        // In the worker's order: the first secret is installed at startup, before
        // the lockdown. By the time S3 sees these credentials they are stale.
        let old = fetch(&runtime)?;
        anyhow::ensure!(old.key_id() == "TESTOLDLOCALONLY");
        connection.execute_batch(&secret_sql_at(&secrets, "us-east-2", &old, Some(&endpoint)))?;
        connection.execute_batch(&crate::worker::engine_file_access(
            "s3://test/lake/",
            None,
            None,
        ))?;
        anyhow::ensure!(
            read(&connection).is_err(),
            "stale credentials were accepted"
        );
        // Refreshed after the lockdown, as the worker's refresher does.
        let new = fetch(&runtime)?;
        anyhow::ensure!(new.key_id() == "TESTNEWLOCALONLY");
        connection.execute_batch(&secret_sql_at(&secrets, "us-east-2", &new, Some(&endpoint)))?;
        anyhow::ensure!(read(&connection)? == 42);
        let provider = connection.query_row(
            "SELECT provider FROM duckdb_secrets() WHERE name='aws_s3'",
            [],
            |row| row.get::<_, String>(0),
        )?;
        anyhow::ensure!(
            provider == "config",
            "the secret is {provider}, not explicit keys"
        );
        Ok(())
    })
    .await
    .map_err(anyhow::Error::from)
    .and_then(|result| result);
    let requests = credentials.requests.lock().unwrap().clone();
    credential_task.abort();
    storage_task.abort();
    let _ = std::fs::remove_file(token_path);
    result?;
    anyhow::ensure!(
        requests.len() == 2
            && requests
                .iter()
                .all(|value| value.as_deref() == Some("test-pod-identity-token")),
        "the agent was not called with the token file: {requests:?}"
    );
    println!(
        "PASS: pod identity credentials used the token file, and a secret refreshed after the engine lockdown replaced stale ones"
    );
    Ok(())
}

async fn s3_scope_handler(State(state): State<S3State>, request: Request) -> (StatusCode, String) {
    let authorization = request
        .headers()
        .get("authorization")
        .map(|value| value.to_str().unwrap_or_default().to_owned());
    state
        .requests
        .lock()
        .unwrap()
        .push((request.uri().path().to_owned(), authorization));
    (StatusCode::NOT_FOUND, "missing".into())
}

async fn s3_secret_scope() -> anyhow::Result<()> {
    let state = S3State::default();
    let (port, task) = spawn_server(state.clone(), |state, headers| {
        Box::pin(s3_scope_handler(state, headers))
    })
    .await?;
    let connection = extensions()?;
    connection.execute_batch(&format!(
        "CREATE SECRET scoped (TYPE S3, KEY_ID 'test', SECRET 'test', ENDPOINT '127.0.0.1:{port}',
         URL_STYLE 'path', USE_SSL false, REGION 'us-east-1', SCOPE 's3://test/lake/')"
    ))?;
    let _ = connection.execute_batch("SELECT * FROM read_csv('s3://test/lake/fixture.csv')");
    anyhow::ensure!(
        state.requests.lock().unwrap().iter().any(|(path, authorization)| path.starts_with("/test/lake/") && authorization.is_some()),
        "the scoped request was not signed"
    );
    state.requests.lock().unwrap().clear();
    connection.execute_batch(&format!(
        "CREATE SECRET neighbor (TYPE S3, KEY_ID '', SECRET '', ENDPOINT '127.0.0.1:{port}',
         URL_STYLE 'path', USE_SSL false, REGION 'us-east-1', SCOPE 's3://test-other/lake/')"
    ))?;
    let _ = connection.execute_batch("SELECT * FROM read_csv('s3://test-other/lake/fixture.csv')");
    let requests = state.requests.lock().unwrap();
    anyhow::ensure!(
        requests
            .iter()
            .any(|(path, _)| path.starts_with("/test-other/lake/")),
        "neighbor request was not observed"
    );
    anyhow::ensure!(
        requests
            .iter()
            .all(|(_, authorization)| authorization.is_none()),
        "scoped credential was sent to the neighbor"
    );
    anyhow::ensure!(
        !requests.iter().any(|(path, _)| path.starts_with("/test/")),
        "neighbor request used the scoped bucket"
    );
    drop(requests);
    task.abort();
    println!("PASS: scoped S3 credentials were not sent to a prefix-neighbor bucket");
    Ok(())
}

pub async fn run(args: &[String]) -> anyhow::Result<()> {
    match args
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>()
        .as_slice()
    {
        ["--extension-bundle"] => extension_bundle(),
        ["--aws-credentials"] => aws_credentials().await,
        ["--s3-secret-scope"] => s3_secret_scope().await,
        _ => anyhow::bail!(
            "engine-security requires --extension-bundle, --aws-credentials, or --s3-secret-scope"
        ),
    }
}
