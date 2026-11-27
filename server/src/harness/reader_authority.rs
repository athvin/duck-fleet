//! Real reader/writer authority over HTTP and pgwire, with no credential mocks.
use super::{Ctx, request_json, require, sql};
use crate::client::{Client, RemoteConnection};
use anyhow::{Context, Result};
use reqwest::Method;
use serde_json::{Value, json};
use std::time::Duration;
use uuid::Uuid;

struct Principal {
    name: String,
    token: String,
    compute: Value,
}

async fn issue(admin: &Client, tag: &str) -> Result<Principal> {
    let name = format!("reader-{tag}-{}", Uuid::new_v4().simple());
    let issued = request_json(
        admin,
        Method::POST,
        "/v1/credentials",
        Some(&json!({"principal":name})),
    )
    .await?;
    let policy = request_json(
        admin,
        Method::GET,
        &format!("/v1/principals/{name}/policy"),
        None,
    )
    .await?;
    Ok(Principal {
        name,
        token: issued["token"].as_str().context("issued token")?.into(),
        compute: policy["compute"].clone(),
    })
}

async fn grant(admin: &Client, principal: &Principal, role: &str, pool: &str) -> Result<()> {
    let mut compute = principal.compute.clone();
    compute[0]["pool"] = json!(pool);
    request_json(
        admin,
        Method::PUT,
        &format!("/v1/principals/{}/policy", principal.name),
        Some(&json!({
            "roles":[role], "permissions":[], "compute":compute, "disabled":false,
        })),
    )
    .await?;
    Ok(())
}

async fn denied(connection: &RemoteConnection, statement: &str) -> Result<()> {
    require(
        connection.execute(statement, Vec::new()).await.is_err(),
        format!("reader statement was not denied: {statement}"),
    )
}

struct Pg(Option<postgres::Client>);

impl Drop for Pg {
    fn drop(&mut self) {
        if let Some(pg) = self.0.take() {
            tokio::task::spawn_blocking(move || drop(pg));
        }
    }
}

impl Pg {
    async fn connect(ctx: &Ctx, token: &str) -> Result<Self> {
        let host = reqwest::Url::parse(&ctx.url)?
            .host_str()
            .context("gateway hostname")?
            .to_owned();
        let token = token.to_owned();
        let pg = tokio::task::spawn_blocking(move || {
            postgres::Config::new()
                .host(&host)
                .port(5432)
                .user("reader")
                .dbname("lake")
                .password(token)
                .connect_timeout(Duration::from_secs(5))
                .connect(postgres::NoTls)
        })
        .await??;
        Ok(Self(Some(pg)))
    }

    async fn query(
        &mut self,
        statement: &str,
    ) -> Result<Vec<postgres::SimpleQueryMessage>, postgres::Error> {
        let mut pg = self.0.take().expect("test connection exists");
        let statement = statement.to_owned();
        let (pg, result) = tokio::task::spawn_blocking(move || {
            let result = pg.simple_query(&statement);
            (pg, result)
        })
        .await
        .expect("pgwire test task did not panic");
        self.0 = Some(pg);
        result
    }
}

pub async fn checks(ctx: &Ctx) -> Result<()> {
    let admin = ctx.client()?;
    let table = format!("reader_authority_{}", Uuid::new_v4().simple());
    println!("reader authority: opening writer fixture");
    let writer = ctx.open().await?;
    sql(
        &writer,
        &format!("CREATE TABLE {table}(id INTEGER, note VARCHAR)"),
    )
    .await?;
    sql(
        &writer,
        &format!("INSERT INTO {table} VALUES (1, 'approved')"),
    )
    .await?;
    let reader_pool =
        std::env::var("DUCKFLEET_READER_POOL").unwrap_or_else(|_| "small-reader".into());
    let catalog_dsn = std::env::var("DUCKFLEET_READER_DSN").unwrap_or_else(|_| {
        "host=postgres port=5432 dbname=lake user=duckfleet_catalog_reader password=local-lake-reader-password".into()
    });
    let data_path = std::env::var("DUCKFLEET_READER_DATA_PATH")
        .unwrap_or_else(|_| "s3://duckfleet/lake/".into());
    let principal = issue(&admin, "viewer").await?;
    grant(&admin, &principal, "viewer", &reader_pool).await?;

    println!("reader authority: opening viewer");
    let reader =
        RemoteConnection::open(&ctx.url, &principal.token, "reader-http", None, None, 30).await?;
    let rows = sql(&reader, &format!("SELECT id, note FROM {table}")).await?;
    require(
        rows == vec![vec![Some("1".into()), Some("approved".into())]],
        "reader rows differ",
    )?;
    require(
        reader.placement["authority"] == "reader",
        "reader session did not report reader authority",
    )?;
    denied(
        &reader,
        &format!("INSERT INTO {table} VALUES (2, 'denied')"),
    )
    .await?;
    denied(&reader, "CREATE TABLE reader_escape(id INTEGER)").await?;
    denied(
        &reader,
        "CALL ducklake_set_option('lake', 'data_inlining_row_limit', 100)",
    )
    .await?;
    denied(
        &reader,
        "CALL ducklake_cleanup_old_files('lake', cleanup_all => true)",
    )
    .await?;
    denied(
        &reader,
        &format!("COPY (SELECT 1 AS id) TO '{data_path}reader-escape.parquet' (FORMAT PARQUET)"),
    )
    .await?;
    sql(&reader, "SET allow_extension_repositories='forbidden'").await?;
    denied(&reader, "INSTALL fts").await?;
    // Workers disable external access after startup (#185), so SQL cannot
    // attach another catalog or database at all, even read-only with the
    // reader's own credentials: there is no alternate path to write through.
    denied(
        &reader,
        &format!("ATTACH 'ducklake:postgres:{catalog_dsn}' AS alternate_{table} (DATA_PATH '{data_path}', DUCKLAKE_VERSION '1.0', READ_ONLY)"),
    )
    .await?;
    denied(
        &reader,
        &format!("ATTACH 'postgres:{catalog_dsn}' AS direct_{table}"),
    )
    .await?;

    if let Ok(environment) = sql(&reader, "SELECT read_text('/proc/self/environ')").await {
        let text = format!("{environment:?}");
        for secret in [
            "local-lake-catalog-password",
            "local-object-store-password",
            "local-platform-migration-password",
        ] {
            require(
                !text.contains(secret),
                "reader discovered a writer or unrelated credential",
            )?;
        }
    }

    println!("reader authority: HTTP denial checks passed; opening pgwire");
    let mut pg = Pg::connect(ctx, &principal.token).await?;
    pg.query(&format!("SELECT id FROM {table}"))
        .await
        .context("pgwire reader query failed")?;
    require(
        pg.query(&format!("INSERT INTO {table} VALUES (3, 'denied')"))
            .await
            .is_err(),
        "pgwire reader write was not denied",
    )?;

    println!("reader authority: changing viewer to writer");
    grant(&admin, &principal, "analyst", "small").await?;
    denied(&reader, "SELECT 1").await?;
    let promoted = RemoteConnection::open(
        &ctx.url,
        &principal.token,
        "promoted-writer",
        None,
        None,
        30,
    )
    .await?;
    sql(
        &promoted,
        &format!("INSERT INTO {table} VALUES (4, 'writer')"),
    )
    .await?;
    grant(&admin, &principal, "viewer", &reader_pool).await?;
    denied(&promoted, "SELECT 1").await?;

    drop(reader);
    drop(promoted);
    drop(pg);
    sql(&writer, &format!("DROP TABLE {table}")).await?;
    println!(
        "PASS: reader/writer authority holds over HTTP, pgwire, alternate data paths, and role changes"
    );
    Ok(())
}
