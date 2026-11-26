use crate::util;
use anyhow::{Result, bail, ensure};
use futures_util::TryStreamExt;
use object_store::{
    ClientOptions, ObjectStore, ObjectStoreExt, PutPayload, RetryConfig, aws::AmazonS3Builder,
    path::Path as ObjectPath,
};
use std::{sync::Arc, time::Duration};

fn value(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.into())
}

fn store(access_key: &str, secret_key: &str) -> Result<Arc<dyn ObjectStore>> {
    let options = ClientOptions::new()
        .with_allow_http(true)
        .with_connect_timeout(Duration::from_secs(2))
        .with_timeout(Duration::from_secs(5));
    Ok(Arc::new(
        AmazonS3Builder::new()
            .with_bucket_name("duckfleet")
            .with_region("us-east-1")
            .with_virtual_hosted_style_request(false)
            .with_endpoint("http://object-store:9000")
            .with_access_key_id(access_key)
            .with_secret_access_key(secret_key)
            .with_client_options(options)
            .with_retry(RetryConfig {
                max_retries: 0,
                retry_timeout: Duration::from_secs(5),
                ..Default::default()
            })
            .build()?,
    ))
}

fn rc(args: &[String], exists_ok: bool) -> Result<()> {
    let output = util::run_status(
        ["rc"]
            .into_iter()
            .map(String::from)
            .chain(args.iter().cloned())
            .collect::<Vec<_>>(),
    )?;
    if !output.status.success() {
        let message = format!(
            "{}{}",
            util::stdout_text(&output),
            util::stderr_text(&output)
        );
        if !(exists_ok && message.to_ascii_lowercase().contains("exist")) {
            bail!("{message}");
        }
    }
    Ok(())
}

fn provision_reader() -> Result<()> {
    let access = value("DUCKFLEET_OBJECT_ACCESS_KEY", "DUCKFLEETLOCAL");
    let secret = value("DUCKFLEET_OBJECT_SECRET_KEY", "local-object-store-password");
    let reader_secret = value(
        "DUCKFLEET_READER_OBJECT_SECRET_KEY",
        "local-lake-reader-object-password",
    );
    rc(
        &[
            "alias".into(),
            "set".into(),
            "local".into(),
            "http://object-store:9000".into(),
            access,
            secret,
        ],
        false,
    )?;
    rc(
        &[
            "admin".into(),
            "user".into(),
            "add".into(),
            "local".into(),
            "DUCKFLEETREADER".into(),
            reader_secret,
        ],
        true,
    )?;
    let policy = serde_json::json!({
        "Version":"2012-10-17",
        "Statement":[
            {"Effect":"Allow","Action":["s3:GetObject"],"Resource":"arn:aws:s3:::duckfleet/lake/*"},
            {"Effect":"Allow","Action":["s3:ListBucket"],"Resource":"arn:aws:s3:::duckfleet","Condition":{"StringLike":{"s3:prefix":["lake","lake/*"]}}}
        ]
    });
    let created = util::run_with_stdin_status(
        [
            "rc",
            "admin",
            "policy",
            "create",
            "local",
            "duckfleet-lake-reader",
            "/dev/stdin",
        ],
        &policy.to_string(),
    )?;
    if !created.status.success() {
        let message = format!(
            "{}{}",
            util::stdout_text(&created),
            util::stderr_text(&created)
        );
        ensure!(
            message.to_ascii_lowercase().contains("exist"),
            "reader policy failed: {message}"
        );
    }
    rc(
        &[
            "admin".into(),
            "policy".into(),
            "attach".into(),
            "local".into(),
            "duckfleet-lake-reader".into(),
            "--user".into(),
            "DUCKFLEETREADER".into(),
        ],
        false,
    )
}

fn denied(error: impl std::fmt::Display) -> Result<()> {
    let message = error.to_string();
    ensure!(
        message.contains("AccessDenied") || message.contains("403"),
        "expected access denial, got: {message}"
    );
    Ok(())
}

pub async fn run(args: Vec<String>) -> Result<()> {
    let verify = args == ["--verify"];
    if !verify && !args.is_empty() {
        bail!("usage: object-store [--verify]");
    }
    let access = value("DUCKFLEET_OBJECT_ACCESS_KEY", "DUCKFLEETLOCAL");
    let secret = value("DUCKFLEET_OBJECT_SECRET_KEY", "local-object-store-password");
    let reader_secret = value(
        "DUCKFLEET_READER_OBJECT_SECRET_KEY",
        "local-lake-reader-object-password",
    );
    for attempt in 0..60 {
        let output = util::run_status([
            "rc",
            "alias",
            "set",
            "local",
            "http://object-store:9000",
            &access,
            &secret,
        ])?;
        if output.status.success()
            && util::run_status(["rc", "ls", "local"]).is_ok_and(|output| output.status.success())
        {
            break;
        }
        if attempt == 59 {
            bail!("object store did not become ready");
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    if !verify {
        rc(
            &[
                "mb".into(),
                "--ignore-existing".into(),
                "local/duckfleet".into(),
            ],
            true,
        )?;
        provision_reader()?;
        println!("Local bucket and scoped reader ready");
        return Ok(());
    }
    let admin = store(&access, &secret)?;
    let reader = store("DUCKFLEETREADER", &reader_secret)?;
    let prefix = ObjectPath::from("lake");
    let files: Vec<_> = admin
        .list(Some(&prefix))
        .try_filter_map(|meta| async move {
            Ok((meta.location.as_ref().ends_with(".parquet")).then_some(meta))
        })
        .try_collect()
        .await?;
    ensure!(
        !files.is_empty(),
        "no Parquet objects in DuckLake data prefix"
    );
    for file in files.iter().take(3) {
        let body = reader.get_range(&file.location, 0..4).await?;
        ensure!(
            body.as_ref() == b"PAR1",
            "object is not Parquet: {}",
            file.location
        );
    }
    match reader
        .put(
            &ObjectPath::from("lake/reader-write-probe"),
            PutPayload::from("denied"),
        )
        .await
    {
        Err(error) => denied(error)?,
        Ok(_) => bail!("reader object-store write was not denied"),
    }
    match reader
        .delete(&ObjectPath::from("lake/reader-write-probe"))
        .await
    {
        Err(error) => denied(error)?,
        Ok(_) => bail!("reader object-store delete was not denied"),
    }
    rc(
        &[
            "alias".into(),
            "set".into(),
            "reader".into(),
            "http://object-store:9000".into(),
            "DUCKFLEETREADER".into(),
            reader_secret,
        ],
        false,
    )?;
    let output = util::run_status(["rc", "mb", "reader/duckfleet-reader-escalation"])?;
    if output.status.success() {
        bail!("reader bucket creation was not denied");
    }
    denied(format!(
        "{}{}",
        util::stdout_text(&output),
        util::stderr_text(&output)
    ))?;
    println!(
        "PASS: {} Parquet objects in S3; reader reads and write denials verified",
        files.len()
    );
    Ok(())
}
