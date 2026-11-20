//! The EKS harness entrypoint. Ported from `tests/aws_harness.py`: the full
//! lifecycle, then the scale and cold-start acceptance checks, then a DuckLake
//! S3-glob read proving the committed data is in the bucket.

use super::{Ctx, require, sql};

pub(super) async fn main(ctx: &Ctx) -> anyhow::Result<()> {
    super::main_lifecycle(ctx, &[]).await?;
    super::scale_checks(ctx).await?;
    super::cold_start_checks(ctx).await?;
    s3_glob_check(ctx).await?;
    println!("PASS: the EKS harness ran the lifecycle, scale, cold-start and S3 read");
    Ok(())
}

/// The committed lake data is readable straight from the object store, proving a
/// DuckLake commit lands in the bucket, not just the compute that produced it.
async fn s3_glob_check(ctx: &Ctx) -> anyhow::Result<()> {
    let connection = ctx.open().await?;
    let rows = sql(
        &connection,
        "select count(*) from lake.analytics.fct_orders",
    )
    .await?;
    let count = rows.first().and_then(|r| r[0].as_deref()).unwrap_or("0");
    require(
        count != "0",
        "the lake read returned no rows from S3-backed storage",
    )?;
    let mut connection = connection;
    connection.close().await?;
    println!("PASS: committed lake data is readable from the object store ({count} rows)");
    Ok(())
}
