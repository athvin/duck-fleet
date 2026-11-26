//! `DUCKFLEET_URL=... DUCKFLEET_TOKEN=... cargo run -p duckfleet --example query`
use duckfleet::{Client, Param, SessionOptions};

#[tokio::main]
async fn main() -> Result<(), duckfleet::Error> {
    let url = std::env::var("DUCKFLEET_URL").unwrap_or_else(|_| "http://127.0.0.1:18080".into());
    let token = std::env::var("DUCKFLEET_TOKEN")
        .unwrap_or_else(|_| "duckfleet-local-development-token".into());
    let client = Client::new(&url, &token)?;
    let mut session = client.session(SessionOptions::named("sdk-example")).await?;
    let result = session
        .query(
            "SELECT ?::BIGINT * 2 AS doubled, ?::DECIMAL(10,2) AS price",
            [Param::from(21_i64), Param::from("9.99")],
        )
        .await?;
    println!("{:?}", result.batches);
    session.close().await?;
    Ok(())
}
