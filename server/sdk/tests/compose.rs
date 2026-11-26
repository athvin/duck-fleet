//! Real-stack SDK tests. Run against Compose with:
//!
//! ```sh
//! cargo test -p duckfleet --test compose -- --ignored --test-threads=1
//! ```
//!
//! `DUCKFLEET_URL` and `DUCKFLEET_TOKEN` default to the local development stack.

use arrow_array::{
    Array, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float64Array, Int32Array,
    Int64Array, ListArray, StringArray, StructArray, TimestampMicrosecondArray,
};
use duckfleet::{
    Client, ComputeRequest, ErrorKind, JobRequest, Mode, Param, SessionOptions, Statement,
};
use serde_json::{Value, json};
use std::time::Duration;

fn endpoint() -> (String, String) {
    (
        std::env::var("DUCKFLEET_URL").unwrap_or_else(|_| "http://127.0.0.1:18080".into()),
        std::env::var("DUCKFLEET_TOKEN")
            .unwrap_or_else(|_| "duckfleet-local-development-token".into()),
    )
}

fn client() -> Client {
    let (url, token) = endpoint();
    Client::new(&url, &token).unwrap()
}

fn schema() -> String {
    format!(
        "sdk_it_{}",
        &uuid::Uuid::new_v4().simple().to_string()[..12]
    )
}

async fn admin(method: reqwest::Method, path: &str, body: Option<Value>) -> Value {
    let bytes = client()
        .request(method, path, body.as_ref(), Duration::from_secs(15))
        .await
        .unwrap_or_else(|error| panic!("{path}: {error:?}"));
    serde_json::from_slice(&bytes).unwrap_or(Value::Null)
}

fn count(result: &duckfleet::QueryResult) -> i64 {
    result.rows[0][0].as_deref().unwrap().parse().unwrap()
}

#[tokio::test]
#[ignore = "needs a running DuckFleet stack"]
async fn sessions_keep_temporary_tables_transactions_and_roll_back_on_close() {
    let client = client();
    let schema = schema();
    let mut session = client
        .session(SessionOptions::named("sdk-session"))
        .await
        .unwrap();
    session
        .execute("CREATE TEMP TABLE scratch AS SELECT 41 AS n", Vec::new())
        .await
        .unwrap();
    let result = session
        .query("SELECT n + ? FROM scratch", [1_i64])
        .await
        .unwrap();
    assert_eq!(result.rows[0][0].as_deref(), Some("42"));
    session
        .execute(&format!("CREATE SCHEMA {schema}"), Vec::new())
        .await
        .unwrap();
    session
        .execute(&format!("CREATE TABLE {schema}.t(n BIGINT)"), Vec::new())
        .await
        .unwrap();
    session.execute("BEGIN", Vec::new()).await.unwrap();
    session
        .query(&format!("INSERT INTO {schema}.t VALUES (?)"), [1_i64])
        .await
        .unwrap();
    session.rollback().await.unwrap();
    session.execute("BEGIN", Vec::new()).await.unwrap();
    session
        .query(&format!("INSERT INTO {schema}.t VALUES (?)"), [2_i64])
        .await
        .unwrap();
    session.commit().await.unwrap();
    session.execute("BEGIN", Vec::new()).await.unwrap();
    session
        .query(&format!("INSERT INTO {schema}.t VALUES (?)"), [3_i64])
        .await
        .unwrap();
    session.close().await.unwrap();

    let mut check = client
        .session(SessionOptions::named("sdk-session-check"))
        .await
        .unwrap();
    let rows = check
        .execute(&format!("SELECT n FROM {schema}.t ORDER BY n"), Vec::new())
        .await
        .unwrap();
    assert_eq!(
        rows.rows,
        vec![vec![Some("2".to_owned())]],
        "rollback and close semantics"
    );
    let missing = check
        .execute("SELECT * FROM scratch", Vec::new())
        .await
        .unwrap_err();
    assert_eq!(
        missing.kind(),
        ErrorKind::Sql,
        "temp tables are session-scoped"
    );
    check
        .execute(&format!("DROP SCHEMA {schema} CASCADE"), Vec::new())
        .await
        .unwrap();
    check.close().await.unwrap();
}

#[tokio::test]
#[ignore = "needs a running DuckFleet stack"]
async fn typed_parameters_and_arrow_results_round_trip() {
    let mut session = client()
        .session(SessionOptions::named("sdk-types"))
        .await
        .unwrap();
    let result = session
        .query(
            "SELECT ?::BOOLEAN AS b, ?::BIGINT AS i, ?::DOUBLE AS d, ?::DECIMAL(18,4) AS dec,
                    ?::DATE AS dt, ?::TIMESTAMP AS ts, ?::VARCHAR AS s, ?::INTEGER AS n,
                    '\\xAA\\xBB'::BLOB AS bin, [1, 2, NULL]::INTEGER[] AS l,
                    {'a': 1, 'b': 'x'} AS st, ? AS u",
            [
                Param::from(true),
                Param::from(9_007_199_254_740_993_i64),
                Param::from(0.1_f64),
                Param::from("12345.6789"),
                Param::from("2026-01-02"),
                Param::from("2026-01-02 03:04:05.123456"),
                Param::from("héllo"),
                Param::from(None::<i64>),
                Param::from(u64::MAX),
            ],
        )
        .await
        .unwrap();
    let batch = &result.batches[0];
    let column = |name: &str| batch.column_by_name(name).unwrap().clone();
    assert!(
        column("b")
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap()
            .value(0)
    );
    assert_eq!(
        column("i")
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        9_007_199_254_740_993
    );
    assert_eq!(
        column("d")
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0),
        0.1
    );
    let decimal = column("dec");
    let decimal = decimal.as_any().downcast_ref::<Decimal128Array>().unwrap();
    assert_eq!((decimal.value(0), decimal.scale()), (123_456_789, 4));
    assert_eq!(
        column("dt")
            .as_any()
            .downcast_ref::<Date32Array>()
            .unwrap()
            .value(0),
        20_455
    );
    assert_eq!(
        column("ts")
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap()
            .value(0),
        1_767_323_045_123_456
    );
    assert_eq!(
        column("s")
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "héllo"
    );
    let null = column("n");
    assert!(
        null.as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .is_null(0)
    );
    assert_eq!(
        column("bin")
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap()
            .value(0),
        &[0xAA, 0xBB]
    );
    let list = column("l");
    let list = list.as_any().downcast_ref::<ListArray>().unwrap().value(0);
    let list = list.as_any().downcast_ref::<Int32Array>().unwrap();
    assert_eq!((list.len(), list.value(1), list.is_null(2)), (3, 2, true));
    let structure = column("st");
    let structure = structure.as_any().downcast_ref::<StructArray>().unwrap();
    assert_eq!(structure.num_columns(), 2);
    assert_eq!(
        result.rows[0][11].as_deref(),
        Some(u64::MAX.to_string().as_str())
    );
    let refused = session
        .query("SELECT ?", [Param::from(f64::INFINITY)])
        .await
        .unwrap_err();
    assert_eq!(refused.kind(), ErrorKind::InvalidRequest);
    session.close().await.unwrap();
}

#[tokio::test]
#[ignore = "needs a running DuckFleet stack"]
async fn failures_map_to_stable_variants_without_replay() {
    let client = client();
    let schema = schema();
    let mut session = client
        .session(SessionOptions::named("sdk-failures"))
        .await
        .unwrap();
    let sql = session
        .execute("SELECT * FROM missing_table", Vec::new())
        .await
        .unwrap_err();
    assert_eq!(sql.kind(), ErrorKind::Sql);
    assert!(sql.request_id().is_some());

    let busy_sql = "SELECT sum(sin(i)) FROM range(100000000000) AS source(i)";
    let timeout = session
        .query_with(
            busy_sql,
            Vec::<Param>::new(),
            Statement {
                timeout: Duration::from_secs(1),
                ..Statement::default()
            },
        )
        .await
        .unwrap_err();
    assert_eq!(timeout.kind(), ErrorKind::Timeout, "{timeout:?}");

    let (canceled, cancel) = tokio::join!(session.query(busy_sql, Vec::<Param>::new()), async {
        tokio::time::sleep(Duration::from_secs(2)).await;
        session.cancel().await
    });
    cancel.unwrap();
    assert_eq!(canceled.unwrap_err().kind(), ErrorKind::Canceled);

    let (first, second) = tokio::join!(session.query(busy_sql, Vec::<Param>::new()), async {
        tokio::time::sleep(Duration::from_millis(500)).await;
        let result = session.query("SELECT 1", Vec::<Param>::new()).await;
        session.cancel().await.unwrap();
        result
    });
    assert!(first.is_err());
    assert_eq!(second.unwrap_err().kind(), ErrorKind::SessionBusy);

    session
        .execute(&format!("CREATE SCHEMA {schema}"), Vec::new())
        .await
        .unwrap();
    session
        .execute(
            &format!("CREATE TABLE {schema}.t AS SELECT 0 AS v"),
            Vec::new(),
        )
        .await
        .unwrap();
    let insert = format!("INSERT INTO {schema}.t VALUES (?)");
    let statement = Statement::default();
    session
        .query_with(&insert, [7_i64], statement.clone())
        .await
        .unwrap();
    let duplicate = session
        .query_with(&insert, [7_i64], statement)
        .await
        .unwrap_err();
    assert_eq!(duplicate.kind(), ErrorKind::DuplicateRequest);
    let sevens = session
        .execute(
            &format!("SELECT count(*) FROM {schema}.t WHERE v = 7"),
            Vec::new(),
        )
        .await
        .unwrap();
    assert_eq!(
        count(&sevens),
        1,
        "a duplicate request ID never re-executes"
    );

    let mut other = client
        .session(SessionOptions::named("sdk-conflict"))
        .await
        .unwrap();
    let update = format!("UPDATE {schema}.t SET v = v + 1");
    session.execute("BEGIN", Vec::new()).await.unwrap();
    other.execute("BEGIN", Vec::new()).await.unwrap();
    session.execute(&update, Vec::new()).await.unwrap();
    other.execute(&update, Vec::new()).await.unwrap();
    session.commit().await.unwrap();
    let conflict = other.commit().await.unwrap_err();
    assert_eq!(conflict.kind(), ErrorKind::Conflict, "{conflict:?}");
    let _ = other.rollback().await;
    other.close().await.unwrap();

    let position = admin(reqwest::Method::GET, "/v1/budget", None).await;
    admin(
        reqwest::Method::PUT,
        "/v1/budget",
        Some(json!({"prepaid_micro_usd": 0})),
    )
    .await;
    let refused = session.execute("SELECT 1", Vec::new()).await;
    admin(
        reqwest::Method::PUT,
        "/v1/budget",
        Some(json!({"prepaid_micro_usd": position["prepaid_micro_usd"]})),
    )
    .await;
    let refused = refused.unwrap_err();
    assert_eq!(refused.kind(), ErrorKind::BudgetRefused, "{refused:?}");
    assert!(refused.retry_safe());

    session
        .execute(&format!("DROP SCHEMA {schema} CASCADE"), Vec::new())
        .await
        .unwrap();
    let path = session.path();
    session.close().await.unwrap();
    let lost = client
        .request(
            reqwest::Method::POST,
            &format!("{path}/queries"),
            Some(&json!({"request_id": uuid::Uuid::new_v4(), "sql": "SELECT 1"})),
            Duration::from_secs(15),
        )
        .await
        .unwrap_err();
    assert_eq!(lost.kind(), ErrorKind::SessionLost);
}

#[tokio::test]
#[ignore = "needs a running DuckFleet stack"]
async fn compute_selection_and_independent_concurrent_sessions() {
    let client = client();
    let options = SessionOptions::named("sdk-compute").compute(ComputeRequest {
        mode: Some(Mode::Persistent),
        pool: Some("small".into()),
        ..ComputeRequest::default()
    });
    let (mut left, mut right) =
        tokio::join!(client.session(options.clone()), client.session(options));
    let (left, right) = (left.as_mut().unwrap(), right.as_mut().unwrap());
    assert_eq!(left.placement["pool"], "small", "{}", left.placement);
    left.execute("CREATE TEMP TABLE mine AS SELECT 'left' AS who", Vec::new())
        .await
        .unwrap();
    right
        .execute(
            "CREATE TEMP TABLE mine AS SELECT 'right' AS who",
            Vec::new(),
        )
        .await
        .unwrap();
    let (l, r) = tokio::join!(
        left.execute("SELECT who FROM mine", Vec::new()),
        right.execute("SELECT who FROM mine", Vec::new())
    );
    assert_eq!(l.unwrap().rows[0][0].as_deref(), Some("left"));
    assert_eq!(r.unwrap().rows[0][0].as_deref(), Some("right"));
    left.close().await.unwrap();
    right.close().await.unwrap();
}

#[tokio::test]
#[ignore = "needs a running DuckFleet stack"]
async fn durable_jobs_wait_cancel_download_and_respect_result_read() {
    let client = client();
    let request: JobRequest = serde_json::from_value(json!({
        "request_id": uuid::Uuid::new_v4(),
        "sql": "SELECT i, repeat('x', 100) AS payload FROM range(200000) t(i)",
    }))
    .unwrap();
    let id = request.request_id;
    client.submit_job(&request).await.unwrap();
    let status = client
        .wait_job(id, Duration::from_millis(500), Duration::from_secs(120))
        .await
        .unwrap();
    assert_eq!(status["status"], "succeeded", "{status}");
    let directory = std::env::temp_dir().join(format!("duckfleet-sdk-{id}"));
    let manifest = client
        .download_job_result(id, &directory, false)
        .await
        .unwrap();
    assert_eq!(manifest.rows, 200_000);
    assert!(manifest.chunks.len() > 1, "the fixture spans chunks");
    let mut rows = 0;
    for chunk in &manifest.chunks {
        let file =
            std::fs::File::open(directory.join(format!("{:06}.arrow", chunk.chunk_id))).unwrap();
        for batch in arrow_ipc::reader::StreamReader::try_new(file, None).unwrap() {
            rows += batch.unwrap().num_rows();
        }
    }
    assert_eq!(rows, 200_000);
    std::fs::remove_file(directory.join("000001.arrow")).unwrap();
    let resumed = client
        .download_job_result(id, &directory, true)
        .await
        .unwrap();
    assert_eq!(resumed.generation, manifest.generation);
    std::fs::remove_dir_all(&directory).unwrap();

    let principal = format!(
        "sdk-other-{}",
        &uuid::Uuid::new_v4().simple().to_string()[..8]
    );
    let issued = admin(
        reqwest::Method::POST,
        "/v1/credentials",
        Some(json!({"principal": principal})),
    )
    .await;
    let (url, _) = endpoint();
    let other = Client::new(&url, issued["token"].as_str().unwrap()).unwrap();
    let denied = other.job_result(id).await.unwrap_err();
    assert!(
        matches!(denied.kind(), ErrorKind::NotFound | ErrorKind::Forbidden),
        "{denied:?}"
    );
    admin(
        reqwest::Method::DELETE,
        &format!(
            "/v1/credentials/{}",
            issued["credential_id"].as_str().unwrap()
        ),
        None,
    )
    .await;

    let long: JobRequest = serde_json::from_value(json!({
        "request_id": uuid::Uuid::new_v4(),
        "sql": "SELECT sum(sin(i)) FROM range(100000000000) AS source(i)",
    }))
    .unwrap();
    client.submit_job(&long).await.unwrap();
    tokio::time::sleep(Duration::from_secs(2)).await;
    client.cancel_job(long.request_id).await.unwrap();
    let status = client
        .wait_job(
            long.request_id,
            Duration::from_millis(500),
            Duration::from_secs(60),
        )
        .await
        .unwrap();
    assert_eq!(status["status"], "canceled", "{status}");
}
