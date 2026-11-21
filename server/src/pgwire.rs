//! A Postgres wire-protocol front over the ordinary execution machinery.
//!
//! The decision this implements (recorded 2026-11-16): HTTP + Arrow stays the
//! native data plane, and this is a **compatibility layer, not a rewrite** — a
//! TCP listener that speaks enough of the v3 protocol for `psql` and the
//! simple-protocol path every driver and BI tool is built on. Each connection
//! authenticates with a platform credential as its password and is pinned to one
//! ordinary worker session, so history, principal attribution, metering, and
//! failure classification all hold with no code of their own here.
//!
//! The unsupported surface refuses rather than misbehaves: COPY, LISTEN/NOTIFY,
//! cursors, CancelRequest, and extended-protocol bind parameters (interpolate
//! client-side, as psql and psycopg2 already do). Results travel in text format.
use anyhow::{Context, Result, bail};
use arrow_array::{Array, RecordBatch, cast::AsArray, types};
use arrow_schema::{DataType, TimeUnit};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufStream};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Semaphore;
use uuid::Uuid;

use crate::{ApiError, credentials::Identity};

/// Separate bound on wire sockets, including unauthenticated and idle clients.
/// Backend sessions also acquire the gateway's shared HTTP/pgwire/platform permit.
/// Without a socket bound, every accepted
/// socket spawned an unbounded task, so a client that connected and sent nothing
/// parked a task forever.
const MAX_CONNECTIONS: usize = 64;

/// How long a connection has to get through the startup handshake. A peer that
/// opens a socket and never speaks must not hold a connection slot indefinitely.
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
/// Revalidate idle connections too, including a client parked halfway through a
/// message or waiting to Sync after an extended-protocol error.
const AUTH_CHECK_INTERVAL: Duration = Duration::from_secs(5);
const WRITE_TIMEOUT: Duration = Duration::from_secs(5);
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(1);

const PROTOCOL_V3: i32 = 196_608;
const SSL_REQUEST: i32 = 80_877_103;
const CANCEL_REQUEST: i32 = 80_877_102;

/// What the gateway hands this module: just enough to authenticate a token and
/// run statements through a worker session under a named principal.
pub(crate) trait Platform: Send + Sync + 'static {
    fn authenticate(
        &self,
        token: String,
    ) -> impl std::future::Future<Output = std::result::Result<Identity, ApiError>> + Send;
    fn revalidate(
        &self,
        identity: &Identity,
    ) -> impl std::future::Future<Output = std::result::Result<(), ApiError>> + Send;
    fn open_session(
        &self,
        identity: &Identity,
    ) -> impl std::future::Future<Output = std::result::Result<Session, ApiError>> + Send;
    fn query(
        &self,
        identity: &Identity,
        session: &Session,
        request_id: Uuid,
        sql: String,
    ) -> impl std::future::Future<Output = QueryOutcome> + Send;
    fn close_session(&self, session: Session) -> impl std::future::Future<Output = ()> + Send;
    fn draining(&self) -> bool;
}

pub(crate) struct Session {
    pub endpoint: String,
    pub session_id: String,
}

pub(crate) enum QueryOutcome {
    /// Arrow IPC bytes from the worker.
    Rows(Vec<u8>),
    /// A classified platform failure: kind and message, already scrubbed.
    Refused { kind: String, message: String },
}

pub(crate) async fn serve<P: Platform>(platform: Arc<P>, listener: TcpListener) {
    let connections = Arc::new(Semaphore::new(MAX_CONNECTIONS));
    loop {
        match listener.accept().await {
            Ok((socket, _)) => {
                // A permit per connection, held for its whole life. When the cap is
                // reached the socket is dropped at once rather than queued, so a flood
                // of idle connections cannot grow an unbounded pile of parked tasks.
                let Ok(permit) = connections.clone().try_acquire_owned() else {
                    tracing::warn!("pgwire connection limit reached; refusing a connection");
                    drop(socket);
                    continue;
                };
                let platform = platform.clone();
                tokio::spawn(async move {
                    let _permit = permit;
                    if let Err(error) = connection(platform, socket).await {
                        tracing::debug!(error = format!("{error:#}"), "pgwire connection ended");
                    }
                });
            }
            Err(error) => {
                tracing::warn!(%error, "pgwire accept failed");
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }
}

async fn connection<P: Platform>(platform: Arc<P>, socket: TcpStream) -> Result<()> {
    let mut stream = BufStream::new(socket);
    // Only the startup exchange is bounded, not the serving loop beyond it: a peer
    // that opens a socket and then says nothing used to park a connection slot until
    // the process stopped, but an authenticated session may legitimately sit idle
    // between statements.
    let startup = tokio::time::timeout(HANDSHAKE_TIMEOUT, read_startup(&mut stream))
        .await
        .context("pgwire startup timed out")??;
    let outcome = match startup {
        // SSLRequest answered, CancelRequest dropped (no cancellation path over this
        // front): both end the connection without a session.
        None => Ok(()),
        Some(rest) => established(platform, &mut stream, &rest).await,
    };
    graceful_shutdown(&mut stream).await;
    outcome
}

/// Send FIN after the already flushed outcome, then discard pending input. A
/// terminal auth check can run before a pipelined statement is consumed; simply
/// dropping a socket with unread receive data can send RST and erase the prior
/// statement's successfully delivered result. No discarded bytes are executed.
/// Session cleanup has already run, and a silent or flooding peer cannot retain
/// the connection beyond this short shutdown budget.
async fn graceful_shutdown(stream: &mut BufStream<TcpStream>) {
    let _ = tokio::time::timeout(SHUTDOWN_TIMEOUT, async {
        stream.shutdown().await?;
        let mut discarded = [0_u8; 4096];
        while stream.read(&mut discarded).await? != 0 {
            tokio::task::yield_now().await;
        }
        Ok::<(), std::io::Error>(())
    })
    .await;
}

/// Reads the startup phase, returning the startup packet body once a v3 client
/// speaks it, or `None` when the exchange ends without a session (an SSLRequest the
/// client does not follow up, or a CancelRequest).
async fn read_startup(stream: &mut BufStream<TcpStream>) -> Result<Option<Vec<u8>>> {
    // SSLRequests are answered 'N' (this listener is in-cluster plain TCP; transport
    // security is the exposure layer's concern), and a client may retry the startup
    // message afterwards.
    loop {
        let length = stream.read_i32().await.context("startup length")?;
        anyhow::ensure!((8..=10_000).contains(&length), "malformed startup length");
        let code = stream.read_i32().await.context("startup code")?;
        match code {
            SSL_REQUEST => {
                stream.write_all(b"N").await?;
                flush(stream).await?;
            }
            CANCEL_REQUEST => return Ok(None),
            PROTOCOL_V3 => {
                let mut rest = vec![0; usize::try_from(length - 8)?];
                stream.read_exact(&mut rest).await?;
                return Ok(Some(rest));
            }
            _ => bail!("unsupported protocol {code}"),
        }
    }
}

async fn established<P: Platform>(
    platform: Arc<P>,
    stream: &mut BufStream<TcpStream>,
    startup: &[u8],
) -> Result<()> {
    let mut user = String::new();
    for pair in startup.split(|b| *b == 0).collect::<Vec<_>>().chunks(2) {
        if let [key, _] = pair
            && matches!(*key, b"pool" | b"duckfleet_pool")
        {
            error_response(
                stream,
                "0A000",
                "pgwire uses the configured default pool; explicit pool selection is unsupported",
            )
            .await?;
            return Ok(());
        }
        if let [key, value] = pair
            && *key == b"user"
        {
            user = String::from_utf8_lossy(value).into_owned();
        }
    }
    if platform.draining() {
        error_response(stream, "57P01", "the gateway is shutting down").await?;
        return Ok(());
    }
    // The password is a platform credential: the bootstrap token or one issued
    // through /v1/credentials. The startup "user" is advisory — identity comes
    // from the credential, exactly as it does over HTTP.
    write_message(stream, b'R', &3_i32.to_be_bytes()).await?;
    flush(stream).await?;
    // Bounded like the startup read above: a peer that is asked for a password and
    // never sends one must not hold its connection slot open indefinitely.
    let (kind, body) = tokio::time::timeout(HANDSHAKE_TIMEOUT, read_message(stream))
        .await
        .context("pgwire password read timed out")??;
    anyhow::ensure!(kind == b'p', "expected a password message");
    let token = String::from_utf8_lossy(body.split(|b| *b == 0).next().unwrap_or(&[])).into_owned();
    let identity = match platform.authenticate(token).await {
        Ok(identity) => identity,
        Err(error) => {
            return refuse_access(stream, error).await;
        }
    };
    if !user.is_empty() && user != identity.principal {
        tracing::debug!(
            user,
            principal = identity.principal,
            "pgwire startup user differs from credential"
        );
    }
    write_message(stream, b'R', &0_i32.to_be_bytes()).await?;
    for (name, value) in [
        ("server_version", "15.0 (DuckFleet)"),
        ("client_encoding", "UTF8"),
        ("DateStyle", "ISO"),
        ("integer_datetimes", "on"),
    ] {
        let mut body = Vec::new();
        body.extend_from_slice(name.as_bytes());
        body.push(0);
        body.extend_from_slice(value.as_bytes());
        body.push(0);
        write_message(stream, b'S', &body).await?;
    }
    let mut key_data = Vec::new();
    key_data.extend_from_slice(&1_i32.to_be_bytes());
    key_data.extend_from_slice(&1_i32.to_be_bytes());
    write_message(stream, b'K', &key_data).await?;
    ready(stream).await?;

    // The session opens on the first statement, so a client that connects and
    // leaves costs nothing; from then on the connection and the session live and
    // die together, which is the pinning the transaction semantics need.
    let mut session: Option<Session> = None;
    // The unnamed prepared statement, for the parameterless extended protocol.
    let mut prepared: Option<String> = None;
    let outcome = connection_loop(
        platform.as_ref(),
        stream,
        &identity,
        &mut session,
        &mut prepared,
    )
    .await;
    if let Some(open) = session {
        platform.close_session(open).await;
    }
    outcome
}

async fn connection_loop<P: Platform>(
    platform: &P,
    stream: &mut BufStream<TcpStream>,
    identity: &Identity,
    session: &mut Option<Session>,
    prepared: &mut Option<String>,
) -> Result<()> {
    let mut auth_checks = tokio::time::interval_at(
        tokio::time::Instant::now() + AUTH_CHECK_INTERVAL,
        AUTH_CHECK_INTERVAL,
    );
    auth_checks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        let Some((kind, body)) =
            read_authenticated(platform, stream, identity, &mut auth_checks).await?
        else {
            return Ok(());
        };
        match kind {
            b'X' => return Ok(()),
            b'Q' => {
                let sql = String::from_utf8_lossy(body.split(|b| *b == 0).next().unwrap_or(&[]))
                    .into_owned();
                if sql.trim().is_empty() {
                    write_message(stream, b'I', &[]).await?;
                } else {
                    run_statement(platform, stream, identity, session, &sql).await?;
                }
                ready(stream).await?;
            }
            b'P' => {
                // Parse: name, query, parameter type count. Only the unnamed
                // statement, only without parameters.
                let mut parts = body.split(|b| *b == 0);
                let name = parts.next().unwrap_or(&[]);
                let query = String::from_utf8_lossy(parts.next().unwrap_or(&[])).into_owned();
                if !name.is_empty() {
                    error_response(
                        stream,
                        "0A000",
                        "named prepared statements are not supported",
                    )
                    .await?;
                    sync_to_ready(platform, stream, identity, &mut auth_checks).await?;
                    continue;
                }
                *prepared = Some(query);
                write_message(stream, b'1', &[]).await?;
            }
            b'B' => {
                // Bind: refuse actual parameters; a zero-parameter bind completes.
                if bind_has_parameters(&body) {
                    error_response(
                        stream,
                        "0A000",
                        "bind parameters are not supported; interpolate client-side \
                         as psql and psycopg2 do",
                    )
                    .await?;
                    sync_to_ready(platform, stream, identity, &mut auth_checks).await?;
                    continue;
                }
                write_message(stream, b'2', &[]).await?;
            }
            b'D' => {
                // Describe: the row shape is only known after execution here, and
                // NoData keeps simple drivers moving; the real RowDescription
                // precedes the rows at Execute.
                write_message(stream, b'n', &[]).await?;
            }
            b'E' => match prepared.take() {
                Some(sql) if !sql.trim().is_empty() => {
                    run_statement(platform, stream, identity, session, &sql).await?;
                }
                Some(_) => write_message(stream, b'I', &[]).await?,
                None => {
                    error_response(stream, "26000", "no statement has been parsed").await?;
                }
            },
            b'S' => ready(stream).await?,
            b'H' => flush(stream).await?,
            b'd' | b'c' | b'f' => {
                error_response(stream, "0A000", "COPY is not supported").await?;
                ready(stream).await?;
            }
            other => {
                error_response(
                    stream,
                    "0A000",
                    &format!("unsupported protocol message {:?}", other as char),
                )
                .await?;
                ready(stream).await?;
            }
        }
    }
}

/// True when a Bind message carries any parameter values.
fn bind_has_parameters(body: &[u8]) -> bool {
    // portal name, statement name, then i16 format-code count and codes, then the
    // i16 parameter count this cares about.
    let mut cursor = 0;
    for _ in 0..2 {
        while cursor < body.len() && body[cursor] != 0 {
            cursor += 1;
        }
        cursor += 1;
    }
    let formats = i16::from_be_bytes(match body.get(cursor..cursor + 2) {
        Some(bytes) => bytes.try_into().unwrap_or([0, 0]),
        None => return false,
    });
    cursor += 2 + usize::try_from(formats.max(0)).unwrap_or(0) * 2;
    i16::from_be_bytes(match body.get(cursor..cursor + 2) {
        Some(bytes) => bytes.try_into().unwrap_or([0, 0]),
        None => return false,
    }) > 0
}

async fn run_statement<P: Platform>(
    platform: &P,
    stream: &mut BufStream<TcpStream>,
    identity: &Identity,
    session: &mut Option<Session>,
    sql: &str,
) -> Result<()> {
    // Validate at the statement boundary even if the next periodic check is not
    // due. In particular a successful handshake never authorizes a later lazy
    // session open after that same credential has been revoked.
    if let Err(error) = platform.revalidate(identity).await {
        return refuse_access(stream, error).await;
    }
    if session.is_none() {
        match platform.open_session(identity).await {
            Ok(opened) => *session = Some(opened),
            Err(error) => {
                if matches!(error.0.kind, "unauthorized" | "forbidden") {
                    return refuse_access(stream, error).await;
                }
                error_response(
                    stream,
                    sqlstate(error.0.kind),
                    &format!("could not open a session: {} [{}]", error.1, error.0.kind),
                )
                .await?;
                return Ok(());
            }
        }
    }
    // Opening may wait for capacity. The credential must still be valid when
    // execution is about to begin, and the outer connection owns cleanup if not.
    if let Err(error) = platform.revalidate(identity).await {
        return refuse_access(stream, error).await;
    }
    let open = session.as_ref().expect("session was just opened");
    let request_id = Uuid::new_v4();
    // The caller must know the ID while this synchronous operation is waiting.
    // A notice describes submission, never successful durable acceptance.
    submitted_notice(stream, request_id).await?;
    match platform
        .query(identity, open, request_id, sql.to_owned())
        .await
    {
        QueryOutcome::Rows(arrow) => {
            tokio::time::timeout(WRITE_TIMEOUT, write_arrow(stream, sql, &arrow))
                .await
                .context("pgwire result write timed out")?
        }
        QueryOutcome::Refused { kind, message } => {
            let message =
                format!("{message}; request_id={request_id}; status=/v1/requests/{request_id}");
            if matches!(kind.as_str(), "unauthorized" | "forbidden") {
                // A final gateway-side check may catch a revocation that raced
                // this statement boundary. Close the session on this path too.
                tokio::time::timeout(
                    WRITE_TIMEOUT,
                    error_response(stream, sqlstate(&kind), &format!("{message} [{kind}]")),
                )
                .await
                .context("pgwire refusal write timed out")??;
                bail!("pgwire statement access refused [{kind}]");
            }
            error_response(stream, sqlstate(&kind), &format!("{message} [{kind}]")).await
        }
    }
}

/// Classified platform kinds, spelled as SQLSTATEs a driver can act on.
fn sqlstate(kind: &str) -> &'static str {
    match kind {
        "sql_error" => "42601",
        "timeout" | "queue_timeout" | "canceled" | "budget_exceeded" => "57014",
        "unauthorized" | "forbidden" => "28P01",
        "unknown_session" | "session_lost" | "queue_drained" | "draining" | "starting"
        | "unavailable" => "57P01",
        "capacity" | "queue_capacity" | "session_busy" | "budget_exhausted" | "budget_job_cost" => {
            "53300"
        }
        "transaction_conflict" => "40001",
        _ => "XX000",
    }
}

async fn write_arrow(stream: &mut BufStream<TcpStream>, sql: &str, arrow: &[u8]) -> Result<()> {
    let reader = arrow_ipc::reader::StreamReader::try_new(std::io::Cursor::new(arrow), None)
        .context("arrow stream")?;
    let schema = reader.schema();
    let mut description = Vec::new();
    description.extend_from_slice(&i16::try_from(schema.fields().len())?.to_be_bytes());
    for field in schema.fields() {
        description.extend_from_slice(field.name().as_bytes());
        description.push(0);
        description.extend_from_slice(&0_i32.to_be_bytes()); // table oid
        description.extend_from_slice(&0_i16.to_be_bytes()); // column number
        description.extend_from_slice(&type_oid(field.data_type()).to_be_bytes());
        description.extend_from_slice(&(-1_i16).to_be_bytes()); // typlen
        description.extend_from_slice(&(-1_i32).to_be_bytes()); // typmod
        description.extend_from_slice(&0_i16.to_be_bytes()); // text format
    }
    write_message(stream, b'T', &description).await?;
    let mut rows: i64 = 0;
    for batch in reader {
        let batch = batch.context("arrow batch")?;
        rows += write_batch(stream, &batch).await?;
    }
    let verb = sql
        .split_whitespace()
        .next()
        .unwrap_or("SELECT")
        .to_ascii_uppercase();
    let tag = match verb.as_str() {
        "INSERT" => format!("INSERT 0 {rows}"),
        "SELECT" | "WITH" | "SHOW" | "FROM" => format!("SELECT {rows}"),
        other => other.to_owned(),
    };
    let mut complete = tag.into_bytes();
    complete.push(0);
    write_message(stream, b'C', &complete).await?;
    Ok(())
}

async fn write_batch(stream: &mut BufStream<TcpStream>, batch: &RecordBatch) -> Result<i64> {
    for row in 0..batch.num_rows() {
        let mut body = Vec::new();
        body.extend_from_slice(&i16::try_from(batch.num_columns())?.to_be_bytes());
        for column in batch.columns() {
            match text_value(column.as_ref(), row)? {
                None => body.extend_from_slice(&(-1_i32).to_be_bytes()),
                Some(text) => {
                    body.extend_from_slice(&i32::try_from(text.len())?.to_be_bytes());
                    body.extend_from_slice(text.as_bytes());
                }
            }
        }
        write_message(stream, b'D', &body).await?;
    }
    Ok(i64::try_from(batch.num_rows()).unwrap_or(0))
}

/// The pg type a result column claims. Everything renders as text on the wire;
/// the OID is what lets a driver parse that text into the right native type.
fn type_oid(data_type: &DataType) -> i32 {
    match data_type {
        DataType::Boolean => 16,
        DataType::Int8 | DataType::Int16 => 21,
        DataType::Int32 | DataType::UInt8 | DataType::UInt16 => 23,
        DataType::Int64 | DataType::UInt32 | DataType::UInt64 => 20,
        DataType::Float32 => 700,
        DataType::Float64 => 701,
        DataType::Decimal128(..) => 1700,
        DataType::Date32 => 1082,
        DataType::Timestamp(..) => 1114,
        _ => 25,
    }
}

fn text_value(array: &dyn Array, row: usize) -> Result<Option<String>> {
    if array.is_null(row) {
        return Ok(None);
    }
    let rendered = match array.data_type() {
        DataType::Boolean => if array.as_boolean().value(row) {
            "t"
        } else {
            "f"
        }
        .to_owned(),
        DataType::Int8 => array
            .as_primitive::<types::Int8Type>()
            .value(row)
            .to_string(),
        DataType::Int16 => array
            .as_primitive::<types::Int16Type>()
            .value(row)
            .to_string(),
        DataType::Int32 => array
            .as_primitive::<types::Int32Type>()
            .value(row)
            .to_string(),
        DataType::Int64 => array
            .as_primitive::<types::Int64Type>()
            .value(row)
            .to_string(),
        DataType::UInt8 => array
            .as_primitive::<types::UInt8Type>()
            .value(row)
            .to_string(),
        DataType::UInt16 => array
            .as_primitive::<types::UInt16Type>()
            .value(row)
            .to_string(),
        DataType::UInt32 => array
            .as_primitive::<types::UInt32Type>()
            .value(row)
            .to_string(),
        DataType::UInt64 => array
            .as_primitive::<types::UInt64Type>()
            .value(row)
            .to_string(),
        DataType::Float32 => array
            .as_primitive::<types::Float32Type>()
            .value(row)
            .to_string(),
        DataType::Float64 => array
            .as_primitive::<types::Float64Type>()
            .value(row)
            .to_string(),
        DataType::Utf8 => array.as_string::<i32>().value(row).to_owned(),
        DataType::LargeUtf8 => array.as_string::<i64>().value(row).to_owned(),
        DataType::Decimal128(_, scale) => format_decimal(
            array.as_primitive::<types::Decimal128Type>().value(row),
            *scale,
        ),
        DataType::Date32 => format_date(array.as_primitive::<types::Date32Type>().value(row)),
        DataType::Timestamp(unit, _) => {
            let raw = match unit {
                TimeUnit::Second => {
                    array
                        .as_primitive::<types::TimestampSecondType>()
                        .value(row)
                        * 1_000_000
                }
                TimeUnit::Millisecond => {
                    array
                        .as_primitive::<types::TimestampMillisecondType>()
                        .value(row)
                        * 1_000
                }
                TimeUnit::Microsecond => array
                    .as_primitive::<types::TimestampMicrosecondType>()
                    .value(row),
                TimeUnit::Nanosecond => {
                    array
                        .as_primitive::<types::TimestampNanosecondType>()
                        .value(row)
                        / 1_000
                }
            };
            format_timestamp(raw)
        }
        other => bail!("result type {other:?} has no wire rendering; cast to VARCHAR in SQL"),
    };
    Ok(Some(rendered))
}

fn format_decimal(raw: i128, scale: i8) -> String {
    if scale <= 0 {
        return raw.to_string();
    }
    let factor = 10_u128.pow(u32::from(scale.unsigned_abs()));
    let sign = if raw < 0 { "-" } else { "" };
    let magnitude = raw.unsigned_abs();
    format!(
        "{sign}{}.{:0width$}",
        magnitude / factor,
        magnitude % factor,
        width = scale.unsigned_abs() as usize
    )
}

/// Civil date from days since the epoch — Howard Hinnant's algorithm, so dates
/// render without pulling a calendar crate into the tree.
fn civil_from_days(days: i64) -> (i64, i64, i64) {
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let day_of_era = z - era * 146_097;
    let year_of_era =
        (day_of_era - day_of_era / 1460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_point = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_point + 2) / 5 + 1;
    let month = if month_point < 10 {
        month_point + 3
    } else {
        month_point - 9
    };
    (if month <= 2 { year + 1 } else { year }, month, day)
}

fn format_date(days: i32) -> String {
    let (year, month, day) = civil_from_days(i64::from(days));
    format!("{year:04}-{month:02}-{day:02}")
}

fn format_timestamp(micros: i64) -> String {
    const DAY: i64 = 86_400_000_000;
    let (year, month, day) = civil_from_days(micros.div_euclid(DAY));
    let in_day = micros.rem_euclid(DAY);
    let seconds = in_day / 1_000_000;
    format!(
        "{year:04}-{month:02}-{day:02} {:02}:{:02}:{:02}.{:06}",
        seconds / 3600,
        seconds % 3600 / 60,
        seconds % 60,
        in_day % 1_000_000
    )
}

async fn ready(stream: &mut BufStream<TcpStream>) -> Result<()> {
    write_message(stream, b'Z', b"I").await?;
    flush(stream).await?;
    Ok(())
}

/// After an extended-protocol refusal, discard until Sync so the client's
/// pipeline and ours agree where the conversation stands.
async fn sync_to_ready<P: Platform>(
    platform: &P,
    stream: &mut BufStream<TcpStream>,
    identity: &Identity,
    auth_checks: &mut tokio::time::Interval,
) -> Result<()> {
    loop {
        let Some((kind, _)) = read_authenticated(platform, stream, identity, auth_checks).await?
        else {
            bail!("disconnected during error recovery");
        };
        if kind == b'S' {
            return ready(stream).await;
        }
        if kind == b'X' {
            bail!("terminated during error recovery");
        }
    }
}

/// Keep the same read future pinned while validation ticks run. `read_message`
/// consumes a header and then a body using `read_exact`, so dropping it after a
/// successful check would lose already consumed bytes and corrupt the protocol.
async fn read_authenticated<P: Platform>(
    platform: &P,
    stream: &mut BufStream<TcpStream>,
    identity: &Identity,
    checks: &mut tokio::time::Interval,
) -> Result<Option<(u8, Vec<u8>)>> {
    let denied = {
        let message = read_message(stream);
        tokio::pin!(message);
        loop {
            tokio::select! {
                biased;
                _ = checks.tick() => {
                    if let Err(error) = platform.revalidate(identity).await {
                        break error;
                    }
                }
                message = &mut message => return Ok(message.ok()),
            }
        }
    };
    // Only a terminal refusal drops a partial read. The connection is then
    // closed, so it can never interpret the remaining body as another header.
    refuse_access(stream, denied).await?;
    Ok(None)
}

/// Report authentication refusal or backend failure with its actual SQLSTATE,
/// then unwind through the session-close path. A peer not reading its socket
/// cannot indefinitely delay that cleanup.
async fn refuse_access(stream: &mut BufStream<TcpStream>, error: ApiError) -> Result<()> {
    tokio::time::timeout(
        WRITE_TIMEOUT,
        error_response(
            stream,
            sqlstate(error.0.kind),
            &format!("{} [{}]", error.1, error.0.kind),
        ),
    )
    .await
    .context("pgwire refusal write timed out")??;
    bail!("pgwire access refused [{}]", error.0.kind)
}

async fn submitted_notice(stream: &mut BufStream<TcpStream>, request_id: Uuid) -> Result<()> {
    let message = format!(
        "query submitted: request_id={request_id}; status=/v1/requests/{request_id}; acceptance pending"
    );
    let mut body = Vec::new();
    for (field, value) in [(b'S', "NOTICE"), (b'C', "00000"), (b'M', message.as_str())] {
        body.push(field);
        body.extend_from_slice(value.as_bytes());
        body.push(0);
    }
    body.push(0);
    write_message(stream, b'N', &body).await?;
    flush(stream).await
}

async fn error_response(
    stream: &mut BufStream<TcpStream>,
    code: &str,
    message: &str,
) -> Result<()> {
    let mut body = Vec::new();
    for (field, value) in [(b'S', "ERROR"), (b'C', code), (b'M', message)] {
        body.push(field);
        body.extend_from_slice(value.as_bytes());
        body.push(0);
    }
    body.push(0);
    write_message(stream, b'E', &body).await?;
    flush(stream).await?;
    Ok(())
}

async fn write_message(stream: &mut BufStream<TcpStream>, kind: u8, body: &[u8]) -> Result<()> {
    tokio::time::timeout(WRITE_TIMEOUT, async {
        stream.write_u8(kind).await?;
        stream
            .write_i32(i32::try_from(body.len() + 4).context("message length")?)
            .await?;
        stream.write_all(body).await?;
        Ok(())
    })
    .await
    .context("pgwire message write timed out")?
}

async fn flush(stream: &mut BufStream<TcpStream>) -> Result<()> {
    tokio::time::timeout(WRITE_TIMEOUT, stream.flush())
        .await
        .context("pgwire flush timed out")??;
    Ok(())
}

async fn read_message(stream: &mut BufStream<TcpStream>) -> Result<(u8, Vec<u8>)> {
    let kind = stream.read_u8().await?;
    let length = stream.read_i32().await?;
    anyhow::ensure!(
        (4..=10_000_000).contains(&length),
        "malformed message length"
    );
    let mut body = vec![0; usize::try_from(length - 4)?];
    stream.read_exact(&mut body).await?;
    Ok((kind, body))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        Mutex,
        atomic::{AtomicU8, AtomicUsize, Ordering},
    };
    use tokio::sync::Notify;

    #[derive(Default)]
    struct TestPlatform {
        // 0 valid, 1 rejected credential, 2 unavailable identity backend.
        access: AtomicU8,
        checks: AtomicUsize,
        opened: AtomicUsize,
        closed: AtomicUsize,
        statements: Mutex<Vec<String>>,
        request_ids: Mutex<Vec<Uuid>>,
        hold_open: bool,
        hold_query: bool,
        entered: Notify,
        resume: Notify,
    }

    impl Platform for TestPlatform {
        async fn authenticate(&self, token: String) -> std::result::Result<Identity, ApiError> {
            assert_eq!(token, "test-token");
            Ok(Identity {
                principal: "test-user".into(),
                credential_id: Some("test-credential".into()),
                expires_at: None,
            })
        }

        async fn revalidate(&self, identity: &Identity) -> std::result::Result<(), ApiError> {
            assert_eq!(identity.credential_id.as_deref(), Some("test-credential"));
            self.checks.fetch_add(1, Ordering::SeqCst);
            match self.access.load(Ordering::SeqCst) {
                0 => Ok(()),
                1 => Err(ApiError::new(
                    crate::failure::UNAUTHORIZED,
                    "credential revoked",
                )),
                _ => Err(ApiError::new(
                    crate::failure::UNAVAILABLE,
                    "identity backend unavailable",
                )),
            }
        }

        async fn open_session(
            &self,
            identity: &Identity,
        ) -> std::result::Result<Session, ApiError> {
            assert_eq!(identity.principal, "test-user");
            self.opened.fetch_add(1, Ordering::SeqCst);
            if self.hold_open {
                self.entered.notify_one();
                self.resume.notified().await;
            }
            Ok(Session {
                endpoint: "worker".into(),
                session_id: "session".into(),
            })
        }

        async fn query(
            &self,
            _identity: &Identity,
            _session: &Session,
            request_id: Uuid,
            sql: String,
        ) -> QueryOutcome {
            self.request_ids.lock().unwrap().push(request_id);
            self.statements.lock().unwrap().push(sql);
            if self.hold_query {
                self.entered.notify_one();
                self.resume.notified().await;
            }
            let mut bytes = Vec::new();
            {
                let mut writer = arrow_ipc::writer::StreamWriter::try_new(
                    &mut bytes,
                    &arrow_schema::Schema::empty(),
                )
                .unwrap();
                writer.finish().unwrap();
            }
            QueryOutcome::Rows(bytes)
        }

        async fn close_session(&self, _session: Session) {
            self.closed.fetch_add(1, Ordering::SeqCst);
        }

        fn draining(&self) -> bool {
            false
        }
    }

    async fn test_connection(
        platform: Arc<TestPlatform>,
    ) -> (BufStream<TcpStream>, tokio::task::JoinHandle<Result<()>>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let client = TcpStream::connect(listener.local_addr().unwrap())
            .await
            .unwrap();
        let (server, _) = listener.accept().await.unwrap();
        let task = tokio::spawn(connection(platform, server));
        let mut client = BufStream::new(client);
        let startup = b"user\0test-user\0database\0lake\0\0";
        client
            .write_i32(i32::try_from(startup.len() + 8).unwrap())
            .await
            .unwrap();
        client.write_i32(PROTOCOL_V3).await.unwrap();
        client.write_all(startup).await.unwrap();
        flush(&mut client).await.unwrap();
        let (kind, body) = response(&mut client).await;
        assert_eq!((kind, body), (b'R', 3_i32.to_be_bytes().to_vec()));
        write_message(&mut client, b'p', b"test-token\0")
            .await
            .unwrap();
        flush(&mut client).await.unwrap();
        successful_response(&mut client).await;
        (client, task)
    }

    #[tokio::test]
    async fn explicit_startup_pool_selection_is_refused_before_backend_open() {
        for key in ["pool", "duckfleet_pool"] {
            let platform = Arc::new(TestPlatform::default());
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let client = TcpStream::connect(listener.local_addr().unwrap())
                .await
                .unwrap();
            let (server, _) = listener.accept().await.unwrap();
            let task = tokio::spawn(connection(platform.clone(), server));
            let mut client = BufStream::new(client);
            let startup = format!("user\0test-user\0{key}\0interactive\0\0");
            client
                .write_i32(i32::try_from(startup.len() + 8).unwrap())
                .await
                .unwrap();
            client.write_i32(PROTOCOL_V3).await.unwrap();
            client.write_all(startup.as_bytes()).await.unwrap();
            flush(&mut client).await.unwrap();
            refusal(&mut client, "0A000").await;
            client.shutdown().await.unwrap();
            finished(task).await;
            assert_eq!(platform.opened.load(Ordering::SeqCst), 0);
            assert!(platform.statements.lock().unwrap().is_empty());
        }
    }

    async fn response(client: &mut BufStream<TcpStream>) -> (u8, Vec<u8>) {
        tokio::time::timeout(Duration::from_secs(8), read_message(client))
            .await
            .unwrap()
            .unwrap()
    }

    async fn successful_response(client: &mut BufStream<TcpStream>) {
        loop {
            let (kind, body) = response(client).await;
            assert_ne!(
                kind,
                b'E',
                "unexpected error: {}",
                String::from_utf8_lossy(&body)
            );
            if kind == b'Z' {
                return;
            }
        }
    }

    async fn statement(client: &mut BufStream<TcpStream>, sql: &str) {
        let mut body = sql.as_bytes().to_vec();
        body.push(0);
        write_message(client, b'Q', &body).await.unwrap();
        flush(client).await.unwrap();
    }

    async fn refusal(client: &mut BufStream<TcpStream>, code: &str) {
        let (kind, body) = response(client).await;
        assert_eq!(kind, b'E');
        assert!(
            body.split(|b| *b == 0)
                .any(|field| field == format!("C{code}").as_bytes()),
            "unexpected SQLSTATE: {}",
            String::from_utf8_lossy(&body)
        );
    }

    async fn finished(task: tokio::task::JoinHandle<Result<()>>) {
        // A terminal access refusal is an intentional connection error. Joining
        // proves cleanup completed before asserting its counter.
        let _ = tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn query_notice_exposes_same_request_id_before_backend_finishes() {
        let platform = Arc::new(TestPlatform {
            hold_query: true,
            ..Default::default()
        });
        let (mut client, task) = test_connection(platform.clone()).await;
        statement(&mut client, "select 1").await;
        let (kind, body) = response(&mut client).await;
        assert_eq!(kind, b'N');
        tokio::time::timeout(Duration::from_secs(2), platform.entered.notified())
            .await
            .unwrap();
        let id = platform.request_ids.lock().unwrap()[0];
        let notice = String::from_utf8(body).unwrap();
        assert!(notice.contains(&format!("request_id={id}")));
        assert!(notice.contains(&format!("status=/v1/requests/{id}")));
        assert!(notice.contains("acceptance pending"));
        assert!(!task.is_finished());
        platform.resume.notify_one();
        successful_response(&mut client).await;
        write_message(&mut client, b'X', &[]).await.unwrap();
        flush(&mut client).await.unwrap();
        finished(task).await;
    }

    #[tokio::test]
    async fn revoked_handshake_identity_cannot_open_a_lazy_session() {
        let platform = Arc::new(TestPlatform::default());
        let (mut client, task) = test_connection(platform.clone()).await;
        platform.access.store(1, Ordering::SeqCst);
        statement(&mut client, "select 1").await;
        refusal(&mut client, "28P01").await;
        finished(task).await;
        assert_eq!(platform.opened.load(Ordering::SeqCst), 0);
        assert!(platform.statements.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn backend_revalidation_failure_is_not_an_invalid_password() {
        let platform = Arc::new(TestPlatform::default());
        let (mut client, task) = test_connection(platform.clone()).await;
        platform.access.store(2, Ordering::SeqCst);
        statement(&mut client, "select 1").await;
        refusal(&mut client, "57P01").await;
        finished(task).await;
        assert_eq!(platform.opened.load(Ordering::SeqCst), 0);
        assert!(platform.statements.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn idle_revocation_closes_an_open_session_without_another_message() {
        let platform = Arc::new(TestPlatform::default());
        let (mut client, task) = test_connection(platform.clone()).await;
        statement(&mut client, "begin").await;
        successful_response(&mut client).await;
        platform.access.store(1, Ordering::SeqCst);
        refusal(&mut client, "28P01").await;
        finished(task).await;
        assert_eq!(platform.closed.load(Ordering::SeqCst), 1);
        assert_eq!(*platform.statements.lock().unwrap(), ["begin"]);
    }

    #[tokio::test]
    async fn fragmented_message_survives_successful_idle_revalidation_ticks() {
        let platform = Arc::new(TestPlatform::default());
        let (mut client, task) = test_connection(platform.clone()).await;
        let body = b"select 12345\0";
        // read_message has consumed its complete header and half of its body
        // before two successful auth checks fire. Restarting the read at either
        // tick would interpret the remaining SQL bytes as a protocol header.
        client.write_u8(b'Q').await.unwrap();
        client
            .write_i32(i32::try_from(body.len() + 4).unwrap())
            .await
            .unwrap();
        client.write_all(&body[..5]).await.unwrap();
        flush(&mut client).await.unwrap();
        tokio::time::timeout(Duration::from_secs(12), async {
            while platform.checks.load(Ordering::SeqCst) < 2 {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap();
        client.write_all(&body[5..]).await.unwrap();
        flush(&mut client).await.unwrap();
        successful_response(&mut client).await;
        assert_eq!(*platform.statements.lock().unwrap(), ["select 12345"]);
        write_message(&mut client, b'X', &[]).await.unwrap();
        flush(&mut client).await.unwrap();
        finished(task).await;
        assert_eq!(platform.closed.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn revocation_during_open_closes_the_session_without_executing() {
        let platform = Arc::new(TestPlatform {
            hold_open: true,
            ..Default::default()
        });
        let (mut client, task) = test_connection(platform.clone()).await;
        statement(&mut client, "select 1").await;
        tokio::time::timeout(Duration::from_secs(2), platform.entered.notified())
            .await
            .unwrap();
        platform.access.store(1, Ordering::SeqCst);
        platform.resume.notify_one();
        refusal(&mut client, "28P01").await;
        finished(task).await;
        assert!(platform.statements.lock().unwrap().is_empty());
        assert_eq!(platform.closed.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn executing_statement_finishes_but_revoked_followup_is_denied() {
        let platform = Arc::new(TestPlatform {
            hold_query: true,
            ..Default::default()
        });
        let (mut client, task) = test_connection(platform.clone()).await;
        statement(&mut client, "select 1").await;
        tokio::time::timeout(Duration::from_secs(2), platform.entered.notified())
            .await
            .unwrap();
        platform.access.store(1, Ordering::SeqCst);
        // Queue a second statement while the first is executing, then cross an
        // idle-check interval. Neither buffered SQL nor revocation cancels the
        // active statement, and the buffered follow-up must never execute.
        statement(&mut client, "select 2").await;
        tokio::time::sleep(AUTH_CHECK_INTERVAL + Duration::from_millis(100)).await;
        assert_eq!(platform.closed.load(Ordering::SeqCst), 0);
        assert!(!task.is_finished());
        platform.resume.notify_one();
        successful_response(&mut client).await;
        refusal(&mut client, "28P01").await;
        let mut trailing = [0_u8; 1];
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(2), client.read(&mut trailing))
                .await
                .unwrap()
                .unwrap(),
            0,
            "completed result and refusal must be followed by FIN, not reset"
        );
        client.shutdown().await.unwrap();
        finished(task).await;
        assert_eq!(*platform.statements.lock().unwrap(), ["select 1"]);
        assert_eq!(platform.closed.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn revocation_during_extended_error_recovery_still_closes_session() {
        let platform = Arc::new(TestPlatform::default());
        let (mut client, task) = test_connection(platform.clone()).await;
        statement(&mut client, "begin").await;
        successful_response(&mut client).await;
        write_message(&mut client, b'P', b"named\0select 1\0\0\0")
            .await
            .unwrap();
        flush(&mut client).await.unwrap();
        refusal(&mut client, "0A000").await;
        platform.access.store(1, Ordering::SeqCst);
        refusal(&mut client, "28P01").await;
        finished(task).await;
        assert_eq!(platform.closed.load(Ordering::SeqCst), 1);
        assert_eq!(*platform.statements.lock().unwrap(), ["begin"]);
    }

    #[test]
    fn dates_and_timestamps_render_without_a_calendar_crate() {
        assert_eq!(format_date(0), "1970-01-01");
        assert_eq!(format_date(20455), "2026-01-02");
        assert_eq!(format_date(-1), "1969-12-31");
        assert_eq!(format_timestamp(0), "1970-01-01 00:00:00.000000");
        assert_eq!(
            format_timestamp(1_767_225_600_000_000 + 3_661_000_001),
            "2026-01-01 01:01:01.000001"
        );
        assert_eq!(format_timestamp(-1), "1969-12-31 23:59:59.999999");
    }

    #[test]
    fn decimals_keep_their_scale_and_sign() {
        assert_eq!(format_decimal(12345, 2), "123.45");
        assert_eq!(format_decimal(-12345, 2), "-123.45");
        assert_eq!(format_decimal(5, 3), "0.005");
        assert_eq!(format_decimal(42, 0), "42");
    }

    #[test]
    fn classified_kinds_become_sqlstates_a_driver_acts_on() {
        assert_eq!(sqlstate("transaction_conflict"), "40001");
        assert_eq!(sqlstate("timeout"), "57014");
        assert_eq!(sqlstate("sql_error"), "42601");
        assert_eq!(sqlstate("invented_later"), "XX000");
    }

    #[test]
    fn a_bind_with_parameters_is_recognised() {
        // portal "", statement "", zero format codes, one parameter.
        let with = [0u8, 0, 0, 0, 0, 1];
        assert!(bind_has_parameters(&with));
        let without = [0u8, 0, 0, 0, 0, 0];
        assert!(!bind_has_parameters(&without));
    }
}
