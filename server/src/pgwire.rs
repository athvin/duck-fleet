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
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufStream};
use tokio::net::{TcpListener, TcpStream};

const PROTOCOL_V3: i32 = 196_608;
const SSL_REQUEST: i32 = 80_877_103;
const CANCEL_REQUEST: i32 = 80_877_102;

/// What the gateway hands this module: just enough to authenticate a token and
/// run statements through a worker session under a named principal.
pub(crate) trait Platform: Send + Sync + 'static {
    fn authenticate(
        &self,
        token: String,
    ) -> impl std::future::Future<Output = Option<String>> + Send;
    fn open_session(
        &self,
        principal: String,
    ) -> impl std::future::Future<Output = Result<Session>> + Send;
    fn query(
        &self,
        session: &Session,
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
    loop {
        match listener.accept().await {
            Ok((socket, _)) => {
                let platform = platform.clone();
                tokio::spawn(async move {
                    if let Err(error) = connection(platform, socket).await {
                        tracing::debug!(error = format!("{error:#}"), "pgwire connection ended");
                    }
                });
            }
            Err(error) => {
                tracing::warn!(%error, "pgwire accept failed");
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
        }
    }
}

async fn connection<P: Platform>(platform: Arc<P>, socket: TcpStream) -> Result<()> {
    let mut stream = BufStream::new(socket);
    // Startup phase: SSLRequests are answered 'N' (this listener is in-cluster
    // plain TCP; transport security is the exposure layer's concern), and a
    // client may retry the startup message afterwards.
    loop {
        let length = stream.read_i32().await.context("startup length")?;
        anyhow::ensure!((8..=10_000).contains(&length), "malformed startup length");
        let code = stream.read_i32().await.context("startup code")?;
        match code {
            SSL_REQUEST => {
                stream.write_all(b"N").await?;
                stream.flush().await?;
            }
            CANCEL_REQUEST => return Ok(()),
            PROTOCOL_V3 => {
                let mut rest = vec![0; usize::try_from(length - 8)?];
                stream.read_exact(&mut rest).await?;
                return established(platform, stream, &rest).await;
            }
            _ => bail!("unsupported protocol {code}"),
        }
    }
}

async fn established<P: Platform>(
    platform: Arc<P>,
    mut stream: BufStream<TcpStream>,
    startup: &[u8],
) -> Result<()> {
    let mut user = String::new();
    for pair in startup.split(|b| *b == 0).collect::<Vec<_>>().chunks(2) {
        if let [key, value] = pair
            && *key == b"user"
        {
            user = String::from_utf8_lossy(value).into_owned();
        }
    }
    if platform.draining() {
        error_response(&mut stream, "57P01", "the gateway is shutting down").await?;
        return Ok(());
    }
    // The password is a platform credential: the bootstrap token or one issued
    // through /v1/credentials. The startup "user" is advisory — identity comes
    // from the credential, exactly as it does over HTTP.
    write_message(&mut stream, b'R', &3_i32.to_be_bytes()).await?;
    stream.flush().await?;
    let (kind, body) = read_message(&mut stream).await?;
    anyhow::ensure!(kind == b'p', "expected a password message");
    let token = String::from_utf8_lossy(body.split(|b| *b == 0).next().unwrap_or(&[])).into_owned();
    let Some(principal) = platform.authenticate(token).await else {
        error_response(&mut stream, "28P01", "invalid platform credential").await?;
        return Ok(());
    };
    if !user.is_empty() && user != principal {
        tracing::debug!(
            user,
            principal,
            "pgwire startup user differs from credential"
        );
    }
    write_message(&mut stream, b'R', &0_i32.to_be_bytes()).await?;
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
        write_message(&mut stream, b'S', &body).await?;
    }
    let mut key_data = Vec::new();
    key_data.extend_from_slice(&1_i32.to_be_bytes());
    key_data.extend_from_slice(&1_i32.to_be_bytes());
    write_message(&mut stream, b'K', &key_data).await?;
    ready(&mut stream).await?;

    // The session opens on the first statement, so a client that connects and
    // leaves costs nothing; from then on the connection and the session live and
    // die together, which is the pinning the transaction semantics need.
    let mut session: Option<Session> = None;
    // The unnamed prepared statement, for the parameterless extended protocol.
    let mut prepared: Option<String> = None;
    let outcome = connection_loop(
        platform.as_ref(),
        &mut stream,
        &principal,
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
    principal: &str,
    session: &mut Option<Session>,
    prepared: &mut Option<String>,
) -> Result<()> {
    loop {
        let (kind, body) = match read_message(stream).await {
            Ok(message) => message,
            // A dropped connection is how psql says goodbye under Ctrl-C.
            Err(_) => return Ok(()),
        };
        match kind {
            b'X' => return Ok(()),
            b'Q' => {
                let sql = String::from_utf8_lossy(body.split(|b| *b == 0).next().unwrap_or(&[]))
                    .into_owned();
                if sql.trim().is_empty() {
                    write_message(stream, b'I', &[]).await?;
                } else {
                    run_statement(platform, stream, principal, session, &sql).await?;
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
                    sync_to_ready(stream).await?;
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
                    sync_to_ready(stream).await?;
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
                    run_statement(platform, stream, principal, session, &sql).await?;
                }
                Some(_) => write_message(stream, b'I', &[]).await?,
                None => {
                    error_response(stream, "26000", "no statement has been parsed").await?;
                }
            },
            b'S' => ready(stream).await?,
            b'H' => stream.flush().await?,
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
    principal: &str,
    session: &mut Option<Session>,
    sql: &str,
) -> Result<()> {
    if session.is_none() {
        match platform.open_session(principal.to_owned()).await {
            Ok(opened) => *session = Some(opened),
            Err(error) => {
                error_response(
                    stream,
                    "53300",
                    &format!("could not open a session: {error:#}"),
                )
                .await?;
                return Ok(());
            }
        }
    }
    let open = session.as_ref().expect("session was just opened");
    match platform.query(open, sql.to_owned()).await {
        QueryOutcome::Rows(arrow) => write_arrow(stream, sql, &arrow).await,
        QueryOutcome::Refused { kind, message } => {
            error_response(stream, sqlstate(&kind), &format!("{message} [{kind}]")).await
        }
    }
}

/// Classified platform kinds, spelled as SQLSTATEs a driver can act on.
fn sqlstate(kind: &str) -> &'static str {
    match kind {
        "sql_error" => "42601",
        "timeout" | "canceled" | "budget_exceeded" => "57014",
        "unauthorized" | "forbidden" => "28P01",
        "unknown_session" | "draining" | "starting" | "unavailable" => "57P01",
        "capacity" | "budget_exhausted" | "budget_job_cost" => "53300",
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
    stream.flush().await?;
    Ok(())
}

/// After an extended-protocol refusal, discard until Sync so the client's
/// pipeline and ours agree where the conversation stands.
async fn sync_to_ready(stream: &mut BufStream<TcpStream>) -> Result<()> {
    loop {
        let (kind, _) = read_message(stream).await?;
        if kind == b'S' {
            return ready(stream).await;
        }
        if kind == b'X' {
            bail!("terminated during error recovery");
        }
    }
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
    stream.flush().await?;
    Ok(())
}

async fn write_message(stream: &mut BufStream<TcpStream>, kind: u8, body: &[u8]) -> Result<()> {
    stream.write_u8(kind).await?;
    stream
        .write_i32(i32::try_from(body.len() + 4).context("message length")?)
        .await?;
    stream.write_all(body).await?;
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
