//! Single-statement job execution. SQL completion is independent of delivery.
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use duckdb::{Connection, Params, Statement, StreamErrorKind, ffi};
use std::fmt;

pub(crate) use duckdb::EngineCompletion;

#[derive(Clone, Copy)]
pub(crate) struct Limits {
    pub max_rows: u64,
    // A bound on retained imported batches, not on native allocation before
    // conversion. The engine/cgroup memory limits still govern oversized values.
    pub max_batch_bytes: usize,
}

#[derive(Debug)]
pub(crate) enum Error {
    Preparation(duckdb::Error),
    UnsupportedStatement,
    InvalidLimits,
    Stream(duckdb::StreamError),
    Limit(&'static str),
}
impl Error {
    pub fn not_executed(&self) -> bool {
        matches!(
            self,
            Self::Preparation(_) | Self::UnsupportedStatement | Self::InvalidLimits
        ) || matches!(self, Self::Stream(error) if error.kind == StreamErrorKind::Binding)
    }
}
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Preparation(error) => write!(f, "{error}"),
            Self::UnsupportedStatement => f.write_str(
                "jobs support one SELECT, INSERT, UPDATE, DELETE, CREATE, ALTER, or DROP statement; \
                 transactions, scripts, and MERGE are unsupported",
            ),
            Self::InvalidLimits => f.write_str("job stream limits must be positive"),
            Self::Stream(error) => write!(f, "{error}"),
            Self::Limit(message) => f.write_str(message),
        }
    }
}
impl std::error::Error for Error {}

pub(crate) struct Prepared<'conn>(Statement<'conn>);
pub(crate) struct Stream<'stmt> {
    inner: duckdb::EngineStream<'stmt>,
    limits: Limits,
    rows: u64,
    refusal: Option<&'static str>,
}

pub(crate) fn prepare<'conn>(conn: &'conn Connection, sql: &str) -> Result<Prepared<'conn>, Error> {
    let statement = conn.prepare_single(sql).map_err(Error::Preparation)?;
    // Use the native statement category rather than a SQL prefix or a blanket
    // acceptance of categories added by a future engine.
    if !matches!(
        statement.statement_type(),
        ffi::duckdb_statement_type_DUCKDB_STATEMENT_TYPE_SELECT
            | ffi::duckdb_statement_type_DUCKDB_STATEMENT_TYPE_INSERT
            | ffi::duckdb_statement_type_DUCKDB_STATEMENT_TYPE_UPDATE
            | ffi::duckdb_statement_type_DUCKDB_STATEMENT_TYPE_DELETE
            | ffi::duckdb_statement_type_DUCKDB_STATEMENT_TYPE_CREATE
            | ffi::duckdb_statement_type_DUCKDB_STATEMENT_TYPE_ALTER
            | ffi::duckdb_statement_type_DUCKDB_STATEMENT_TYPE_DROP
    ) {
        return Err(Error::UnsupportedStatement);
    }
    Ok(Prepared(statement))
}

impl Prepared<'_> {
    pub fn start<P: Params>(&mut self, params: P, limits: Limits) -> Result<Stream<'_>, Error> {
        if limits.max_rows == 0 || limits.max_batch_bytes == 0 {
            return Err(Error::InvalidLimits);
        }
        let inner = self.0.start_arrow_stream(params).map_err(Error::Stream)?;
        Ok(Stream {
            inner,
            limits,
            rows: 0,
            refusal: None,
        })
    }
}

impl Stream<'_> {
    /// Read this before schema/encoding/upload: a materialized statement has
    /// already finished even though none of its result has been delivered.
    pub fn completion(&self) -> EngineCompletion {
        self.inner.completion()
    }

    pub fn schema(&mut self) -> Result<SchemaRef, Error> {
        self.inner.schema().map_err(Error::Stream)
    }

    pub fn next_batch(&mut self) -> Result<Option<RecordBatch>, Error> {
        if let Some(message) = self.refusal {
            return Err(Error::Limit(message));
        }
        let batch = self.inner.next_batch().map_err(Error::Stream)?;
        if let Some(batch) = &batch {
            let total = self.rows.checked_add(batch.num_rows() as u64);
            let message = if total.is_none_or(|rows| rows > self.limits.max_rows) {
                Some("job result exceeds its row limit")
            } else if batch.get_array_memory_size() > self.limits.max_batch_bytes {
                Some("job result contains an oversized Arrow batch")
            } else {
                None
            };
            if let Some(message) = message {
                self.refusal = Some(message);
                return Err(Error::Limit(message));
            }
            self.rows = total.expect("row count checked");
        }
        Ok(batch)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::{Context, Result, ensure};
    use arrow_array::{Array, Int64Array};
    use arrow_schema::{DataType, TimeUnit};
    use duckdb::{
        core::{DataChunkHandle, LogicalTypeHandle, LogicalTypeId},
        vtab::{BindInfo, InitInfo, TableFunctionInfo, VTab},
    };
    use std::sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    };
    use std::time::{Duration, Instant};

    fn limits() -> Limits {
        Limits {
            max_rows: 10_000_000,
            max_batch_bytes: 16 * 1024 * 1024,
        }
    }

    fn connection() -> Result<Connection> {
        let connection = Connection::open_in_memory()?;
        connection.execute_batch(
            "SET threads=1; SET streaming_buffer_size='64KiB'; \
             SET preserve_insertion_order=false;",
        )?;
        Ok(connection)
    }

    fn scalar(connection: &Connection, sql: &str) -> Result<i64> {
        Ok(connection.query_row(sql, [], |row| row.get(0))?)
    }

    #[test]
    fn scripts_are_rejected_without_executing_a_prefix_and_merge_stays_unsupported() -> Result<()> {
        let connection = connection()?;
        connection.execute_batch("CREATE TABLE target(id BIGINT, value BIGINT)")?;
        let rejected = prepare(&connection, "INSERT INTO target VALUES(1,10); SELECT 1");
        ensure!(matches!(
            rejected,
            Err(Error::Preparation(duckdb::Error::MultipleStatement))
        ));
        ensure!(scalar(&connection, "SELECT count(*) FROM target")? == 0);
        for sql in ["", "-- no statement", "BEGIN", "SET threads=2"] {
            let error = prepare(&connection, sql)
                .err()
                .context("expected refusal")?;
            ensure!(error.not_executed());
        }
        let merge = "MERGE INTO target t USING (VALUES(1,10)) s(id,value) ON t.id=s.id \
                     WHEN MATCHED THEN UPDATE SET value=s.value \
                     WHEN NOT MATCHED THEN INSERT(id,value) VALUES(s.id,s.value)";
        let native = connection.prepare_single(merge)?;
        ensure!(
            native.statement_type() == ffi::duckdb_statement_type_DUCKDB_STATEMENT_TYPE_MERGE_INTO
        );
        ensure!(matches!(
            prepare(&connection, merge),
            Err(Error::UnsupportedStatement)
        ));
        ensure!(scalar(&connection, "SELECT count(*) FROM target")? == 0);
        Ok(())
    }

    #[test]
    fn materialized_ctas_and_returning_report_success_before_schema_or_delivery() -> Result<()> {
        let connection = connection()?;
        connection.execute_batch(
            "CREATE SEQUENCE once_counter START 1; CREATE TABLE target(id BIGINT)",
        )?;
        let observer = connection.try_clone()?;
        for (sql, count) in [
            (
                "INSERT INTO target VALUES(nextval('once_counter')) RETURNING id",
                1,
            ),
            (
                "CREATE TABLE copied AS SELECT nextval('once_counter') AS id FROM range(3)",
                3,
            ),
        ] {
            let mut prepared = prepare(&connection, sql)?;
            let stream = prepared.start([], limits())?;
            ensure!(stream.completion() == EngineCompletion::Succeeded);
            let table = if count == 1 { "target" } else { "copied" };
            ensure!(scalar(&observer, &format!("SELECT count(*) FROM {table}"))? == count);
            // Delivery can fail without even importing schema. Dropping this
            // successful result must not replay SQL or erase its completion.
            drop(stream);
        }
        ensure!(scalar(&observer, "SELECT max(id) FROM copied")? == 4);
        ensure!(scalar(&observer, "SELECT nextval('once_counter')")? == 5);
        Ok(())
    }

    #[test]
    fn schema_and_batches_use_the_same_execution_and_survive_native_result_drop() -> Result<()> {
        let connection = connection()?;
        connection.execute_batch("CREATE SEQUENCE seq START 1")?;
        let batch = {
            let mut prepared =
                prepare(&connection, "SELECT nextval('seq') AS value FROM range(3)")?;
            let mut stream = prepared.start([], limits())?;
            ensure!(stream.schema()?.field(0).name() == "value");
            let batch = stream.next_batch()?.context("expected rows")?;
            ensure!(stream.next_batch()?.is_none());
            ensure!(stream.completion() == EngineCompletion::Succeeded);
            ensure!(stream.next_batch()?.is_none());
            batch
        };
        ensure!(scalar(&connection, "SELECT nextval('seq')")? == 4);
        drop(connection);
        let values = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        ensure!(values.values().as_ref() == [1, 2, 3]);
        Ok(())
    }

    #[test]
    fn executed_schema_preserves_decimal_timezone_nested_and_null_types() -> Result<()> {
        let connection = connection()?;
        let mut prepared = prepare(
            &connection,
            "SELECT ?::DECIMAL(20,4) AS amount, \
            TIMESTAMPTZ '2024-01-01 12:34:56+00' AS at, [1,2] AS items, \
            {'name':'hello','value':7} AS record, NULL::BIGINT AS missing, \
            18446744073709551615::UBIGINT AS unsigned, '\\x00ff'::BLOB AS bytes",
        )?;
        let mut stream = prepared.start(["1234567890123456.1234"], limits())?;
        let schema = stream.schema()?;
        ensure!(schema.field(0).data_type() == &DataType::Decimal128(20, 4));
        ensure!(matches!(
            schema.field(1).data_type(),
            DataType::Timestamp(TimeUnit::Microsecond, Some(_))
        ));
        ensure!(matches!(schema.field(2).data_type(), DataType::List(_)));
        ensure!(matches!(schema.field(3).data_type(), DataType::Struct(_)));
        ensure!(schema.field(5).data_type() == &DataType::UInt64);
        let batch = stream.next_batch()?.context("expected typed row")?;
        ensure!(batch.column(4).is_null(0));
        ensure!(stream.next_batch()?.is_none());
        drop(stream);
        // Rebind the same prepared query with another parameter value without
        // reusing stale Arrow arrays or a result from the first execution.
        let mut stream = prepared.start(["1.0000"], limits())?;
        ensure!(stream.schema()? == schema);
        ensure!(stream.next_batch()?.context("second result")?.num_rows() == 1);
        Ok(())
    }

    #[test]
    fn empty_result_has_schema_and_only_checked_eof_completes_a_stream() -> Result<()> {
        let connection = connection()?;
        let mut prepared = prepare(&connection, "SELECT 1::BIGINT AS value WHERE false")?;
        let mut stream = prepared.start([], limits())?;
        ensure!(stream.schema()?.field(0).data_type() == &DataType::Int64);
        ensure!(stream.next_batch()?.is_none());
        ensure!(stream.completion() == EngineCompletion::Succeeded);
        Ok(())
    }

    struct LateError;
    impl VTab for LateError {
        type BindData = ();
        type InitData = AtomicUsize;
        fn bind(info: &BindInfo) -> Result<(), Box<dyn std::error::Error>> {
            info.add_result_column("value", LogicalTypeHandle::from(LogicalTypeId::Bigint));
            Ok(())
        }
        fn init(info: &InitInfo) -> Result<Self::InitData, Box<dyn std::error::Error>> {
            info.set_max_threads(1);
            Ok(AtomicUsize::new(0))
        }
        fn func(
            info: &TableFunctionInfo<Self>,
            output: &mut DataChunkHandle,
        ) -> Result<(), Box<dyn std::error::Error>> {
            // Registration clones and owns this exact typed extra-info value.
            let observed = unsafe { &*info.get_extra_info::<Arc<AtomicBool>>() };
            if observed.load(Ordering::Acquire) {
                return Err("late_fetch_marker".into());
            }
            let calls = info.get_init_data().fetch_add(1, Ordering::Relaxed);
            if calls >= 1024 {
                return Err("stream_never_yielded".into());
            }
            let mut column = output.flat_vector(0);
            // The declared BIGINT column has i64 physical values, and the slice
            // is bounded by the engine-provided vector's capacity.
            let values = unsafe { column.as_mut_slice::<i64>() };
            values.fill(calls as i64);
            let length = values.len();
            output.set_len(length);
            Ok(())
        }
    }

    #[test]
    fn error_after_an_observed_batch_never_becomes_successful_eof() -> Result<()> {
        let connection = connection()?;
        let observed = Arc::new(AtomicBool::new(false));
        connection
            .register_table_function_with_extra_info::<LateError, _>("late_error", &observed)?;
        let mut prepared = prepare(&connection, "SELECT value FROM late_error()")?;
        let mut stream = prepared.start([], limits())?;
        ensure!(stream.completion() == EngineCompletion::Pending);
        ensure!(
            stream
                .next_batch()?
                .context("first batch must precede failure")?
                .num_rows()
                > 0
        );
        observed.store(true, Ordering::Release);
        let mut error = None;
        for _ in 0..1024 {
            match stream.next_batch() {
                Ok(Some(_)) => {}
                Ok(None) => anyhow::bail!("late failure was incorrectly treated as EOF"),
                Err(found) => {
                    error = Some(found);
                    break;
                }
            }
        }
        let error = error.context("expected bounded late failure")?;
        ensure!(error.to_string().contains("late_fetch_marker"), "{error}");
        ensure!(!error.not_executed());
        ensure!(stream.completion() == EngineCompletion::Failed);
        ensure!(stream.next_batch().is_err());
        Ok(())
    }

    #[test]
    fn interruption_is_an_error_and_dropping_pending_stream_never_claims_success() -> Result<()> {
        let connection = connection()?;
        let interrupt = connection.interrupt_handle();
        let mut prepared = prepare(&connection, "SELECT i FROM range(1000000000) t(i)")?;
        let mut stream = prepared.start([], limits())?;
        ensure!(stream.next_batch()?.is_some());
        ensure!(stream.completion() == EngineCompletion::Pending);
        interrupt.interrupt();
        let began = Instant::now();
        ensure!(stream.next_batch().is_err());
        ensure!(began.elapsed() < Duration::from_secs(2));
        ensure!(stream.completion() == EngineCompletion::Failed);
        drop(stream);
        drop(prepared);
        ensure!(scalar(&connection, "SELECT 1")? == 1);
        let mut prepared = prepare(&connection, "SELECT i FROM range(1000000000) t(i)")?;
        let mut stream = prepared.start([], limits())?;
        ensure!(stream.next_batch()?.is_some());
        ensure!(stream.completion() == EngineCompletion::Pending);
        drop(stream);
        drop(prepared);
        ensure!(scalar(&connection, "SELECT 2")? == 2);
        Ok(())
    }

    #[test]
    fn row_and_batch_limits_stop_fetching_without_overwriting_known_sql_success() -> Result<()> {
        let connection = connection()?;
        let mut prepared = prepare(&connection, "SELECT i FROM range(10000) t(i)")?;
        let mut stream = prepared.start(
            [],
            Limits {
                max_rows: 1,
                ..limits()
            },
        )?;
        ensure!(matches!(stream.next_batch(), Err(Error::Limit(_))));
        ensure!(stream.completion() == EngineCompletion::Pending);
        ensure!(matches!(stream.next_batch(), Err(Error::Limit(_))));
        drop(stream);
        drop(prepared);
        connection.execute_batch("CREATE TABLE large_value(value VARCHAR)")?;
        let mut prepared = prepare(
            &connection,
            "INSERT INTO large_value VALUES(repeat('x', 1024)) RETURNING value",
        )?;
        let mut stream = prepared.start(
            [],
            Limits {
                max_batch_bytes: 128,
                ..limits()
            },
        )?;
        ensure!(stream.completion() == EngineCompletion::Succeeded);
        ensure!(matches!(stream.next_batch(), Err(Error::Limit(_))));
        ensure!(stream.completion() == EngineCompletion::Succeeded);
        drop(stream);
        ensure!(scalar(&connection, "SELECT count(*) FROM large_value")? == 1);
        Ok(())
    }

    #[test]
    fn large_result_is_fetched_in_bounded_batches_beyond_interactive_limits() -> Result<()> {
        let connection = connection()?;
        // 32 MiB of values, larger than both interactive response limits. The
        // test retains only the current batch and verifies actual streaming mode.
        let mut prepared = prepare(&connection, "SELECT i FROM range(4194304) t(i)")?;
        let mut stream = prepared.start([], limits())?;
        ensure!(stream.completion() == EngineCompletion::Pending);
        let mut rows = 0;
        let mut bytes = 0;
        let mut largest = 0;
        while let Some(batch) = stream.next_batch()? {
            rows += batch.num_rows();
            bytes += batch.get_array_memory_size();
            largest = largest.max(batch.get_array_memory_size());
        }
        ensure!(rows == 4_194_304);
        ensure!(bytes > 16 * 1024 * 1024);
        ensure!(largest < 1024 * 1024);
        ensure!(stream.completion() == EngineCompletion::Succeeded);
        Ok(())
    }

    #[test]
    fn active_execution_can_be_interrupted_from_another_thread() -> Result<()> {
        let connection = connection()?;
        let mut prepared = prepare(
            &connection,
            "SELECT sum(sin(i::DOUBLE)) FROM range(100000000000) t(i)",
        )?;
        let handle = connection.interrupt_handle();
        let done = Arc::new(AtomicBool::new(false));
        let observed = done.clone();
        let interrupter = std::thread::spawn(move || {
            // Repeated interrupts avoid a timing guess about native startup;
            // stop promptly once execute/fetch has unwound.
            while !observed.load(Ordering::Acquire) {
                std::thread::sleep(Duration::from_millis(10));
                handle.interrupt();
            }
        });
        let began = Instant::now();
        let result = match prepared.start([], limits()) {
            Err(error) => Err(error),
            Ok(mut stream) => loop {
                match stream.next_batch() {
                    Ok(Some(_)) => {}
                    Ok(None) => break Ok(()),
                    Err(error) => break Err(error),
                }
            },
        };
        done.store(true, Ordering::Release);
        interrupter.join().expect("interrupt thread");
        let error = result
            .err()
            .context("expected active execution interruption")?;
        ensure!(matches!(error, Error::Stream(ref inner) if inner.kind == StreamErrorKind::Engine));
        ensure!(!error.not_executed());
        ensure!(began.elapsed() < Duration::from_secs(2));
        Ok(())
    }
}
