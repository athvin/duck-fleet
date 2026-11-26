//! The DuckFleet ADBC driver: ADBC over DuckFleet's HTTP/Arrow API, built on
//! the `duckfleet` SDK (decision 0001). Sessions, typed parameters, Arrow
//! results, cancellation, and classified failures are the SDK's; this crate
//! adds the ADBC surface: database/connection options (URI, token, compute),
//! autocommit transactions, single-row parameter binding, catalog metadata
//! (`get_info`, `get_table_types`, `get_objects`, `get_table_schema`), and
//! streamed `RecordBatchReader` results. SQL is never replayed.
//!
//! Bulk ingest, partitions, Substrait, statistics, and multi-row binding
//! return NOT_IMPLEMENTED.

mod metadata;

use adbc_core::error::{Error, Result, Status};
use adbc_core::options::{
    InfoCode, ObjectDepth, OptionConnection, OptionDatabase, OptionStatement, OptionValue,
};
use adbc_core::{CancelHandle, Optionable, PartitionedResult};
use arrow_array::cast::AsArray;
use arrow_array::types::*;
use arrow_array::{Array, RecordBatch, RecordBatchIterator, RecordBatchReader};
use arrow_schema::{DataType, Schema};
use duckfleet::{ErrorKind, Param, SessionOptions};
use std::collections::HashSet;
use std::sync::Arc;

pub(crate) fn runtime() -> &'static tokio::runtime::Runtime {
    static RUNTIME: std::sync::OnceLock<tokio::runtime::Runtime> = std::sync::OnceLock::new();
    RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("tokio runtime")
    })
}

fn unsupported(what: &str) -> Error {
    Error::with_message_and_status(
        format!("{what} is not implemented by the DuckFleet ADBC prototype"),
        Status::NotImplemented,
    )
}

/// Classified DuckFleet failures keep their identity: status, SQLSTATE, and a
/// message that names `failure_kind`, `retry_safe`, and the request ID.
pub(crate) fn convert(error: duckfleet::Error) -> Error {
    let (status, sqlstate) = match error.kind() {
        ErrorKind::Sql => (Status::InvalidArguments, *b"42601"),
        ErrorKind::Conflict => (Status::InvalidState, *b"40001"),
        ErrorKind::Canceled => (Status::Cancelled, *b"57014"),
        ErrorKind::Timeout | ErrorKind::QueueTimeout => (Status::Timeout, *b"57014"),
        ErrorKind::Unauthorized => (Status::Unauthenticated, *b"28000"),
        ErrorKind::Forbidden => (Status::Unauthorized, *b"42501"),
        ErrorKind::NotFound => (Status::NotFound, *b"42P01"),
        ErrorKind::InvalidRequest | ErrorKind::DuplicateRequest => {
            (Status::InvalidArguments, *b"22023")
        }
        ErrorKind::TransportUncertain | ErrorKind::SessionLost => (Status::IO, *b"08006"),
        ErrorKind::Unavailable | ErrorKind::BudgetRefused => (Status::IO, *b"53000"),
        _ => (Status::Unknown, *b"XX000"),
    };
    let mut converted = Error::with_message_and_status(
        format!(
            "{} (failure_kind: {}, retry_safe: {}, request_id: {})",
            error.message,
            error.failure_kind().unwrap_or("unknown"),
            error.retry_safe(),
            error.request_id().unwrap_or("none")
        ),
        status,
    );
    converted.sqlstate = sqlstate.map(|b| b as std::os::raw::c_char);
    converted
}

#[derive(Default)]
pub struct DuckFleetDriver;

impl adbc_core::Driver for DuckFleetDriver {
    type DatabaseType = DuckFleetDatabase;

    fn new_database(&mut self) -> Result<Self::DatabaseType> {
        Ok(DuckFleetDatabase::default())
    }

    fn new_database_with_opts(
        &mut self,
        opts: impl IntoIterator<Item = (OptionDatabase, OptionValue)>,
    ) -> Result<Self::DatabaseType> {
        let mut database = DuckFleetDatabase::default();
        for (key, value) in opts {
            database.set_option(key, value)?;
        }
        Ok(database)
    }
}

#[derive(Default)]
pub struct DuckFleetDatabase {
    uri: Option<String>,
    token: Option<String>,
    compute: duckfleet::ComputeRequest,
}

fn string(value: OptionValue) -> Result<String> {
    match value {
        OptionValue::String(text) => Ok(text),
        _ => Err(Error::with_message_and_status(
            "option must be a string",
            Status::InvalidArguments,
        )),
    }
}

impl Optionable for DuckFleetDatabase {
    type Option = OptionDatabase;

    fn set_option(&mut self, key: Self::Option, value: OptionValue) -> Result<()> {
        match key {
            OptionDatabase::Uri => self.uri = Some(string(value)?),
            OptionDatabase::Password => self.token = Some(string(value)?),
            OptionDatabase::Other(name) if name == "adbc.duckfleet.token" => {
                self.token = Some(string(value)?)
            }
            OptionDatabase::Other(name) if name.starts_with("adbc.duckfleet.") => {
                let text = string(value)?;
                match &name["adbc.duckfleet.".len()..] {
                    "pool" => self.compute.pool = Some(text),
                    "memory" => self.compute.memory = Some(text),
                    "disk" => self.compute.disk = Some(text),
                    "cpu" => {
                        self.compute.cpu = Some(text.parse().map_err(|_| {
                            Error::with_message_and_status(
                                "adbc.duckfleet.cpu must be a positive whole number",
                                Status::InvalidArguments,
                            )
                        })?)
                    }
                    "mode" => {
                        self.compute.mode = Some(match text.as_str() {
                            "persistent" => duckfleet::Mode::Persistent,
                            "isolated" => duckfleet::Mode::Isolated,
                            _ => {
                                return Err(Error::with_message_and_status(
                                    "adbc.duckfleet.mode must be persistent or isolated",
                                    Status::InvalidArguments,
                                ));
                            }
                        })
                    }
                    other => {
                        return Err(unsupported(&format!(
                            "database option adbc.duckfleet.{other}"
                        )));
                    }
                }
            }
            OptionDatabase::Username => {}
            other => return Err(unsupported(&format!("database option {}", other.as_ref()))),
        }
        Ok(())
    }

    fn get_option_string(&self, key: Self::Option) -> Result<String> {
        match key {
            OptionDatabase::Uri => self.uri.clone().ok_or_else(|| unsupported("unset uri")),
            _ => Err(unsupported("reading this option")),
        }
    }
    fn get_option_bytes(&self, _: Self::Option) -> Result<Vec<u8>> {
        Err(unsupported("bytes options"))
    }
    fn get_option_int(&self, _: Self::Option) -> Result<i64> {
        Err(unsupported("int options"))
    }
    fn get_option_double(&self, _: Self::Option) -> Result<f64> {
        Err(unsupported("double options"))
    }
}

impl adbc_core::Database for DuckFleetDatabase {
    type ConnectionType = DuckFleetConnection;

    fn new_connection(&self) -> Result<Self::ConnectionType> {
        self.new_connection_with_opts(std::iter::empty())
    }

    fn new_connection_with_opts(
        &self,
        opts: impl IntoIterator<Item = (OptionConnection, OptionValue)>,
    ) -> Result<Self::ConnectionType> {
        let (Some(uri), Some(token)) = (&self.uri, &self.token) else {
            return Err(Error::with_message_and_status(
                "set uri and password (the DuckFleet token) before connecting",
                Status::InvalidState,
            ));
        };
        let client = duckfleet::Client::new(uri, token).map_err(convert)?;
        let mut options = SessionOptions::named("adbc");
        if self.compute.mode.is_some()
            || self.compute.pool.is_some()
            || self.compute.cpu.is_some()
            || self.compute.memory.is_some()
            || self.compute.disk.is_some()
        {
            options = options.compute(self.compute.clone());
        }
        let session = runtime()
            .block_on(client.session(options))
            .map_err(convert)?;
        let mut connection = DuckFleetConnection {
            session: Arc::new(session),
            autocommit: true,
        };
        for (key, value) in opts {
            connection.set_option(key, value)?;
        }
        Ok(connection)
    }
}

pub struct DuckFleetConnection {
    session: Arc<duckfleet::Session>,
    autocommit: bool,
}

impl DuckFleetConnection {
    fn run(&self, sql: &str) -> Result<()> {
        runtime()
            .block_on(self.session.execute(sql, Vec::new()))
            .map(|_| ())
            .map_err(convert)
    }
}

impl Drop for DuckFleetConnection {
    /// Closing a DuckFleet session rolls back uncommitted work.
    fn drop(&mut self) {
        let path = self.session.path();
        let client = self.session.client().clone();
        let _ = runtime().block_on(client.request(
            duckfleet::Method::DELETE,
            &path,
            None,
            std::time::Duration::from_secs(30),
        ));
    }
}

impl Optionable for DuckFleetConnection {
    type Option = OptionConnection;

    fn set_option(&mut self, key: Self::Option, value: OptionValue) -> Result<()> {
        match key {
            OptionConnection::AutoCommit => {
                let enabled = string(value)? == "true";
                if enabled == self.autocommit {
                    return Ok(());
                }
                if enabled {
                    self.run("COMMIT")?;
                } else {
                    self.run("BEGIN")?;
                }
                self.autocommit = enabled;
                Ok(())
            }
            other => Err(unsupported(&format!(
                "connection option {}",
                other.as_ref()
            ))),
        }
    }
    fn get_option_string(&self, key: Self::Option) -> Result<String> {
        match key {
            OptionConnection::AutoCommit => Ok(self.autocommit.to_string()),
            _ => Err(unsupported("reading this option")),
        }
    }
    fn get_option_bytes(&self, _: Self::Option) -> Result<Vec<u8>> {
        Err(unsupported("bytes options"))
    }
    fn get_option_int(&self, _: Self::Option) -> Result<i64> {
        Err(unsupported("int options"))
    }
    fn get_option_double(&self, _: Self::Option) -> Result<f64> {
        Err(unsupported("double options"))
    }
}

struct SessionCancel(Arc<duckfleet::Session>);

impl CancelHandle for SessionCancel {
    fn try_cancel(&self) -> Result<()> {
        runtime()
            .block_on(self.0.cancel())
            .map(|_| ())
            .map_err(convert)
    }
}

impl adbc_core::Connection for DuckFleetConnection {
    type StatementType = DuckFleetStatement;

    fn new_statement(&mut self) -> Result<Self::StatementType> {
        Ok(DuckFleetStatement {
            session: self.session.clone(),
            sql: None,
            bound: None,
        })
    }

    fn cancel(&mut self) -> Result<()> {
        SessionCancel(self.session.clone()).try_cancel()
    }

    fn get_cancel_handle(&self) -> Box<dyn CancelHandle> {
        Box::new(SessionCancel(self.session.clone()))
    }

    fn get_info(
        &self,
        codes: Option<HashSet<InfoCode>>,
    ) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        let version = runtime()
            .block_on(self.session.execute("SELECT version()", Vec::new()))
            .map_err(convert)?
            .rows
            .first()
            .and_then(|row| row.first().cloned().flatten())
            .unwrap_or_default();
        metadata::info(codes, &version)
    }

    fn get_objects(
        &self,
        depth: ObjectDepth,
        catalog: Option<&str>,
        db_schema: Option<&str>,
        table_name: Option<&str>,
        table_type: Option<Vec<&str>>,
        column_name: Option<&str>,
    ) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        metadata::objects(
            &self.session,
            metadata::Filter {
                depth,
                catalog,
                db_schema,
                table_name,
                table_type,
                column_name,
            },
        )
    }

    fn get_table_schema(
        &self,
        catalog: Option<&str>,
        db_schema: Option<&str>,
        table_name: &str,
    ) -> Result<Schema> {
        let quote = |s: &str| format!("\"{}\"", s.replace('"', "\"\""));
        let name = [catalog, db_schema, Some(table_name)]
            .into_iter()
            .flatten()
            .map(quote)
            .collect::<Vec<_>>()
            .join(".");
        let result = runtime()
            .block_on(
                self.session
                    .execute(&format!("SELECT * FROM {name} LIMIT 0"), Vec::new()),
            )
            .map_err(convert)?;
        Ok(result.schema.as_ref().clone())
    }

    fn get_table_types(&self) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        metadata::table_types()
    }

    fn get_statistic_names(&self) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        Err(unsupported("get_statistic_names"))
    }

    fn get_statistics(
        &self,
        _catalog: Option<&str>,
        _db_schema: Option<&str>,
        _table_name: Option<&str>,
        _approximate: bool,
    ) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        Err(unsupported("get_statistics"))
    }

    fn commit(&mut self) -> Result<()> {
        if self.autocommit {
            return Err(Error::with_message_and_status(
                "commit needs autocommit disabled",
                Status::InvalidState,
            ));
        }
        self.run("COMMIT")?;
        self.run("BEGIN")
    }

    fn rollback(&mut self) -> Result<()> {
        if self.autocommit {
            return Err(Error::with_message_and_status(
                "rollback needs autocommit disabled",
                Status::InvalidState,
            ));
        }
        self.run("ROLLBACK")?;
        self.run("BEGIN")
    }

    fn read_partition(
        &self,
        _partition: impl AsRef<[u8]>,
    ) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        Err(unsupported("read_partition"))
    }
}

pub struct DuckFleetStatement {
    session: Arc<duckfleet::Session>,
    sql: Option<String>,
    bound: Option<RecordBatch>,
}

/// One bound row becomes DuckFleet's typed scalar parameters. Types the wire
/// cannot carry exactly are refused before any request.
fn parameters(batch: &RecordBatch) -> Result<Vec<Param>> {
    if batch.num_rows() != 1 {
        return Err(unsupported("binding other than exactly one parameter row"));
    }
    batch
        .columns()
        .iter()
        .map(|column| {
            if column.is_null(0) {
                return Ok(Param::Null);
            }
            Ok(match column.data_type() {
                DataType::Boolean => Param::Bool(column.as_boolean().value(0)),
                DataType::Int8 => Param::from(column.as_primitive::<Int8Type>().value(0)),
                DataType::Int16 => Param::from(column.as_primitive::<Int16Type>().value(0)),
                DataType::Int32 => Param::from(column.as_primitive::<Int32Type>().value(0)),
                DataType::Int64 => Param::from(column.as_primitive::<Int64Type>().value(0)),
                DataType::UInt8 => Param::from(column.as_primitive::<UInt8Type>().value(0)),
                DataType::UInt16 => Param::from(column.as_primitive::<UInt16Type>().value(0)),
                DataType::UInt32 => Param::from(column.as_primitive::<UInt32Type>().value(0)),
                DataType::UInt64 => Param::from(column.as_primitive::<UInt64Type>().value(0)),
                DataType::Float32 => Param::from(column.as_primitive::<Float32Type>().value(0)),
                DataType::Float64 => Param::from(column.as_primitive::<Float64Type>().value(0)),
                DataType::Utf8 => Param::from(column.as_string::<i32>().value(0)),
                DataType::LargeUtf8 => Param::from(column.as_string::<i64>().value(0)),
                DataType::Null => Param::Null,
                other => {
                    return Err(Error::with_message_and_status(
                        format!(
                            "parameter type {other} cannot be bound exactly; bind it as text and cast in SQL"
                        ),
                        Status::NotImplemented,
                    ));
                }
            })
        })
        .collect()
}

impl DuckFleetStatement {
    fn query(&mut self) -> Result<duckfleet::QueryResult> {
        let sql = self.sql.clone().ok_or_else(|| {
            Error::with_message_and_status("set_sql_query was not called", Status::InvalidState)
        })?;
        let params = match &self.bound {
            Some(batch) => parameters(batch)?,
            None => Vec::new(),
        };
        runtime()
            .block_on(self.session.query(&sql, params))
            .map_err(convert)
    }
}

impl Optionable for DuckFleetStatement {
    type Option = OptionStatement;
    fn set_option(&mut self, key: Self::Option, _: OptionValue) -> Result<()> {
        Err(unsupported(&format!("statement option {}", key.as_ref())))
    }
    fn get_option_string(&self, _: Self::Option) -> Result<String> {
        Err(unsupported("statement options"))
    }
    fn get_option_bytes(&self, _: Self::Option) -> Result<Vec<u8>> {
        Err(unsupported("statement options"))
    }
    fn get_option_int(&self, _: Self::Option) -> Result<i64> {
        Err(unsupported("statement options"))
    }
    fn get_option_double(&self, _: Self::Option) -> Result<f64> {
        Err(unsupported("statement options"))
    }
}

impl adbc_core::Statement for DuckFleetStatement {
    fn bind(&mut self, batch: RecordBatch) -> Result<()> {
        self.bound = Some(batch);
        Ok(())
    }

    fn bind_stream(&mut self, mut reader: Box<dyn RecordBatchReader + Send>) -> Result<()> {
        let batch = reader
            .next()
            .transpose()
            .map_err(|e| Error::with_message_and_status(e.to_string(), Status::InvalidData))?
            .ok_or_else(|| unsupported("an empty parameter stream"))?;
        if reader.next().is_some() {
            return Err(unsupported("binding more than one parameter batch"));
        }
        self.bind(batch)
    }

    fn execute(&mut self) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        let result = self.query()?;
        Ok(Box::new(RecordBatchIterator::new(
            result.batches.into_iter().map(Ok),
            result.schema,
        )))
    }

    fn execute_update(&mut self) -> Result<Option<i64>> {
        let result = self.query()?;
        Ok(result
            .rows
            .first()
            .and_then(|row| row.first().cloned().flatten())
            .and_then(|count| count.parse::<i64>().ok()))
    }

    fn execute_schema(&mut self) -> Result<Schema> {
        let sql = self.sql.clone().unwrap_or_default();
        self.sql = Some(format!("SELECT * FROM ({sql}) LIMIT 0"));
        let schema = self.query().map(|result| result.schema.as_ref().clone());
        self.sql = Some(sql);
        schema
    }

    fn execute_partitions(&mut self) -> Result<PartitionedResult> {
        Err(unsupported("execute_partitions"))
    }

    fn get_parameter_schema(&self) -> Result<Schema> {
        Err(unsupported("get_parameter_schema"))
    }

    fn prepare(&mut self) -> Result<()> {
        Ok(())
    }

    fn set_sql_query(&mut self, query: impl AsRef<str>) -> Result<()> {
        self.sql = Some(query.as_ref().to_owned());
        Ok(())
    }

    fn set_substrait_plan(&mut self, _plan: impl AsRef<[u8]>) -> Result<()> {
        Err(unsupported("Substrait plans"))
    }

    fn cancel(&mut self) -> Result<()> {
        SessionCancel(self.session.clone()).try_cancel()
    }

    fn get_cancel_handle(&self) -> Box<dyn CancelHandle> {
        Box::new(SessionCancel(self.session.clone()))
    }
}

adbc_ffi::export_driver!(AdbcDuckfleetInit, DuckFleetDriver);
