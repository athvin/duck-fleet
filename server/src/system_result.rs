//! Bounded, exact system-query results. JSON encodes integers and decimals as
//! strings; Arrow preserves their native types. The response owns its memory slot.
use crate::{
    ApiError, failure,
    metadata::{ResultType, SystemType},
    system_sql::OutputColumn,
};
use arrow_array::{
    ArrayRef, BooleanArray, Decimal128Array, Float64Array, Int64Array, RecordBatch, StringArray,
    TimestampMicrosecondArray,
};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use axum::{
    body::Bytes,
    http::header,
    response::{IntoResponse, Response},
};
use chrono::{DateTime, SecondsFormat, Utc};
use serde::Deserialize;
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    io::{self, Write},
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::{sync::OwnedSemaphorePermit, time::Instant};

pub(crate) const MAX_ROWS: usize = 10_000;
pub(crate) const MAX_BYTES: usize = 8 * 1024 * 1024;

#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Format {
    #[default]
    Json,
    Arrow,
}

pub(crate) struct SystemResult {
    // Drop this before bytes/permit so a completed response cannot leave its
    // timer armed while the HTTP connection starts another response.
    _delivery: Option<crate::http_transport::DeliveryGuard>,
    bytes: Vec<u8>,
    format: Format,
    // Retained by Bytes::from_owner until the HTTP body is delivered/dropped, or
    // by the pgwire response until its final bounded write finishes.
    _permit: Option<OwnedSemaphorePermit>,
}
impl SystemResult {
    pub fn with_http_delivery(mut self, connection: &crate::http_transport::Connection) -> Self {
        self._delivery = Some(connection.delivery());
        self
    }

    pub fn arrow(&self) -> &[u8] {
        debug_assert_eq!(self.format, Format::Arrow);
        &self.bytes
    }
    #[cfg(test)]
    pub fn from_arrow_for_test(bytes: Vec<u8>, permit: Option<OwnedSemaphorePermit>) -> Self {
        Self {
            _delivery: None,
            bytes,
            format: Format::Arrow,
            _permit: permit,
        }
    }
}
impl AsRef<[u8]> for SystemResult {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}
impl IntoResponse for SystemResult {
    fn into_response(self) -> Response {
        let content_type = match self.format {
            Format::Json => "application/json",
            Format::Arrow => "application/vnd.apache.arrow.stream",
        };
        (
            [
                (header::CONTENT_TYPE, content_type),
                (header::CACHE_CONTROL, "no-store"),
            ],
            Bytes::from_owner(self),
        )
            .into_response()
    }
}

#[derive(Clone, Debug)]
pub(crate) enum Cell {
    Null,
    Bool(bool),
    I64(i64),
    F64(f64),
    Text(String),
    Timestamp(i64),
    Decimal(i128),
}
pub(crate) struct Table {
    pub columns: Vec<OutputColumn>,
    pub rows: Vec<Vec<Cell>>,
}
pub(crate) struct Snapshot {
    pub at: SystemTime,
    pub namespace: String,
    pub bounds: Value,
}

pub(crate) fn unavailable_result() -> ApiError {
    ApiError::new(
        failure::SYSTEM_QUERY_RESULT,
        "system query returned an unsupported or invalid typed result",
    )
}
pub(crate) fn too_wide() -> ApiError {
    ApiError::new(
        failure::SYSTEM_QUERY_TOO_WIDE,
        "system query exceeds its row or byte limit; narrow the source bounds or projection",
    )
}
pub(crate) fn timeout() -> ApiError {
    ApiError::new(
        failure::SYSTEM_QUERY_TIMEOUT,
        "system query exceeded its three-second deadline",
    )
}
pub(crate) fn micros(time: SystemTime) -> Result<i64, ApiError> {
    let value = match time.duration_since(UNIX_EPOCH) {
        Ok(value) => i128::try_from(value.as_micros()).map_err(|_| unavailable_result())?,
        Err(error) => {
            -i128::try_from(error.duration().as_micros()).map_err(|_| unavailable_result())?
        }
    };
    i64::try_from(value).map_err(|_| unavailable_result())
}
pub(crate) fn timestamp(value: i64) -> Result<String, ApiError> {
    DateTime::<Utc>::from_timestamp_micros(value)
        .map(|v| v.to_rfc3339_opts(SecondsFormat::Micros, true))
        .ok_or_else(unavailable_result)
}
fn decimal_text(value: i128, scale: u8) -> String {
    if scale == 0 {
        return value.to_string();
    }
    let digits = value.unsigned_abs().to_string();
    let width = usize::from(scale) + 1;
    let padded = format!("{digits:0>width$}");
    let split = padded.len() - usize::from(scale);
    format!(
        "{}{}.{}",
        if value < 0 { "-" } else { "" },
        &padded[..split],
        &padded[split..]
    )
}
fn decimal(text: &str, precision: u8, scale: u8) -> Result<i128, ApiError> {
    if precision == 0 || precision > 38 || scale > precision || text.len() > 80 {
        return Err(unavailable_result());
    }
    let (negative, text) = text.strip_prefix('-').map_or((false, text), |v| (true, v));
    let (whole, fraction) = text.split_once('.').unwrap_or((text, ""));
    if whole.is_empty()
        || !whole.bytes().all(|b| b.is_ascii_digit())
        || !fraction.bytes().all(|b| b.is_ascii_digit())
        || fraction.len() > usize::from(scale)
    {
        return Err(unavailable_result());
    }
    let mut value = 0_i128;
    for digit in whole
        .bytes()
        .chain(fraction.bytes())
        .chain(std::iter::repeat_n(
            b'0',
            usize::from(scale) - fraction.len(),
        ))
    {
        value = value
            .checked_mul(10)
            .and_then(|v| v.checked_add(i128::from(digit - b'0')))
            .ok_or_else(unavailable_result)?;
    }
    if value >= 10_i128.pow(u32::from(precision)) {
        return Err(unavailable_result());
    }
    Ok(if negative { -value } else { value })
}

pub(crate) fn row(
    row: &tokio_postgres::Row,
    columns: &[OutputColumn],
) -> Result<Vec<Cell>, ApiError> {
    if row.len() != columns.len() {
        return Err(unavailable_result());
    }
    columns
        .iter()
        .enumerate()
        .map(|(index, column)| {
            let cell = match column.ty {
                ResultType::Scalar(SystemType::Bool) => row
                    .try_get::<_, Option<bool>>(index)
                    .map_err(|_| unavailable_result())?
                    .map(Cell::Bool),
                ResultType::Scalar(SystemType::Int64) => row
                    .try_get::<_, Option<i64>>(index)
                    .map_err(|_| unavailable_result())?
                    .map(Cell::I64),
                ResultType::Scalar(SystemType::Float64) => row
                    .try_get::<_, Option<f64>>(index)
                    .map_err(|_| unavailable_result())?
                    .map(Cell::F64),
                ResultType::Scalar(SystemType::Utf8 | SystemType::JsonText) => row
                    .try_get::<_, Option<String>>(index)
                    .map_err(|_| unavailable_result())?
                    .map(Cell::Text),
                ResultType::Scalar(SystemType::TimestampUtc) => row
                    .try_get::<_, Option<SystemTime>>(index)
                    .map_err(|_| unavailable_result())?
                    .map(|v| micros(v).map(Cell::Timestamp))
                    .transpose()?,
                ResultType::Decimal128 { precision, scale } => row
                    .try_get::<_, Option<String>>(index)
                    .map_err(|_| unavailable_result())?
                    .map(|v| decimal(&v, precision, scale).map(Cell::Decimal))
                    .transpose()?,
            }
            .unwrap_or(Cell::Null);
            if matches!(cell, Cell::F64(value) if !value.is_finite())
                || (matches!(cell, Cell::Null) && !column.nullable)
            {
                return Err(unavailable_result());
            }
            Ok(cell)
        })
        .collect()
}

pub(crate) fn type_name(ty: ResultType) -> String {
    match ty {
        ResultType::Scalar(SystemType::Bool) => "BOOLEAN".into(),
        ResultType::Scalar(SystemType::Int64) => "BIGINT".into(),
        ResultType::Scalar(SystemType::Float64) => "DOUBLE".into(),
        ResultType::Scalar(SystemType::Utf8) => "VARCHAR".into(),
        ResultType::Scalar(SystemType::JsonText) => "JSON_TEXT".into(),
        ResultType::Scalar(SystemType::TimestampUtc) => "TIMESTAMPTZ".into(),
        ResultType::Decimal128 { precision, scale } => format!("DECIMAL({precision},{scale})"),
    }
}
pub(crate) fn descriptor(column: &OutputColumn) -> Value {
    json!({"name": column.name, "type": type_name(column.ty), "nullable": column.nullable, "units": column.units})
}
fn json_cell(cell: &Cell, ty: ResultType) -> Result<Value, ApiError> {
    Ok(match cell {
        Cell::Null => Value::Null,
        Cell::Bool(v) => json!(v),
        Cell::I64(v) => json!(v.to_string()),
        Cell::F64(v) if v.is_finite() => json!(v),
        Cell::Text(v) => json!(v),
        Cell::Timestamp(v) => json!(timestamp(*v)?),
        Cell::Decimal(v) => match ty {
            ResultType::Decimal128 { scale, .. } => json!(decimal_text(*v, scale)),
            _ => return Err(unavailable_result()),
        },
        _ => return Err(unavailable_result()),
    })
}
fn arrow_type(ty: ResultType) -> DataType {
    match ty {
        ResultType::Scalar(SystemType::Bool) => DataType::Boolean,
        ResultType::Scalar(SystemType::Int64) => DataType::Int64,
        ResultType::Scalar(SystemType::Float64) => DataType::Float64,
        ResultType::Scalar(SystemType::Utf8 | SystemType::JsonText) => DataType::Utf8,
        ResultType::Scalar(SystemType::TimestampUtc) => {
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
        }
        ResultType::Decimal128 { precision, scale } => DataType::Decimal128(precision, scale as i8),
    }
}
fn array(table: &Table, index: usize) -> Result<ArrayRef, ApiError> {
    macro_rules! collect {
        ($variant:ident) => {
            table
                .rows
                .iter()
                .map(|r| match r.get(index) {
                    Some(Cell::Null) => Ok(None),
                    Some(Cell::$variant(v)) => Ok(Some(*v)),
                    _ => Err(unavailable_result()),
                })
                .collect::<Result<Vec<_>, _>>()?
        };
    }
    Ok(match table.columns[index].ty {
        ResultType::Scalar(SystemType::Bool) => Arc::new(BooleanArray::from(collect!(Bool))),
        ResultType::Scalar(SystemType::Int64) => Arc::new(Int64Array::from(collect!(I64))),
        ResultType::Scalar(SystemType::Float64) => Arc::new(Float64Array::from(collect!(F64))),
        ResultType::Scalar(SystemType::Utf8 | SystemType::JsonText) => Arc::new(StringArray::from(
            table
                .rows
                .iter()
                .map(|r| match r.get(index) {
                    Some(Cell::Null) => Ok(None),
                    Some(Cell::Text(v)) => Ok(Some(v.as_str())),
                    _ => Err(unavailable_result()),
                })
                .collect::<Result<Vec<_>, _>>()?,
        )),
        ResultType::Scalar(SystemType::TimestampUtc) => {
            Arc::new(TimestampMicrosecondArray::from(collect!(Timestamp)).with_timezone("UTC"))
        }
        ResultType::Decimal128 { precision, scale } => Arc::new(
            Decimal128Array::from(collect!(Decimal))
                .with_precision_and_scale(precision, scale as i8)
                .map_err(|_| unavailable_result())?,
        ),
    })
}
struct BoundedWriter {
    bytes: Vec<u8>,
    deadline: Instant,
    failure: Option<WriterFailure>,
}
#[derive(Clone, Copy)]
enum WriterFailure {
    Size,
    Time,
}
impl BoundedWriter {
    fn error(&self) -> ApiError {
        match self.failure {
            Some(WriterFailure::Size) => too_wide(),
            Some(WriterFailure::Time) => timeout(),
            None => unavailable_result(),
        }
    }
}
impl Write for BoundedWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.failure = if Instant::now() >= self.deadline {
            Some(WriterFailure::Time)
        } else if self.bytes.len().saturating_add(buf.len()) > MAX_BYTES {
            Some(WriterFailure::Size)
        } else {
            None
        };
        if self.failure.is_some() {
            return Err(io::Error::other("bounded system output refused"));
        }
        self.bytes.extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
pub(crate) fn encode(
    table: Table,
    snapshot: Snapshot,
    format: Format,
    permit: OwnedSemaphorePermit,
    deadline: Instant,
) -> Result<SystemResult, ApiError> {
    if table.rows.len() > MAX_ROWS || table.columns.len() > 32 {
        return Err(too_wide());
    }
    if table.rows.iter().any(|r| r.len() != table.columns.len()) {
        return Err(unavailable_result());
    }
    let at = timestamp(micros(snapshot.at)?)?;
    let mut writer = BoundedWriter {
        bytes: Vec::new(),
        deadline,
        failure: None,
    };
    match format {
        Format::Json => {
            let rows = table
                .rows
                .iter()
                .map(|r| {
                    r.iter()
                        .zip(&table.columns)
                        .map(|(v, c)| json_cell(v, c.ty))
                        .collect::<Result<Vec<_>, _>>()
                })
                .collect::<Result<Vec<_>, _>>()?;
            let value = json!({"system_contract_version":1,"snapshot_at":at,"namespace_id":snapshot.namespace,"applied_bounds":snapshot.bounds,"columns":table.columns.iter().map(descriptor).collect::<Vec<_>>(),"row_count":rows.len(),"rows":rows});
            serde_json::to_writer(&mut writer, &value).map_err(|_| writer.error())?;
        }
        Format::Arrow => {
            let fields = table
                .columns
                .iter()
                .map(|c| {
                    let mut metadata = HashMap::from([("system_type".into(), type_name(c.ty))]);
                    if let Some(units) = c.units {
                        metadata.insert("units".into(), units.into());
                    }
                    Field::new(&c.name, arrow_type(c.ty), c.nullable).with_metadata(metadata)
                })
                .collect::<Vec<_>>();
            let schema = Arc::new(Schema::new(fields).with_metadata(HashMap::from([
                ("system_contract_version".into(), "1".into()),
                ("snapshot_at".into(), at),
                ("namespace_id".into(), snapshot.namespace),
                ("applied_bounds".into(), snapshot.bounds.to_string()),
            ])));
            let arrays = (0..table.columns.len())
                .map(|i| array(&table, i))
                .collect::<Result<Vec<_>, _>>()?;
            let batch =
                RecordBatch::try_new(schema.clone(), arrays).map_err(|_| unavailable_result())?;
            let encoded = (|| -> Result<(), arrow_schema::ArrowError> {
                let mut stream = arrow_ipc::writer::StreamWriter::try_new(&mut writer, &schema)?;
                stream.write(&batch)?;
                stream.finish()
            })();
            encoded.map_err(|_| writer.error())?;
        }
    }
    if Instant::now() >= deadline {
        return Err(timeout());
    }
    Ok(SystemResult {
        _delivery: None,
        bytes: writer.bytes,
        format,
        _permit: Some(permit),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn decimal_and_utc_values_are_exact() {
        let value = decimal("9007199254740993", 38, 0).unwrap();
        assert_eq!(decimal_text(value, 0), "9007199254740993");
        assert_eq!(
            decimal_text(decimal("-0.000001", 38, 6).unwrap(), 6),
            "-0.000001"
        );
        assert!(decimal("1.0000001", 38, 6).is_err());
        assert!(decimal("NaN", 38, 0).is_err());
        assert!(decimal("100000000000000000000000000000000000000", 38, 0).is_err());
        assert_eq!(timestamp(-1).unwrap(), "1969-12-31T23:59:59.999999Z");
    }
    #[tokio::test]
    async fn response_retains_memory_admission_until_body_drop() {
        let slots = Arc::new(tokio::sync::Semaphore::new(1));
        let result = SystemResult::from_arrow_for_test(
            vec![1, 2, 3],
            Some(slots.clone().acquire_owned().await.unwrap()),
        );
        let response = result.into_response();
        assert_eq!(slots.available_permits(), 0);
        drop(response);
        assert_eq!(slots.available_permits(), 1);
    }
}
