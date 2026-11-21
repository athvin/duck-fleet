//! Additive fallible execution API used by DuckFleet's durable jobs.
use std::{ffi::CStr, fmt, mem, ptr, sync::Arc};

use arrow::{
    array::{Array, StructArray},
    datatypes::{DataType, Schema, SchemaRef},
    ffi::{FFI_ArrowArray, FFI_ArrowSchema, from_ffi},
    record_batch::{RecordBatch, RecordBatchOptions},
};

use crate::{ffi, raw_statement::RawStatement};

/// Engine outcome, independently of schema conversion or result delivery.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EngineCompletion {
    /// No successful end-of-stream or materialized result was observed.
    Pending,
    /// Native execution completed successfully. External side effects are not
    /// covered by a general transaction or exactly-once guarantee.
    Succeeded,
    /// Native execution reported an error before a success milestone.
    Failed,
}

/// Which phase failed. A conversion failure does not undo completed SQL.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StreamErrorKind {
    /// Parameters could not be bound; execution was not attempted.
    Binding,
    /// Native execution or a later native fetch reported an error.
    Engine,
    /// Converting executed result metadata or data to Arrow failed.
    Conversion,
}

/// An owned diagnostic: no reference into a destroyed native result or error.
#[derive(Clone, Debug)]
pub struct StreamError {
    /// The phase that failed, independently of the engine completion state.
    pub kind: StreamErrorKind,
    /// Owned diagnostic text. Applications must redact secrets before exposure.
    pub message: String,
    /// Native error category, when the C API supplied one.
    pub native_code: Option<ffi::duckdb_error_type>,
}

impl fmt::Display for StreamError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}
impl std::error::Error for StreamError {}
impl StreamError {
    pub(crate) fn binding(error: crate::Error) -> Self {
        Self {
            kind: StreamErrorKind::Binding,
            message: error.to_string(),
            native_code: None,
        }
    }
    fn conversion(message: impl fmt::Display) -> Self {
        Self {
            kind: StreamErrorKind::Conversion,
            message: message.to_string(),
            native_code: None,
        }
    }
    fn engine(message: impl Into<String>, native_code: Option<ffi::duckdb_error_type>) -> Self {
        Self {
            kind: StreamErrorKind::Engine,
            message: message.into(),
            native_code,
        }
    }
}

struct NativeResult(ffi::duckdb_result);
impl NativeResult {
    fn error(&mut self) -> Option<StreamError> {
        let message = unsafe { ffi::duckdb_result_error(&mut self.0) };
        if message.is_null() {
            None
        } else {
            Some(StreamError::engine(
                unsafe { CStr::from_ptr(message) }.to_string_lossy().into_owned(),
                Some(unsafe { ffi::duckdb_result_error_type(&mut self.0) }),
            ))
        }
    }
}
impl Drop for NativeResult {
    fn drop(&mut self) {
        unsafe { ffi::duckdb_destroy_result(&mut self.0) };
    }
}
struct ArrowOptions(ffi::duckdb_arrow_options);
impl Drop for ArrowOptions {
    fn drop(&mut self) {
        unsafe { ffi::duckdb_destroy_arrow_options(&mut self.0) };
    }
}
struct LogicalType(ffi::duckdb_logical_type);
impl Drop for LogicalType {
    fn drop(&mut self) {
        unsafe { ffi::duckdb_destroy_logical_type(&mut self.0) };
    }
}
struct Chunk(ffi::duckdb_data_chunk);
impl Drop for Chunk {
    fn drop(&mut self) {
        unsafe { ffi::duckdb_destroy_data_chunk(&mut self.0) };
    }
}
struct NativeError(ffi::duckdb_error_data);
impl Drop for NativeError {
    fn drop(&mut self) {
        unsafe { ffi::duckdb_destroy_error_data(&mut self.0) };
    }
}
fn conversion_result(error: ffi::duckdb_error_data) -> Result<(), StreamError> {
    let error = NativeError(error);
    if error.0.is_null() || !unsafe { ffi::duckdb_error_data_has_error(error.0) } {
        return Ok(());
    }
    let message = unsafe { ffi::duckdb_error_data_message(error.0) };
    Err(StreamError {
        kind: StreamErrorKind::Conversion,
        message: if message.is_null() {
            "native Arrow conversion failed".to_owned()
        } else {
            unsafe { CStr::from_ptr(message) }.to_string_lossy().into_owned()
        },
        native_code: Some(unsafe { ffi::duckdb_error_data_error_type(error.0) }),
    })
}

// These are public Arrow C Data Interface records, not Rust object layouts.
// libduckdb-sys additionally tests each field offset against arrow-rs.
const _: () = {
    assert!(mem::size_of::<FFI_ArrowArray>() == mem::size_of::<ffi::ArrowArray>());
    assert!(mem::align_of::<FFI_ArrowArray>() == mem::align_of::<ffi::ArrowArray>());
    assert!(mem::size_of::<FFI_ArrowSchema>() == mem::size_of::<ffi::ArrowSchema>());
    assert!(mem::align_of::<FFI_ArrowSchema>() == mem::align_of::<ffi::ArrowSchema>());
};

/// One executed native result, borrowing its statement for the whole lifetime.
///
/// No iterator facade is provided: every fetch is fallible, including EOF. The
/// native result may be materialized even when streaming is requested. Call
/// `completion` immediately after starting, before fallible `schema`/encoding.
/// Drop frees the result without draining it or claiming successful completion.
#[must_use]
pub struct EngineStream<'stmt> {
    _statement: &'stmt mut RawStatement,
    ffi_schema: Option<FFI_ArrowSchema>,
    schema: Option<SchemaRef>,
    options: Option<ArrowOptions>,
    result: NativeResult,
    completion: EngineCompletion,
    failure: Option<StreamError>,
    eof: bool,
}

impl<'stmt> EngineStream<'stmt> {
    pub(crate) fn start(statement: &'stmt mut RawStatement) -> Result<Self, StreamError> {
        statement.reset_result();
        let mut result = NativeResult(unsafe { mem::zeroed() });
        let status = unsafe { ffi::duckdb_execute_prepared_streaming(statement.ptr(), &mut result.0) };
        if let Some(error) = result.error() {
            return Err(error);
        }
        if status != ffi::DuckDBSuccess || result.0.internal_data.is_null() {
            return Err(StreamError::engine(
                "native execution failed without a result diagnostic",
                None,
            ));
        }
        let completion = if unsafe { ffi::duckdb_result_is_streaming(result.0) } {
            EngineCompletion::Pending
        } else {
            EngineCompletion::Succeeded
        };
        // No schema conversion may precede returning this completion evidence.
        Ok(Self {
            _statement: statement,
            ffi_schema: None,
            schema: None,
            options: None,
            result,
            completion,
            failure: None,
            eof: false,
        })
    }

    /// The engine milestone observed so far, unaffected by delivery failures.
    pub fn completion(&self) -> EngineCompletion {
        self.completion
    }

    /// Import the schema and Arrow options from this executed result, not the
    /// prepared statement's possibly stale/unbound metadata.
    pub fn schema(&mut self) -> Result<SchemaRef, StreamError> {
        if let Some(error) = &self.failure {
            return Err(error.clone());
        }
        if let Some(schema) = &self.schema {
            return Ok(schema.clone());
        }
        let outcome = self.import_schema();
        if let Err(error) = &outcome {
            self.failure = Some(error.clone());
        }
        outcome
    }

    fn import_schema(&mut self) -> Result<SchemaRef, StreamError> {
        let options = ArrowOptions(unsafe { ffi::duckdb_result_get_arrow_options(&mut self.result.0) });
        if options.0.is_null() {
            return Err(StreamError::conversion("missing result Arrow options"));
        }
        let count = unsafe { ffi::duckdb_column_count(&mut self.result.0) };
        let mut types = Vec::new();
        let mut names = Vec::new();
        for column in 0..count {
            let ty = LogicalType(unsafe { ffi::duckdb_column_logical_type(&mut self.result.0, column) });
            let name = unsafe { ffi::duckdb_column_name(&mut self.result.0, column) };
            if ty.0.is_null() || name.is_null() {
                return Err(StreamError::conversion("missing executed column metadata"));
            }
            types.push(ty);
            names.push(name);
        }
        let mut raw_types = types.iter().map(|ty| ty.0).collect::<Vec<_>>();
        let mut ffi_schema = FFI_ArrowSchema::empty();
        // C conversion copies metadata into its independently owned schema.
        conversion_result(unsafe {
            ffi::duckdb_to_arrow_schema(
                options.0,
                raw_types.as_mut_ptr(),
                names.as_mut_ptr(),
                count,
                ptr::from_mut(&mut ffi_schema).cast(),
            )
        })?;
        let schema = Arc::new(Schema::try_from(&ffi_schema).map_err(StreamError::conversion)?);
        self.options = Some(options);
        self.ffi_schema = Some(ffi_schema);
        self.schema = Some(schema.clone());
        Ok(schema)
    }

    /// Fetch one batch. A null native chunk is successful EOF only after checking
    /// the live result error (including late execution/autocommit errors).
    pub fn next_batch(&mut self) -> Result<Option<RecordBatch>, StreamError> {
        if let Some(error) = &self.failure {
            return Err(error.clone());
        }
        if self.eof {
            return Ok(None);
        }
        let outcome = self.fetch();
        if let Err(error) = &outcome {
            if error.kind == StreamErrorKind::Engine && self.completion == EngineCompletion::Pending {
                self.completion = EngineCompletion::Failed;
            }
            self.failure = Some(error.clone());
        }
        outcome
    }

    fn fetch(&mut self) -> Result<Option<RecordBatch>, StreamError> {
        let schema = self.schema()?;
        let chunk = Chunk(unsafe { ffi::duckdb_fetch_chunk(self.result.0) });
        if chunk.0.is_null() {
            if let Some(error) = self.result.error() {
                return Err(error);
            }
            self.eof = true;
            self.completion = EngineCompletion::Succeeded;
            return Ok(None);
        }
        let rows =
            usize::try_from(unsafe { ffi::duckdb_data_chunk_get_size(chunk.0) }).map_err(StreamError::conversion)?;
        let mut array = FFI_ArrowArray::empty();
        conversion_result(unsafe {
            ffi::duckdb_data_chunk_to_arrow(
                self.options.as_ref().expect("schema initialized options").0,
                chunk.0,
                ptr::from_mut(&mut array).cast(),
            )
        })?;
        if array.is_empty() {
            return Err(StreamError::conversion("native chunk produced no Arrow array"));
        }
        let data = unsafe { from_ffi(array, self.ffi_schema.as_ref().expect("schema initialized FFI schema")) }
            .map_err(StreamError::conversion)?;
        data.validate_full().map_err(StreamError::conversion)?;
        if !matches!(data.data_type(), DataType::Struct(_)) || data.len() != rows {
            return Err(StreamError::conversion("native batch schema or row count mismatch"));
        }
        let structure = StructArray::from(data);
        if structure.null_count() != 0 {
            return Err(StreamError::conversion("native result contains null top-level rows"));
        }
        let batch = RecordBatch::try_new_with_options(
            schema,
            structure.columns().to_vec(),
            &RecordBatchOptions::new().with_row_count(Some(rows)),
        )
        .map_err(StreamError::conversion)?;
        Ok(Some(batch))
    }
}
