//! `duckfleet export`: turn a query or a completed durable job into one local
//! file. Exports always travel through the durable-job result path, so the
//! interactive row/byte caps never change. Verified Arrow IPC chunks land in a
//! sibling `<output>.duckfleet-partial` directory, are converted one chunk at a
//! time, and the final file is published only after every chunk verified.

use crate::client::{Client, RemoteError};
use arrow_array::RecordBatch;
use arrow_schema::{DataType, Schema, SchemaRef};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Format {
    Csv,
    Json,
    Arrow,
    Parquet,
}

impl Format {
    pub fn name(self) -> &'static str {
        match self {
            Format::Csv => "csv",
            Format::Json => "json",
            Format::Arrow => "arrow",
            Format::Parquet => "parquet",
        }
    }
}

pub enum Source {
    Sql(Box<duckfleet::protocol::JobRequest>),
    Job(Uuid),
}

pub struct Options {
    pub source: Source,
    pub output: String,
    pub format: Format,
    pub force: bool,
    pub resume: bool,
    pub delimiter: u8,
}

pub fn infer_format(output: &str, explicit: Option<&str>) -> Result<Format, String> {
    let parse = |name: &str| match name {
        "csv" => Some(Format::Csv),
        "json" => Some(Format::Json),
        "arrow" => Some(Format::Arrow),
        "parquet" => Some(Format::Parquet),
        _ => None,
    };
    if let Some(name) = explicit {
        return parse(name).ok_or_else(|| "--format must be csv, json, arrow, or parquet".into());
    }
    let lower = output.to_ascii_lowercase();
    lower
        .rsplit_once('.')
        .and_then(|(_, extension)| match extension {
            "arrows" | "ipc" | "feather" => Some(Format::Arrow),
            other => parse(other),
        })
        .ok_or_else(|| {
            "cannot infer export format from the output name; use --format csv|json|arrow|parquet"
                .into()
        })
}

/// The partial directory sits next to the destination so the final publish is
/// a same-filesystem link or rename, never a copy.
pub fn partial_directory(output: &Path) -> PathBuf {
    let name = output
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_else(|| "export".into());
    output.with_file_name(format!("{name}.duckfleet-partial"))
}

/// Per-format type support is checked against the manifest schema before any
/// chunk is downloaded, so an unsupported conversion never publishes a file.
pub fn check_schema(format: Format, schema: &Schema) -> Result<(), String> {
    if format == Format::Csv
        && let Some(field) = schema.fields().iter().find(|field| {
            field.data_type().is_nested() || matches!(field.data_type(), DataType::Dictionary(..))
        })
    {
        return Err(format!(
            "CSV cannot represent nested column {} ({}); export it as json, arrow, or parquet, or cast it to VARCHAR in SQL",
            field.name(),
            field.data_type()
        ));
    }
    Ok(())
}

pub async fn run(url: &str, token: &str, options: Options) -> Result<(), RemoteError> {
    let output = PathBuf::from(&options.output);
    let partial = partial_directory(&output);
    let partial_exists = tokio::fs::symlink_metadata(&partial).await.is_ok();
    if options.resume && matches!(options.source, Source::Sql(_)) {
        return Err(invalid(
            "--resume needs --job <request-id>; SQL is never resubmitted to resume a download",
        ));
    }
    if partial_exists && !options.resume {
        return Err(invalid(format!(
            "a partial export already exists at {}; rerun with --job <request-id> --resume or remove it",
            partial.display()
        )));
    }
    if !options.force && tokio::fs::symlink_metadata(&output).await.is_ok() {
        return Err(invalid(format!(
            "{} already exists; pass --force to replace it after a verified download",
            output.display()
        )));
    }
    let client = Client::new(url, token)?;
    let id = match &options.source {
        Source::Sql(request) => {
            let id = request.request_id;
            eprintln!("sql: request_id {id}; submitting durable export job");
            client.submit_job(request).await?;
            wait_for_sql(&client, id).await?;
            id
        }
        Source::Job(id) => *id,
    };
    let delivered = deliver(&client, id, &output, &partial, &options).await;
    delivered.map_err(|mut error| {
        error.request_id.get_or_insert_with(|| id.to_string().into());
        error.retry_safe = false;
        error.message = format!(
            "{}; the job result was not exported (request_id: {id}); download again with `duckfleet export --job {id} --output {} --resume`; SQL was not resubmitted",
            error.message,
            output.display()
        );
        error
    })
}

async fn wait_for_sql(client: &Client, id: Uuid) -> Result<(), RemoteError> {
    loop {
        let status = tokio::select! {
            status = client.job(id) => status?,
            _ = tokio::signal::ctrl_c() => {
                eprintln!("canceling export job request_id: {id}");
                let _ = client.cancel_job(id).await;
                return Err(job_error(id, "export_canceled", "cancellation was requested; inspect the job before exporting it; SQL was not resubmitted"));
            }
        };
        if status["state"] == "terminal" || status["request"]["state"] == "terminal" {
            if status["status"] != "succeeded" {
                return Err(RemoteError {
                    status: None,
                    failure_kind: status["failure_kind"]
                        .as_str()
                        .map(|kind| kind.to_owned())
                        .or_else(|| Some("export_sql_failed".into())),
                    retry_safe: false,
                    message: format!(
                        "export SQL finished with status {}; no file was written",
                        status["status"]
                    ),
                    request_id: Some(id.to_string().into()),
                    acceptance: Some("accepted".into()),
                    status_url: Some(format!("/v1/jobs/{id}").into()),
                });
            }
            eprintln!(
                "sql: succeeded (request_id {id}, {} rows)",
                status["result_rows"]
            );
            if status["result_state"] != "ready" {
                return Err(job_error(
                    id,
                    "result_unavailable",
                    &format!(
                        "SQL succeeded but its result is {}; nothing was exported and the SQL must not be treated as safe to retry",
                        status["result_state"]
                    ),
                ));
            }
            return Ok(());
        }
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_millis(500)) => {}
            _ = tokio::signal::ctrl_c() => {
                eprintln!("canceling export job request_id: {id}");
                let _ = client.cancel_job(id).await;
                return Err(job_error(id, "export_canceled", "cancellation was requested; inspect the job before exporting it; SQL was not resubmitted"));
            }
        }
    }
}

async fn deliver(
    client: &Client,
    id: Uuid,
    output: &Path,
    partial: &Path,
    options: &Options,
) -> Result<(), RemoteError> {
    let resume = options.resume && tokio::fs::symlink_metadata(partial).await.is_ok();
    let preview = client.job_result(id).await?;
    if let Ok(manifest) = serde_json::from_value::<duckfleet::protocol::Manifest>(preview.clone()) {
        check_schema(options.format, &manifest.schema)
            .map_err(|message| job_error(id, "unsupported_export_type", &message))?;
    }
    eprintln!(
        "download: verifying result chunks into {}",
        partial.display()
    );
    let manifest =
        crate::cli::download_job_result(client, id, &partial.to_string_lossy(), resume).await?;
    check_schema(options.format, &manifest.schema)
        .map_err(|message| job_error(id, "unsupported_export_type", &message))?;
    let chunks = manifest
        .chunks
        .iter()
        .map(|chunk| partial.join(format!("{:06}.arrow", chunk.chunk_id)))
        .collect::<Vec<_>>();
    let temporary = partial.join(format!("output.{}", options.format.name()));
    let schema = Arc::new(manifest.schema.clone());
    let (format, delimiter) = (options.format, options.delimiter);
    let target = temporary.clone();
    let rows =
        tokio::task::spawn_blocking(move || convert(format, schema, &chunks, &target, delimiter))
            .await
            .map_err(|_| job_error(id, "export_conversion_failed", "conversion task failed"))?
            .map_err(|message| job_error(id, "export_conversion_failed", &message))?;
    if rows != manifest.rows {
        return Err(job_error(
            id,
            "export_conversion_failed",
            "converted row count differs from the verified manifest",
        ));
    }
    publish(&temporary, output, options.force)
        .await
        .map_err(|message| job_error(id, "export_publish_failed", &message))?;
    tokio::fs::remove_dir_all(partial).await.map_err(|_| {
        job_error(
            id,
            "export_cleanup_failed",
            "the export was published but its partial directory could not be removed",
        )
    })?;
    println!(
        "exported {rows} rows as {} to {} (request_id: {id})",
        options.format.name(),
        output.display()
    );
    Ok(())
}

/// Without `--force`, a hard link publishes atomically and refuses to replace
/// a destination created meanwhile. With it, rename replaces only the named
/// file, and only after the complete verified file exists.
async fn publish(temporary: &Path, output: &Path, force: bool) -> Result<(), String> {
    if force {
        if tokio::fs::symlink_metadata(output)
            .await
            .is_ok_and(|meta| meta.is_dir())
        {
            return Err("--force replaces files only, not directories".into());
        }
        tokio::fs::rename(temporary, output)
            .await
            .map_err(|error| format!("cannot replace {}: {error}", output.display()))
    } else {
        tokio::fs::hard_link(temporary, output)
            .await
            .map_err(|error| {
                format!(
                    "cannot publish {} without replacing an existing file: {error}",
                    output.display()
                )
            })?;
        tokio::fs::remove_file(temporary)
            .await
            .map_err(|error| format!("cannot remove the temporary export: {error}"))
    }
}

enum Sink {
    Csv(arrow_csv::Writer<BufWriter<std::fs::File>>),
    Json(arrow_json::ArrayWriter<BufWriter<std::fs::File>>),
    Arrow(arrow_ipc::writer::FileWriter<BufWriter<std::fs::File>>),
    Parquet(parquet::arrow::ArrowWriter<std::fs::File>),
}

const PARQUET_ROW_GROUP_ROWS: usize = 122_880;
const PARQUET_BUFFER_BYTES: usize = 64 * 1024 * 1024;

/// Streams every verified chunk into the destination format. Memory is bounded
/// by one chunk plus, for Parquet, one in-progress row group.
pub fn convert(
    format: Format,
    schema: SchemaRef,
    chunks: &[PathBuf],
    target: &Path,
    delimiter: u8,
) -> Result<u64, String> {
    let file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(target)
        .map_err(|error| format!("cannot create the temporary export: {error}"))?;
    let bad =
        |error: &dyn std::fmt::Display| format!("{} conversion failed: {error}", format.name());
    let mut sink = match format {
        Format::Csv => Sink::Csv(
            arrow_csv::WriterBuilder::new()
                .with_header(true)
                .with_delimiter(delimiter)
                .with_null(String::new())
                .build(BufWriter::new(file)),
        ),
        Format::Json => Sink::Json(arrow_json::ArrayWriter::new(BufWriter::new(file))),
        Format::Arrow => Sink::Arrow(
            arrow_ipc::writer::FileWriter::try_new(BufWriter::new(file), &schema)
                .map_err(|error| bad(&error))?,
        ),
        Format::Parquet => {
            let properties = parquet::file::properties::WriterProperties::builder()
                .set_compression(parquet::basic::Compression::SNAPPY)
                .set_max_row_group_row_count(Some(PARQUET_ROW_GROUP_ROWS))
                .build();
            Sink::Parquet(
                parquet::arrow::ArrowWriter::try_new(file, schema.clone(), Some(properties))
                    .map_err(|error| bad(&error))?,
            )
        }
    };
    let mut write = |batch: &RecordBatch| -> Result<(), String> {
        match &mut sink {
            Sink::Csv(writer) => writer.write(batch).map_err(|error| bad(&error)),
            Sink::Json(writer) => writer.write(batch).map_err(|error| bad(&error)),
            Sink::Arrow(writer) => writer.write(batch).map_err(|error| bad(&error)),
            Sink::Parquet(writer) => {
                writer.write(batch).map_err(|error| bad(&error))?;
                if writer.in_progress_size() > PARQUET_BUFFER_BYTES {
                    writer.flush().map_err(|error| bad(&error))?;
                }
                Ok(())
            }
        }
    };
    // CSV writes its header on the first batch; an empty batch keeps the
    // header for an empty result.
    if format == Format::Csv {
        write(&RecordBatch::new_empty(schema.clone()))?;
    }
    let mut rows = 0_u64;
    for chunk in chunks {
        let reader = arrow_ipc::reader::StreamReader::try_new(
            std::io::BufReader::new(std::fs::File::open(chunk).map_err(|error| bad(&error))?),
            None,
        )
        .map_err(|error| bad(&error))?;
        if reader.schema() != schema {
            return Err("chunk schema differs from the verified manifest".into());
        }
        for batch in reader {
            let batch = batch.map_err(|error| bad(&error))?;
            rows += batch.num_rows() as u64;
            write(&batch)?;
        }
    }
    let file = match sink {
        Sink::Csv(writer) => writer.into_inner().into_inner().map_err(|e| bad(&e))?,
        Sink::Json(mut writer) => {
            writer.finish().map_err(|error| bad(&error))?;
            let mut inner = writer.into_inner();
            inner.write_all(b"\n").map_err(|error| bad(&error))?;
            inner.into_inner().map_err(|e| bad(&e))?
        }
        Sink::Arrow(mut writer) => {
            writer.finish().map_err(|error| bad(&error))?;
            writer
                .into_inner()
                .map_err(|error| bad(&error))?
                .into_inner()
                .map_err(|e| bad(&e))?
        }
        Sink::Parquet(writer) => writer.into_inner().map_err(|error| bad(&error))?,
    };
    file.sync_all().map_err(|error| bad(&error))?;
    Ok(rows)
}

fn job_error(id: Uuid, kind: &str, message: &str) -> RemoteError {
    RemoteError {
        status: None,
        failure_kind: Some(kind.to_owned()),
        retry_safe: false,
        message: message.to_owned(),
        request_id: Some(id.to_string().into()),
        acceptance: Some("accepted".into()),
        status_url: Some(format!("/v1/jobs/{id}").into()),
    }
}

fn invalid(message: impl Into<String>) -> RemoteError {
    RemoteError {
        status: None,
        failure_kind: Some("invalid_request".into()),
        retry_safe: true,
        message: message.into(),
        request_id: None,
        acceptance: Some("not_accepted".into()),
        status_url: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{
        ArrayRef, BooleanArray, Decimal128Array, Int64Array, StringArray, TimestampMicrosecondArray,
    };
    use arrow_schema::{Field, TimeUnit};

    #[test]
    fn format_inference_requires_a_known_extension_or_explicit_format() {
        assert_eq!(infer_format("out.CSV", None).unwrap(), Format::Csv);
        assert_eq!(infer_format("a/b.parquet", None).unwrap(), Format::Parquet);
        assert_eq!(infer_format("x.arrows", None).unwrap(), Format::Arrow);
        assert_eq!(infer_format("x.bin", Some("json")).unwrap(), Format::Json);
        assert!(infer_format("x.bin", None).is_err());
        assert!(infer_format("x.csv", Some("xlsx")).is_err());
        assert_eq!(
            partial_directory(Path::new("dir/out.csv")),
            PathBuf::from("dir/out.csv.duckfleet-partial")
        );
    }

    #[test]
    fn csv_refuses_nested_columns_before_download() {
        let nested = Schema::new(vec![Field::new(
            "tags",
            DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
            true,
        )]);
        assert!(check_schema(Format::Csv, &nested).is_err());
        assert!(check_schema(Format::Json, &nested).is_ok());
        assert!(check_schema(Format::Parquet, &nested).is_ok());
    }

    #[test]
    fn every_format_streams_chunks_and_empty_results_keep_their_schema() {
        let directory = std::env::temp_dir().join(format!("duckfleet-export-{}", Uuid::new_v4()));
        std::fs::create_dir(&directory).unwrap();
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("amount", DataType::Decimal128(10, 2), true),
            Field::new("active", DataType::Boolean, true),
            Field::new("at", DataType::Timestamp(TimeUnit::Microsecond, None), true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef,
                Arc::new(StringArray::from(vec![Some("a"), None])),
                Arc::new(
                    Decimal128Array::from(vec![Some(1234), None])
                        .with_precision_and_scale(10, 2)
                        .unwrap(),
                ),
                Arc::new(BooleanArray::from(vec![Some(true), None])),
                Arc::new(TimestampMicrosecondArray::from(vec![Some(0), None])),
            ],
        )
        .unwrap();
        let write_chunk = |name: &str, batches: &[RecordBatch]| {
            let path = directory.join(name);
            let mut writer = arrow_ipc::writer::StreamWriter::try_new(
                std::fs::File::create(&path).unwrap(),
                &schema,
            )
            .unwrap();
            for batch in batches {
                writer.write(batch).unwrap();
            }
            writer.finish().unwrap();
            path
        };
        let full = vec![
            write_chunk("0.arrow", std::slice::from_ref(&batch)),
            write_chunk("1.arrow", std::slice::from_ref(&batch)),
        ];
        let empty = vec![write_chunk("e.arrow", &[])];
        for format in [Format::Csv, Format::Json, Format::Arrow, Format::Parquet] {
            let target = directory.join(format!("full.{}", format.name()));
            assert_eq!(
                convert(format, schema.clone(), &full, &target, b',').unwrap(),
                4
            );
            let target = directory.join(format!("empty.{}", format.name()));
            assert_eq!(
                convert(format, schema.clone(), &empty, &target, b',').unwrap(),
                0
            );
        }
        let csv = std::fs::read_to_string(directory.join("full.csv")).unwrap();
        assert_eq!(csv.lines().next().unwrap(), "id,name,amount,active,at");
        assert_eq!(csv.lines().count(), 5);
        assert_eq!(
            std::fs::read_to_string(directory.join("empty.csv")).unwrap(),
            "id,name,amount,active,at\n"
        );
        let json: serde_json::Value =
            serde_json::from_slice(&std::fs::read(directory.join("full.json")).unwrap()).unwrap();
        assert_eq!(json.as_array().unwrap().len(), 4);
        let empty: serde_json::Value =
            serde_json::from_slice(&std::fs::read(directory.join("empty.json")).unwrap()).unwrap();
        assert_eq!(empty, serde_json::json!([]));
        let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
            std::fs::File::open(directory.join("full.parquet")).unwrap(),
        )
        .unwrap();
        assert_eq!(reader.metadata().file_metadata().num_rows(), 4);
        let reader = arrow_ipc::reader::FileReader::try_new(
            std::fs::File::open(directory.join("empty.arrow")).unwrap(),
            None,
        )
        .unwrap();
        assert_eq!(reader.schema(), schema);
        std::fs::remove_dir_all(directory).unwrap();
    }
}
