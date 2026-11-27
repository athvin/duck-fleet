//! ADBC catalog metadata from the session's authorized `information_schema`.
//!
//! `get_objects` is rendered as JSON in the canonical nested shape and decoded
//! against `adbc_core::schemas::GET_OBJECTS_SCHEMA`, so the Arrow layout is
//! exactly the one driver managers expect. Filters are ADBC search patterns
//! (`%` and `_`) and are bound as parameters, never interpolated.

use crate::{convert, runtime};
use adbc_core::error::{Error, Result, Status};
use adbc_core::options::{InfoCode, ObjectDepth};
use adbc_core::schemas::{GET_INFO_SCHEMA, GET_OBJECTS_SCHEMA, GET_TABLE_TYPES_SCHEMA};
use arrow_array::{
    ArrayRef, RecordBatch, RecordBatchIterator, RecordBatchReader, StringArray, UInt32Array,
    UnionArray, new_empty_array,
};
use arrow_schema::DataType;
use duckfleet::Param;
use serde_json::{Value, json};
use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

type Reader = Box<dyn RecordBatchReader + Send + 'static>;
/// table name -> (table type, columns)
type Tables = BTreeMap<String, (String, Vec<Value>)>;
/// catalog -> schema -> tables
type Tree = BTreeMap<String, BTreeMap<String, Tables>>;

fn reader(batch: RecordBatch) -> Reader {
    let schema = batch.schema();
    Box::new(RecordBatchIterator::new(vec![Ok(batch)], schema))
}

fn invalid(error: impl std::fmt::Display) -> Error {
    Error::with_message_and_status(error.to_string(), Status::Internal)
}

pub(crate) fn table_types() -> Result<Reader> {
    let batch = RecordBatch::try_new(
        GET_TABLE_TYPES_SCHEMA.clone(),
        vec![Arc::new(StringArray::from(vec![
            "BASE TABLE",
            "VIEW",
            "LOCAL TEMPORARY",
        ]))],
    )
    .map_err(invalid)?;
    Ok(reader(batch))
}

/// String-valued info entries in a dense union; every child array is present,
/// as the union layout requires, but only `string_value` (and `int64_value`
/// for the ADBC version) carries rows.
pub(crate) fn info(codes: Option<HashSet<InfoCode>>, engine_version: &str) -> Result<Reader> {
    let entries: Vec<(InfoCode, Value)> = vec![
        (InfoCode::VendorName, json!("DuckFleet")),
        (
            InfoCode::VendorVersion,
            json!(format!("DuckDB {engine_version}")),
        ),
        (InfoCode::DriverName, json!("ADBC DuckFleet Driver")),
        (InfoCode::DriverVersion, json!(env!("CARGO_PKG_VERSION"))),
        (InfoCode::DriverArrowVersion, json!("60")),
        (InfoCode::DriverAdbcVersion, json!(1_001_000_i64)),
    ];
    let selected: Vec<(InfoCode, Value)> = entries
        .into_iter()
        .filter(|(code, _)| codes.as_ref().is_none_or(|wanted| wanted.contains(code)))
        .collect();
    let DataType::Union(fields, _) = GET_INFO_SCHEMA.field(1).data_type().clone() else {
        return Err(invalid("unexpected GetInfo schema"));
    };
    let mut names = Vec::new();
    let mut type_ids = Vec::new();
    let mut offsets = Vec::new();
    let mut strings = Vec::new();
    let mut integers = Vec::new();
    for (code, value) in &selected {
        names.push(u32::from(code));
        if let Some(number) = value.as_i64() {
            type_ids.push(2_i8);
            offsets.push(i32::try_from(integers.len()).map_err(invalid)?);
            integers.push(number);
        } else {
            type_ids.push(0_i8);
            offsets.push(i32::try_from(strings.len()).map_err(invalid)?);
            strings.push(value.as_str().unwrap_or_default().to_owned());
        }
    }
    let children: Vec<ArrayRef> = fields
        .iter()
        .map(|(id, field)| match id {
            0 => Arc::new(StringArray::from(strings.clone())) as ArrayRef,
            2 => Arc::new(arrow_array::Int64Array::from(integers.clone())) as ArrayRef,
            _ => new_empty_array(field.data_type()),
        })
        .collect();
    let union = UnionArray::try_new(fields, type_ids.into(), Some(offsets.into()), children)
        .map_err(invalid)?;
    let batch = RecordBatch::try_new(
        GET_INFO_SCHEMA.clone(),
        vec![Arc::new(UInt32Array::from(names)), Arc::new(union)],
    )
    .map_err(invalid)?;
    Ok(reader(batch))
}

pub(crate) struct Filter<'a> {
    pub depth: ObjectDepth,
    pub catalog: Option<&'a str>,
    pub db_schema: Option<&'a str>,
    pub table_name: Option<&'a str>,
    pub table_type: Option<Vec<&'a str>>,
    pub column_name: Option<&'a str>,
}

fn rows(
    session: &duckfleet::Session,
    sql: &str,
    params: Vec<Param>,
) -> Result<Vec<Vec<Option<String>>>> {
    runtime()
        .block_on(session.query(sql, params))
        .map(|result| result.rows)
        .map_err(convert)
}

fn pattern(value: Option<&str>) -> Param {
    Param::from(value.unwrap_or("%"))
}

pub(crate) fn objects(session: &duckfleet::Session, filter: Filter<'_>) -> Result<Reader> {
    let wants_schemas = !matches!(filter.depth, ObjectDepth::Catalogs);
    let wants_tables = matches!(
        filter.depth,
        ObjectDepth::Tables | ObjectDepth::Columns | ObjectDepth::All
    );
    let wants_columns = matches!(filter.depth, ObjectDepth::Columns | ObjectDepth::All);
    let catalog_rows = rows(
        session,
        "SELECT DISTINCT catalog_name FROM information_schema.schemata WHERE catalog_name LIKE ? ORDER BY 1",
        vec![pattern(filter.catalog)],
    )?;
    let mut tree: Tree = catalog_rows
        .into_iter()
        .filter_map(|row| row.into_iter().next().flatten())
        .map(|catalog| (catalog, BTreeMap::new()))
        .collect();
    if wants_schemas {
        for row in rows(
            session,
            "SELECT catalog_name, schema_name FROM information_schema.schemata WHERE catalog_name LIKE ? AND schema_name LIKE ? ORDER BY 1, 2",
            vec![pattern(filter.catalog), pattern(filter.db_schema)],
        )? {
            if let [Some(catalog), Some(schema)] = row.as_slice()
                && let Some(schemas) = tree.get_mut(catalog)
            {
                schemas.entry(schema.clone()).or_default();
            }
        }
    }
    if wants_tables {
        let types = filter.table_type.clone().unwrap_or_default();
        for row in rows(
            session,
            "SELECT table_catalog, table_schema, table_name, table_type FROM information_schema.tables WHERE table_catalog LIKE ? AND table_schema LIKE ? AND table_name LIKE ? ORDER BY 1, 2, 3",
            vec![
                pattern(filter.catalog),
                pattern(filter.db_schema),
                pattern(filter.table_name),
            ],
        )? {
            if let [Some(catalog), Some(schema), Some(table), Some(kind)] = row.as_slice()
                && (types.is_empty() || types.iter().any(|t| t.eq_ignore_ascii_case(kind)))
                && let Some(tables) = tree.get_mut(catalog).and_then(|s| s.get_mut(schema))
            {
                tables.insert(table.clone(), (kind.clone(), Vec::new()));
            }
        }
    }
    if wants_columns {
        for row in rows(
            session,
            "SELECT table_catalog, table_schema, table_name, column_name, ordinal_position::INTEGER, data_type, is_nullable, column_default FROM information_schema.columns WHERE table_catalog LIKE ? AND table_schema LIKE ? AND table_name LIKE ? AND column_name LIKE ? ORDER BY 1, 2, 3, 5",
            vec![
                pattern(filter.catalog),
                pattern(filter.db_schema),
                pattern(filter.table_name),
                pattern(filter.column_name),
            ],
        )? {
            let cell = |i: usize| row.get(i).cloned().flatten();
            let (Some(catalog), Some(schema), Some(table), Some(column)) =
                (cell(0), cell(1), cell(2), cell(3))
            else {
                continue;
            };
            if let Some((_, columns)) = tree
                .get_mut(&catalog)
                .and_then(|s| s.get_mut(&schema))
                .and_then(|t| t.get_mut(&table))
            {
                columns.push(json!({
                    "column_name": column,
                    "ordinal_position": cell(4).and_then(|p| p.parse::<i32>().ok()),
                    "xdbc_type_name": cell(5),
                    "xdbc_nullable": cell(6).map(|n| if n == "YES" { 1 } else { 0 }),
                    "xdbc_is_nullable": cell(6),
                    "xdbc_column_def": cell(7),
                }));
            }
        }
    }
    let mut lines = String::new();
    for (catalog, schemas) in tree {
        let db_schemas = wants_schemas.then(|| {
            schemas
                .into_iter()
                .map(|(schema, tables)| {
                    json!({
                        "db_schema_name": schema,
                        "db_schema_tables": wants_tables.then(|| tables
                            .into_iter()
                            .map(|(table, (kind, columns))| json!({
                                "table_name": table,
                                "table_type": kind,
                                "table_columns": wants_columns.then_some(columns),
                                "table_constraints": wants_columns.then(Vec::<Value>::new),
                            }))
                            .collect::<Vec<_>>()),
                    })
                })
                .collect::<Vec<_>>()
        });
        lines.push_str(
            &json!({"catalog_name": catalog, "catalog_db_schemas": db_schemas}).to_string(),
        );
        lines.push('\n');
    }
    let mut decoder = arrow_json::ReaderBuilder::new(GET_OBJECTS_SCHEMA.clone())
        .build_decoder()
        .map_err(invalid)?;
    decoder.decode(lines.as_bytes()).map_err(invalid)?;
    let batch = decoder
        .flush()
        .map_err(invalid)?
        .unwrap_or_else(|| RecordBatch::new_empty(GET_OBJECTS_SCHEMA.clone()));
    Ok(reader(batch))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn info_is_a_valid_dense_union_and_honors_requested_codes() {
        let batch = info(None, "v2.0.1").unwrap().next().unwrap().unwrap();
        assert_eq!(batch.num_rows(), 6);
        assert_eq!(batch.schema(), *GET_INFO_SCHEMA);
        let only = info(Some([InfoCode::DriverName].into_iter().collect()), "v2.0.1")
            .unwrap()
            .next()
            .unwrap()
            .unwrap();
        assert_eq!(only.num_rows(), 1);
    }

    #[test]
    fn table_types_use_the_canonical_schema() {
        let batch = table_types().unwrap().next().unwrap().unwrap();
        assert_eq!(batch.schema(), *GET_TABLE_TYPES_SCHEMA);
        assert_eq!(batch.num_rows(), 3);
    }
}
