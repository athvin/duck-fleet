use crate::client::{QueryResult, RemoteConnection, RemoteError};
use serde_json::Value;

pub enum Request {
    Schemas,
    Relations {
        kind: RelationKind,
        schema: Option<String>,
    },
    Describe(String),
    Preview(String, u32),
}

#[derive(Clone, Copy)]
pub enum RelationKind {
    Table,
    View,
}

#[derive(Debug, PartialEq, Eq)]
struct RelationName {
    catalog: Option<String>,
    schema: Option<String>,
    relation: String,
}

#[derive(Debug)]
struct ResolvedRelation {
    catalog: String,
    schema: String,
    relation: String,
}

pub async fn run(
    url: &str,
    token: &str,
    request: Request,
    format: &str,
    queue_timeout: u64,
) -> Result<(), RemoteError> {
    let mut connection = RemoteConnection::open(url, token, "catalog", None, None, 90).await?;
    let result = execute(&connection, request, queue_timeout).await;
    let close = connection.close().await;
    let (rows, notice) = result?;
    if let Some(notice) = notice {
        eprintln!("{notice}");
    }
    crate::cli::emit(&rows, format);
    close?;
    Ok(())
}

pub async fn execute(
    connection: &RemoteConnection,
    request: Request,
    queue_timeout: u64,
) -> Result<(QueryResult, Option<String>), RemoteError> {
    match request {
        Request::Schemas => {
            let rows = query(
                connection,
                "SELECT schema_name AS name FROM information_schema.schemata WHERE catalog_name = current_catalog() AND schema_name NOT IN ('information_schema', 'duckfleet_system') ORDER BY schema_name",
                Vec::new(),
                queue_timeout,
            )
            .await?;
            Ok((rows, None))
        }
        Request::Relations { kind, schema } => {
            let relation_type = match kind {
                RelationKind::Table => "BASE TABLE",
                RelationKind::View => "VIEW",
            };
            if let Some(schema) = &schema {
                let exists = query(
                    connection,
                    "SELECT schema_name FROM information_schema.schemata WHERE catalog_name = current_catalog() AND schema_name = ?",
                    vec![Value::String(schema.clone())],
                    queue_timeout,
                )
                .await?;
                if exists.rows.is_empty() {
                    return Err(local_error(
                        "missing_schema",
                        format!("schema {schema:?} does not exist in the current catalog"),
                    ));
                }
            }
            let (sql, parameters) = match schema {
                Some(schema) => (
                    "SELECT table_schema AS schema_name, table_name AS relation_name, table_type AS relation_type FROM information_schema.tables WHERE table_catalog = current_catalog() AND table_schema = ? AND table_type = ? ORDER BY table_name",
                    vec![Value::String(schema), Value::String(relation_type.into())],
                ),
                None => (
                    "SELECT table_schema AS schema_name, table_name AS relation_name, table_type AS relation_type FROM information_schema.tables WHERE table_catalog = current_catalog() AND table_schema NOT IN ('information_schema', 'duckfleet_system') AND table_type = ? ORDER BY table_schema, table_name",
                    vec![Value::String(relation_type.into())],
                ),
            };
            let rows = query(connection, sql, parameters, queue_timeout).await?;
            Ok((rows, None))
        }
        Request::Describe(name) => {
            let rows = describe(connection, &name, queue_timeout).await?;
            Ok((rows, None))
        }
        Request::Preview(name, limit) => {
            let rows = preview(connection, &name, limit, queue_timeout).await?;
            Ok((rows, Some(format!("preview_limit: {limit}"))))
        }
    }
}

pub async fn tables(
    connection: &RemoteConnection,
    queue_timeout: u64,
) -> Result<QueryResult, RemoteError> {
    execute(
        connection,
        Request::Relations {
            kind: RelationKind::Table,
            schema: None,
        },
        queue_timeout,
    )
    .await
    .map(|(rows, _)| rows)
}

pub async fn describe(
    connection: &RemoteConnection,
    name: &str,
    queue_timeout: u64,
) -> Result<QueryResult, RemoteError> {
    let parsed = parse_relation(name)?;
    let relation = resolve(connection, &parsed, queue_timeout).await?;
    query(
        connection,
        "SELECT table_schema AS schema_name, table_name AS relation_name, ordinal_position, column_name, data_type, is_nullable FROM information_schema.columns WHERE table_catalog = ? AND table_schema = ? AND table_name = ? ORDER BY ordinal_position",
        vec![
            Value::String(relation.catalog),
            Value::String(relation.schema),
            Value::String(relation.relation),
        ],
        queue_timeout,
    )
    .await
}

pub async fn preview(
    connection: &RemoteConnection,
    name: &str,
    limit: u32,
    queue_timeout: u64,
) -> Result<QueryResult, RemoteError> {
    let parsed = parse_relation(name)?;
    let relation = resolve(connection, &parsed, queue_timeout).await?;
    let qualified = format!(
        "{}.{}.{}",
        quote_identifier(&relation.catalog),
        quote_identifier(&relation.schema),
        quote_identifier(&relation.relation)
    );
    query(
        connection,
        &format!("SELECT * FROM {qualified} LIMIT {limit}"),
        Vec::new(),
        queue_timeout,
    )
    .await
}

async fn resolve(
    connection: &RemoteConnection,
    name: &RelationName,
    queue_timeout: u64,
) -> Result<ResolvedRelation, RemoteError> {
    let mut predicates = vec!["table_name = ?".to_owned()];
    let mut parameters = vec![Value::String(name.relation.clone())];
    if let Some(schema) = &name.schema {
        predicates.push("table_schema = ?".into());
        parameters.push(Value::String(schema.clone()));
    } else {
        predicates.push("table_schema NOT IN ('information_schema', 'duckfleet_system')".into());
    }
    if let Some(catalog) = &name.catalog {
        predicates.push("table_catalog = ?".into());
        parameters.push(Value::String(catalog.clone()));
    } else {
        predicates.push("table_catalog = current_catalog()".into());
    }
    let mut rows = query(
        connection,
        &format!(
            "SELECT table_catalog, table_schema, table_name FROM information_schema.tables WHERE {} ORDER BY table_schema",
            predicates.join(" AND ")
        ),
        parameters,
        queue_timeout,
    )
    .await?;
    if rows.rows.is_empty() && name.catalog.is_none() && name.schema.is_none() {
        rows = query(
            connection,
            "SELECT DISTINCT table_catalog, table_schema, table_name FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = ?",
            vec![Value::String(name.relation.clone())],
            queue_timeout,
        )
        .await?;
    }
    if rows.rows.is_empty() {
        return Err(local_error(
            "missing_relation",
            format!("relation {name:?} does not exist in the current catalog"),
        ));
    }
    if rows.rows.len() != 1 {
        return Err(local_error(
            "ambiguous_relation",
            format!(
                "unqualified relation {:?} exists in multiple schemas; use schema.relation",
                name.relation
            ),
        ));
    }
    let row = &rows.rows[0];
    let cell = |index: usize| {
        row.get(index)
            .and_then(Option::as_deref)
            .map(str::to_owned)
            .ok_or_else(|| {
                local_error(
                    "invalid_catalog_response",
                    "catalog returned a null identifier",
                )
            })
    };
    Ok(ResolvedRelation {
        catalog: cell(0)?,
        schema: cell(1)?,
        relation: cell(2)?,
    })
}

async fn query(
    connection: &RemoteConnection,
    sql: &str,
    parameters: Vec<Value>,
    queue_timeout: u64,
) -> Result<QueryResult, RemoteError> {
    connection
        .execute_with_queue_timeout(sql, parameters, queue_timeout)
        .await
        .map(|(_, rows)| rows)
}

fn quote_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

fn local_error(kind: &str, message: impl Into<String>) -> RemoteError {
    RemoteError {
        status: None,
        failure_kind: Some(kind.into()),
        retry_safe: true,
        message: message.into(),
        request_id: None,
        acceptance: Some("not_accepted".into()),
        status_url: None,
    }
}

pub fn validate_relation(name: &str) -> Result<(), String> {
    parse_relation(name)
        .map(|_| ())
        .map_err(|error| error.to_string())
}

fn parse_relation(input: &str) -> Result<RelationName, RemoteError> {
    let mut parts = Vec::new();
    let mut current = String::new();
    let mut quoted = false;
    let mut was_quoted = false;
    let mut chars = input.chars().peekable();
    while let Some(character) = chars.next() {
        if quoted {
            if character == '"' {
                if chars.peek() == Some(&'"') {
                    chars.next();
                    current.push('"');
                } else {
                    quoted = false;
                    if chars.peek().is_some_and(|next| *next != '.') {
                        return Err(local_error(
                            "invalid_identifier",
                            "a quoted identifier must be followed by a dot or end of input",
                        ));
                    }
                }
            } else {
                current.push(character);
            }
        } else {
            match character {
                '"' if current.is_empty() => {
                    quoted = true;
                    was_quoted = true;
                }
                '"' => {
                    return Err(local_error(
                        "invalid_identifier",
                        "quotes must begin an identifier component",
                    ));
                }
                '.' => {
                    if current.is_empty() {
                        return Err(local_error(
                            "invalid_identifier",
                            "empty identifier component",
                        ));
                    }
                    parts.push(if was_quoted {
                        current.clone()
                    } else {
                        current.trim().to_ascii_lowercase()
                    });
                    current.clear();
                    was_quoted = false;
                }
                _ => current.push(character),
            }
        }
    }
    if quoted || current.is_empty() || parts.len() > 2 {
        return Err(local_error(
            "invalid_identifier",
            "relation must be [catalog.][schema.]name with balanced double quotes",
        ));
    }
    if was_quoted {
        parts.push(current);
    } else {
        parts.push(current.trim().to_ascii_lowercase());
    }
    if parts.iter().any(|part| part.is_empty()) {
        return Err(local_error(
            "invalid_identifier",
            "empty identifier component",
        ));
    }
    let relation = parts.pop().unwrap();
    let schema = if parts.is_empty() {
        None
    } else {
        Some(parts.pop().unwrap())
    };
    let catalog = parts.pop();
    Ok(RelationName {
        catalog,
        schema,
        relation,
    })
}

pub fn validate_limit(value: Option<&str>) -> Result<u32, String> {
    value.map_or(Ok(20), |value| {
        value
            .parse::<u32>()
            .ok()
            .filter(|limit| (1..=1000).contains(limit))
            .ok_or_else(|| "--limit must be a whole number from 1 to 1000 for preview".into())
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quoted_identifiers_never_become_sql_fragments() {
        assert_eq!(
            parse_relation("Analytics.Events").unwrap(),
            RelationName {
                catalog: None,
                schema: Some("analytics".into()),
                relation: "events".into(),
            }
        );
        assert_eq!(
            parse_relation("lake.\"Odd.Schema\".\"dot.name\"").unwrap(),
            RelationName {
                catalog: Some("lake".into()),
                schema: Some("Odd.Schema".into()),
                relation: "dot.name".into(),
            }
        );
        assert_eq!(
            parse_relation("\"a\".\"quote\"\"name\"").unwrap().relation,
            "quote\"name"
        );
        for invalid in [
            "a..b",
            ".a",
            "a.",
            "a.b.c.d",
            "a.\"unterminated",
            "a.\"b\"junk",
            "a.b\"c",
        ] {
            assert!(parse_relation(invalid).is_err(), "accepted {invalid:?}");
        }
        let injection = parse_relation("analytics.events; DROP TABLE events").unwrap();
        assert_eq!(injection.relation, "events; drop table events");
        assert_eq!(quote_identifier("a\"b"), "\"a\"\"b\"");
    }

    #[test]
    fn preview_limits_are_bounded() {
        assert_eq!(validate_limit(None).unwrap(), 20);
        assert_eq!(validate_limit(Some("1")).unwrap(), 1);
        assert_eq!(validate_limit(Some("1000")).unwrap(), 1000);
        for invalid in ["0", "1001", "-1", "1.5", "many"] {
            assert!(validate_limit(Some(invalid)).is_err());
        }
    }
}
