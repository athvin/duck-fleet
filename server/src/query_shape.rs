//! Best-effort, versioned token shape; never an execution or deduplication identity.
use crate::sql_limits::{self, GuardError, Limits};
use ring::digest::{Context, SHA256};
use serde_json::Value;
use sqlparser::{dialect::DuckDbDialect, keywords::Keyword, tokenizer::Token};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ShapeStatus {
    Captured,
    Unsupported,
    TooLarge,
    ParseError,
}
impl ShapeStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Captured => "captured",
            Self::Unsupported => "unsupported",
            Self::TooLarge => "too_large",
            Self::ParseError => "parse_error",
        }
    }
}
pub(crate) struct QueryShape {
    pub fingerprint: Option<String>,
    pub version: i32,
    pub status: ShapeStatus,
}
fn failed(status: ShapeStatus) -> QueryShape {
    QueryShape {
        fingerprint: None,
        version: 1,
        status,
    }
}
fn field(hash: &mut Context, value: &str) {
    hash.update(&(value.len() as u64).to_be_bytes());
    hash.update(value.as_bytes());
}

pub(crate) fn capture(sql: &str, parameters: &[Value]) -> QueryShape {
    let dialect = DuckDbDialect {};
    let checked = match sql_limits::checked_tokens(sql, &dialect, Limits::SYSTEM) {
        Ok(t) => t,
        Err(e) => {
            return failed(match e {
                GuardError::TooLarge => ShapeStatus::TooLarge,
                GuardError::Unsupported => ShapeStatus::Unsupported,
                GuardError::ParseError => ShapeStatus::ParseError,
            });
        }
    };
    match sql_limits::parse(&checked, &dialect) {
        Ok(ast) if ast.len() == 1 => drop(ast),
        Ok(_) => return failed(ShapeStatus::Unsupported),
        Err(_) => return failed(ShapeStatus::ParseError),
    }
    if parameters.len() > 512 {
        return failed(ShapeStatus::TooLarge);
    }
    let mut hash = Context::new(&SHA256);
    field(&mut hash, "duckfleet/query-shape/v1");
    for token in checked.tokens() {
        match token {
            Token::Word(w) if w.quote_style.is_some() => {
                field(&mut hash, "quoted_identifier");
                field(&mut hash, &w.quote_style.unwrap().to_string());
                field(&mut hash, &w.value);
            }
            Token::Word(w) if matches!(w.keyword, Keyword::TRUE | Keyword::FALSE) => {
                field(&mut hash, "literal:bool")
            }
            Token::Word(w) if w.keyword == Keyword::NULL => field(&mut hash, "literal:null"),
            Token::Word(w) if w.keyword != Keyword::NoKeyword => {
                field(&mut hash, "keyword");
                field(&mut hash, &w.value.to_ascii_uppercase());
            }
            Token::Word(w) => {
                field(&mut hash, "identifier");
                field(&mut hash, &w.value);
            }
            Token::Number(n, long) => {
                field(
                    &mut hash,
                    if *long {
                        "literal:long"
                    } else if n.contains(['e', 'E']) {
                        "literal:exponent"
                    } else if n.contains('.') {
                        "literal:decimal"
                    } else {
                        "literal:integer"
                    },
                );
            }
            Token::SingleQuotedString(_) => field(&mut hash, "literal:text"),
            Token::DollarQuotedString(_) => field(&mut hash, "literal:dollar_text"),
            Token::EscapedStringLiteral(_) => field(&mut hash, "literal:escaped_text"),
            Token::NationalStringLiteral(_) => field(&mut hash, "literal:national_text"),
            Token::HexStringLiteral(_) => field(&mut hash, "literal:hex"),
            Token::Placeholder(p) => {
                field(&mut hash, "parameter");
                field(&mut hash, p);
            }
            Token::SemiColon => {}
            Token::Comma
            | Token::Period
            | Token::LParen
            | Token::RParen
            | Token::LBracket
            | Token::RBracket
            | Token::LBrace
            | Token::RBrace
            | Token::Eq
            | Token::DoubleEq
            | Token::Neq
            | Token::Lt
            | Token::Gt
            | Token::LtEq
            | Token::GtEq
            | Token::Plus
            | Token::Minus
            | Token::Mul
            | Token::Div
            | Token::Mod
            | Token::DuckIntDiv
            | Token::StringConcat
            | Token::DoubleColon
            | Token::Colon
            | Token::Assignment
            | Token::Arrow
            | Token::LongArrow
            | Token::RArrow
            | Token::Ampersand
            | Token::Pipe
            | Token::Caret => {
                field(&mut hash, "punctuation");
                field(&mut hash, &token.to_string());
            }
            _ => return failed(ShapeStatus::Unsupported),
        }
    }
    field(&mut hash, "parameter_types");
    for value in parameters {
        let tag = match value {
            Value::Null => "null",
            Value::Bool(_) => "bool",
            Value::String(_) => "text",
            Value::Number(n) if n.is_i64() => "bigint",
            Value::Number(n) if n.is_u64() => "ubigint",
            Value::Number(_) => "double",
            _ => return failed(ShapeStatus::Unsupported),
        };
        field(&mut hash, tag);
    }
    let fingerprint = hash
        .finish()
        .as_ref()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    QueryShape {
        fingerprint: Some(fingerprint),
        version: 1,
        status: ShapeStatus::Captured,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    #[test]
    fn literal_values_disappear_but_types_and_identifiers_do_not() {
        let a = capture("select x from t where x=1 and y='secret'", &[]);
        let b = capture(
            "SELECT x FROM t WHERE x = 999 AND y = 'other' -- comment",
            &[],
        );
        assert_eq!(a.status, ShapeStatus::Captured);
        assert_eq!(a.fingerprint, b.fingerprint);
        assert_ne!(
            a.fingerprint,
            capture("SELECT x FROM t WHERE x=1.0 AND y='secret'", &[]).fingerprint
        );
        assert_ne!(
            a.fingerprint,
            capture("SELECT x FROM other WHERE x=1 AND y='secret'", &[]).fingerprint
        );
        assert_ne!(
            capture("SELECT \"Case\" FROM t", &[]).fingerprint,
            capture("SELECT \"case\" FROM t", &[]).fingerprint
        );
        assert_ne!(
            capture("SELECT $1", &[json!(1)]).fingerprint,
            capture("SELECT $1", &[json!(1.0)]).fingerprint
        );
        assert_eq!(
            capture("SELECT $1", &[json!(1)]).fingerprint,
            capture("SELECT $1", &[json!(900)]).fingerprint
        );
    }
    #[test]
    fn version_one_shape_is_frozen() {
        assert_eq!(
            capture("SELECT 1", &[]).fingerprint.as_deref(),
            Some("e8981225e97de704cc1bda09bbf4cce84e5268d6051fb02fca71b0d2fdc9f356")
        );
    }
    #[test]
    fn capture_failure_is_explicit_and_never_an_execution_error() {
        assert_eq!(capture("SELECT (", &[]).status, ShapeStatus::ParseError);
        assert_eq!(
            capture(&"x".repeat(65537), &[]).status,
            ShapeStatus::TooLarge
        );
        assert_eq!(
            capture("SELECT $1", &[json!({"x":1})]).status,
            ShapeStatus::Unsupported
        );
        assert_eq!(
            capture("SELECT 1; SELECT 2", &[]).status,
            ShapeStatus::Unsupported
        );
        assert_eq!(capture("SELECT (", &[]).version, 1);
    }
}
