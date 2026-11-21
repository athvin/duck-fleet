//! Bounds apply before AST construction, including flat chains and AST destruction.
use sqlparser::{
    dialect::Dialect,
    keywords::Keyword,
    parser::Parser,
    tokenizer::{Token, TokenWithSpan, Tokenizer},
};

#[derive(Clone, Copy)]
pub(crate) struct Limits {
    bytes: usize,
    tokens: usize,
    constructions: usize,
    nesting: usize,
    recursion: usize,
}
impl Limits {
    pub const SYSTEM: Self = Self {
        bytes: 64 * 1024,
        tokens: 512,
        constructions: 64,
        nesting: 32,
        recursion: 64,
    };
}

pub(crate) struct CheckedTokens {
    tokens: Vec<TokenWithSpan>,
    limits: Limits,
}
impl CheckedTokens {
    pub fn tokens(&self) -> impl Iterator<Item = &Token> {
        self.tokens.iter().map(|t| &t.token)
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum GuardError {
    TooLarge,
    Unsupported,
    ParseError,
}

pub(crate) fn checked_tokens(
    sql: &str,
    dialect: &dyn Dialect,
    limits: Limits,
) -> Result<CheckedTokens, GuardError> {
    if sql.len() > limits.bytes {
        return Err(GuardError::TooLarge);
    }
    if sql.contains('\0') {
        return Err(GuardError::Unsupported);
    }
    // Tokenization allocation itself is bounded by input bytes, before token count.
    let tokens: Vec<_> = Tokenizer::new(dialect, sql)
        .tokenize_with_location()
        .map_err(|_| GuardError::ParseError)?
        .into_iter()
        .filter(|t| !matches!(t.token, Token::Whitespace(_) | Token::EOF))
        .collect();
    if tokens.len() > limits.tokens {
        return Err(GuardError::TooLarge);
    }
    let mut stack = Vec::new();
    let mut construction = 0;
    for item in &tokens {
        let token = &item.token;
        match token {
            Token::LParen => stack.push(0),
            Token::LBracket => stack.push(1),
            Token::LBrace => stack.push(2),
            Token::RParen | Token::RBracket | Token::RBrace => {
                let expected = match token {
                    Token::RParen => 0,
                    Token::RBracket => 1,
                    _ => 2,
                };
                if stack.pop() != Some(expected) {
                    return Err(GuardError::ParseError);
                }
            }
            _ => {}
        }
        if stack.len() > limits.nesting {
            return Err(GuardError::TooLarge);
        }
        // Count syntax-bearing construction tokens, not just parentheses. This
        // bounds flat binary/unary trees, nested CASE/SELECT and access chains.
        // Commas only separate bounded vectors; counting them as tree depth would
        // prevent the documented 100-ID list and 32 explicit output columns.
        let leaf = match token {
            Token::Word(w) => w.quote_style.is_some() || w.keyword == Keyword::NoKeyword,
            Token::Number(..)
            | Token::SingleQuotedString(_)
            | Token::DoubleQuotedString(_)
            | Token::DollarQuotedString(_)
            | Token::EscapedStringLiteral(_)
            | Token::UnicodeStringLiteral(_)
            | Token::NationalStringLiteral(_)
            | Token::HexStringLiteral(_)
            | Token::Placeholder(_)
            | Token::Comma => true,
            _ => false,
        };
        if !leaf {
            construction += 1;
        }
        if construction > limits.constructions {
            return Err(GuardError::TooLarge);
        }
    }
    if !stack.is_empty() {
        return Err(GuardError::ParseError);
    }
    Ok(CheckedTokens { tokens, limits })
}

pub(crate) fn parse(
    tokens: &CheckedTokens,
    dialect: &dyn Dialect,
) -> Result<Vec<sqlparser::ast::Statement>, GuardError> {
    Parser::new(dialect)
        .with_recursion_limit(tokens.limits.recursion)
        .with_tokens_with_locations(tokens.tokens.clone())
        .parse_statements()
        .map_err(|_| GuardError::ParseError)
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlparser::dialect::DuckDbDialect;
    #[test]
    fn bounds_flat_chains_nested_syntax_and_tokenization() {
        let d = DuckDbDialect {};
        for sql in [
            format!("SELECT {}1", "- ".repeat(100)),
            format!("SELECT {}1{}", "(".repeat(100), ")".repeat(100)),
            format!("SELECT {}", vec!["1=1"; 100].join(" OR ")),
        ] {
            assert!(matches!(
                checked_tokens(&sql, &d, Limits::SYSTEM),
                Err(GuardError::TooLarge)
            ));
        }
        assert!(matches!(
            checked_tokens(&" ".repeat(65537), &d, Limits::SYSTEM),
            Err(GuardError::TooLarge)
        ));
        // Quoted text and comments do not contribute false syntax nesting.
        let t = checked_tokens("SELECT '(((((', 1 /* ((( */", &d, Limits::SYSTEM).unwrap();
        assert_eq!(parse(&t, &d).unwrap().len(), 1);
    }
    #[test]
    fn bounded_parse_and_drop_on_small_stack() {
        std::thread::Builder::new()
            .stack_size(512 * 1024)
            .spawn(|| {
                let d = DuckDbDialect {};
                let inputs = [
                    format!("SELECT {}1{}", "(".repeat(30), ")".repeat(30)),
                    format!("SELECT {}", vec!["1=1"; 15].join(" OR ")),
                    format!("SELECT {}1", "NOT ".repeat(55)),
                    "SELECT CASE WHEN TRUE THEN CASE WHEN FALSE THEN 1 ELSE 2 END ELSE 3 END"
                        .into(),
                ];
                for _ in 0..20 {
                    for sql in &inputs {
                        if let Ok(t) = checked_tokens(sql, &d, Limits::SYSTEM) {
                            drop(parse(&t, &d));
                        }
                    }
                }
            })
            .unwrap()
            .join()
            .unwrap();
    }
}
