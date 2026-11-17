//! Keeps configured credentials out of logs, errors and API responses.
//!
//! DuckDB quotes the failing statement in its error text and libpq quotes the
//! connection target, while `main` prints the whole `anyhow` chain to stderr as it
//! exits. A credential interpolated into startup SQL therefore reaches the container
//! log on every boot failure unless it is masked first.
use anyhow::Result;
use duckdb::Connection;

/// Environment variables whose values must never be printed.
const SECRET_VARS: [&str; 5] = [
    "CATALOG_DSN",
    "PLATFORM_DSN",
    "S3_ACCESS_KEY",
    "S3_SECRET_KEY",
    "DUCKFLEET_TOKEN",
];
const MASK: &str = "[redacted]";

#[derive(Clone)]
pub(crate) struct Redactor {
    secrets: Vec<String>,
}

impl Redactor {
    pub(crate) fn from_env() -> Self {
        Self::new(
            SECRET_VARS
                .iter()
                .filter_map(|name| std::env::var(name).ok()),
        )
    }

    fn new(values: impl IntoIterator<Item = String>) -> Self {
        let mut secrets = Vec::new();
        for value in values {
            // Both libpq and DuckDB rewrite connection strings, so a reformatted DSN
            // would not match the configured value verbatim. Masking the password
            // field as well catches the credential on its own.
            secrets.extend(dsn_passwords(&value));
            if !value.trim().is_empty() {
                secrets.push(value);
            }
        }
        // Longest first: masking a short secret first could leave the remainder of a
        // longer secret that contains it still readable.
        secrets.sort_by(|a, b| b.len().cmp(&a.len()).then_with(|| a.cmp(b)));
        secrets.dedup();
        Self { secrets }
    }

    pub(crate) fn scrub(&self, text: &str) -> String {
        self.secrets
            .iter()
            .fold(text.to_owned(), |text, secret| text.replace(secret, MASK))
    }

    /// Replaces a failure rather than wrapping it. An `anyhow` source chain would
    /// still expose the original text when formatted with `{:#}`.
    pub(crate) fn guard<T, E: Into<anyhow::Error>>(
        &self,
        step: &str,
        result: Result<T, E>,
    ) -> Result<T> {
        result.map_err(|error| {
            anyhow::anyhow!("{step}: {}", self.scrub(&format!("{:#}", error.into())))
        })
    }
}

/// Names the host a connection string points at. A hostname is not a credential, and
/// an unreachable-catalog failure is not actionable without it.
pub(crate) fn dsn_host(dsn: &str) -> String {
    dsn_field(dsn, "host").unwrap_or_else(|| "unknown".into())
}

fn dsn_field(dsn: &str, wanted: &str) -> Option<String> {
    dsn.split_whitespace()
        .filter_map(|field| field.split_once('='))
        .find(|(key, _)| key.eq_ignore_ascii_case(wanted))
        .map(|(_, value)| value.trim_matches('\'').to_owned())
        .filter(|value| !value.is_empty())
}

/// Scrubs inline password values out of arbitrary text: `password=secret`,
/// `password='secret'`, `password: "secret"` all become `password=[redacted]`.
///
/// The `Redactor` masks the platform's own credentials, which it knows. This
/// handles the ones it cannot know: a caller who writes a connection string into
/// SQL gets the whole string quoted back by the engine's error, and that text
/// reaches the response and the durable history row. Whatever the caller's
/// mistake, a history table must not hold a live credential.
pub(crate) fn scrub_passwords(text: &str) -> String {
    let lowered = text.to_ascii_lowercase();
    let bytes = text.as_bytes();
    let mut output = String::with_capacity(text.len());
    let mut position = 0;
    while let Some(found) = lowered[position..].find("password") {
        let key_end = position + found + "password".len();
        output.push_str(&text[position..key_end]);
        position = key_end;
        // A separator, with optional spaces either side; "password" with no value
        // following is prose, not a credential.
        let mut cursor = position;
        while bytes.get(cursor) == Some(&b' ') {
            cursor += 1;
        }
        if !matches!(bytes.get(cursor), Some(b'=' | b':')) {
            continue;
        }
        cursor += 1;
        while bytes.get(cursor) == Some(&b' ') {
            cursor += 1;
        }
        if cursor >= bytes.len() {
            continue;
        }
        let separator_end = cursor;
        // The value: quoted to its closing quote, bare to the next whitespace or
        // quote character. The mask swallows the quotes with the value.
        let value_end = match bytes[cursor] {
            quote @ (b'\'' | b'"') => text[cursor + 1..]
                .find(quote as char)
                .map(|end| cursor + 1 + end + 1)
                .unwrap_or(text.len()),
            _ => text[cursor..]
                .find(|c: char| c.is_whitespace() || c == '\'' || c == '"')
                .map(|end| cursor + end)
                .unwrap_or(text.len()),
        };
        output.push_str(&text[position..separator_end]);
        output.push_str(MASK);
        position = value_end;
    }
    output.push_str(&text[position..]);
    output
}

/// Extracts `password` field values from a libpq keyword/value connection string.
fn dsn_passwords(dsn: &str) -> Vec<String> {
    dsn.split_whitespace()
        .filter_map(|field| field.split_once('='))
        .filter(|(key, _)| key.eq_ignore_ascii_case("password"))
        .map(|(_, value)| value.trim_matches('\'').to_owned())
        .filter(|value| !value.is_empty())
        .collect()
}

/// Owns the only path that runs credential-bearing SQL, so a statement added later
/// cannot leak a secret through its error without changing this type.
pub(crate) struct Startup {
    conn: Connection,
    redactor: Redactor,
}

impl Startup {
    pub(crate) fn new(conn: Connection, redactor: Redactor) -> Self {
        Self { conn, redactor }
    }
    pub(crate) fn run(&self, step: &str, sql: &str) -> Result<()> {
        self.redactor.guard(step, self.conn.execute_batch(sql))
    }
    pub(crate) fn into_connection(self) -> Connection {
        self.conn
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inline_passwords_are_scrubbed_whatever_the_spelling() {
        for (given, expected) in [
            (
                "connect 'host=h password=hunter2 dbname=lake' failed",
                "connect 'host=h password=[redacted] dbname=lake' failed",
            ),
            (
                "password='quoted secret' trailing",
                "password=[redacted] trailing",
            ),
            (
                "PASSWORD = \"Quoted\" and password: bare",
                "PASSWORD = [redacted] and password: [redacted]",
            ),
            // An unterminated quote swallows to the end rather than leaking a tail.
            ("password='no closing quote", "password=[redacted]"),
            // Prose mentioning a password is not a credential.
            ("wrong password supplied", "wrong password supplied"),
            ("the password is required", "the password is required"),
            ("no secrets here", "no secrets here"),
        ] {
            assert_eq!(scrub_passwords(given), expected, "{given}");
        }
    }

    const DSN: &str = "host=postgres port=5432 dbname=lake user=duckfleet password=s3cr3t-value";

    fn redactor() -> Redactor {
        Redactor::new([DSN.to_owned(), "AKIAEXAMPLE".to_owned()])
    }

    #[test]
    fn scrubs_a_whole_dsn_and_its_password_alone() {
        let scrubbed = redactor().scrub(&format!(
            "Failed to attach 'ducklake:postgres:{DSN}': connection refused"
        ));
        assert!(!scrubbed.contains("s3cr3t-value"), "{scrubbed}");
        assert!(scrubbed.contains(MASK));
        // A reformatted DSN no longer matches the configured value, so the password
        // has to be masked on its own as well.
        let reformatted = redactor().scrub("libpq: password=s3cr3t-value rejected");
        assert!(!reformatted.contains("s3cr3t-value"), "{reformatted}");
    }

    #[test]
    fn guard_keeps_the_step_and_drops_the_source_chain() {
        let inner = anyhow::anyhow!("outer").context(format!("attaching {DSN}"));
        let error = redactor()
            .guard("catalog attach", Err::<(), _>(inner))
            .unwrap_err();
        let rendered = format!("{error:#}");
        assert!(!rendered.contains("s3cr3t-value"), "{rendered}");
        assert!(rendered.starts_with("catalog attach:"), "{rendered}");
    }

    #[test]
    fn a_shorter_secret_cannot_leave_part_of_a_longer_one() {
        let scrubbed = Redactor::new(["abc".to_owned(), "abcdef".to_owned()]).scrub("value=abcdef");
        assert_eq!(scrubbed, format!("value={MASK}"));
    }

    #[test]
    fn names_the_host_without_exposing_the_rest() {
        assert_eq!(dsn_host(DSN), "postgres");
        assert_eq!(dsn_host("host='quoted-host' port=5432"), "quoted-host");
        assert_eq!(dsn_host("port=5432 dbname=lake"), "unknown");
        assert_eq!(dsn_host("host= port=5432"), "unknown");
    }

    #[test]
    fn blank_configuration_never_masks_everything() {
        let scrubbed = Redactor::new(["".to_owned(), "   ".to_owned()]).scrub("nothing secret");
        assert_eq!(scrubbed, "nothing secret");
    }

    /// The guarantee is structural rather than a matter of review: DuckDB quotes the
    /// failing statement, so an engine error is only safe to propagate once scrubbed.
    /// Every execution either goes through `Startup::run` or throws the engine error
    /// away. A new statement that does neither fails here.
    #[test]
    fn sql_execution_never_propagates_an_unscrubbed_error() {
        // Assembled from pieces so this assertion cannot match its own source.
        let call = concat!(".execute", "_batch(");
        assert_eq!(
            include_str!("secrets.rs").matches(call).count(),
            1,
            "Startup::run must remain the only scrubbing execution path"
        );
        for tail in include_str!("worker.rs").split(call).skip(1) {
            // A call whose `map_err` is further away than this is also reported: the
            // fix in either case is to route the statement through `Startup::run`.
            let window: String = tail.chars().take(160).collect();
            assert!(
                window.contains("map_err(|_|"),
                "SQL in worker.rs must scrub or discard the engine error: {window}"
            );
        }
    }
}
