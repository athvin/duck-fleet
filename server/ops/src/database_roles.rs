use anyhow::{Result, bail};
use serde_json::{Map, Value, json};
use std::collections::{BTreeMap, BTreeSet};

const PARTS: [&str; 6] = [
    "migrator",
    "gateway",
    "worker",
    "maintainer",
    "catalog",
    "catalog_reader",
];

fn dsn_key(part: &str) -> &'static str {
    match part {
        "migrator" => "PLATFORM_MIGRATION_DSN",
        "gateway" => "PLATFORM_GATEWAY_DSN",
        "worker" => "PLATFORM_WORKER_DSN",
        "maintainer" => "PLATFORM_MAINTENANCE_DSN",
        "catalog" => "CATALOG_DSN",
        "catalog_reader" => "CATALOG_READER_DSN",
        _ => unreachable!(),
    }
}

fn valid_namespace(namespace: &str) -> bool {
    regex::Regex::new(r"^[a-z][a-z0-9-]{0,62}$")
        .expect("namespace regex")
        .is_match(namespace)
}

fn valid_host(host: &str) -> bool {
    regex::Regex::new(r"^[a-zA-Z0-9_.-]+$")
        .expect("host regex")
        .is_match(host)
}

pub fn role_names(namespace: &str) -> Result<BTreeMap<String, String>> {
    if !valid_namespace(namespace) {
        bail!("namespace must be a lowercase Kubernetes namespace name");
    }
    let digest = ring::digest::digest(&ring::digest::SHA256, namespace.as_bytes());
    let hash = digest
        .as_ref()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let prefix = format!(
        "df_{}_{}",
        &namespace.replace('-', "_")[..namespace.len().min(36)],
        &hash[..8]
    );
    Ok(PARTS
        .iter()
        .map(|part| ((*part).to_owned(), format!("{prefix}_{part}")))
        .collect())
}

pub fn secret(namespace: &str, name: &str, host: &str) -> Result<Value> {
    if !valid_namespace(namespace) {
        bail!("namespace must be a lowercase Kubernetes namespace name");
    }
    if !valid_host(host) {
        bail!("host must be a DNS name or IPv4 address");
    }
    let roles = role_names(namespace)?;
    let mut values = Map::new();
    for key in [
        "POSTGRES_PASSWORD",
        "DUCKFLEET_TOKEN",
        "DUCKFLEET_INTERNAL_TOKEN",
    ] {
        values.insert(key.to_owned(), json!(crate::util::random_hex(32)?));
    }
    for part in PARTS {
        let password = crate::util::random_hex(32)?;
        values.insert(
            format!("DATABASE_{}_PASSWORD", part.to_uppercase()),
            json!(password),
        );
        let database = if matches!(part, "catalog" | "catalog_reader") {
            "lake"
        } else {
            "platform"
        };
        values.insert(
            dsn_key(part).to_owned(),
            json!(format!(
                "host={host} port=5432 user={} password={password} dbname={database}",
                roles[part]
            )),
        );
    }
    Ok(json!({
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {"name": name, "namespace": namespace},
        "type": "Opaque",
        "stringData": Value::Object(values),
    }))
}

fn secret_values(document: &Value) -> Result<BTreeMap<String, String>> {
    let mut values = BTreeMap::new();
    if let Some(data) = document.get("data").and_then(Value::as_object) {
        for (key, value) in data {
            values.insert(
                key.clone(),
                crate::util::decode_base64(value.as_str().unwrap_or_default())?,
            );
        }
    }
    if let Some(data) = document.get("stringData").and_then(Value::as_object) {
        for (key, value) in data {
            values.insert(key.clone(), value.as_str().unwrap_or_default().to_owned());
        }
    }
    Ok(values)
}

pub fn retarget(document: &Value, host: &str) -> Result<Value> {
    if !valid_host(host) {
        bail!("host must be a DNS name or IPv4 address");
    }
    let values = secret_values(document)?;
    let mut rewritten = Map::new();
    let host_field = regex::Regex::new(r"(^|\s)host=\S+")?;
    for part in PARTS {
        let key = dsn_key(part);
        let original = values
            .get(key)
            .ok_or_else(|| anyhow::anyhow!("Secret lacks {key}"))?;
        let matches: Vec<_> = host_field.captures_iter(original).collect();
        if matches.len() != 1 {
            bail!("{key} must have one unquoted host= field for retargeting");
        }
        let updated = host_field
            .replace_all(original, |captures: &regex::Captures<'_>| {
                format!("{}host={host}", &captures[1])
            })
            .into_owned();
        rewritten.insert(key.to_owned(), json!(updated));
    }
    Ok(json!({"stringData": Value::Object(rewritten)}))
}

pub fn check_secret(document: &Value) -> Result<()> {
    let values = secret_values(document)?;
    let mut required: BTreeSet<String> =
        PARTS.iter().map(|part| dsn_key(part).to_owned()).collect();
    required.extend([
        "DUCKFLEET_TOKEN".to_owned(),
        "DUCKFLEET_INTERNAL_TOKEN".to_owned(),
    ]);
    let missing: Vec<_> = required
        .difference(&values.keys().cloned().collect())
        .cloned()
        .collect();
    if !missing.is_empty() {
        bail!(
            "Secret predates separated database roles; stop compute and follow the schema3 provisioning upgrade: missing {}",
            missing.join(", ")
        );
    }
    if values["DUCKFLEET_TOKEN"] == values["DUCKFLEET_INTERNAL_TOKEN"] {
        bail!("public bootstrap and internal tokens must be distinct");
    }
    let credentials: BTreeSet<_> = PARTS
        .iter()
        .map(|part| values[dsn_key(part)].clone())
        .collect();
    if credentials.len() != PARTS.len() {
        bail!("each component requires its own database credential");
    }
    Ok(())
}

pub fn upgrade(mut generated: Value, original: &Value) -> Result<Value> {
    let existing = secret_values(original)?;
    for key in ["POSTGRES_PASSWORD", "DUCKFLEET_TOKEN"] {
        if !existing.contains_key(key) {
            bail!("existing Secret lacks {key}; refusing partial upgrade");
        }
    }
    let values = generated["stringData"]
        .as_object_mut()
        .expect("generated values");
    let keys: Vec<String> = values.keys().cloned().collect();
    for key in keys {
        if let Some(value) = existing.get(&key)
            && (key != "CATALOG_DSN" || existing.contains_key("DATABASE_CATALOG_PASSWORD"))
        {
            values.insert(key, json!(value));
        }
    }
    for (key, value) in existing {
        values.entry(key).or_insert_with(|| json!(value));
    }
    Ok(generated)
}

pub fn run(args: Vec<String>) -> Result<()> {
    if args.is_empty() {
        bail!("usage: database-roles <namespace> [--name NAME] [--host HOST] [--check|--upgrade]");
    }
    let namespace = args[0].clone();
    let mut options = args[1..].to_vec();
    let name =
        crate::util::option(&mut options, "--name").unwrap_or_else(|| "duckfleet-dev".into());
    let host = crate::util::option(&mut options, "--host").unwrap_or_else(|| "postgres".into());
    let check = crate::util::flag(&mut options, "--check");
    let upgrade_requested = crate::util::flag(&mut options, "--upgrade");
    let retarget_host = crate::util::option(&mut options, "--retarget");
    crate::util::reject_extra(&options, 0)?;
    if check || upgrade_requested || retarget_host.is_some() {
        let mut input = String::new();
        std::io::Read::read_to_string(&mut std::io::stdin(), &mut input)?;
        let document: Value = serde_json::from_str(&input)?;
        if check {
            return check_secret(&document);
        }
        if let Some(host) = retarget_host {
            println!("{}", serde_json::to_string(&retarget(&document, &host)?)?);
            return Ok(());
        }
        let generated = secret(&namespace, &name, &host)?;
        println!(
            "{}",
            serde_json::to_string(&upgrade(generated, &document)?)?
        );
        return Ok(());
    }
    println!(
        "{}",
        serde_json::to_string(&secret(&namespace, &name, &host)?)?
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roles_are_stable_scoped_and_distinct() {
        let first = secret("duckfleet-dev", "duckfleet-dev", "postgres").unwrap();
        let second = secret("duckfleet-dev", "duckfleet-dev", "postgres").unwrap();
        check_secret(&first).unwrap();
        assert_eq!(
            role_names("duckfleet-dev").unwrap(),
            role_names("duckfleet-dev").unwrap()
        );
        assert_ne!(
            role_names("duckfleet-dev").unwrap(),
            role_names("duckfleet-other").unwrap()
        );
        assert_ne!(
            first["stringData"]["DUCKFLEET_TOKEN"],
            second["stringData"]["DUCKFLEET_TOKEN"]
        );
        assert!(role_names("DuckFleet").is_err());
    }

    #[test]
    fn retarget_changes_only_host_fields() {
        let document = secret("duckfleet-dev", "duckfleet-dev", "postgres").unwrap();
        let original = document.clone();
        let moved = retarget(&document, "db.example.com").unwrap();
        for part in PARTS {
            let key = dsn_key(part);
            let before = original["stringData"][key].as_str().unwrap();
            let after = moved["stringData"][key].as_str().unwrap();
            assert!(after.contains("host=db.example.com"));
            assert_eq!(
                before
                    .split(' ')
                    .filter(|field| !field.starts_with("host="))
                    .collect::<Vec<_>>(),
                after
                    .split(' ')
                    .filter(|field| !field.starts_with("host="))
                    .collect::<Vec<_>>()
            );
        }
    }
}
