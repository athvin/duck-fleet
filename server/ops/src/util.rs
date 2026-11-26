use anyhow::{Context, Result, bail};
use base64::Engine;
use ring::rand::{SecureRandom, SystemRandom};
use serde::Deserialize;
use serde_json::{Map, Value};
use std::{
    collections::{BTreeMap, BTreeSet},
    ffi::OsStr,
    fs,
    path::{Path, PathBuf},
    process::{Command, Output},
};

pub fn root() -> PathBuf {
    let current = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
    if current.join("compose.yaml").is_file() {
        return current;
    }
    let compiled_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("repository root")
        .to_path_buf();
    if compiled_root.is_dir() {
        compiled_root
    } else {
        current
    }
}

pub fn read_json(path: impl AsRef<Path>) -> Result<Value> {
    let path = path.as_ref();
    serde_json::from_slice(&fs::read(path).with_context(|| format!("read {}", path.display()))?)
        .with_context(|| format!("parse JSON from {}", path.display()))
}

pub fn read_json_from_string(text: &str) -> Result<Value> {
    serde_json::from_str(text).context("parse embedded JSON")
}

pub fn write_json(path: impl AsRef<Path>, value: &Value) -> Result<()> {
    let path = path.as_ref();
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut bytes = serde_json::to_vec_pretty(value)?;
    bytes.push(b'\n');
    fs::write(path, bytes).with_context(|| format!("write {}", path.display()))
}

pub fn read_yaml(path: impl AsRef<Path>) -> Result<Value> {
    let path = path.as_ref();
    serde_yaml::from_slice(&fs::read(path).with_context(|| format!("read {}", path.display()))?)
        .with_context(|| format!("parse YAML from {}", path.display()))
}

pub fn yaml_documents(text: &str) -> Result<Vec<Value>> {
    serde_yaml::Deserializer::from_str(text)
        .map(|document| Value::deserialize(document).with_context(|| "parse YAML document"))
        .collect()
}

pub fn yaml_documents_from_reader(reader: impl std::io::Read) -> Result<Vec<Value>> {
    let mut text = String::new();
    std::io::Read::read_to_string(&mut reader.take(64 * 1024 * 1024), &mut text)?;
    yaml_documents(&text)
}

pub fn yaml_string(documents: &[Value]) -> Result<String> {
    let mut output = String::new();
    for document in documents {
        output.push_str("---\n");
        output.push_str(&serde_yaml::to_string(document)?);
    }
    Ok(output)
}

pub fn env_map(value: Option<&Value>) -> BTreeMap<String, String> {
    value
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|entry| {
            Some((
                entry.get("name")?.as_str()?.to_owned(),
                entry
                    .get("value")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_owned(),
            ))
        })
        .collect()
}

pub fn object(value: &Value) -> &Map<String, Value> {
    value.as_object().unwrap_or_else(|| {
        static EMPTY: std::sync::OnceLock<Map<String, Value>> = std::sync::OnceLock::new();
        EMPTY.get_or_init(Map::new)
    })
}

pub fn array(value: Option<&Value>) -> &[Value] {
    value
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or_default()
}

pub fn text(value: Option<&Value>) -> &str {
    value.and_then(Value::as_str).unwrap_or_default()
}

pub fn decode_base64(value: &str) -> Result<String> {
    let bytes = base64::engine::general_purpose::STANDARD.decode(value)?;
    String::from_utf8(bytes).context("decoded Secret value is not UTF-8")
}

pub fn encode_base64(value: &[u8]) -> String {
    base64::engine::general_purpose::STANDARD.encode(value)
}

pub fn random_hex(bytes: usize) -> Result<String> {
    let mut value = vec![0_u8; bytes];
    SystemRandom::new()
        .fill(&mut value)
        .map_err(|_| anyhow::anyhow!("secure random generation failed"))?;
    Ok(value.iter().map(|byte| format!("{byte:02x}")).collect())
}

pub fn run<I, S>(command: I) -> Result<Output>
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    let command: Vec<String> = command
        .into_iter()
        .map(|part| part.as_ref().to_string_lossy().into_owned())
        .collect();
    let (program, args) = command.split_first().context("empty command")?;
    let output = Command::new(program)
        .args(args)
        .current_dir(root())
        .output()
        .with_context(|| format!("run {program}"))?;
    if !output.status.success() {
        bail!(
            "command failed ({}): {}\n{}{}",
            output.status,
            command.join(" "),
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
    Ok(output)
}

pub fn run_status<I, S>(command: I) -> Result<Output>
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    let command: Vec<String> = command
        .into_iter()
        .map(|part| part.as_ref().to_string_lossy().into_owned())
        .collect();
    let (program, args) = command.split_first().context("empty command")?;
    Command::new(program)
        .args(args)
        .current_dir(root())
        .output()
        .with_context(|| format!("run {program}"))
}

pub fn run_with_stdin_status<I, S>(command: I, input: &str) -> Result<Output>
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    let command: Vec<String> = command
        .into_iter()
        .map(|part| part.as_ref().to_string_lossy().into_owned())
        .collect();
    let (program, args) = command.split_first().context("empty command")?;
    let mut child = Command::new(program)
        .args(args)
        .current_dir(root())
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .with_context(|| format!("run {program}"))?;
    std::io::Write::write_all(
        &mut child.stdin.take().context("open child stdin")?,
        input.as_bytes(),
    )?;
    Ok(child.wait_with_output()?)
}

pub fn run_with_stdin<I, S>(command: I, input: &str) -> Result<Output>
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    let command: Vec<String> = command
        .into_iter()
        .map(|part| part.as_ref().to_string_lossy().into_owned())
        .collect();
    let output = run_with_stdin_status(&command, input)?;
    if !output.status.success() {
        bail!(
            "command failed ({}): {}\n{}{}",
            output.status,
            command.join(" "),
            stdout_text(&output),
            stderr_text(&output)
        );
    }
    Ok(output)
}

pub fn output_text<I, S>(command: I) -> Result<String>
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    let output = run(command)?;
    Ok(String::from_utf8(output.stdout)?.trim().to_owned())
}

pub fn stdout_text(output: &Output) -> String {
    String::from_utf8_lossy(&output.stdout).into_owned()
}

pub fn stderr_text(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}

pub fn option(args: &mut Vec<String>, name: &str) -> Option<String> {
    let index = args.iter().position(|arg| arg == name)?;
    args.remove(index);
    if index >= args.len() {
        return Some(String::new());
    }
    Some(args.remove(index))
}

pub fn flag(args: &mut Vec<String>, name: &str) -> bool {
    if let Some(index) = args.iter().position(|arg| arg == name) {
        args.remove(index);
        true
    } else {
        false
    }
}

pub fn reject_extra(args: &[String], allowed_positionals: usize) -> Result<()> {
    if args.len() > allowed_positionals || args.iter().any(|arg| arg.starts_with('-')) {
        bail!("unexpected arguments: {}", args.join(" "));
    }
    Ok(())
}

pub fn string_set(value: Option<&Value>) -> BTreeSet<String> {
    array(value)
        .iter()
        .filter_map(Value::as_str)
        .map(str::to_owned)
        .collect()
}
