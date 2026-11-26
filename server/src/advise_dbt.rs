//! `duckfleet advise dbt`: an offline, advisory scan of dbt artifacts for
//! DAG branches that may rewrite the same DuckLake table concurrently.
//!
//! DuckLake detects conflicts at commit with data-file granularity: two
//! transactions that both write a table conflict unless both only append.
//! This reads `target/manifest.json`, derives each node's writes from its
//! materialization and its literal hook / compiled SQL, and warns when two
//! writers are not ordered by an actual dependency. It never edits project
//! files, changes the DAG, opens sessions, or retries anything.

use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::Path;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum WriteKind {
    /// Only adds rows: two appends never conflict.
    Append,
    /// Deletes, updates, merges, replaces, or alters: conflicts with any write.
    Rewrite,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Write {
    pub relation: String,
    pub kind: WriteKind,
    pub source: String,
}

#[derive(Debug, Default)]
pub struct NodeAnalysis {
    pub id: String,
    pub path: String,
    pub writes: Vec<Write>,
    pub unknown: Vec<String>,
}

/// Static analysis of one manifest. Errors mean the artifacts cannot be
/// analyzed at all; partial coverage is reported, never hidden.
pub fn analyze(manifest: &Value) -> Result<Value, String> {
    let nodes = manifest["nodes"]
        .as_object()
        .ok_or("manifest.json has no nodes object; run dbt compile first")?;
    let mut analyses = BTreeMap::new();
    let mut parents: HashMap<String, Vec<String>> = HashMap::new();
    let mut models = 0usize;
    for (id, node) in nodes {
        let kind = node["resource_type"].as_str().unwrap_or_default();
        if !matches!(kind, "model" | "seed" | "snapshot") {
            continue;
        }
        if node["config"]["enabled"] == false {
            continue;
        }
        if kind == "model" {
            models += 1;
        }
        parents.insert(
            id.clone(),
            node["depends_on"]["nodes"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(Value::as_str)
                .map(str::to_owned)
                .collect(),
        );
        analyses.insert(id.clone(), analyze_node(id, node, kind));
    }
    if models == 0 {
        return Err("manifest.json contains no enabled models to analyze".into());
    }
    let ancestors = ancestor_sets(&parents);
    let ordered = |a: &str, b: &str| {
        ancestors.get(a).is_some_and(|set| set.contains(b))
            || ancestors.get(b).is_some_and(|set| set.contains(a))
    };
    // Per relation, each model's strongest write and every place it comes from.
    let mut writers: BTreeMap<&str, BTreeMap<&str, (WriteKind, Vec<&str>)>> = BTreeMap::new();
    for analysis in analyses.values() {
        for write in &analysis.writes {
            let entry = writers
                .entry(write.relation.as_str())
                .or_default()
                .entry(analysis.id.as_str())
                .or_insert((write.kind, Vec::new()));
            entry.0 = entry.0.max(write.kind);
            entry.1.push(write.source.as_str());
        }
    }
    let mut warnings = Vec::new();
    for (relation, models) in &writers {
        let list = models.iter().collect::<Vec<_>>();
        for (index, (left, (left_kind, left_sources))) in list.iter().enumerate() {
            for (right, (right_kind, right_sources)) in list.iter().skip(index + 1) {
                if ordered(left, right)
                    || (*left_kind == WriteKind::Append && *right_kind == WriteKind::Append)
                {
                    continue;
                }
                let later = analyses.get(**right);
                warnings.push(json!({
                    "relation": relation,
                    "models": [left, right],
                    "writes": [
                        {"model": left, "kind": kind_name(*left_kind), "sources": left_sources},
                        {"model": right, "kind": kind_name(*right_kind), "sources": right_sources},
                    ],
                    "dag_relationship": "independent: neither model is an ancestor of the other, so dbt may run them concurrently",
                    "confidence": "static_possibility",
                    "why": "DuckLake checks conflicts at commit per data file; two writers of one table conflict unless both only append",
                    "remedy": format!(
                        "order them explicitly, e.g. add `-- depends_on: {{{{ ref('{}') }}}}` to {}, or write to separate tables",
                        name_of(left),
                        later.map(|a| a.path.as_str()).unwrap_or("the later model")
                    ),
                    "observed_conflicts": [],
                }));
            }
        }
    }
    let unknown = analyses
        .values()
        .filter(|a| !a.unknown.is_empty())
        .map(|a| json!({"model": a.id, "reasons": a.unknown}))
        .collect::<Vec<_>>();
    let analyzed = analyses.len() - unknown.len();
    Ok(json!({
        "project": manifest["metadata"]["project_name"],
        "dbt_version": manifest["metadata"]["dbt_version"],
        "nodes_considered": analyses.len(),
        "analyzed": analyzed,
        "unknown": unknown.len(),
        "unknown_coverage": unknown,
        "warnings": warnings,
        "writes": analyses
            .values()
            .map(|a| json!({"model": a.id, "writes": a.writes.iter().map(|w| json!({"relation": w.relation, "kind": kind_name(w.kind), "source": w.source})).collect::<Vec<_>>()}))
            .collect::<Vec<_>>(),
        "note": "advisory static analysis: a warning is a possibility the DAG permits, not proof that two writes will overlap at runtime; nothing was changed",
    }))
}

fn analyze_node(id: &str, node: &Value, resource: &str) -> NodeAnalysis {
    let mut analysis = NodeAnalysis {
        id: id.to_owned(),
        path: node["original_file_path"].as_str().unwrap_or(id).to_owned(),
        ..NodeAnalysis::default()
    };
    let database = node["database"].as_str().unwrap_or_default().to_owned();
    let schema = node["schema"].as_str().unwrap_or_default().to_owned();
    let own = node["relation_name"]
        .as_str()
        .and_then(|name| parse_name(&tokenize(name), 0).map(|(parts, _)| parts))
        .map(|parts| qualify(&parts, &database, &schema))
        .or_else(|| {
            let alias = node["alias"].as_str().or_else(|| node["name"].as_str())?;
            Some(qualify(&[alias.to_owned()], &database, &schema))
        });
    let config = &node["config"];
    let materialized = config["materialized"].as_str().unwrap_or(match resource {
        "seed" => "seed",
        "snapshot" => "snapshot",
        _ => "",
    });
    let own_kind = match materialized {
        "ephemeral" => None,
        "table" | "view" | "seed" | "snapshot" | "materialized_view" => Some(WriteKind::Rewrite),
        "incremental" => {
            let strategy = config["incremental_strategy"].as_str();
            let keyed = !config["unique_key"].is_null();
            match strategy {
                Some("append") => Some(WriteKind::Append),
                Some("delete+insert" | "merge" | "insert_overwrite" | "microbatch") => {
                    Some(WriteKind::Rewrite)
                }
                None if keyed => Some(WriteKind::Rewrite),
                None => Some(WriteKind::Append),
                Some(other) => {
                    analysis
                        .unknown
                        .push(format!("unsupported incremental strategy {other:?}"));
                    None
                }
            }
        }
        other => {
            analysis
                .unknown
                .push(format!("unsupported materialization {other:?}"));
            None
        }
    };
    if let (Some(kind), Some(relation)) = (own_kind, own.clone()) {
        analysis.writes.push(Write {
            relation,
            kind,
            source: format!("{materialized} materialization"),
        });
    }
    if resource == "model" && materialized != "ephemeral" {
        match node["compiled_code"].as_str() {
            Some(code) => scan(&mut analysis, code, "compiled SQL", &database),
            None => analysis
                .unknown
                .push("no compiled SQL; run dbt compile so writes can be analyzed".into()),
        }
    }
    for hook in ["pre-hook", "post-hook", "pre_hook", "post_hook"] {
        for (index, entry) in config[hook].as_array().into_iter().flatten().enumerate() {
            let sql = entry["sql"]
                .as_str()
                .or_else(|| entry.as_str())
                .unwrap_or_default();
            scan(
                &mut analysis,
                sql,
                &format!("{} {}", hook.replace('_', "-"), index + 1),
                &database,
            );
        }
    }
    analysis.writes.sort();
    analysis.writes.dedup();
    analysis
}

/// Statements a model or hook runs resolve unqualified names in the session's
/// default `main` schema, not the model's configured schema.
fn scan(analysis: &mut NodeAnalysis, sql: &str, source: &str, database: &str) {
    if sql.contains("{{") || sql.contains("{%") {
        analysis.unknown.push(format!(
            "{source} is dynamic (unrendered Jinja); its targets are unknown"
        ));
        return;
    }
    match writes_in(sql) {
        Ok(found) => {
            for (parts, kind) in found {
                analysis.writes.push(Write {
                    relation: qualify(&parts, database, "main"),
                    kind,
                    source: source.to_owned(),
                });
            }
        }
        Err(reason) => analysis.unknown.push(format!("{source}: {reason}")),
    }
}

#[derive(Clone, Debug, PartialEq)]
enum Token {
    Word(String),
    Quoted(String),
    Dot,
    Semicolon,
    Open,
    Close,
    Other,
}

fn tokenize(sql: &str) -> Vec<Token> {
    let chars: Vec<char> = sql.chars().collect();
    let mut tokens = Vec::new();
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        if c.is_whitespace() {
            i += 1;
        } else if c == '-' && chars.get(i + 1) == Some(&'-') {
            while i < chars.len() && chars[i] != '\n' {
                i += 1;
            }
        } else if c == '/' && chars.get(i + 1) == Some(&'*') {
            i += 2;
            while i + 1 < chars.len() && !(chars[i] == '*' && chars[i + 1] == '/') {
                i += 1;
            }
            i += 2;
        } else if c == '\'' {
            i += 1;
            while i < chars.len() {
                if chars[i] == '\'' && chars.get(i + 1) == Some(&'\'') {
                    i += 2;
                } else if chars[i] == '\'' {
                    break;
                } else {
                    i += 1;
                }
            }
            i += 1;
            tokens.push(Token::Other);
        } else if c == '"' {
            let mut value = String::new();
            i += 1;
            while i < chars.len() {
                if chars[i] == '"' && chars.get(i + 1) == Some(&'"') {
                    value.push('"');
                    i += 2;
                } else if chars[i] == '"' {
                    break;
                } else {
                    value.push(chars[i]);
                    i += 1;
                }
            }
            i += 1;
            tokens.push(Token::Quoted(value));
        } else if c.is_alphanumeric() || c == '_' {
            let start = i;
            while i < chars.len()
                && (chars[i].is_alphanumeric() || chars[i] == '_' || chars[i] == '$')
            {
                i += 1;
            }
            tokens.push(Token::Word(chars[start..i].iter().collect()));
        } else {
            tokens.push(match c {
                '.' => Token::Dot,
                ';' => Token::Semicolon,
                '(' => Token::Open,
                ')' => Token::Close,
                _ => Token::Other,
            });
            i += 1;
        }
    }
    tokens
}

fn is(tokens: &[Token], index: usize, word: &str) -> bool {
    matches!(tokens.get(index), Some(Token::Word(w)) if w.eq_ignore_ascii_case(word))
}

/// Parses `name`, `schema.name`, or `catalog.schema.name` at `index`.
fn parse_name(tokens: &[Token], mut index: usize) -> Option<(Vec<String>, usize)> {
    let mut parts = Vec::new();
    loop {
        match tokens.get(index)? {
            Token::Word(word) => parts.push(word.clone()),
            Token::Quoted(word) => parts.push(word.clone()),
            _ => return None,
        }
        index += 1;
        if tokens.get(index) == Some(&Token::Dot) && parts.len() < 3 {
            index += 1;
        } else {
            return Some((parts, index));
        }
    }
}

/// Write targets of literal SQL. `Err` means the SQL builds statements at run
/// time, so its targets cannot be known statically.
pub fn writes_in(sql: &str) -> Result<Vec<(Vec<String>, WriteKind)>, String> {
    let tokens = tokenize(sql);
    let mut writes = Vec::new();
    let mut statement_start = true;
    let mut depth = 0i32;
    let mut i = 0;
    while i < tokens.len() {
        let token = &tokens[i];
        match token {
            Token::Semicolon => {
                statement_start = true;
                depth = 0;
                i += 1;
                continue;
            }
            Token::Open => depth += 1,
            Token::Close => depth -= 1,
            _ => {}
        }
        if ["execute", "prepare", "call"]
            .iter()
            .any(|w| is(&tokens, i, w))
            && statement_start
        {
            return Err("runs dynamic or procedural SQL (EXECUTE, PREPARE, or CALL)".into());
        }
        let name_at = |start: usize| parse_name(&tokens, start).map(|(parts, _)| parts);
        if is(&tokens, i, "insert") {
            let mut j = i + 1;
            let mut kind = WriteKind::Append;
            if is(&tokens, j, "or") {
                if is(&tokens, j + 1, "replace") {
                    kind = WriteKind::Rewrite;
                }
                j += 2;
            }
            if is(&tokens, j, "into") {
                j += 1;
            }
            if let Some(parts) = name_at(j) {
                let rest = &tokens[j..];
                let upsert = rest.windows(2).any(|w| {
                    matches!((&w[0], &w[1]), (Token::Word(a), Token::Word(b)) if a.eq_ignore_ascii_case("do") && b.eq_ignore_ascii_case("update"))
                });
                writes.push((parts, if upsert { WriteKind::Rewrite } else { kind }));
            }
        } else if is(&tokens, i, "delete") && is(&tokens, i + 1, "from") {
            if let Some(parts) = name_at(i + 2) {
                writes.push((parts, WriteKind::Rewrite));
            }
        } else if is(&tokens, i, "update") && !(i > 0 && is(&tokens, i - 1, "do")) {
            if let Some(parts) = name_at(i + 1) {
                writes.push((parts, WriteKind::Rewrite));
            }
        } else if is(&tokens, i, "merge") && is(&tokens, i + 1, "into") {
            if let Some(parts) = name_at(i + 2) {
                writes.push((parts, WriteKind::Rewrite));
            }
        } else if is(&tokens, i, "truncate") {
            let j = if is(&tokens, i + 1, "table") {
                i + 2
            } else {
                i + 1
            };
            if let Some(parts) = name_at(j) {
                writes.push((parts, WriteKind::Rewrite));
            }
        } else if is(&tokens, i, "create") || is(&tokens, i, "drop") || is(&tokens, i, "alter") {
            let mut j = i + 1;
            if is(&tokens, j, "or") && is(&tokens, j + 1, "replace") {
                j += 2;
            }
            let temporary = is(&tokens, j, "temp") || is(&tokens, j, "temporary");
            if temporary {
                j += 1;
            }
            if is(&tokens, j, "table") || is(&tokens, j, "view") {
                j += 1;
                if is(&tokens, j, "if") {
                    j += if is(&tokens, j + 1, "not") { 3 } else { 2 };
                }
                if !temporary && let Some(parts) = name_at(j) {
                    writes.push((parts, WriteKind::Rewrite));
                }
            }
        } else if is(&tokens, i, "copy")
            && statement_start
            && let Some((parts, next)) = parse_name(&tokens, i + 1)
            && (is(&tokens, next, "from") || tokens.get(next) == Some(&Token::Open))
        {
            writes.push((parts, WriteKind::Append));
        }
        if !matches!(token, Token::Open | Token::Close) && depth <= 0 {
            statement_start = false;
        }
        i += 1;
    }
    Ok(writes)
}

/// DuckDB identifiers compare case-insensitively, quoted or not. Missing
/// parts come from the given default database and schema.
fn qualify(parts: &[String], database: &str, schema: &str) -> String {
    let lower = |s: &str| s.to_ascii_lowercase();
    match parts {
        [name] => format!("{}.{}.{}", lower(database), lower(schema), lower(name)),
        [schema, name] => format!("{}.{}.{}", lower(database), lower(schema), lower(name)),
        [database, schema, name] => {
            format!("{}.{}.{}", lower(database), lower(schema), lower(name))
        }
        _ => parts.join("."),
    }
}

fn ancestor_sets(parents: &HashMap<String, Vec<String>>) -> HashMap<String, BTreeSet<String>> {
    let mut memo: HashMap<String, BTreeSet<String>> = HashMap::new();
    fn visit(
        id: &str,
        parents: &HashMap<String, Vec<String>>,
        memo: &mut HashMap<String, BTreeSet<String>>,
        stack: &mut BTreeSet<String>,
    ) -> BTreeSet<String> {
        if let Some(found) = memo.get(id) {
            return found.clone();
        }
        if !stack.insert(id.to_owned()) {
            return BTreeSet::new();
        }
        let mut set = BTreeSet::new();
        for parent in parents.get(id).into_iter().flatten() {
            set.insert(parent.clone());
            set.extend(visit(parent, parents, memo, stack));
        }
        stack.remove(id);
        memo.insert(id.to_owned(), set.clone());
        set
    }
    let mut stack = BTreeSet::new();
    for id in parents.keys() {
        visit(id, parents, &mut memo, &mut stack);
    }
    memo
}

fn kind_name(kind: WriteKind) -> &'static str {
    match kind {
        WriteKind::Append => "append",
        WriteKind::Rewrite => "rewrite",
    }
}

fn name_of(id: &str) -> &str {
    id.rsplit('.').next().unwrap_or(id)
}

pub fn load(project_dir: &Path) -> Result<Value, String> {
    let path = project_dir.join("target").join("manifest.json");
    let metadata = std::fs::metadata(&path)
        .map_err(|_| format!("{} is missing; run dbt compile first", path.display()))?;
    if metadata.len() > 512 * 1024 * 1024 {
        return Err("manifest.json exceeds 512 MiB".into());
    }
    let bytes = std::fs::read(&path).map_err(|e| format!("cannot read {}: {e}", path.display()))?;
    serde_json::from_slice(&bytes).map_err(|e| format!("manifest.json is not valid JSON: {e}"))
}

/// Attaches recorded `transaction_conflict` history to the warnings by dbt
/// node ID. Model identity is visible only to callers allowed to see it;
/// nothing else from history is copied into the report.
pub fn correlate(report: &mut Value, history: &Value) {
    let conflicts = history["queries"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|q| q["failure_kind"] == "transaction_conflict")
        .filter_map(|q| {
            let model = q["context"]["dbt"]["node_id"].as_str()?;
            Some((
                model.to_owned(),
                json!({
                    "query_id": q["query_id"],
                    "model": model,
                    "invocation_id": q["context"]["dbt"]["invocation_id"],
                    "model_run_id": q["context"]["model_run_id"],
                    "finished_at": q["finished_at"],
                }),
            ))
        })
        .collect::<Vec<_>>();
    for warning in report["warnings"].as_array_mut().into_iter().flatten() {
        let models = warning["models"].clone();
        let observed = conflicts
            .iter()
            .filter(|(model, _)| {
                models
                    .as_array()
                    .is_some_and(|m| m.iter().any(|x| x == model.as_str()))
            })
            .map(|(_, record)| record.clone())
            .collect::<Vec<_>>();
        if !observed.is_empty() {
            warning["confidence"] = json!("observed_race");
        }
        warning["observed_conflicts"] = json!(observed);
    }
}

pub fn render(report: &Value) -> String {
    let mut out = String::new();
    for warning in report["warnings"].as_array().into_iter().flatten() {
        out.push_str(&format!(
            "WARNING {}: {} and {} may write it concurrently\n",
            warning["relation"].as_str().unwrap_or_default(),
            warning["models"][0].as_str().unwrap_or_default(),
            warning["models"][1].as_str().unwrap_or_default()
        ));
        for write in warning["writes"].as_array().into_iter().flatten() {
            out.push_str(&format!(
                "  {} {} ({})\n",
                write["model"].as_str().unwrap_or_default(),
                write["kind"].as_str().unwrap_or_default(),
                write["sources"]
                    .as_array()
                    .map(|s| s
                        .iter()
                        .filter_map(Value::as_str)
                        .collect::<Vec<_>>()
                        .join(", "))
                    .unwrap_or_default()
            ));
        }
        out.push_str(&format!(
            "  dag: {}\n  confidence: {}\n  remedy: {}\n",
            warning["dag_relationship"].as_str().unwrap_or_default(),
            warning["confidence"].as_str().unwrap_or_default(),
            warning["remedy"].as_str().unwrap_or_default()
        ));
        for observed in warning["observed_conflicts"]
            .as_array()
            .into_iter()
            .flatten()
        {
            out.push_str(&format!(
                "  observed transaction_conflict: query {} model {}\n",
                observed["query_id"].as_str().unwrap_or_default(),
                observed["model"].as_str().unwrap_or_default()
            ));
        }
    }
    for unknown in report["unknown_coverage"].as_array().into_iter().flatten() {
        for reason in unknown["reasons"].as_array().into_iter().flatten() {
            out.push_str(&format!(
                "UNKNOWN {}: {}\n",
                unknown["model"].as_str().unwrap_or_default(),
                reason.as_str().unwrap_or_default()
            ));
        }
    }
    if let Some(note) = report["history_note"].as_str() {
        out.push_str(&format!("history: {note}\n"));
    }
    out.push_str(&format!(
        "summary: {} warning(s); {} node(s) analyzed, {} with unknown coverage\n{}\n",
        report["warnings"].as_array().map_or(0, Vec::len),
        report["analyzed"],
        report["unknown"],
        report["note"].as_str().unwrap_or_default()
    ));
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn names(sql: &str) -> Vec<(String, WriteKind)> {
        writes_in(sql)
            .unwrap()
            .into_iter()
            .map(|(parts, kind)| (qualify(&parts, "lake", "main"), kind))
            .collect()
    }

    #[test]
    fn literal_writes_are_found_with_their_kind_and_qualification() {
        use WriteKind::*;
        assert_eq!(names("select * from a join b using (id)"), vec![]);
        assert_eq!(
            names("insert into s.t values (1)"),
            vec![("lake.s.t".into(), Append)]
        );
        assert_eq!(
            names("insert into t select 1 on conflict (id) do update set v = 1"),
            vec![("lake.main.t".into(), Rewrite)]
        );
        assert_eq!(
            names(
                "delete from \"Lake\".\"S\".\"T\" where x = 'delete from other'; update u set a = 1"
            ),
            vec![
                ("lake.s.t".into(), Rewrite),
                ("lake.main.u".into(), Rewrite)
            ]
        );
        assert_eq!(
            names("create or replace table x.y as select 1"),
            vec![("lake.x.y".into(), Rewrite)]
        );
        assert_eq!(names("create temp table scratch as select 1"), vec![]);
        assert_eq!(
            names("drop table if exists t; truncate table v"),
            vec![
                ("lake.main.t".into(), Rewrite),
                ("lake.main.v".into(), Rewrite)
            ]
        );
        assert_eq!(
            names("merge into tgt using src on tgt.id = src.id when matched then delete"),
            vec![("lake.main.tgt".into(), Rewrite)]
        );
        assert_eq!(names("-- delete from commented\nselect 1"), vec![]);
        assert_eq!(
            names("copy t from 'file.csv'"),
            vec![("lake.main.t".into(), Append)]
        );
        assert!(writes_in("execute stmt").is_err());
    }

    fn manifest(nodes: Value) -> Value {
        json!({"metadata": {"project_name": "p", "dbt_version": "2"}, "nodes": nodes})
    }

    fn model(name: &str, parents: &[&str], hooks: &[&str]) -> Value {
        json!({
            "resource_type": "model",
            "database": "lake", "schema": "s", "alias": name,
            "relation_name": format!("\"lake\".\"s\".\"{name}\""),
            "original_file_path": format!("models/{name}.sql"),
            "config": {"materialized": "table", "post-hook": hooks.iter().map(|h| json!({"sql": h})).collect::<Vec<_>>()},
            "depends_on": {"nodes": parents.iter().map(|p| format!("model.p.{p}")).collect::<Vec<_>>()},
            "compiled_code": "select 1",
        })
    }

    #[test]
    fn only_independent_non_append_writers_of_one_table_warn() {
        let report = analyze(&manifest(json!({
            "model.p.shared": model("shared", &[], &[]),
            "model.p.a": model("a", &["shared"], &["delete from s.shared where k = 'a'"]),
            "model.p.m": model("m", &[], &["delete from shared"]),
            "model.p.b": model("b", &["shared"], &["delete from s.shared where k = 'b'"]),
            "model.p.c": model("c", &["a", "b"], &["delete from lake.s.shared"]),
            "model.p.x": model("x", &[], &["insert into s.events values (1)"]),
            "model.p.y": model("y", &[], &["insert into s.events values (2)"]),
            "model.p.dyn": model("dyn", &[], &["{{ 'delete from ' ~ this }}"]),
        })))
        .unwrap();
        let warnings = report["warnings"].as_array().unwrap();
        assert_eq!(warnings.len(), 1, "{report:#}");
        assert_eq!(warnings[0]["relation"], "lake.s.shared");
        assert_eq!(warnings[0]["models"], json!(["model.p.a", "model.p.b"]));
        assert!(warnings[0]["remedy"].as_str().unwrap().contains("ref('a')"));
        assert_eq!(report["unknown"], 1);
        assert_eq!(report["analyzed"], 7);
    }

    #[test]
    fn missing_compiled_sql_and_custom_materializations_are_unknown_not_clear() {
        let mut missing = model("m", &[], &[]);
        missing["compiled_code"] = Value::Null;
        let mut custom = model("c", &[], &[]);
        custom["config"]["materialized"] = json!("my_custom");
        let report = analyze(&manifest(
            json!({"model.p.m": missing, "model.p.c": custom}),
        ))
        .unwrap();
        assert_eq!(report["unknown"], 2);
        assert_eq!(report["analyzed"], 0);
        assert!(analyze(&json!({"nodes": {}})).is_err());
        assert!(analyze(&json!({})).is_err());
    }

    #[test]
    fn observed_conflicts_attach_by_model_identity_only() {
        let mut report = analyze(&manifest(json!({
            "model.p.a": model("a", &[], &["delete from s.t"]),
            "model.p.b": model("b", &[], &["delete from s.t"]),
        })))
        .unwrap();
        correlate(
            &mut report,
            &json!({"queries": [
                {"query_id": "q1", "failure_kind": "transaction_conflict", "sql": "secret", "context": {"dbt": {"node_id": "model.p.b", "invocation_id": "i"}}},
                {"query_id": "q2", "failure_kind": "transaction_conflict", "context": null},
            ]}),
        );
        let warning = &report["warnings"][0];
        assert_eq!(warning["confidence"], "observed_race");
        assert_eq!(warning["observed_conflicts"][0]["query_id"], "q1");
        assert!(!report.to_string().contains("secret"));
    }
}
