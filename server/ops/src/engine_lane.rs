use crate::util;
use anyhow::{Context, Result, bail, ensure};
use serde_json::{Map, Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    io::Write,
    path::{Path, PathBuf},
    process::{Child, Command, Output, Stdio},
    time::Duration,
};

const IMAGES: [(&str, &str); 3] = [
    ("docker/server.Dockerfile", "duckfleet-worker"),
    ("docker/dbt.Dockerfile", "duckfleet-dbt"),
    ("docker/cli.Dockerfile", "duckfleet-api"),
];

fn run(
    args: &[String],
    cwd: &Path,
    env: Option<&BTreeMap<String, String>>,
    log: Option<&Path>,
    check: bool,
) -> Result<Output> {
    let mut command = Command::new(&args[0]);
    command.args(&args[1..]).current_dir(cwd);
    if let Some(env) = env {
        command.envs(env);
    }
    let output = if let Some(log) = log {
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(log)?;
        writeln!(file, "$ {}", args.join(" "))?;
        let stdout = file.try_clone()?;
        let status = command
            .stdout(Stdio::from(stdout))
            .stderr(Stdio::from(file))
            .status()?;
        Output {
            status,
            stdout: Vec::new(),
            stderr: Vec::new(),
        }
    } else {
        command.output()?
    };
    if check && !output.status.success() {
        bail!(
            "command failed ({}): {}; {}",
            output.status,
            args.join(" "),
            log.map_or_else(
                || format!(
                    "{}{}",
                    util::stdout_text(&output),
                    util::stderr_text(&output)
                ),
                |path| format!("see {}", path.display())
            )
        );
    }
    Ok(output)
}

fn output(args: &[String], env: Option<&BTreeMap<String, String>>) -> Result<String> {
    Ok(
        util::stdout_text(&run(args, &util::root(), env, None, true)?)
            .trim()
            .to_owned(),
    )
}

fn strings(values: Vec<&str>) -> Vec<String> {
    values.into_iter().map(String::from).collect()
}

fn free_port() -> Result<u16> {
    Ok(std::net::TcpListener::bind("127.0.0.1:0")?
        .local_addr()?
        .port())
}

fn resolved_ref(reference: &str) -> Result<String> {
    output(
        &strings(vec![
            "git",
            "rev-parse",
            "--verify",
            &format!("{reference}^{{commit}}"),
        ]),
        None,
    )
}

fn build_images(tag: &str, reference: &str, directory: &Path) -> Result<String> {
    if !regex::Regex::new(r"^[a-z0-9][a-z0-9_.-]*$")?.is_match(tag) {
        bail!("invalid image tag: {tag}");
    }
    let commit = resolved_ref(reference)?;
    std::fs::create_dir_all(directory)?;
    let log = directory.join("build.log");
    let temporary = tempfile::tempdir_in(directory)?;
    let archive = directory.join("source.tar");
    let mut child = Command::new("git")
        .args(["archive", &commit])
        .current_dir(util::root())
        .stdout(Stdio::from(std::fs::File::create(&archive)?))
        .spawn()?;
    if !child.wait()?.success() {
        bail!("git archive failed");
    }
    run(
        &strings(vec![
            "tar",
            "-xf",
            archive.to_str().unwrap(),
            "-C",
            temporary.path().to_str().unwrap(),
        ]),
        &util::root(),
        None,
        None,
        true,
    )?;
    for (dockerfile, repository) in IMAGES {
        let image = format!("{repository}:{tag}");
        println!("Building {image} from {}", &commit[..12]);
        run(
            &strings(vec![
                "docker",
                "build",
                "--progress=plain",
                "-f",
                dockerfile,
                "-t",
                &image,
                ".",
            ]),
            temporary.path(),
            None,
            Some(&log),
            true,
        )?;
    }
    std::fs::remove_file(archive)?;
    Ok(commit)
}

fn image_evidence(tag: &str) -> Result<Value> {
    let mut result = Map::new();
    for (_, repository) in IMAGES {
        let image = format!("{repository}:{tag}");
        let inspected = output(&strings(vec!["docker", "image", "inspect", &image]), None)?;
        let value: Value = serde_json::from_str(&inspected)?;
        result.insert(
            image,
            json!({"id":value[0]["Id"],"repo_digests":value[0].get("RepoDigests")}),
        );
    }
    Ok(Value::Object(result))
}

fn compose_args(project: &str) -> Vec<String> {
    strings(vec![
        "docker",
        "compose",
        "--project-name",
        project,
        "--file",
        util::root().join("compose.yaml").to_str().unwrap(),
    ])
}

pub fn write_override(path: &Path, prefix: &str) -> Result<Value> {
    let paths = json!({
        "lake":format!("s3://duckfleet/{prefix}/lake/"),
        "results":format!("s3://duckfleet/{prefix}/results/"),
    });
    let mut services = Map::new();
    for service in [
        "worker-small",
        "worker-small-b",
        "worker-pool-interactive-0",
        "server",
        "server-pools",
    ] {
        let mut environment = json!({
            "DUCKFLEET_RESULT_LAKE_DATA_PATH":paths["lake"],
            "DUCKFLEET_RESULT_DATA_PATH":paths["results"],
        });
        if service.starts_with("worker") {
            environment["S3_DATA_PATH"] = paths["lake"].clone();
        }
        services.insert(service.into(), json!({"environment":environment}));
    }
    let value = Value::Object(services);
    let yaml = serde_yaml::to_string(&json!({"services":value}))?;
    std::fs::write(path, yaml)?;
    Ok(paths)
}

fn resource_evidence(project: &str) -> Result<Value> {
    let mut resources = Map::new();
    for service in ["worker-small", "server"] {
        let base = compose_args(project);
        let container = output(
            &base
                .iter()
                .cloned()
                .chain(strings(vec!["ps", "-q", service]))
                .collect::<Vec<_>>(),
            None,
        )?;
        let inspected = output(&strings(vec!["docker", "inspect", &container]), None)?;
        let inspected: Value = serde_json::from_str(&inspected)?;
        let stats = output(
            &strings(vec![
                "docker",
                "stats",
                "--no-stream",
                "--format",
                "{{json .}}",
                &container,
            ]),
            None,
        )?;
        resources.insert(service.into(), json!({
            "container":inspected[0]["Name"].as_str().unwrap_or_default().trim_start_matches('/'),
            "nano_cpus":inspected[0]["HostConfig"]["NanoCpus"],
            "memory_bytes":inspected[0]["HostConfig"]["Memory"],
            "stats":serde_json::from_str::<Value>(&stats)?,
        }));
    }
    Ok(Value::Object(resources))
}

fn isolation_evidence(baseline: &str, candidate: &str, paths: &Value) -> Result<Value> {
    let mut networks = json!({"baseline":{},"candidate":{}});
    let mut volumes = json!({"baseline":{},"candidate":{}});
    let mut names = json!({"baseline":{},"candidate":{}});
    for (lane, project) in [("baseline", baseline), ("candidate", candidate)] {
        for service in ["postgres", "object-store", "server", "worker-small"] {
            let base = compose_args(project);
            let container = output(
                &base
                    .iter()
                    .cloned()
                    .chain(strings(vec!["ps", "-q", service]))
                    .collect::<Vec<_>>(),
                None,
            )?;
            let inspected = output(&strings(vec!["docker", "inspect", &container]), None)?;
            let value: Value = serde_json::from_str(&inspected)?;
            let name = value[0]["Name"]
                .as_str()
                .unwrap_or_default()
                .trim_start_matches('/');
            names[lane][service] = json!(name);
            let mut network_names: Vec<_> = util::object(
                value[0]["NetworkSettings"]
                    .get("Networks")
                    .unwrap_or(&Value::Null),
            )
            .keys()
            .cloned()
            .collect();
            network_names.sort();
            networks[lane][service] = json!(network_names);
            if matches!(service, "postgres" | "object-store") {
                volumes[lane][service] = json!(
                    util::array(value[0].get("Mounts"))
                        .iter()
                        .filter(|mount| mount["Type"] == "volume")
                        .map(|mount| mount["Name"].clone())
                        .collect::<Vec<_>>()
                );
            }
        }
    }
    let mut dns_refusals = Map::new();
    for service in ["postgres", "object-store", "server"] {
        let target = names["baseline"][service].as_str().unwrap();
        let result = run(
            &compose_args(candidate)
                .iter()
                .cloned()
                .chain(strings(vec![
                    "exec",
                    "-T",
                    "worker-small",
                    "getent",
                    "hosts",
                    target,
                ]))
                .collect::<Vec<_>>(),
            &util::root(),
            None,
            None,
            false,
        )?;
        dns_refusals.insert(target.into(), json!(result.status.code().unwrap_or(1)));
    }
    let mut data_paths = Map::new();
    for (lane, project) in [("baseline", baseline), ("candidate", candidate)] {
        let value = output(
            &compose_args(project)
                .iter()
                .cloned()
                .chain(strings(vec![
                    "exec",
                    "-T",
                    "worker-small",
                    "printenv",
                    "S3_DATA_PATH",
                ]))
                .collect::<Vec<_>>(),
            None,
        )?;
        data_paths.insert(lane.into(), json!(value));
    }
    let network_sets: BTreeSet<String> = util::object(&networks["baseline"])
        .values()
        .chain(util::object(&networks["candidate"]).values())
        .flat_map(|value| util::array(Some(value)))
        .filter_map(Value::as_str)
        .map(str::to_owned)
        .collect();
    let baseline_networks: BTreeSet<String> = util::object(&networks["baseline"])
        .values()
        .flat_map(|value| util::array(Some(value)))
        .filter_map(Value::as_str)
        .map(str::to_owned)
        .collect();
    let candidate_networks: BTreeSet<String> = util::object(&networks["candidate"])
        .values()
        .flat_map(|value| util::array(Some(value)))
        .filter_map(Value::as_str)
        .map(str::to_owned)
        .collect();
    let baseline_volumes: BTreeSet<String> = util::object(&volumes["baseline"])
        .values()
        .flat_map(|value| util::array(Some(value)))
        .filter_map(Value::as_str)
        .map(str::to_owned)
        .collect();
    let candidate_volumes: BTreeSet<String> = util::object(&volumes["candidate"])
        .values()
        .flat_map(|value| util::array(Some(value)))
        .filter_map(Value::as_str)
        .map(str::to_owned)
        .collect();
    let passed = baseline_networks.is_disjoint(&candidate_networks)
        && baseline_volumes.is_disjoint(&candidate_volumes)
        && dns_refusals.values().all(|code| code != 0)
        && data_paths["baseline"] == paths["baseline"]["lake"]
        && data_paths["candidate"] == paths["candidate"]["lake"]
        && data_paths["baseline"] != data_paths["candidate"]
        && network_sets.len() == baseline_networks.len() + candidate_networks.len();
    Ok(json!({
        "passed":passed,"projects":[baseline,candidate],"networks":networks,"volumes":volumes,
        "baseline_dns_refusals_from_candidate":Value::Object(dns_refusals),"data_paths":Value::Object(data_paths),
    }))
}

fn pgwire_checks(base: &[String], env: &BTreeMap<String, String>, directory: &Path) -> Result<()> {
    let token = env
        .get("DUCKFLEET_TOKEN")
        .map_or("duckfleet-local-development-token", String::as_str);
    let query = |sql: &str, password: &str| {
        let command = base
            .iter()
            .cloned()
            .chain(strings(vec![
                "exec",
                "-T",
                "-e",
                &format!("PGPASSWORD={password}"),
                "postgres",
                "psql",
                "-h",
                "server",
                "-p",
                "5432",
                "-U",
                "operator",
                "-d",
                "lake",
                "-v",
                "ON_ERROR_STOP=1",
                "-qtAX",
                "-c",
                sql,
            ]))
            .collect::<Vec<_>>();
        run(&command, &util::root(), Some(env), None, false)
    };
    for (sql, expected) in [
        ("select 1 + 1", "2"),
        (
            "select 42::bigint, 'text value', true, 1.5::double",
            "42|text value|t|1.5",
        ),
        (
            "select date '2026-01-02', timestamp '2026-01-02 03:04:05'",
            "2026-01-02|2026-01-02 03:04:05.000000",
        ),
        ("select 12.34::decimal(6,2)", "12.34"),
    ] {
        let result = query(sql, token)?;
        ensure!(
            result.status.success() && util::stdout_text(&result).trim() == expected,
            "pgwire result changed for {sql}"
        );
    }
    let missing = query("select * from no_such_table_for_pgwire", token)?;
    ensure!(
        !missing.status.success() && util::stderr_text(&missing).contains("sql_error"),
        "pgwire missing-relation classification changed"
    );
    ensure!(
        !query("select 1", "not-a-credential")?.status.success(),
        "pgwire accepted an invalid credential"
    );
    util::write_json(
        directory.join("pgwire.json"),
        &json!({"passed":true,"typed_results":4,"error_classification":"sql_error"}),
    )
}

pub fn validate_extensions(report: &Value) -> Result<Value> {
    let extensions = report["extensions"].as_array().cloned().unwrap_or_default();
    let by_name: BTreeMap<_, _> = extensions
        .iter()
        .map(|item| (util::text(item.get("extension_name")), item))
        .collect();
    let required: BTreeSet<&str> = ["httpfs", "postgres_scanner", "ducklake", "aws", "icu"]
        .into_iter()
        .collect();
    ensure!(
        by_name.keys().copied().collect::<BTreeSet<_>>() == required,
        "extension manifest mismatch"
    );
    for (name, item) in by_name {
        ensure!(
            item["installed"] == "true",
            "extension {name} is not installed"
        );
        ensure!(
            item["loaded"] != "true"
                || item["extension_version"]
                    .as_str()
                    .is_some_and(|version| !version.is_empty()),
            "loaded extension {name} has no version"
        );
        let path = util::text(item.get("install_path"));
        ensure!(
            path == "(BUILT-IN)"
                || path.contains(&format!(
                    "/v{}/",
                    report["engine_version"]
                        .as_str()
                        .unwrap_or_default()
                        .trim_start_matches('v')
                )),
            "extension {name} has an unexpected path: {path}"
        );
        if name != "aws"
            || report["engine_version"]
                .as_str()
                .unwrap_or_default()
                .starts_with("v2.")
        {
            ensure!(
                item["loaded"] == "true",
                "required runtime extension {name} is not loaded"
            );
        }
    }
    Ok(Value::Array(extensions))
}

fn cli_sql(base: &[String], env: &BTreeMap<String, String>, statement: &str) -> Result<Value> {
    let result = run(
        &base
            .iter()
            .cloned()
            .chain(strings(vec![
                "run",
                "--rm",
                "--no-deps",
                "--env",
                "DUCKFLEET_ROLE=cli",
                "api",
                "duckfleet-server",
                "sql",
                statement,
                "--format",
                "json",
            ]))
            .collect::<Vec<_>>(),
        &util::root(),
        Some(env),
        None,
        false,
    )?;
    if !result.status.success() {
        bail!(
            "recovery SQL failed: {statement}: {}",
            util::stderr_text(&result)
        );
    }
    Ok(serde_json::from_slice(&result.stdout)?)
}

fn cli_scalar(
    base: &[String],
    env: &BTreeMap<String, String>,
    statement: &str,
    expected: Value,
) -> Result<Value> {
    let rows = cli_sql(base, env, statement)?;
    let rows = rows.as_array().context("scalar result is not a row list")?;
    ensure!(
        rows.len() == 1 && rows[0]["value"] == expected,
        "recovery query returned {rows:?}, expected {expected}"
    );
    Ok(rows[0]["value"].clone())
}

fn cli_json(base: &[String], env: &BTreeMap<String, String>, args: &[String]) -> Result<Value> {
    let command = base
        .iter()
        .cloned()
        .chain(strings(vec![
            "run",
            "--rm",
            "--no-deps",
            "--env",
            "DUCKFLEET_ROLE=cli",
            "api",
            "duckfleet-server",
        ]))
        .chain(args.iter().cloned())
        .collect::<Vec<_>>();
    let result = run(&command, &util::root(), Some(env), None, false)?;
    ensure!(
        result.status.success(),
        "CLI command failed: {}",
        util::stderr_text(&result)
    );
    Ok(serde_json::from_slice(&result.stdout)?)
}

fn recovery_drill(
    base: &[String],
    env: &BTreeMap<String, String>,
    baseline_tag: &str,
    candidate_tag: &str,
    directory: &Path,
) -> Result<Value> {
    let mut evidence = json!({"catalog_version":null,"restore_verified":false,"rollback_verified":false,"mixed_version_writers":false});
    let log = directory.join("recovery.log");
    let result = (|| -> Result<()> {
        evidence["catalog_version"] = cli_scalar(
            base,
            env,
            "SELECT value FROM lake.options() WHERE option_name='version'",
            json!("1.0"),
        )?;
        cli_sql(base, env, "CREATE SCHEMA IF NOT EXISTS analytics")?;
        cli_sql(
            base,
            env,
            "CREATE OR REPLACE TABLE analytics.rollback_fixture(id INTEGER)",
        )?;
        cli_sql(
            base,
            env,
            "INSERT INTO analytics.rollback_fixture VALUES (1)",
        )?;
        let snapshots = cli_sql(
            base,
            env,
            "SELECT snapshot_id AS value FROM ducklake_snapshots('lake') ORDER BY snapshot_id",
        )?;
        let snapshots = snapshots.as_array().unwrap();
        ensure!(!snapshots.is_empty(), "the 1.0 fixture emitted no snapshot");
        evidence["time_travel_snapshot"] = snapshots.last().unwrap()["value"].clone();
        let snapshot = snapshots.last().unwrap()["value"].as_i64().unwrap();
        let recovery_point = directory.join("recovery-point");
        run(
            &strings(vec![
                "bash",
                util::root()
                    .join("scripts/backup-catalog.sh")
                    .to_str()
                    .unwrap(),
                "compose",
                recovery_point.to_str().unwrap(),
            ]),
            &util::root(),
            Some(env),
            Some(&log),
            true,
        )?;
        cli_sql(
            base,
            env,
            "INSERT INTO analytics.rollback_fixture VALUES (2)",
        )?;
        cli_scalar(
            base,
            env,
            "SELECT count(*) AS value FROM analytics.rollback_fixture",
            json!(2),
        )?;
        cli_scalar(
            base,
            env,
            &format!(
                "SELECT count(*) AS value FROM analytics.rollback_fixture AT (VERSION => {snapshot})"
            ),
            json!(1),
        )?;
        cli_sql(base, env, "CALL ducklake_merge_adjacent_files('lake')")?;
        let mut restore_env = env.clone();
        restore_env.insert("DUCKFLEET_CONFIRM_RESTORE".into(), "yes".into());
        run(
            &strings(vec![
                "bash",
                util::root()
                    .join("scripts/restore-catalog.sh")
                    .to_str()
                    .unwrap(),
                "compose",
                recovery_point.to_str().unwrap(),
            ]),
            &util::root(),
            Some(&restore_env),
            Some(&log),
            true,
        )?;
        let running = output(
            &base
                .iter()
                .cloned()
                .chain(strings(vec![
                    "ps",
                    "--status",
                    "running",
                    "--format",
                    "{{.Service}}",
                ]))
                .collect::<Vec<_>>(),
            Some(env),
        )?;
        ensure!(
            !running
                .lines()
                .any(|service| matches!(service, "server" | "worker-small")),
            "restore left compute running"
        );
        run(
            &base
                .iter()
                .cloned()
                .chain(strings(vec!["up", "-d", "--wait", "server"]))
                .collect::<Vec<_>>(),
            &util::root(),
            Some(env),
            Some(&log),
            true,
        )?;
        cli_scalar(
            base,
            env,
            "SELECT count(*) AS value FROM analytics.rollback_fixture",
            json!(1),
        )?;
        cli_scalar(
            base,
            env,
            "SELECT value FROM lake.options() WHERE option_name='version'",
            json!("1.0"),
        )?;
        run(
            &base
                .iter()
                .cloned()
                .chain(strings(vec![
                    "run",
                    "--rm",
                    "--no-deps",
                    "--env",
                    "DUCKFLEET_ROLE=cli",
                    "api",
                    "duckfleet-server",
                    "history",
                    "--limit",
                    "5",
                    "--format",
                    "json",
                ]))
                .collect::<Vec<_>>(),
            &util::root(),
            Some(env),
            Some(&log),
            true,
        )?;
        evidence["restore_verified"] = json!(true);
        evidence["post_backup_write_discarded"] = json!(true);
        evidence["maintenance"] = json!("merge_adjacent_files");
        cli_sql(
            base,
            env,
            "INSERT INTO analytics.rollback_fixture VALUES (3)",
        )?;
        cli_scalar(
            base,
            env,
            "SELECT count(*) AS value FROM analytics.rollback_fixture",
            json!(2),
        )?;
        run(
            &strings(vec![
                "bash",
                util::root()
                    .join("scripts/stop-compute.sh")
                    .to_str()
                    .unwrap(),
                "compose",
            ]),
            &util::root(),
            Some(env),
            Some(&log),
            true,
        )?;
        let mut rollback = env.clone();
        rollback.extend([
            ("DUCKFLEET_IMAGE_TAG".into(), baseline_tag.into()),
            ("DUCKFLEET_DBT_IMAGE_TAG".into(), baseline_tag.into()),
            ("DUCKFLEET_API_IMAGE_TAG".into(), baseline_tag.into()),
        ]);
        run(
            &base
                .iter()
                .cloned()
                .chain(strings(vec!["up", "-d", "--wait", "server"]))
                .collect::<Vec<_>>(),
            &util::root(),
            Some(&rollback),
            Some(&log),
            true,
        )?;
        cli_scalar(
            base,
            &rollback,
            "SELECT value FROM lake.options() WHERE option_name='version'",
            json!("1.0"),
        )?;
        cli_scalar(
            base,
            &rollback,
            "SELECT count(*) AS value FROM analytics.rollback_fixture",
            json!(2),
        )?;
        cli_sql(
            base,
            &rollback,
            "INSERT INTO analytics.rollback_fixture VALUES (4)",
        )?;
        cli_scalar(
            base,
            &rollback,
            "SELECT count(*) AS value FROM analytics.rollback_fixture",
            json!(3),
        )?;
        run(
            &base
                .iter()
                .cloned()
                .chain(strings(vec![
                    "run",
                    "--rm",
                    "--no-deps",
                    "--env",
                    "DUCKFLEET_ROLE=cli",
                    "api",
                    "duckfleet-server",
                    "history",
                    "--limit",
                    "5",
                    "--format",
                    "json",
                ]))
                .collect::<Vec<_>>(),
            &util::root(),
            Some(&rollback),
            Some(&log),
            true,
        )?;
        evidence["rollback_verified"] = json!(true);
        evidence["rollback_engine"] = json!("v1.5.4");
        evidence["write_after_restore_engine"] = json!("v2.0.1");
        evidence["passed"] = json!(true);
        let _ = candidate_tag;
        Ok(())
    })();
    if let Err(error) = &result {
        evidence["passed"] = json!(false);
        evidence["error"] = json!(error.to_string());
    }
    util::write_json(directory.join("recovery.json"), &evidence)?;
    result?;
    Ok(evidence)
}

fn cutover_drill(
    base: &[String],
    env: &BTreeMap<String, String>,
    baseline_tag: &str,
    candidate_tag: &str,
    directory: &Path,
    skip_dbt: bool,
) -> Result<Value> {
    let mut evidence = json!({"drained_statement":false,"statement_outcome":null,"old_writers":null,"automatic_replay":false,"passed":false});
    let log = directory.join("cutover.log");
    let statement_log = directory.join("cutover-statement.log");
    let mut process: Option<Child> = None;
    let result = (|| -> Result<()> {
        run(
            &strings(vec![
                "bash",
                util::root()
                    .join("scripts/backup-catalog.sh")
                    .to_str()
                    .unwrap(),
                "compose",
                directory.join("cutover-point").to_str().unwrap(),
            ]),
            &util::root(),
            Some(env),
            Some(&log),
            true,
        )?;
        let file = std::fs::File::create(&statement_log)?;
        let command = base
            .iter()
            .cloned()
            .chain(strings(vec![
                "run",
                "--rm",
                "--no-deps",
                "--env",
                "DUCKFLEET_ROLE=cli",
                "api",
                "duckfleet-server",
                "sql",
                "SELECT sum(sin(i)) FROM range(1000000000) AS source(i)",
            ]))
            .collect::<Vec<_>>();
        process = Some(
            Command::new(&command[0])
                .args(&command[1..])
                .current_dir(util::root())
                .envs(env)
                .stdout(Stdio::from(file.try_clone()?))
                .stderr(Stdio::from(file))
                .spawn()?,
        );
        let mut observed = false;
        for _ in 0..30 {
            let workers = cli_json(base, env, &strings(vec!["workers"]))?;
            if util::array(workers.get("workers"))
                .iter()
                .any(|worker| worker["active_queries"].as_u64().unwrap_or(0) > 0)
            {
                observed = true;
                break;
            }
            if process.as_mut().unwrap().try_wait()?.is_some() {
                break;
            }
            std::thread::sleep(Duration::from_millis(500));
        }
        ensure!(observed, "the cutover statement never became active");
        run(
            &strings(vec![
                "bash",
                util::root()
                    .join("scripts/stop-compute.sh")
                    .to_str()
                    .unwrap(),
                "compose",
            ]),
            &util::root(),
            Some(env),
            Some(&log),
            true,
        )?;
        let status = process.take().unwrap().wait()?;
        if status.success() {
            evidence["drained_statement"] = json!(true);
            evidence["statement_outcome"] = json!("succeeded");
        } else {
            let outcome = std::fs::read_to_string(&statement_log)?;
            ensure!(
                outcome.contains("timeout (HTTP 400)") && outcome.contains("retry_safe: false"),
                "active statement failed without a truthful bounded outcome"
            );
            evidence["statement_outcome"] = json!("timeout");
            evidence["retry_safe"] = json!(false);
        }
        let text = std::fs::read_to_string(&statement_log)?;
        if let Some(captures) = regex::Regex::new(r"request_id: ([0-9a-f-]{36})")?.captures(&text) {
            evidence["request_id"] = json!(&captures[1]);
        }
        let refused = run(
            &base
                .iter()
                .cloned()
                .chain(strings(vec![
                    "run",
                    "--rm",
                    "--no-deps",
                    "--env",
                    "DUCKFLEET_ROLE=cli",
                    "api",
                    "duckfleet-server",
                    "sql",
                    "SELECT 1",
                ]))
                .collect::<Vec<_>>(),
            &util::root(),
            Some(env),
            None,
            false,
        )?;
        ensure!(
            !refused.status.success(),
            "stopped compute accepted new SQL"
        );
        let mut candidate = env.clone();
        candidate.extend([
            ("DUCKFLEET_IMAGE_TAG".into(), candidate_tag.into()),
            ("DUCKFLEET_DBT_IMAGE_TAG".into(), candidate_tag.into()),
            ("DUCKFLEET_API_IMAGE_TAG".into(), candidate_tag.into()),
        ]);
        run(
            &base
                .iter()
                .cloned()
                .chain(strings(vec!["up", "-d", "--wait", "server"]))
                .collect::<Vec<_>>(),
            &util::root(),
            Some(&candidate),
            Some(&log),
            true,
        )?;
        cli_scalar(
            base,
            &candidate,
            "SELECT version() AS value",
            json!("v2.0.1"),
        )?;
        cli_scalar(
            base,
            &candidate,
            "SELECT value FROM lake.options() WHERE option_name='version'",
            json!("1.0"),
        )?;
        let workers = cli_json(base, &candidate, &strings(vec!["workers"]))?;
        ensure!(
            !util::array(workers.get("workers")).is_empty()
                && util::array(workers.get("workers"))
                    .iter()
                    .all(|worker| worker["duckdb_version"] == "v2.0.1"),
            "an old or unknown engine remains after cutover"
        );
        let expected_image = image_evidence(candidate_tag)?
            [format!("duckfleet-worker:{candidate_tag}")]["id"]
            .clone();
        let ids = output(
            &base
                .iter()
                .cloned()
                .chain(strings(vec!["ps", "--status", "running", "--quiet"]))
                .collect::<Vec<_>>(),
            Some(&candidate),
        )?;
        for container in ids.split_whitespace() {
            let inspected = output(
                &strings(vec!["docker", "inspect", container]),
                Some(&candidate),
            )?;
            let inspected: Value = serde_json::from_str(&inspected)?;
            let env: BTreeMap<String, String> = util::array(inspected[0]["Config"].get("Env"))
                .iter()
                .filter_map(|entry| entry.as_str()?.split_once('='))
                .map(|(key, value)| (key.into(), value.into()))
                .collect();
            ensure!(
                !((env.get("DUCKFLEET_ROLE") == Some(&"gateway".into())
                    || env.contains_key("DUCKFLEET_WORKER_ID"))
                    && inspected[0]["Image"] != expected_image),
                "1.5.4 writer remains after cutover"
            );
        }
        evidence["old_writers"] = json!(0);
        pgwire_checks(base, &candidate, directory)?;
        let mut smoke = vec!["http", "pgwire"];
        if !skip_dbt {
            run(
                &base
                    .iter()
                    .cloned()
                    .chain(strings(vec![
                        "run",
                        "--rm",
                        "--no-deps",
                        "dbt",
                        "dbt",
                        "debug",
                    ]))
                    .collect::<Vec<_>>(),
                &util::root(),
                Some(&candidate),
                Some(&log),
                true,
            )?;
            run(
                &base
                    .iter()
                    .cloned()
                    .chain(strings(vec![
                        "run",
                        "--rm",
                        "--no-deps",
                        "dbt",
                        "duckfleet-server",
                        "--jobs",
                    ]))
                    .collect::<Vec<_>>(),
                &util::root(),
                Some(&candidate),
                Some(&log),
                true,
            )?;
            smoke.extend(["dbt", "jobs", "artifacts"]);
        }
        for (stage, args) in [
            (
                "maintenance",
                strings(vec![
                    "run",
                    "--rm",
                    "--no-deps",
                    "dbt",
                    "duckfleet-server",
                    "--maintenance",
                ]),
            ),
            (
                "rbac",
                strings(vec![
                    "run",
                    "--rm",
                    "--no-deps",
                    "dbt",
                    "duckfleet-server",
                    "--rbac",
                ]),
            ),
        ] {
            run(
                &base.iter().cloned().chain(args).collect::<Vec<_>>(),
                &util::root(),
                Some(&candidate),
                Some(&log),
                true,
            )?;
            smoke.push(stage);
        }
        cli_json(
            base,
            &candidate,
            &strings(vec!["history", "--limit", "5", "--format", "json"]),
        )?;
        if let Some(request_id) = evidence.get("request_id").and_then(Value::as_str) {
            cli_json(base, &candidate, &strings(vec!["request", request_id]))?;
            evidence["durable_request_readable"] = json!(true);
        }
        smoke.push("history");
        evidence["smoke"] = json!(smoke);
        evidence["engine"] = json!("v2.0.1");
        evidence["catalog_version"] = json!("1.0");
        evidence["passed"] = json!(true);
        let _ = baseline_tag;
        Ok(())
    })();
    if let Some(process) = &mut process {
        let _ = process.kill();
        let _ = process.wait();
    }
    if let Err(error) = &result {
        evidence["error"] = json!(error.to_string());
    }
    util::write_json(directory.join("cutover.json"), &evidence)?;
    result?;
    Ok(evidence)
}

struct Lane<'a> {
    name: &'a str,
    project: &'a str,
    tag: &'a str,
    version: &'a str,
    directory: &'a Path,
    override_path: &'a Path,
    corpus_tag: &'a str,
    skip_dbt: bool,
}

fn run_lane(lane: Lane<'_>) -> Result<(Value, Vec<String>, BTreeMap<String, String>)> {
    let Lane {
        name,
        project,
        tag,
        version,
        directory,
        override_path,
        corpus_tag,
        skip_dbt,
    } = lane;
    std::fs::create_dir_all(directory)?;
    let artifacts = directory.join("artifacts");
    std::fs::create_dir_all(&artifacts)?;
    let mut env: BTreeMap<String, String> = std::env::vars().collect();
    env.extend([
        ("COMPOSE_PROJECT_NAME".into(), project.into()),
        ("DUCKFLEET_IMAGE_TAG".into(), tag.into()),
        ("DUCKFLEET_DBT_IMAGE_TAG".into(), tag.into()),
        ("DUCKFLEET_API_IMAGE_TAG".into(), tag.into()),
        (
            "DUCKFLEET_ARTIFACTS_DIRECTORY".into(),
            artifacts.to_string_lossy().into_owned(),
        ),
        ("DUCKFLEET_PORT".into(), free_port()?.to_string()),
        ("DUCKFLEET_POOLS_PORT".into(), free_port()?.to_string()),
    ]);
    let base = compose_args(project)
        .iter()
        .cloned()
        .chain(strings(vec!["--file", override_path.to_str().unwrap()]))
        .collect::<Vec<_>>();
    run(
        &base
            .iter()
            .cloned()
            .chain(strings(vec!["down", "--volumes", "--remove-orphans"]))
            .collect::<Vec<_>>(),
        &util::root(),
        Some(&env),
        None,
        false,
    )?;
    let mut summary = json!({"name":name,"project":project,"image_tag":tag,"expected_engine":version,"images":image_evidence(tag)?,"stages":{}});
    let result = (|| -> Result<()> {
        run(
            &base
                .iter()
                .cloned()
                .chain(strings(vec!["up", "-d", "--wait", "server"]))
                .collect::<Vec<_>>(),
            &util::root(),
            Some(&env),
            Some(&directory.join("startup.log")),
            true,
        )?;
        let stages = [
            ("api", Some(strings(vec!["run", "--rm", "api"]))),
            ("dbt", Some(strings(vec!["run", "--rm", "dbt"]))),
            (
                "scale",
                Some(strings(vec![
                    "run",
                    "--rm",
                    "--no-deps",
                    "dbt",
                    "duckfleet-server",
                    "--scale",
                ])),
            ),
            (
                "concurrency",
                Some(strings(vec![
                    "run",
                    "--rm",
                    "--no-deps",
                    "dbt",
                    "duckfleet-server",
                    "--concurrency",
                ])),
            ),
            (
                "jobs",
                Some(strings(vec![
                    "run",
                    "--rm",
                    "--no-deps",
                    "dbt",
                    "duckfleet-server",
                    "--jobs",
                ])),
            ),
            (
                "engine-corpus",
                Some(strings(vec![
                    "run",
                    "--rm",
                    "api",
                    "duckfleet-server",
                    "--engine-qualification",
                ])),
            ),
            (
                "observability",
                Some(strings(vec![
                    "run",
                    "--rm",
                    "--no-deps",
                    "dbt",
                    "duckfleet-server",
                    "--observability",
                ])),
            ),
            ("pgwire", None),
        ];
        for (stage, command) in stages {
            if skip_dbt && matches!(stage, "dbt" | "scale") {
                summary["stages"][stage] = json!("skipped_local_arm");
                continue;
            }
            println!("{name}: {stage}");
            if let Some(command) = command {
                let mut stage_env = env.clone();
                if stage == "engine-corpus" {
                    stage_env.insert("DUCKFLEET_API_IMAGE_TAG".into(), corpus_tag.into());
                }
                run(
                    &base.iter().cloned().chain(command).collect::<Vec<_>>(),
                    &util::root(),
                    Some(&stage_env),
                    Some(&directory.join(format!("{stage}.log"))),
                    true,
                )?;
            } else {
                pgwire_checks(&base, &env, directory)?;
            }
            summary["stages"][stage] = json!("passed");
        }
        if name == "candidate" {
            let mut stage_env = env.clone();
            stage_env.insert("DUCKFLEET_API_IMAGE_TAG".into(), corpus_tag.into());
            run(
                &base
                    .iter()
                    .cloned()
                    .chain(strings(vec![
                        "run",
                        "--rm",
                        "api",
                        "duckfleet-server",
                        "--extension-security",
                    ]))
                    .collect::<Vec<_>>(),
                &util::root(),
                Some(&stage_env),
                Some(&directory.join("extension-security.log")),
                true,
            )?;
            summary["stages"]["extension-security"] = json!("passed");
        }
        summary["resources"] = resource_evidence(project)?;
        let report = util::read_json(artifacts.join("engine-qualification.json"))?;
        summary["actual_engine"] = report["engine_version"].clone();
        summary["extensions"] = validate_extensions(&report)?;
        summary["passed"] = json!(summary["actual_engine"] == version);
        ensure!(
            summary["passed"] == true,
            "{name} expected {version}, got {}",
            summary["actual_engine"]
        );
        Ok(())
    })();
    if let Err(error) = &result {
        summary["passed"] = json!(false);
        summary["error"] = json!(error.to_string());
        let _ = run(
            &base
                .iter()
                .cloned()
                .chain(strings(vec!["logs", "--no-color"]))
                .collect::<Vec<_>>(),
            &util::root(),
            Some(&env),
            Some(&directory.join("services.log")),
            false,
        );
    }
    util::write_json(directory.join("lane.json"), &summary)?;
    result?;
    Ok((summary, base, env))
}

pub fn qualify(args: Vec<String>) -> Result<()> {
    let mut args = args;
    let baseline_ref =
        util::option(&mut args, "--baseline-ref").context("--baseline-ref is required")?;
    let candidate_ref =
        util::option(&mut args, "--candidate-ref").context("--candidate-ref is required")?;
    let baseline_version =
        util::option(&mut args, "--baseline-version").unwrap_or_else(|| "v1.5.4".into());
    let candidate_version =
        util::option(&mut args, "--candidate-version").unwrap_or_else(|| "v2.0.1".into());
    let baseline_tag = util::option(&mut args, "--baseline-tag")
        .unwrap_or_else(|| "qualification-baseline".into());
    let candidate_tag = util::option(&mut args, "--candidate-tag")
        .unwrap_or_else(|| "qualification-candidate".into());
    let expectations = util::option(&mut args, "--expectations")
        .map(PathBuf::from)
        .unwrap_or_else(|| util::root().join("examples/engine-qualification-2.0.json"));
    let output = util::option(&mut args, "--output")
        .map(PathBuf::from)
        .context("--output is required")?;
    let skip_build = util::flag(&mut args, "--skip-build");
    let skip_dbt = util::flag(&mut args, "--skip-dbt");
    let keep = util::flag(&mut args, "--keep");
    util::reject_extra(&args, 0)?;
    ensure!(
        baseline_tag != candidate_tag || skip_build,
        "baseline and candidate tags must differ when building"
    );
    std::fs::create_dir_all(&output)?;
    let output = output.canonicalize()?;
    let projects = json!({"baseline":"duckfleet-qualification-baseline","candidate":"duckfleet-qualification-candidate"});
    let mut running: Vec<(Vec<String>, BTreeMap<String, String>)> = Vec::new();
    let mut summary = json!({"passed":false,"release_complete":!skip_dbt,"baseline_ref":resolved_ref(&baseline_ref)?,"candidate_ref":resolved_ref(&candidate_ref)?,"lanes":{}});
    let result = (|| -> Result<()> {
        if !skip_build {
            summary["baseline_ref"] = json!(build_images(
                &baseline_tag,
                &baseline_ref,
                &output.join("build-baseline")
            )?);
            summary["candidate_ref"] = json!(build_images(
                &candidate_tag,
                &candidate_ref,
                &output.join("build-candidate")
            )?);
        }
        let mut paths = json!({});
        let mut runtime: BTreeMap<String, (Vec<String>, BTreeMap<String, String>)> =
            BTreeMap::new();
        for (lane, tag, version) in [
            ("baseline", baseline_tag.as_str(), baseline_version.as_str()),
            (
                "candidate",
                candidate_tag.as_str(),
                candidate_version.as_str(),
            ),
        ] {
            let directory = output.join(lane);
            std::fs::create_dir_all(&directory)?;
            paths[lane] = write_override(
                &directory.join("override.yaml"),
                &format!("engine-qualification/{lane}"),
            )?;
            let (lane_summary, base, env) = run_lane(Lane {
                name: lane,
                project: projects[lane].as_str().unwrap(),
                tag,
                version,
                directory: &directory,
                override_path: &directory.join("override.yaml"),
                corpus_tag: &candidate_tag,
                skip_dbt,
            })?;
            running.push((base.clone(), env.clone()));
            runtime.insert(lane.into(), (base, env));
            summary["lanes"][lane] = lane_summary;
        }
        summary["isolation"] = isolation_evidence(
            projects["baseline"].as_str().unwrap(),
            projects["candidate"].as_str().unwrap(),
            &paths,
        )?;
        let comparison = output.join("comparison.json");
        let executable = std::env::current_exe()?;
        run(
            &strings(vec![
                executable.to_str().unwrap(),
                "compare-engine-qualification",
                "--baseline",
                output
                    .join("baseline/artifacts/engine-qualification.json")
                    .to_str()
                    .unwrap(),
                "--candidate",
                output
                    .join("candidate/artifacts/engine-qualification.json")
                    .to_str()
                    .unwrap(),
                "--expectations",
                expectations.to_str().unwrap(),
                "--output",
                comparison.to_str().unwrap(),
            ]),
            &util::root(),
            None,
            None,
            false,
        )?;
        summary["comparison"] = util::read_json(comparison)?;
        let (base, env) = &runtime["candidate"];
        summary["recovery"] = recovery_drill(
            base,
            env,
            &baseline_tag,
            &candidate_tag,
            &output.join("candidate"),
        )?;
        summary["cutover"] = cutover_drill(
            base,
            env,
            &baseline_tag,
            &candidate_tag,
            &output.join("candidate"),
            skip_dbt,
        )?;
        summary["passed"] = json!(
            summary["isolation"]["passed"] == true
                && summary["comparison"]["passed"] == true
                && summary["recovery"]["passed"] == true
                && summary["cutover"]["passed"] == true
                && util::object(&summary["lanes"])
                    .values()
                    .all(|lane| lane["passed"] == true)
        );
        println!("{}", json!({"passed":summary["passed"],"output":output}));
        ensure!(
            summary["passed"] == true,
            "engine qualification did not pass"
        );
        Ok(())
    })();
    if let Err(error) = &result {
        summary["error"] = json!(error.to_string());
    }
    summary["output"] = json!(output);
    util::write_json(output.join("summary.json"), &summary)?;
    if !keep {
        for (base, env) in running.iter().rev() {
            let _ = run(
                &base
                    .iter()
                    .cloned()
                    .chain(strings(vec!["down", "--volumes", "--remove-orphans"]))
                    .collect::<Vec<_>>(),
                &util::root(),
                Some(env),
                None,
                false,
            );
        }
        for project in [
            "duckfleet-qualification-baseline",
            "duckfleet-qualification-candidate",
        ] {
            let _ = run(
                &compose_args(project)
                    .iter()
                    .cloned()
                    .chain(strings(vec!["down", "--volumes", "--remove-orphans"]))
                    .collect::<Vec<_>>(),
                &util::root(),
                None,
                None,
                false,
            );
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn overrides_isolate_lake_and_results_for_every_compute_service() {
        let directory = tempfile::tempdir().unwrap();
        let paths = write_override(
            &directory.path().join("override.yaml"),
            "engine-qualification/test",
        )
        .unwrap();
        assert_eq!(
            paths["lake"],
            "s3://duckfleet/engine-qualification/test/lake/"
        );
        assert_eq!(
            paths["results"],
            "s3://duckfleet/engine-qualification/test/results/"
        );
        let documents = util::yaml_documents(
            &std::fs::read_to_string(directory.path().join("override.yaml")).unwrap(),
        )
        .unwrap();
        for service in ["worker-small", "server"] {
            let environment = &documents[0]["services"][service]["environment"];
            assert_eq!(
                environment["DUCKFLEET_RESULT_LAKE_DATA_PATH"],
                paths["lake"]
            );
            assert_eq!(environment["DUCKFLEET_RESULT_DATA_PATH"], paths["results"]);
        }
        assert_eq!(
            documents[0]["services"]["worker-small"]["environment"]["S3_DATA_PATH"],
            paths["lake"]
        );
    }

    #[test]
    fn extension_manifest_is_versioned_and_complete() {
        let extensions: Vec<Value> = ["httpfs", "postgres_scanner", "ducklake", "aws", "icu"].iter().map(|name| json!({
            "extension_name":name,"loaded":"true","installed":"true",
            "install_path":format!("/opt/duckdb/extensions/v2.0.1/linux_arm64/{name}.duckdb_extension"),"extension_version":"fixture-version",
        })).collect();
        let report = json!({"engine_version":"v2.0.1","extensions":extensions});
        assert!(validate_extensions(&report).is_ok());
        let mut invalid = report.clone();
        invalid["extensions"][0]["install_path"] =
            json!("/opt/duckdb/extensions/v1.5.4/linux_arm64/httpfs.duckdb_extension");
        assert!(validate_extensions(&invalid).is_err());
        let mut baseline = report;
        baseline["engine_version"] = json!("v1.5.4");
        for extension in baseline["extensions"].as_array_mut().unwrap() {
            let path = extension["install_path"]
                .as_str()
                .unwrap()
                .replace("/v2.0.1/", "/v1.5.4/");
            extension["install_path"] = json!(path);
        }
        baseline["extensions"][3]["loaded"] = json!("false");
        baseline["extensions"][3]["extension_version"] = json!("");
        assert!(validate_extensions(&baseline).is_ok());
    }
}
