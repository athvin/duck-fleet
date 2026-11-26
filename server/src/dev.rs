use crate::cli_config::DevelopmentSettings;
use crate::client::Client;
use serde_json::Value;
use std::collections::BTreeSet;
use std::io::{IsTerminal, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::time::{Duration, Instant};

pub async fn run(command: &str, settings: DevelopmentSettings, demo: bool, yes: bool) -> i32 {
    let result = match command {
        "dev" => start(&settings, demo).await,
        "status" => status(&settings).await,
        "stop" => stop(&settings),
        "reset" => reset(&settings, yes),
        _ => unreachable!("validated development command"),
    };
    match result {
        Ok(code) => code,
        Err((code, message)) => {
            eprintln!("{message}");
            code
        }
    }
}

fn deployment(settings: &DevelopmentSettings) -> Result<PathBuf, (i32, String)> {
    settings
        .root
        .ancestors()
        .find(|directory| {
            directory.join("compose.yaml").is_file()
                && directory.join("docker/server.Dockerfile").is_file()
        })
        .map(Path::to_path_buf)
        .ok_or_else(|| {
            (
                2,
                "cannot locate the DuckFleet Compose deployment; run from a DuckFleet checkout"
                    .into(),
            )
        })
}

fn port(settings: &DevelopmentSettings) -> Result<u16, (i32, String)> {
    let url = reqwest::Url::parse(&settings.url).map_err(|_| {
        (
            2,
            "the development URL must be an HTTP URL such as http://127.0.0.1:18080".into(),
        )
    })?;
    if url.scheme() != "http"
        || !url.username().is_empty()
        || url.password().is_some()
        || !matches!(url.host_str(), Some("127.0.0.1" | "localhost"))
        || url.path() != "/"
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err((
            2,
            "duckfleet dev requires a local HTTP origin such as http://127.0.0.1:18080".into(),
        ));
    }
    Ok(url.port().unwrap_or(80))
}

fn compose(settings: &DevelopmentSettings, deployment: &Path) -> Result<Command, (i32, String)> {
    let mut command = Command::new("docker");
    command
        .arg("compose")
        .arg("--project-name")
        .arg(&settings.project)
        .arg("--file")
        .arg(deployment.join("compose.yaml"))
        .current_dir(deployment)
        .env("DUCKFLEET_TOKEN", &settings.token)
        .env("DUCKFLEET_PORT", port(settings)?.to_string());
    Ok(command)
}

fn log_hint(settings: &DevelopmentSettings) -> String {
    format!(
        "inspect with: docker compose --project-name {} logs postgres object-store platform-provision platform-migrate worker-small server",
        settings.project
    )
}

fn output(mut command: Command) -> Result<Output, (i32, String)> {
    command.output().map_err(|_| {
        (
            1,
            "cannot run Docker; install Docker Engine with Compose v2 and try again".into(),
        )
    })
}

fn checked(
    mut command: Command,
    settings: &DevelopmentSettings,
    service: &str,
) -> Result<(), (i32, String)> {
    let status = command.status().map_err(|_| {
        (
            1,
            "cannot run Docker; install Docker Engine with Compose v2 and try again".into(),
        )
    })?;
    if status.success() {
        Ok(())
    } else {
        Err((1, format!("{service} failed; {}", log_hint(settings))))
    }
}

async fn ready_workers(settings: &DevelopmentSettings) -> Result<usize, String> {
    let client = Client::new(&settings.url, &settings.token).map_err(|error| error.to_string())?;
    let bytes = client
        .request(
            reqwest::Method::GET,
            "/v1/workers",
            None,
            Duration::from_secs(3),
        )
        .await
        .map_err(|error| error.to_string())?;
    let value: Value =
        serde_json::from_slice(&bytes).map_err(|_| "invalid worker response".to_owned())?;
    Ok(value["workers"]
        .as_array()
        .map(|workers| {
            workers
                .iter()
                .filter(|worker| {
                    worker["ready"] == true
                        && worker["draining"] == false
                        && worker["state"] == "ready"
                })
                .count()
        })
        .unwrap_or(0))
}

async fn start(settings: &DevelopmentSettings, demo: bool) -> Result<i32, (i32, String)> {
    let deployment = deployment(settings)?;
    let mut config = compose(settings, &deployment)?;
    config.arg("config").arg("--quiet");
    let result = output(config)?;
    if !result.status.success() {
        return Err((
            1,
            format!("Compose configuration is invalid; {}", log_hint(settings)),
        ));
    }
    let mut build = compose(settings, &deployment)?;
    build.arg("build");
    checked(build, settings, "Docker image build")?;
    let mut up = compose(settings, &deployment)?;
    up.args(["up", "-d", "--wait", "server"]);
    checked(up, settings, "local stack startup")?;
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        match ready_workers(settings).await {
            Ok(workers) if workers > 0 => break,
            _ if Instant::now() >= deadline => {
                return Err((
                    1,
                    format!(
                        "gateway or worker-small did not become ready; {}",
                        log_hint(settings)
                    ),
                ));
            }
            _ => tokio::time::sleep(Duration::from_secs(1)).await,
        }
    }
    println!(
        "ready: Compose project {} at {}",
        settings.project, settings.url
    );
    if demo {
        load_demo(settings, &deployment)?;
        println!("next: duckfleet sql 'select * from retail.fct_orders limit 5'");
    } else {
        println!("next: duckfleet sql 'select 42'");
    }
    Ok(0)
}

fn load_demo(settings: &DevelopmentSettings, deployment: &Path) -> Result<(), (i32, String)> {
    let marker = settings.root.join(".duckfleet/demo.json");
    if marker.exists() {
        let bytes =
            std::fs::read(&marker).map_err(|_| (1, "cannot read the retail demo marker".into()))?;
        let value: Value = serde_json::from_slice(&bytes).map_err(|_| {
            (
                1,
                "the retail demo marker is malformed; reset this project".into(),
            )
        })?;
        if value["project"] != Value::String(settings.project.clone()) || value["complete"] != true
        {
            return Err((
                1,
                "the retail demo marker belongs to another project".into(),
            ));
        }
        println!("retail demo already loaded for {}", settings.project);
        return Ok(());
    }
    let example = deployment.join("examples/retail-demo");
    if !example.join("dbt_project.yml").is_file() {
        return Err((2, "the bundled retail demo is missing".into()));
    }
    let mut build = compose(settings, deployment)?;
    build.args(["--profile", "test", "build", "dbt"]);
    checked(build, settings, "retail demo image build")?;
    let mut run = compose(settings, deployment)?;
    run.args(["--profile", "test", "run", "--rm", "--no-deps"])
        .arg("--volume")
        .arg(format!("{}:/demo", example.display()))
        .arg("--workdir")
        .arg("/demo")
        .args([
            "dbt",
            "dbt",
            "build",
            "--project-dir",
            "/demo",
            "--profiles-dir",
            "/demo",
        ]);
    checked(run, settings, "retail demo dbt build")?;
    let marker_value =
        serde_json::json!({"project":settings.project,"complete":true,"model":"retail.fct_orders"});
    std::fs::write(
        &marker,
        [
            serde_json::to_vec_pretty(&marker_value)
                .map_err(|_| (1, "cannot encode the retail demo marker".into()))?,
            b"\n".to_vec(),
        ]
        .concat(),
    )
    .map_err(|_| (1, "cannot record retail demo completion".into()))?;
    Ok(())
}

fn service_state(
    settings: &DevelopmentSettings,
    deployment: &Path,
) -> Result<(bool, BTreeSet<String>), (i32, String)> {
    let mut all = compose(settings, deployment)?;
    all.args(["ps", "--all", "--quiet"]);
    let all = output(all)?;
    let mut running = compose(settings, deployment)?;
    running.args(["ps", "--status", "running", "--services"]);
    let running = output(running)?;
    let exists = all.status.success() && !all.stdout.is_empty();
    let services = if running.status.success() {
        String::from_utf8_lossy(&running.stdout)
            .lines()
            .map(str::trim)
            .filter(|service| !service.is_empty())
            .map(str::to_owned)
            .collect()
    } else {
        BTreeSet::new()
    };
    Ok((exists, services))
}

async fn status(settings: &DevelopmentSettings) -> Result<i32, (i32, String)> {
    let deployment = deployment(settings)?;
    let (exists, running) = service_state(settings, &deployment)?;
    println!("project: {}", settings.project);
    println!("endpoint: {}", settings.url);
    let expected: BTreeSet<String> = ["postgres", "object-store", "server", "worker-small"]
        .into_iter()
        .map(str::to_owned)
        .collect();
    if !exists || running.is_empty() {
        println!("state: stopped");
        return Ok(1);
    }
    match ready_workers(settings).await {
        Ok(workers) if workers > 0 && expected.is_subset(&running) => {
            println!("state: ready");
            println!("ready_workers: {workers}");
            Ok(0)
        }
        Ok(workers) => {
            println!("state: degraded");
            println!("ready_workers: {workers}");
            println!(
                "running_services: {}",
                running.into_iter().collect::<Vec<_>>().join(",")
            );
            Ok(3)
        }
        Err(_) if !expected.is_subset(&running) => {
            println!("state: starting");
            println!("next: {}", log_hint(settings));
            Ok(2)
        }
        Err(error) => {
            println!("state: degraded");
            println!("api: {error}");
            Ok(3)
        }
    }
}

fn stop(settings: &DevelopmentSettings) -> Result<i32, (i32, String)> {
    let deployment = deployment(settings)?;
    let mut command = compose(settings, &deployment)?;
    command.arg("stop");
    checked(command, settings, "project stop")?;
    println!(
        "stopped {}; catalog, history, and object-store volumes were retained",
        settings.project
    );
    Ok(0)
}

fn reset(settings: &DevelopmentSettings, yes: bool) -> Result<i32, (i32, String)> {
    let deployment = deployment(settings)?;
    println!("reset project: {}", settings.project);
    println!("deployment: {}", deployment.display());
    let mut ps = compose(settings, &deployment)?;
    ps.args(["ps", "--all"]);
    checked(ps, settings, "project inspection")?;
    let volumes = Command::new("docker")
        .arg("volume")
        .arg("ls")
        .arg("--filter")
        .arg(format!(
            "label=com.docker.compose.project={}",
            settings.project
        ))
        .arg("--format")
        .arg("{{.Name}}")
        .output()
        .map_err(|_| (1, "cannot inspect project volumes".into()))?;
    if !volumes.status.success() {
        return Err((1, "cannot inspect project volumes".into()));
    }
    let names = String::from_utf8_lossy(&volumes.stdout);
    println!("volumes:\n{names}");
    if !yes {
        if !std::io::stdin().is_terminal() {
            return Err((2, "reset requires --yes in non-interactive use".into()));
        }
        print!("type {} to remove only these resources: ", settings.project);
        std::io::stdout()
            .flush()
            .map_err(|_| (2, "cannot prompt for reset confirmation".into()))?;
        let mut confirmation = String::new();
        std::io::stdin()
            .read_line(&mut confirmation)
            .map_err(|_| (2, "cannot read reset confirmation".into()))?;
        if confirmation.trim() != settings.project {
            return Err((
                2,
                "reset confirmation did not match; nothing was removed".into(),
            ));
        }
    }
    let mut down = compose(settings, &deployment)?;
    down.args(["down", "--volumes", "--remove-orphans"]);
    checked(down, settings, "project reset")?;
    let marker = settings.root.join(".duckfleet/demo.json");
    if marker.exists() {
        std::fs::remove_file(marker)
            .map_err(|_| (1, "cannot remove the retail demo marker".into()))?;
    }
    println!("reset {}", settings.project);
    Ok(0)
}

#[cfg(test)]
mod tests {
    #[test]
    fn project_identity_is_never_a_docker_command_fragment() {
        let project = crate::cli_config::new_compose_project_for_test();
        assert!(project.starts_with("duckfleet-"));
        assert_eq!(project.len(), 22);
        assert!(
            project
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
        );
    }
}
