use crate::util;
use anyhow::{Context, Result, bail, ensure};
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};

struct Localizer {
    applied: BTreeMap<String, usize>,
    data_path: String,
}

impl Localizer {
    fn new(data_path: String) -> Self {
        Self {
            applied: BTreeMap::new(),
            data_path,
        }
    }

    fn mark(&mut self, name: &str) {
        *self.applied.entry(name.into()).or_default() += 1;
    }

    fn require(&self, expected: &[String]) -> Result<()> {
        let expected: BTreeSet<_> = expected.iter().map(String::as_str).collect();
        let missing: Vec<_> = expected
            .iter()
            .filter(|name| self.applied.get(**name).copied().unwrap_or(0) == 0)
            .collect();
        ensure!(
            missing.is_empty(),
            "localize matched nothing for {missing:?}; update the transform or the expected render shape"
        );
        let unexpected: Vec<_> = self
            .applied
            .keys()
            .filter(|name| !expected.contains(name.as_str()))
            .collect();
        ensure!(
            unexpected.is_empty(),
            "localize rewrote unexpected sections: {unexpected:?}"
        );
        Ok(())
    }

    fn container(&mut self, spec: &mut Value) -> Result<()> {
        let image = util::text(spec.get("image")).to_owned();
        if image.contains(".dkr.ecr.") {
            let component = if image.contains("/dbt@") || image.contains("/dbt:") {
                "dbt"
            } else {
                "worker"
            };
            spec["image"] = json!(format!("duckfleet-{component}:local"));
            spec["imagePullPolicy"] = json!("Never");
            self.mark("image");
        }
        if spec["resources"]["requests"].get("cpu").is_some() {
            spec["resources"]["requests"]["cpu"] = json!("50m");
            self.mark("cpu-request");
        }
        if spec.get("env").is_none() {
            return Ok(());
        }
        let env = spec["env"].as_array_mut().context("env must be an array")?;
        let index = |name: &str, env: &[Value]| env.iter().position(|entry| entry["name"] == name);
        if let Some(position) =
            index("S3_AUTH_MODE", env).filter(|position| env[*position]["value"] == "pod_identity")
        {
            env[position]["value"] = json!("local");
            let reader = index("DUCKFLEET_AUTHORITY", env)
                .is_some_and(|position| env[position]["value"] == "reader");
            env.extend([
                json!({"name":if reader { "S3_READER_ACCESS_KEY" } else { "S3_ACCESS_KEY" },"value":if reader { "DUCKFLEETREADER" } else { "DUCKFLEETLOCAL" }}),
                json!({"name":if reader { "S3_READER_SECRET_KEY" } else { "S3_SECRET_KEY" },"value":if reader { "local-lake-reader-object-password" } else { "local-object-store-password" }}),
                json!({"name":"S3_ENDPOINT","value":"object-store:9000"}),
            ]);
            self.mark("s3-auth");
        }
        if let Some(position) = index("DUCKFLEET_RESULT_AUTH_MODE", env)
            .filter(|position| env[*position]["value"] == "pod_identity")
        {
            env[position]["value"] = json!("static");
            if let Some(position) = index("DUCKFLEET_RESULT_ALLOW_HTTP", env) {
                env[position]["value"] = json!("true");
            }
            env.extend([
                json!({"name":"DUCKFLEET_RESULT_ACCESS_KEY","value":"DUCKFLEETLOCAL"}),
                json!({"name":"DUCKFLEET_RESULT_SECRET_KEY","value":"local-object-store-password"}),
                json!({"name":"DUCKFLEET_RESULT_ENDPOINT","value":"http://object-store:9000"}),
            ]);
            self.mark("s3-auth");
        }
        if let Some(position) = index("DUCKFLEET_RESULT_DATA_PATH", env) {
            env[position]["value"] = json!(format!(
                "{}/results/",
                self.data_path
                    .trim_end_matches('/')
                    .rsplit_once('/')
                    .map(|(parent, _)| parent)
                    .unwrap_or(self.data_path.trim_end_matches('/'))
            ));
            self.mark("s3-path");
        }
        for name in [
            "S3_DATA_PATH",
            "EXPECT_S3_PREFIX",
            "DUCKFLEET_RESULT_LAKE_DATA_PATH",
        ] {
            if let Some(position) = index(name, env).filter(|position| {
                env[*position]["value"]
                    .as_str()
                    .unwrap_or_default()
                    .starts_with("s3://")
            }) {
                env[position]["value"] = json!(self.data_path);
                self.mark("s3-path");
            }
        }
        Ok(())
    }

    fn pod(&mut self, spec: &mut Value) -> Result<()> {
        if spec["nodeSelector"]
            .get("eks.amazonaws.com/compute-type")
            .is_some()
        {
            spec["nodeSelector"] = json!({"kubernetes.io/os":"linux"});
            self.mark("node-selector");
        }
        for kind in ["containers", "initContainers"] {
            if let Some(containers) = spec.get_mut(kind).and_then(Value::as_array_mut) {
                for container in containers {
                    self.container(container)?;
                }
            }
        }
        Ok(())
    }

    fn document(&mut self, document: &mut Value) -> Result<()> {
        let kind = util::text(document.get("kind")).to_owned();
        if matches!(kind.as_str(), "Deployment" | "StatefulSet" | "Job") {
            self.pod(&mut document["spec"]["template"]["spec"])?;
        }
        if kind == "StatefulSet"
            && let Some(claims) = document["spec"]["volumeClaimTemplates"].as_array_mut()
        {
            for claim in claims {
                claim["spec"]["storageClassName"] = json!("standard");
                claim["spec"]["resources"]["requests"]["storage"] = json!("2Gi");
                self.mark("storage-class");
            }
        }
        if kind == "ConfigMap" && document["data"].get("pod.json").is_some() {
            let mut template =
                util::read_json_from_string(util::text(document["data"].get("pod.json")))?;
            self.pod(&mut template["spec"])?;
            document["data"]["pod.json"] = json!(serde_json::to_string(&template)?);
            self.mark("isolated-pod");
        }
        if kind == "NetworkPolicy" {
            for section in ["ingress", "egress"] {
                if let Some(rules) = document["spec"]
                    .get_mut(section)
                    .and_then(Value::as_array_mut)
                {
                    for rule in rules {
                        let peers = if section == "ingress" {
                            rule.get("from")
                        } else {
                            rule.get("to")
                        };
                        let same_namespace = util::array(peers).iter().any(|peer| {
                            peer.get("podSelector").is_some()
                                && peer.get("namespaceSelector").is_none()
                        });
                        if same_namespace {
                            if rule.get("ports").is_none() {
                                rule["ports"] = json!([]);
                            }
                            let ports = rule["ports"]
                                .as_array_mut()
                                .context("network rule ports must be an array")?;
                            if !ports.iter().any(|port| port["port"] == 9000) {
                                ports.push(json!({"port":9000}));
                                self.mark("object-store-port");
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }
}

pub fn localize(args: Vec<String>) -> Result<()> {
    let mut args = args;
    let expected = util::option(&mut args, "--expect").context("--expect is required")?;
    let data_path =
        util::option(&mut args, "--data-path").unwrap_or_else(|| "s3://duckfleet/lake/".into());
    util::reject_extra(&args, 0)?;
    let expected: Vec<String> = expected
        .split(',')
        .filter(|name| !name.is_empty())
        .map(str::to_owned)
        .collect();
    let mut input = String::new();
    std::io::Read::read_to_string(&mut std::io::stdin(), &mut input)?;
    let mut documents = util::yaml_documents(&input)?;
    let mut localizer = Localizer::new(data_path);
    for document in &mut documents {
        if !document.is_null() {
            localizer.document(document)?;
        }
    }
    localizer.require(&expected)?;
    print!("{}", util::yaml_string(&documents)?);
    eprintln!("localized for kind: {:?}", localizer.applied);
    Ok(())
}

pub fn harness_job(args: Vec<String>) -> Result<()> {
    let mut args = args;
    let name = util::option(&mut args, "--name").context("--name is required")?;
    let command = util::option(&mut args, "--command").context("--command is required")?;
    let claim = util::option(&mut args, "--artifacts-claim");
    let mut environment = Vec::new();
    while let Some(item) = util::option(&mut args, "--env") {
        environment.push(item);
    }
    util::reject_extra(&args, 0)?;
    let mut input = String::new();
    std::io::Read::read_to_string(&mut std::io::stdin(), &mut input)?;
    let mut documents = util::yaml_documents(&input)?;
    let mut jobs: Vec<_> = documents
        .iter_mut()
        .filter(|document| document["kind"] == "Job")
        .collect();
    ensure!(
        jobs.len() == 1,
        "expected exactly one Job to re-point, found {}",
        jobs.len()
    );
    let job = jobs[0].as_object_mut().unwrap();
    job["metadata"]["name"] = json!(name);
    if let Some(annotations) = job["metadata"]["annotations"].as_object_mut() {
        for hook in [
            "helm.sh/hook",
            "helm.sh/hook-delete-policy",
            "helm.sh/hook-weight",
        ] {
            annotations.remove(hook);
        }
        if annotations.is_empty() {
            job["metadata"]
                .as_object_mut()
                .unwrap()
                .remove("annotations");
        }
    }
    let pod = &mut job["spec"]["template"]["spec"];
    let dbt = util::array(pod.get("containers"))
        .iter()
        .position(|container| container["name"] == "dbt")
        .context("the harness Job no longer has a container named dbt")?;
    let rendered_command = {
        let dbt = &mut pod["containers"][dbt];
        dbt["command"] = json!(shell_words::split(&command)?);
        for item in environment {
            let (name, value) = item
                .split_once('=')
                .with_context(|| format!("--env expects NAME=VALUE: {item}"))?;
            ensure!(!name.is_empty(), "--env expects NAME=VALUE");
            if dbt.get("env").is_none() {
                dbt["env"] = json!([]);
            }
            let env = dbt["env"].as_array_mut().unwrap();
            if let Some(entry) = env.iter_mut().find(|entry| entry["name"] == name) {
                entry.as_object_mut().unwrap().remove("valueFrom");
                entry["value"] = json!(value);
            } else {
                env.push(json!({"name":name,"value":value}));
            }
        }
        dbt["command"].clone()
    };
    if let Some(claim) = claim {
        let volume = util::array(pod.get("volumes"))
            .iter()
            .position(|volume| volume["name"] == "artifacts")
            .context("expected an artifacts volume")?;
        let volume = &mut pod["volumes"][volume];
        ensure!(
            volume.get("emptyDir").is_some(),
            "expected an artifacts emptyDir volume to replace"
        );
        volume.as_object_mut().unwrap().remove("emptyDir");
        volume["persistentVolumeClaim"] = json!({"claimName":claim});
    }
    print!("{}", util::yaml_string(&documents)?);
    eprintln!("harness job {name}: {}", rendered_command);
    Ok(())
}

pub fn terminal_state(job: &Value) -> Option<String> {
    for condition in util::array(job["status"].get("conditions")) {
        if condition["status"] != "True" {
            continue;
        }
        if condition["type"] == "Complete" {
            return Some("complete".into());
        }
        if condition["type"] == "Failed" {
            return Some(
                format!(
                    "failed: {} {}",
                    util::text(condition.get("reason")),
                    util::text(condition.get("message"))
                )
                .trim()
                .to_owned(),
            );
        }
    }
    None
}

pub fn await_job(args: Vec<String>) -> Result<()> {
    let mut args = args;
    let context = util::option(&mut args, "--context").context("--context is required")?;
    let namespace = util::option(&mut args, "--namespace").context("--namespace is required")?;
    let name = util::option(&mut args, "--job").context("--job is required")?;
    let timeout = util::option(&mut args, "--timeout")
        .unwrap_or_else(|| "1200".into())
        .parse::<u64>()?;
    let poll = util::option(&mut args, "--poll-seconds")
        .unwrap_or_else(|| "5".into())
        .parse::<u64>()?;
    util::reject_extra(&args, 0)?;
    let kubectl = |arguments: &[String]| {
        util::run_status(
            ["kubectl", "--context", &context, "-n", &namespace]
                .into_iter()
                .map(String::from)
                .chain(arguments.iter().cloned())
                .collect::<Vec<_>>(),
        )
    };
    let started = std::time::Instant::now();
    let mut previous_isolated = String::new();
    loop {
        let output = kubectl(&[
            "get".into(),
            "job".into(),
            name.clone(),
            "-o".into(),
            "json".into(),
        ])?;
        if !output.status.success() {
            bail!("{}", util::stderr_text(&output));
        }
        let job: Value = serde_json::from_slice(&output.stdout)?;
        if let Some(state) = terminal_state(&job) {
            if state == "complete" {
                println!("{name} completed");
                return Ok(());
            }
            eprintln!("{name} {state}");
            for arguments in [
                vec![
                    "logs".into(),
                    format!("job/{name}"),
                    "--all-containers".into(),
                    "--tail=200".into(),
                ],
                vec![
                    "logs".into(),
                    "-l".into(),
                    "duckfleet-compute=isolated".into(),
                    "--all-containers".into(),
                    "--prefix".into(),
                    "--tail=200".into(),
                ],
            ] {
                let output = kubectl(&arguments)?;
                eprint!(
                    "{}{}",
                    util::stdout_text(&output),
                    util::stderr_text(&output)
                );
            }
            bail!("{name} {state}");
        }
        if started.elapsed().as_secs() >= timeout {
            bail!("{name} did not finish within {timeout}s: {}", job["status"]);
        }
        let isolated = kubectl(&[
            "logs".into(),
            "-l".into(),
            "duckfleet-compute=isolated".into(),
            "--all-containers".into(),
            "--prefix".into(),
            "--tail=200".into(),
        ])?;
        let logs = util::stdout_text(&isolated);
        if !logs.is_empty() && logs != previous_isolated {
            eprint!("{logs}");
            previous_isolated = logs;
        }
        println!(
            "waiting: {name} active={}",
            job["status"]["active"].as_u64().unwrap_or(0)
        );
        std::thread::sleep(std::time::Duration::from_secs(poll));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn localizer_rewrites_images_resources_and_isolated_templates() {
        let mut document = json!({"kind":"Deployment","spec":{"template":{"spec":{"nodeSelector":{"eks.amazonaws.com/compute-type":"auto"},"containers":[{"image":"account.dkr.ecr.region.amazonaws.com/duckfleet/worker@sha256:a","resources":{"requests":{"cpu":"1"}},"env":[{"name":"S3_AUTH_MODE","value":"pod_identity"},{"name":"S3_DATA_PATH","value":"s3://bucket/lake/"}]}]}}}});
        let mut localizer = Localizer::new("s3://duckfleet/lake/".into());
        localizer.document(&mut document).unwrap();
        localizer
            .require(&[
                "image".into(),
                "cpu-request".into(),
                "node-selector".into(),
                "s3-auth".into(),
                "s3-path".into(),
            ])
            .unwrap();
        let container = &document["spec"]["template"]["spec"]["containers"][0];
        assert_eq!(container["image"], "duckfleet-worker:local");
        assert_eq!(container["resources"]["requests"]["cpu"], "50m");
        assert!(util::env_map(container.get("env")).contains_key("S3_ACCESS_KEY"));
    }

    #[test]
    fn terminal_job_state_distinguishes_complete_and_failed() {
        assert_eq!(
            terminal_state(&json!({"status":{"conditions":[{"type":"Complete","status":"True"}]}}))
                .as_deref(),
            Some("complete")
        );
        assert!(terminal_state(&json!({"status":{"conditions":[{"type":"Failed","status":"True","reason":"DeadlineExceeded"}]}})).unwrap().contains("DeadlineExceeded"));
    }
}
