use crate::{
    aws_rollout, aws_simple, chart, database_roles, kind_checks, network::Network, team, util,
    worker_resources,
};
use anyhow::{Context, Result, bail, ensure};
use serde_json::{Map, Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
    process::Output,
};

fn image(kind: &str) -> String {
    format!(
        "797781631727.dkr.ecr.us-east-2.amazonaws.com/duckfleet-dev/{kind}@sha256:{}",
        "a".repeat(64)
    )
}

fn helm(extra: &[String]) -> Result<Output> {
    let command = [
        "helm".into(),
        "template".into(),
        "duckfleet".into(),
        util::root()
            .join("infra/helm/duckfleet")
            .to_string_lossy()
            .into_owned(),
        "--namespace".into(),
        "duckfleet-dev".into(),
        "--set".into(),
        format!("workerImage={}", image("worker")),
        "--set".into(),
        format!("dbtImage={}", image("dbt")),
        "--set".into(),
        "isolatedCompute.enabled=true".into(),
        "--set".into(),
        "isolatedCompute.apiServerCidrs[0]=10.96.0.1/32".into(),
    ]
    .into_iter()
    .chain(extra.iter().cloned())
    .collect::<Vec<_>>();
    util::run_status(command)
}

fn objects(extra: &[String]) -> Result<Vec<Value>> {
    let output = helm(extra)?;
    if !output.status.success() {
        bail!("helm template failed: {}", util::stderr_text(&output));
    }
    let documents = util::yaml_documents(&util::stdout_text(&output))?;
    chart::validate_chart(&documents)?;
    Ok(documents
        .into_iter()
        .filter(|document| !document.is_null())
        .collect())
}

fn by_name(objects: &[Value]) -> BTreeMap<(String, String), Value> {
    objects
        .iter()
        .map(|object| {
            (
                (
                    util::text(object.get("kind")).into(),
                    util::text(object["metadata"].get("name")).into(),
                ),
                object.clone(),
            )
        })
        .collect()
}

fn wildcard(pattern: &str, text: &str) -> bool {
    let pattern = pattern.as_bytes();
    let text = text.as_bytes();
    let mut previous = vec![false; text.len() + 1];
    previous[0] = true;
    for token in pattern {
        let mut current = vec![false; text.len() + 1];
        if *token == b'*' {
            current[0] = previous[0];
            for index in 1..=text.len() {
                current[index] = previous[index] || current[index - 1];
            }
        } else {
            for index in 1..=text.len() {
                current[index] =
                    previous[index - 1] && (*token == b'?' || *token == text[index - 1]);
            }
        }
        previous = current;
    }
    previous[text.len()]
}

fn workflow_and_operator_scripts() -> Result<()> {
    let root = util::root();
    let workflow = util::read_yaml(root.join(".github/workflows/aws-dev.yml"))?;
    ensure!(
        workflow["concurrency"]["cancel-in-progress"] == false,
        "deployment concurrency must not cancel in progress"
    );
    let prepare = &workflow["jobs"]["prepare"];
    ensure!(
        prepare["if"] == "github.ref == 'refs/heads/main'",
        "prepare must be main-only"
    );
    ensure!(
        workflow["jobs"]["publish"]["needs"] == "prepare",
        "publish must need prepare"
    );
    ensure!(
        util::string_set(workflow["jobs"]["deploy"].get("needs"))
            == BTreeSet::from(["prepare".into(), "publish".into()]),
        "deploy needs prepare/publish"
    );
    ensure!(
        workflow["jobs"]["suspend"]["needs"] == "deploy",
        "suspend needs deploy"
    );
    let runners: BTreeMap<_, _> =
        util::array(workflow["jobs"]["publish"]["strategy"]["matrix"].get("include"))
            .iter()
            .map(|item| (util::text(item.get("arch")), util::text(item.get("runner"))))
            .collect();
    ensure!(
        runners == BTreeMap::from([("amd64", "ubuntu-24.04"), ("arm64", "ubuntu-24.04-arm")]),
        "native publishing runners changed"
    );
    let steps = util::array(prepare.get("steps"));
    let checks = steps
        .iter()
        .position(|step| util::text(step.get("run")).contains("aws-wait-checks"))
        .context("wait-checks step missing")?;
    let credentials = steps
        .iter()
        .position(|step| util::text(step.get("uses")).starts_with("aws-actions/"))
        .context("OIDC step missing")?;
    ensure!(
        checks < credentials,
        "checks must pass before AWS credentials"
    );
    let action_pin = regex::Regex::new(r"@[0-9a-f]{40}$")?;
    for job in util::object(workflow.get("jobs").unwrap()).values() {
        for step in util::array(job.get("steps")) {
            if let Some(action) = step.get("uses").and_then(Value::as_str) {
                ensure!(action_pin.is_match(action), "unpinned action: {action}");
            }
        }
    }
    let paths = util::array(workflow["on"]["push"].get("paths"));
    for path in [
        "server/src/main.rs",
        "scripts/aws/publish.sh",
        "docker/server.Dockerfile",
        "infra/helm/duckfleet/values.yaml",
        ".github/workflows/aws-dev.yml",
    ] {
        ensure!(
            paths
                .iter()
                .filter_map(Value::as_str)
                .any(|pattern| wildcard(pattern, path)),
            "deployment path filter misses {path}"
        );
    }
    for path in ["README.md", "infra/README.md"] {
        ensure!(
            !paths
                .iter()
                .filter_map(Value::as_str)
                .any(|pattern| wildcard(pattern, path)),
            "docs must not trigger deployment"
        );
    }
    let infra = util::read_yaml(root.join(".github/workflows/infra.yml"))?;
    ensure!(
        infra["permissions"].get("id-token").is_none(),
        "credential-free PR workflow has OIDC"
    );
    ensure!(
        util::text(workflow["jobs"]["suspend"].get("if")).contains("always()"),
        "suspend must always clean up"
    );

    let deploy = std::fs::read_to_string(root.join("scripts/aws/deploy.sh"))?;
    let commands: String = deploy
        .lines()
        .filter(|line| !line.trim_start().starts_with('#'))
        .collect();
    ensure!(
        !commands.contains("--wait"),
        "deploy must not hand the rollout wait to Helm"
    );
    ensure!(
        commands.contains("aws-wait-rollout"),
        "deploy must use the Rust rollout observer"
    );
    ensure!(
        !deploy.contains("can-i create roles") && !deploy.contains("exec deployment/server"),
        "deploy exceeds its identity"
    );
    ensure!(
        std::fs::read_to_string(root.join("scripts/aws/apply.sh"))?
            .contains("bucket=$DUCKFLEET_STATE_BUCKET"),
        "state bucket must come from target.env"
    );
    let backend = std::fs::read_to_string(root.join("infra/terraform/dev/main.tf"))?;
    let backend = backend
        .split("backend \"s3\"")
        .nth(1)
        .and_then(|text| text.split('}').next())
        .unwrap_or_default();
    ensure!(
        !regex::Regex::new(r"(?m)^\s*(bucket|key|region)\s*=")?.is_match(backend),
        "Terraform backend pins target identity"
    );
    let bootstrap = std::fs::read_to_string(root.join("scripts/aws/bootstrap-state.sh"))?;
    ensure!(
        bootstrap.contains("== duckfleet-*")
            && bootstrap.find("get-caller-identity") < bootstrap.find("create-bucket"),
        "state bootstrap ownership check changed"
    );
    let rotation = std::fs::read_to_string(root.join("scripts/aws/rotate-catalog-credential.sh"))?;
    ensure!(
        rotation.contains("ALTER ROLE :\"catalog_role\" PASSWORD")
            && rotation.contains("aws-rotate-secret"),
        "rotation path changed"
    );
    ensure!(
        rotation.find("ALTER ROLE") < rotation.find("apply -f -")
            && rotation.find("apply -f -") < rotation.find("rollout restart"),
        "database/Secret/restart order changed"
    );
    let suspend = std::fs::read_to_string(root.join("scripts/aws/suspend.sh"))?;
    ensure!(
        suspend.contains("--reset-then-reuse-values") && suspend.contains("values-aws.yaml"),
        "suspension must rebase legacy values"
    );
    let access = std::fs::read_to_string(root.join("scripts/aws/check-access.sh"))?;
    ensure!(
        access.contains("auth can-i patch deployments.apps")
            && !access.contains("--subresource=scale"),
        "deploy scale permission must not be widened"
    );
    ensure!(
        std::fs::read_to_string(root.join("server/src/harness/aws_harness.rs"))?
            .contains("cold_start_checks"),
        "AWS isolated check missing"
    );
    Ok(())
}

fn rollout_regressions() -> Result<()> {
    let mut server = json!({"metadata":{"name":"server","generation":3,"annotations":{"duckfleet.io/migration-job":"current"}},"spec":{"replicas":1},"status":{"readyReplicas":1,"updatedReplicas":1,"replicas":1,"observedGeneration":2}});
    let migration = json!({"metadata":{"name":"current"},"status":{"conditions":[{"type":"Complete","status":"True"}]}});
    assert_eq!(
        aws_rollout::evaluate(
            &[server.clone()],
            &[],
            &mut BTreeMap::new(),
            0.0,
            90.0,
            &[],
            &[]
        )
        .0,
        "waiting"
    );
    server["status"]["observedGeneration"] = json!(3);
    assert_eq!(
        aws_rollout::evaluate(
            &[server.clone()],
            &[],
            &mut BTreeMap::new(),
            0.0,
            90.0,
            std::slice::from_ref(&migration),
            &[]
        )
        .0,
        "ready"
    );
    let database = json!({"metadata":{"name":"duckfleet-postgres","generation":1},"spec":{"replicas":1},"status":{"readyReplicas":0,"observedGeneration":1}});
    assert_eq!(
        aws_rollout::evaluate(
            &[server.clone()],
            &[],
            &mut BTreeMap::new(),
            0.0,
            90.0,
            &[],
            &[database]
        )
        .0,
        "waiting"
    );
    let failed = json!({"metadata":{"name":"current"},"status":{"conditions":[{"type":"Failed","status":"True","reason":"DeadlineExceeded"}]}});
    assert!(
        aws_rollout::evaluate(
            &[server.clone()],
            &[],
            &mut BTreeMap::new(),
            0.0,
            90.0,
            &[failed],
            &[]
        )
        .1
        .contains("DeadlineExceeded")
    );
    let old = json!({"metadata":{"name":"old","labels":{"app":"duckfleet-platform-migrate"}},"status":{"conditions":[{"type":"Complete","status":"True"}]}});
    let current = json!({"metadata":{"name":"current","labels":{"app":"duckfleet-platform-migrate"}},"status":{"conditions":[{"type":"Complete","status":"True"}]}});
    assert_eq!(
        aws_rollout::completed_migrations_to_prune(&[server], &[old, current]),
        vec!["old"]
    );
    Ok(())
}

fn rotation_regressions() -> Result<()> {
    let before = json!({"apiVersion":"v1","kind":"Secret","type":"Opaque","metadata":{"name":"duckfleet-dev","namespace":"duckfleet-dev"},"stringData":{
        "DATABASE_CATALOG_PASSWORD":"leaked","POSTGRES_PASSWORD":"administrator-unchanged",
        "CATALOG_DSN":"host=postgres port=6543 dbname=lake_v2 user=duckfleet password=leaked",
        "PLATFORM_DSN":"host=postgres port=6543 dbname=platform_v2 user=duckfleet_gateway password=platform-unchanged",
        "DUCKFLEET_TOKEN":"bearer-token-unchanged"
    }});
    let after = aws_simple::rotate(&before, "fresh")?;
    let values = &after["stringData"];
    ensure!(
        values["DATABASE_CATALOG_PASSWORD"] == "fresh"
            && values["POSTGRES_PASSWORD"] == "administrator-unchanged",
        "rotation changed the wrong key"
    );
    ensure!(
        values["CATALOG_DSN"]
            == "host=postgres port=6543 dbname=lake_v2 user=duckfleet password=fresh",
        "rotation rebuilt the DSN"
    );
    ensure!(
        values["PLATFORM_DSN"] == before["stringData"]["PLATFORM_DSN"]
            && values["DUCKFLEET_TOKEN"] == "bearer-token-unchanged",
        "rotation changed platform/admin credentials"
    );
    ensure!(
        !after.to_string().contains("leaked"),
        "old password survived"
    );
    assert!(
        aws_simple::rotate(
            &json!({"metadata":{},"stringData":{"POSTGRES_PASSWORD":"leaked"}}),
            "fresh"
        )
        .is_err()
    );
    let mut encoded = before.clone();
    let data = util::object(before.get("stringData").unwrap())
        .iter()
        .map(|(key, value)| {
            (
                key.clone(),
                json!(util::encode_base64(value.as_str().unwrap().as_bytes())),
            )
        })
        .collect::<Map<String, Value>>();
    encoded.as_object_mut().unwrap().remove("stringData");
    encoded["data"] = Value::Object(data);
    assert!(aws_simple::rotate(&encoded, "fresh").is_ok());
    Ok(())
}

fn platform_packaging() -> Result<()> {
    let original = objects(&[])?;
    let by_name = by_name(&original);
    let jobs: Vec<_> = original
        .iter()
        .filter(|object| {
            object["kind"] == "Job"
                && object["metadata"]["labels"]["app"] == "duckfleet-platform-migrate"
        })
        .collect();
    ensure!(jobs.len() == 1, "expected one migration Job");
    let job = jobs[0];
    ensure!(
        job["metadata"]["annotations"].get("helm.sh/hook").is_none(),
        "fresh migration has a Helm hook"
    );
    ensure!(
        job["spec"]["template"]["spec"]["serviceAccountName"] == "duckfleet-no-aws"
            && job["spec"]["template"]["spec"]["automountServiceAccountToken"] == false,
        "migration identity is too broad"
    );
    ensure!(
        by_name[&("Deployment".into(), "server".into())]["metadata"]["annotations"]["duckfleet.io/migration-job"]
            == job["metadata"]["name"],
        "gateway migration identity differs"
    );
    let reset = objects(&[
        "--set-string".into(),
        "platformMigration.runId=after-restore".into(),
    ])?;
    let reset_jobs: Vec<_> = reset
        .iter()
        .filter(|object| {
            object["kind"] == "Job"
                && object["metadata"]["labels"]["app"] == "duckfleet-platform-migrate"
        })
        .collect();
    ensure!(
        reset_jobs[0]["metadata"]["name"] != job["metadata"]["name"],
        "restore run ID did not recreate migration"
    );
    let suspended = objects(&["--set".into(), "suspended=true".into()])?;
    ensure!(
        !suspended.iter().any(|object| object["kind"] == "Job"
            && object["metadata"]["labels"]["app"] == "duckfleet-platform-migrate"),
        "suspended release migrated"
    );
    let external = objects(&["--set".into(), "postgres.bundled=false".into()])?;
    ensure!(
        !external
            .iter()
            .any(|object| object["kind"] == "StatefulSet"),
        "external PostgreSQL still renders bundled state"
    );
    ensure!(
        original.iter().all(|object| !matches!(
            object["kind"].as_str(),
            Some("Role" | "RoleBinding" | "ClusterRole" | "ClusterRoleBinding" | "ResourceQuota")
        )),
        "release contains boundary objects"
    );
    let legacy = objects(&["--set".into(), "legacyDatabaseUpgrade=true".into()])?;
    let provision: Vec<_> = legacy
        .iter()
        .filter(|object| object["metadata"]["name"] == "duckfleet-database-provision")
        .collect();
    ensure!(
        provision.len() == 1 && provision[0]["spec"]["backoffLimit"] == 0,
        "legacy provisioning Job missing"
    );
    let container = &provision[0]["spec"]["template"]["spec"]["containers"][0];
    ensure!(
        container["image"] == "postgres:17.6-bookworm" && container.get("envFrom").is_none(),
        "legacy provisioning identity changed"
    );
    let refs: BTreeMap<_, _> = util::array(container.get("env"))
        .iter()
        .map(|item| {
            (
                util::text(item.get("name")),
                util::text(item["valueFrom"]["secretKeyRef"].get("key")),
            )
        })
        .collect();
    ensure!(
        refs["PGPASSWORD"] == "POSTGRES_PASSWORD"
            && refs["DUCKFLEET_DB_CATALOG_READER_PASSWORD"] == "DATABASE_CATALOG_READER_PASSWORD",
        "legacy provisioning Secret refs changed"
    );
    Ok(())
}

fn pool_objects(pools: Value, extra: &[String]) -> Result<Vec<Value>> {
    let default = extra
        .iter()
        .find_map(|arg| arg.strip_prefix("defaultPool="))
        .unwrap_or("small");
    let pool = &pools[default];
    let reviewed = json!({"pool":default,"cpu":pool.get("cpu").cloned().unwrap_or(json!(1)),"memory":pool.get("memory").cloned().unwrap_or(json!("1Gi")),"disk":pool.get("disk").cloned().unwrap_or(json!("2Gi"))});
    let args = vec![
        "--set-json".into(),
        format!("persistentPools={}", serde_json::to_string(&pools)?),
        "--set-json".into(),
        format!(
            "authorization.defaultPersistentGrant={}",
            serde_json::to_string(&reviewed)?
        ),
    ]
    .into_iter()
    .chain(extra.iter().cloned())
    .collect::<Vec<_>>();
    objects(&args)
}

fn named_pools() -> Result<()> {
    let pools = json!({
        "small":{"cpu":1,"memory":"1Gi","disk":"2Gi","workers":2},
        "interactive":{"cpu":2,"memory":"3Gi","disk":"4Gi","workers":2,"maxSessionsPerWorker":7},
        "small-1":{"cpu":1,"memory":"512Mi","disk":"1536Mi","workers":1}
    });
    let rendered = pool_objects(pools.clone(), &[])?;
    let named = by_name(&rendered);
    let config = util::read_json_from_string(util::text(
        named[&("ConfigMap".into(), "duckfleet-pools".into())]["data"].get("pools.json"),
    ))?;
    ensure!(
        config["pools"]["interactive"]["max_sessions_per_worker"] == 7
            && config["pools"]["interactive"]["authority"] == "writer",
        "pool registry lost allocation"
    );
    ensure!(
        named.contains_key(&("Deployment".into(), "worker-pool-small-1-0".into())),
        "pool name collided with legacy replica"
    );
    let interactive = &named[&("Deployment".into(), "worker-pool-interactive-0".into())]["spec"]["template"]
        ["spec"];
    ensure!(
        interactive["containers"][0]["resources"]["requests"]
            == json!({"cpu":"2","memory":"3Gi","ephemeral-storage":"4Gi"}),
        "independent named-pool resources changed"
    );
    ensure!(
        interactive["containers"][0]["resources"]["limits"]["ephemeral-storage"] == "5Gi"
            && interactive["volumes"][0]["emptyDir"]["sizeLimit"] == "4Gi",
        "disk overhead changed"
    );
    let updated = by_name(&pool_objects(
        pools.clone(),
        &["--set".into(), "defaultPool=interactive".into()],
    )?);
    ensure!(
        named[&("Deployment".into(), "server".into())]["spec"]["template"]
            == updated[&("Deployment".into(), "server".into())]["spec"]["template"],
        "pool reload changes gateway pod template"
    );
    ensure!(
        named[&("ConfigMap".into(), "duckfleet-pools".into())]["data"]
            != updated[&("ConfigMap".into(), "duckfleet-pools".into())]["data"],
        "pool update did not change ConfigMap"
    );
    let empty = by_name(&pool_objects(
        json!({"etl":{"cpu":4,"memory":"8Gi","disk":"20Gi","workers":0}}),
        &["--set".into(), "defaultPool=etl".into()],
    )?);
    ensure!(
        !empty
            .keys()
            .any(|(kind, name)| kind == "Deployment" && name.starts_with("worker-")),
        "zero-worker pool rendered compute"
    );
    for args in [
        vec!["--set".into(), "defaultPool=missing".into()],
        vec!["--set".into(), "workerReplicas.small=1".into()],
        vec![
            "--set".into(),
            "persistentPools.interactive.memory=128Mi".into(),
        ],
        vec!["--set".into(), "gatewayMaxSessions=4097".into()],
    ] {
        let full = vec!["--set-json".into(), format!("persistentPools={pools}"), "--set-json".into(), "authorization.defaultPersistentGrant={\"pool\":\"small\",\"cpu\":1,\"memory\":\"1Gi\",\"disk\":\"2Gi\"}".into()]
            .into_iter().chain(args).collect::<Vec<_>>();
        ensure!(
            objects(&full).is_err(),
            "invalid pool configuration rendered"
        );
    }
    let mut reader_pools = pools.clone();
    reader_pools["readers"] =
        json!({"cpu":1,"memory":"1Gi","disk":"2Gi","workers":1,"authority":"reader"});
    let reader = by_name(&pool_objects(reader_pools, &[])?);
    let pod =
        &reader[&("Deployment".into(), "worker-pool-readers-0".into())]["spec"]["template"]["spec"];
    let names: BTreeSet<_> = util::array(pod["containers"][0].get("env"))
        .iter()
        .filter_map(|entry| entry["name"].as_str())
        .collect();
    ensure!(
        pod["serviceAccountName"] == "duckfleet-reader"
            && names.contains("CATALOG_READER_DSN")
            && !names.contains("CATALOG_DSN"),
        "reader/writer identities mixed"
    );
    Ok(())
}

fn queue_and_jobs() -> Result<()> {
    let pools = json!({"small":{"cpu":1,"memory":"1Gi","disk":"2Gi","workers":1},"interactive":{"cpu":2,"memory":"3Gi","disk":"4Gi","workers":1,"maxSessionsPerWorker":7,"jobLimits":{"runtime_seconds":300,"result_bytes":1073741824,"result_rows":10000000,"retention_seconds":86400}}});
    let rendered = by_name(&pool_objects(pools, &[
        "--set".into(), "persistentPools.interactive.admission.maxRunningQueries=4".into(),
        "--set".into(), "admissionLimits.default.maxRunningPerPrincipal=1".into(),
        "--set-json".into(), "jobPolicy={\"ceilings\":{\"runtime_seconds\":900,\"result_bytes\":1073741824,\"result_rows\":10000000,\"retention_seconds\":86400}}".into(),
        "--set".into(), "results.enabled=true".into(), "--set".into(), "isolatedCompute.enabled=false".into(),
    ])?);
    let registry = util::read_json_from_string(util::text(
        rendered[&("ConfigMap".into(), "duckfleet-pools".into())]["data"].get("pools.json"),
    ))?;
    ensure!(
        registry["pools"]["interactive"]["admission"] == json!({"max_running_queries":4}),
        "pool admission override lost"
    );
    ensure!(
        registry["pools"]["interactive"]["job_limits"]["runtime_seconds"] == 300
            && registry["pools"]["small"].get("job_limits").is_none(),
        "named job limits leaked between pools"
    );
    let gateway = &rendered[&("Deployment".into(), "server".into())]["spec"]["template"]["spec"];
    ensure!(
        gateway["serviceAccountName"] == "duckfleet-results",
        "result-only identity missing"
    );
    let env = util::env_map(gateway["containers"][0].get("env"));
    ensure!(
        env["DUCKFLEET_RESULT_AUTH_MODE"] == "pod_identity" && !env.contains_key("S3_DATA_PATH"),
        "gateway result storage has lake credentials"
    );
    let compose = util::read_yaml(util::root().join("compose.yaml"))?;
    for name in ["server", "server-pools"] {
        let dependencies = &compose["services"][name]["depends_on"];
        ensure!(
            dependencies["platform-migrate"]["condition"] == "service_completed_successfully",
            "migration must precede gateway"
        );
        for (worker, condition) in util::object(dependencies) {
            if worker.starts_with("worker-") {
                ensure!(
                    condition["condition"] == "service_started",
                    "gateway waits for worker readiness"
                );
            }
        }
    }
    Ok(())
}

fn image_and_resource_regressions() -> Result<()> {
    let digest = format!("sha256:{}", "a".repeat(64));
    let amd = json!({"platform":{"os":"linux","architecture":"amd64"}});
    let arm = json!({"platform":{"os":"linux","architecture":"arm64"}});
    let attestation = json!({"platform":{"os":"unknown","architecture":"unknown"},"annotations":{"vnd.docker.reference.type":"attestation-manifest"}});
    assert!(
        aws_simple::image_reference(
            "worker",
            &digest,
            &json!({"manifests":[arm,amd,attestation]})
        )
        .is_ok()
    );
    assert!(
        aws_simple::image_reference("worker", "latest", &json!({"manifests":[amd,arm]})).is_err()
    );
    let mut resources = json!({"requests":{"cpu":"500m","memory":"3Gi","ephemeral-storage":"20Gi"},"limits":{"cpu":"2","memory":"3Gi","ephemeral-storage":"21Gi"}});
    worker_resources::assert_worker_resources(&resources, 2, "fixture", Some("3Gi"), Some("20Gi"))?;
    resources["requests"]["cpu"] = json!("0");
    assert!(
        worker_resources::assert_worker_resources(
            &resources,
            2,
            "fixture",
            Some("3Gi"),
            Some("20Gi")
        )
        .is_err()
    );
    resources["requests"]["cpu"] = json!("3");
    assert!(
        worker_resources::assert_worker_resources(
            &resources,
            2,
            "fixture",
            Some("3Gi"),
            Some("20Gi")
        )
        .is_err()
    );
    resources["requests"]["cpu"] = json!("1");
    resources["limits"]["memory"] = json!("1Gi");
    assert!(
        worker_resources::assert_worker_resources(
            &resources,
            2,
            "fixture",
            Some("3Gi"),
            Some("20Gi")
        )
        .is_err()
    );
    Ok(())
}

fn dns_and_boundary() -> Result<()> {
    let found = vec![("172.20.0.4".into(), 6443)];
    assert_eq!(
        aws_simple::uncovered(&found, &["10.96.0.1/32".into()], &[443])?,
        vec!["172.20.0.4:6443"]
    );
    assert!(aws_simple::uncovered(&found, &["172.20.0.0/16".into()], &[6443])?.is_empty());
    let service = Network::parse("172.20.0.0/16")?;
    assert_eq!(
        service.address_plus(10)?,
        "172.20.0.10".parse::<std::net::IpAddr>()?
    );
    let values = util::read_yaml(util::root().join("infra/helm/duckfleet/values.yaml"))?;
    let ranges: Vec<Network> = util::string_set(values.get("dnsEgressCidrs"))
        .iter()
        .map(|range| Network::parse(range))
        .collect::<Result<_>>()?;
    let resolver = service.address_plus(10)?;
    ensure!(
        ranges.iter().any(|range| range.contains(resolver)),
        "shipped ranges miss dev DNS"
    );
    ensure!(
        ranges.iter().all(Network::is_private),
        "DNS ranges must be private"
    );
    let boundary = aws_simple::boundary_documents("duckfleet-elsewhere", "")?;
    for document in &boundary {
        if document["kind"] == "Namespace" {
            ensure!(
                document["metadata"]["name"] == "duckfleet-elsewhere",
                "Namespace target did not move"
            );
        } else {
            ensure!(
                document["metadata"]["namespace"] == "duckfleet-elsewhere",
                "boundary object escaped namespace"
            );
        }
    }
    let role = boundary
        .iter()
        .find(|document| document["kind"] == "Role")
        .unwrap();
    let resources: Vec<_> = util::array(role.get("rules"))
        .iter()
        .map(|rule| rule["resources"].clone())
        .collect();
    ensure!(
        resources
            == vec![
                json!(["pods"]),
                json!(["pods/log"]),
                json!(["deployments"]),
                json!(["deployments/scale"])
            ],
        "gateway Role changed"
    );
    ensure!(
        !boundary
            .iter()
            .any(|document| document["kind"] == "NodePool"),
        "portable boundary contains Karpenter"
    );
    let cloud = aws_simple::boundary_documents("duckfleet-dev", "chosen")?;
    let pool = cloud
        .iter()
        .find(|document| document["kind"] == "NodePool")
        .unwrap();
    ensure!(
        pool["spec"]["template"]["spec"]["nodeClassRef"]["name"] == "chosen"
            && pool["spec"]["disruption"]["consolidationPolicy"] == "WhenEmpty",
        "node-pool class/disruption changed"
    );
    ensure!(
        pool["spec"]["limits"]["cpu"].is_number()
            || !util::text(pool["spec"]["limits"].get("cpu")).is_empty(),
        "node-pool CPU cap missing or empty"
    );
    Ok(())
}

fn database_packaging() -> Result<()> {
    let document = database_roles::secret("fixture", "duckfleet-dev", "postgres")?;
    database_roles::check_secret(&document)?;
    ensure!(
        document["stringData"]["CATALOG_READER_DSN"]
            .as_str()
            .unwrap()
            .contains("dbname=lake"),
        "reader points at platform DB"
    );
    let changed = database_roles::retarget(&document, "external-postgres")?;
    ensure!(
        changed["stringData"]["CATALOG_DSN"]
            .as_str()
            .unwrap()
            .contains("host=external-postgres"),
        "retarget did not change host"
    );
    let original = json!({"metadata":{"name":"duckfleet-dev"},"stringData":{"POSTGRES_PASSWORD":"admin-preserved","DUCKFLEET_TOKEN":"public-preserved","CATALOG_DSN":"host=postgres user=old_owner password=old dbname=lake"}});
    let upgraded = database_roles::upgrade(
        database_roles::secret("fixture", "duckfleet-dev", "postgres")?,
        &original,
    )?;
    let repeated = database_roles::upgrade(
        database_roles::secret("fixture", "duckfleet-dev", "postgres")?,
        &upgraded,
    )?;
    ensure!(
        upgraded == repeated && upgraded["stringData"]["DUCKFLEET_TOKEN"] == "public-preserved",
        "Secret upgrade is not repeatable"
    );
    let objects = objects(&[])?;
    let pods: Vec<Value> = objects.iter().filter(|object| object["kind"] == "Deployment").map(|object| json!({"metadata":object["spec"]["template"]["metadata"],"spec":object["spec"]["template"]["spec"]})).collect();
    let counts = kind_checks::validate_role_posture(&pods)?;
    ensure!(
        counts["gateway"] == 1 && counts["worker"] > 0,
        "live posture did not count components"
    );
    let dsn = document["stringData"]["CATALOG_DSN"].as_str().unwrap();
    let query = team::catalog_probe(dsn, "fixture")?;
    ensure!(
        query.contains("TYPE postgres, READ_ONLY") && query.contains("connect_timeout=3"),
        "namespace probe lost safe catalog attachment"
    );
    let compose = util::read_yaml(util::root().join("compose.yaml"))?;
    ensure!(
        compose["services"]["platform-migrate"]["depends_on"]["platform-provision"]["condition"]
            == "service_completed_successfully",
        "Compose provisions after migration"
    );
    for name in [
        "worker-small",
        "worker-small-b",
        "worker-pool-interactive-0",
    ] {
        let env = util::object(compose["services"][name].get("environment").unwrap());
        ensure!(
            !env.contains_key("DUCKFLEET_TOKEN") && !env.contains_key("DUCKFLEET_MAINTENANCE_DSN"),
            "Compose worker has owner token"
        );
    }
    Ok(())
}

fn rust_only_tooling() -> Result<()> {
    fn visit(directory: &Path, files: &mut Vec<std::path::PathBuf>) -> Result<()> {
        for entry in std::fs::read_dir(directory)? {
            let path = entry?.path();
            if path.is_dir() {
                visit(&path, files)?;
            } else {
                files.push(path);
            }
        }
        Ok(())
    }
    let root = util::root();
    let mut files = Vec::new();
    for directory in ["scripts", "tests", "docker", ".github"] {
        visit(&root.join(directory), &mut files)?;
    }
    for file in files {
        ensure!(
            file.extension().is_none_or(|extension| extension != "py"),
            "Python source remains at {}",
            file.display()
        );
        let Ok(text) = std::fs::read_to_string(&file) else {
            continue;
        };
        for forbidden in ["python3 ", "pip install", "FROM python:"] {
            ensure!(
                !text.contains(forbidden),
                "Python dependency {forbidden:?} remains in {}",
                file.display()
            );
        }
    }
    Ok(())
}

pub fn run(args: Vec<String>) -> Result<()> {
    util::reject_extra(&args, 0)?;
    rust_only_tooling()?;
    workflow_and_operator_scripts()?;
    rollout_regressions()?;
    rotation_regressions()?;
    platform_packaging()?;
    named_pools()?;
    queue_and_jobs()?;
    image_and_resource_regressions()?;
    dns_and_boundary()?;
    database_packaging()?;
    println!(
        "PASS: Rust infrastructure, workflow, rollout, packaging, IAM, and database-role regressions"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn workflow_glob_matching_is_anchored() {
        assert!(wildcard("server/**", "server/src/main.rs"));
        assert!(wildcard("*.md", "docs/README.md"));
        assert!(wildcard("**/server.Dockerfile", "docker/server.Dockerfile"));
    }

    #[test]
    fn rollout_and_rotation_safety_cases_pass_without_helm() {
        rollout_regressions().unwrap();
        rotation_regressions().unwrap();
        image_and_resource_regressions().unwrap();
    }
}
