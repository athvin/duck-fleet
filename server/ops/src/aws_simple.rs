use crate::{network::Network, util};
use anyhow::{Context, Result, bail};
use serde_json::{Map, Value, json};
use std::collections::BTreeSet;

const REGISTRY: &str = "797781631727.dkr.ecr.us-east-2.amazonaws.com/duckfleet-dev";

pub fn validate_plan(plan: &Value) -> Result<()> {
    if plan["errored"] == true || plan["complete"] != true {
        bail!("Plan is errored or incomplete");
    }
    let allowed: BTreeSet<&str> = [
        "aws_s3_bucket.lake",
        "aws_s3_bucket_public_access_block.lake",
        "aws_s3_bucket_ownership_controls.lake",
        "aws_s3_bucket_server_side_encryption_configuration.lake",
        "aws_s3_bucket_versioning.lake",
        "aws_s3_bucket_policy.lake",
        "aws_s3_bucket_lifecycle_configuration.lake",
        "aws_ecr_repository.images[\"worker\"]",
        "aws_ecr_repository.images[\"dbt\"]",
    ]
    .into_iter()
    .collect();
    for resource in util::array(plan.get("resource_changes")) {
        let address = util::text(resource.get("address"));
        if !allowed.contains(address)
            || resource.get("mode").and_then(Value::as_str) != Some("managed")
        {
            bail!("Resource outside DuckFleet storage/images: {address}");
        }
        let actions: Vec<_> = util::array(resource["change"].get("actions"))
            .iter()
            .filter_map(Value::as_str)
            .collect();
        if actions != ["no-op"] && actions != ["create"] && actions != ["update"] {
            bail!("Destructive action refused: {address}");
        }
        if resource["change"]
            .get("importing")
            .is_some_and(|value| !value.is_null())
            || resource
                .get("previous_address")
                .is_some_and(|value| !value.is_null())
        {
            bail!("Import/move requires a separate reviewed migration");
        }
    }
    Ok(())
}

pub fn plan_guard(args: Vec<String>) -> Result<()> {
    util::reject_extra(&args, 0)?;
    let mut input = String::new();
    std::io::Read::read_to_string(&mut std::io::stdin(), &mut input)?;
    validate_plan(&serde_json::from_str(&input)?)?;
    println!("Plan contains only additive/in-place DuckFleet storage and image changes");
    Ok(())
}

pub fn image_reference(component: &str, digest: &str, index: &Value) -> Result<String> {
    if !matches!(component, "worker" | "dbt")
        || !regex::Regex::new(r"^sha256:[0-9a-f]{64}$")?.is_match(digest)
    {
        bail!("Invalid component or digest");
    }
    let mut platforms = Vec::new();
    for manifest in util::array(index.get("manifests")) {
        let platform = manifest.get("platform");
        let pair = (
            util::text(platform.and_then(|value| value.get("os"))),
            util::text(platform.and_then(|value| value.get("architecture"))),
        );
        if pair == ("unknown", "unknown")
            && manifest["annotations"]["vnd.docker.reference.type"] == "attestation-manifest"
        {
            continue;
        }
        platforms.push((pair.0.to_owned(), pair.1.to_owned()));
    }
    if platforms.len() != 2
        || !platforms.contains(&("linux".into(), "amd64".into()))
        || !platforms.contains(&("linux".into(), "arm64".into()))
    {
        bail!("Expected exactly one linux/amd64 and linux/arm64 image");
    }
    Ok(format!("{REGISTRY}/{component}@{digest}"))
}

pub fn image_values(args: Vec<String>) -> Result<()> {
    util::reject_extra(&args, 0)?;
    let root = util::root().join("artifacts/aws");
    let mut values = Map::new();
    for component in ["worker", "dbt"] {
        let digest = std::fs::read_to_string(root.join(format!("{component}-digest.txt")))?
            .trim()
            .to_owned();
        let index = util::read_json(root.join(format!("{component}-index.json")))?;
        values.insert(
            format!("{component}Image"),
            json!(image_reference(component, &digest, &index)?),
        );
    }
    util::write_json(root.join("values.json"), &Value::Object(values))?;
    println!("Verified both architectures; immutable Helm values written");
    Ok(())
}

pub fn overlay(cidrs: &[String], ports: &[u64]) -> Value {
    let mut cidrs = cidrs.to_vec();
    cidrs.sort();
    let mut ports = ports.to_vec();
    ports.sort_unstable();
    json!({
        "isolatedCompute": {
            "enabled": true,
            "apiServerCidrs": cidrs,
            "apiServerPorts": ports,
            "readySeconds": 420,
            "provisioningSeconds": 240,
            // What the duckfleet NodePool's largest instance (8xlarge: 32 vCPU, up
            // to 256 GiB on r6i/r7i) can allocate to one pod after kube-reserved and
            // the node's daemonsets (#214).
            "maxCpu": 30,
            "maxMemory": "240Gi",
        },
        "nodeSelector": {
            "kubernetes.io/os": "linux",
            "eks.amazonaws.com/compute-type": Value::Null,
            "karpenter.sh/nodepool": "duckfleet",
        },
        "tolerations": [{
            "key": "duckfleet.io/dedicated",
            "operator": "Equal",
            "value": "true",
            "effect": "NoSchedule",
        }],
    })
}

pub fn deploy_values(args: Vec<String>) -> Result<()> {
    if args.is_empty() || args.len() > 2 {
        bail!("usage: aws-deploy-values <context> [namespace]");
    }
    let namespace = args.get(1).map_or("duckfleet-dev", String::as_str);
    let path = util::root().join("artifacts/aws/values.json");
    let output = util::run_status([
        "kubectl",
        "--context",
        &args[0],
        "-n",
        namespace,
        "get",
        "configmap",
        "duckfleet-api-egress",
        "-o",
        "json",
    ])?;
    if !output.status.success() {
        println!(
            "no duckfleet-api-egress ConfigMap; isolated compute stays off (run scripts/aws/apply-boundary.sh to record the API-server address)"
        );
        return Ok(());
    }
    let config: Value = serde_json::from_slice(&output.stdout)?;
    let cidrs: Vec<String> = serde_json::from_str(util::text(config["data"].get("cidrs")))?;
    let ports: Vec<u64> = serde_json::from_str(util::text(config["data"].get("ports")))?;
    if cidrs.is_empty() || ports.is_empty() {
        println!("no API egress addresses recorded; isolated compute stays off");
        return Ok(());
    }
    let mut values = util::read_json(&path)?;
    let overlay = overlay(&cidrs, &ports);
    for (key, value) in util::object(&overlay) {
        values[key] = value.clone();
    }
    // Exposure is per namespace, and values.json is not, so it is always
    // rewritten from this namespace's ConfigMap (or removed when there is none).
    let exposure = util::run_status([
        "kubectl",
        "--context",
        &args[0],
        "-n",
        namespace,
        "get",
        "configmap",
        "duckfleet-exposure",
        "-o",
        "json",
    ])?;
    if exposure.status.success() {
        let config: Value = serde_json::from_slice(&exposure.stdout)?;
        let exposure = exposure_values(&config)?;
        println!(
            "deploy values: exposed at https://{} through IngressClass {}",
            util::text(exposure.get("host")),
            util::text(exposure.get("ingressClassName"))
        );
        values["exposure"] = exposure;
    } else if let Some(object) = values.as_object_mut() {
        object.remove("exposure");
    }
    util::write_json(&path, &values)?;
    println!(
        "deploy values: isolated compute on, API egress to {cidrs:?} ports {ports:?}, workers pinned to the duckfleet pool"
    );
    Ok(())
}

pub fn uncovered(found: &[(String, u64)], ranges: &[String], ports: &[u64]) -> Result<Vec<String>> {
    let networks = ranges
        .iter()
        .map(|range| Network::parse(range))
        .collect::<Result<Vec<_>>>()?;
    Ok(found
        .iter()
        .filter_map(|(address, port)| {
            let address = address.parse().ok()?;
            (!(networks.iter().any(|network| network.contains(address)) && ports.contains(port)))
                .then(|| format!("{address}:{port}"))
        })
        .collect())
}

pub fn check_api_egress(args: Vec<String>) -> Result<()> {
    util::reject_extra(&args, 0)?;
    let root = util::root();
    let mut values = util::read_yaml(root.join("infra/helm/duckfleet/values.yaml"))?;
    let overlay_path = root.join("artifacts/aws/values.json");
    if overlay_path.exists() {
        let overlay = util::read_json(overlay_path)?;
        for (key, value) in util::object(&overlay) {
            if value.is_object() && values.get(key).is_some_and(Value::is_object) {
                for (child, child_value) in util::object(value) {
                    values[key][child] = child_value.clone();
                }
            } else {
                values[key] = value.clone();
            }
        }
    }
    if values["isolatedCompute"]["enabled"] != true {
        println!("isolated compute is disabled; no API egress rule to verify");
        return Ok(());
    }
    let ranges = util::string_set(values["isolatedCompute"].get("apiServerCidrs"));
    let ports = util::array(values["isolatedCompute"].get("apiServerPorts"));
    if ranges.is_empty() || ports.is_empty() {
        bail!(
            "isolated compute is enabled but no API-server egress address is set. Run scripts/aws/apply-boundary.sh to record the endpoints the gateway must reach (#32)"
        );
    }
    println!(
        "API egress names {ranges:?} on ports {ports:?}, recorded from the live endpoints by the ownership boundary"
    );
    Ok(())
}

pub fn check_dns_egress(args: Vec<String>) -> Result<()> {
    if args.len() != 2 {
        bail!("usage: aws-check-dns-egress <cluster> <region>");
    }
    let described = util::output_text([
        "aws",
        "eks",
        "describe-cluster",
        "--name",
        &args[0],
        "--region",
        &args[1],
        "--query",
        "cluster.kubernetesNetworkConfig.serviceIpv4Cidr",
        "--output",
        "text",
    ])?;
    if described.is_empty() || described == "None" {
        bail!("could not read the service CIDR for cluster {}", args[0]);
    }
    let service = Network::parse(&described)?;
    let resolver = service.address_plus(10)?;
    let values = util::read_yaml(util::root().join("infra/helm/duckfleet/values.yaml"))?;
    let ranges: Vec<String> = util::string_set(values.get("dnsEgressCidrs"))
        .into_iter()
        .collect();
    if ranges.is_empty() {
        bail!("values.yaml sets no dnsEgressCidrs; DNS would be denied");
    }
    let networks = ranges
        .iter()
        .map(|range| Network::parse(range))
        .collect::<Result<Vec<_>>>()?;
    let covering: Vec<String> = networks
        .iter()
        .filter(|network| network.contains(resolver))
        .map(ToString::to_string)
        .collect();
    println!(
        "{}",
        serde_json::to_string(&json!({
            "cluster": args[0], "service_cidr": service.to_string(), "expected_resolver": resolver.to_string(),
            "configured": networks.iter().map(ToString::to_string).collect::<Vec<_>>(), "covering": covering,
        }))?
    );
    if covering.is_empty() {
        bail!(
            "dnsEgressCidrs does not cover {resolver}, the resolver pods in {} will use",
            args[0]
        );
    }
    if let Some(network) = networks.iter().find(|network| !network.is_private()) {
        bail!("dnsEgressCidrs must stay private, got {network}");
    }
    println!(
        "PASS: cluster DNS at {resolver} is permitted by {}",
        covering.join(", ")
    );
    Ok(())
}

pub fn render_boundary(args: Vec<String>) -> Result<()> {
    if args.is_empty() || args.len() > 2 {
        bail!("usage: aws-render-boundary <namespace> [node-class]");
    }
    let mut documents = boundary_documents(&args[0], args.get(1).map_or("", String::as_str))?;
    let variable = |name: &str| std::env::var(name).unwrap_or_default();
    let host = variable("DUCKFLEET_EXPOSURE_HOST");
    let certificate = variable("DUCKFLEET_EXPOSURE_CERTIFICATE_ARN");
    let sources = variable("DUCKFLEET_EXPOSURE_SOURCE_CIDRS");
    if !(host.is_empty() && certificate.is_empty()) {
        let sources: Vec<String> = sources
            .split(',')
            .map(str::trim)
            .filter(|cidr| !cidr.is_empty())
            .map(str::to_owned)
            .collect();
        documents.extend(exposure_documents(&args[0], &host, &certificate, &sources)?);
    }
    print!("{}", util::yaml_string(&documents)?);
    Ok(())
}

/// The EKS Auto Mode ALB that exposes one namespace's gateway: an IngressClass
/// and its IngressClassParams (cluster-scoped, so the operator's), plus the
/// ConfigMap that hands the deploy its exposure values. Every fact is required:
/// a default host, certificate, or source range would be invented.
pub fn exposure_documents(
    namespace: &str,
    host: &str,
    certificate_arn: &str,
    source_cidrs: &[String],
) -> Result<Vec<Value>> {
    let host_pattern =
        regex::Regex::new(r"^[a-z0-9]([a-z0-9-]*[a-z0-9])?(\.[a-z0-9]([a-z0-9-]*[a-z0-9])?)+$")?;
    if !host_pattern.is_match(host) {
        bail!("DUCKFLEET_EXPOSURE_HOST must be a DNS name, got {host:?}");
    }
    if !regex::Regex::new(r"^arn:aws[a-z-]*:acm:[a-z0-9-]+:[0-9]{12}:certificate/[0-9a-f-]+$")?
        .is_match(certificate_arn)
    {
        bail!(
            "DUCKFLEET_EXPOSURE_CERTIFICATE_ARN must be an ACM certificate ARN, got {certificate_arn:?}"
        );
    }
    if source_cidrs.is_empty() {
        bail!(
            "DUCKFLEET_EXPOSURE_SOURCE_CIDRS must name the VPC ranges the load balancer sends from"
        );
    }
    for cidr in source_cidrs {
        let network = Network::parse(cidr)?;
        if !network.is_private() || network.prefix_len() < 8 {
            bail!(
                "exposure source {cidr} is not private; the load balancer sends from VPC addresses"
            );
        }
    }
    let path = util::root().join("infra/kubernetes/boundary/eks-exposure/alb.yaml");
    let text = std::fs::read_to_string(path)?
        .replace("${namespace}", namespace)
        .replace("${host}", host)
        .replace("${certificate_arn}", certificate_arn)
        .replace("${source_cidrs}", &serde_json::to_string(source_cidrs)?);
    if regex::Regex::new(r"\$\{[A-Za-z_][A-Za-z0-9_]*\}")?.is_match(&text) {
        bail!("unresolved exposure placeholder");
    }
    let documents: Vec<Value> = util::yaml_documents(&text)?
        .into_iter()
        .filter(|document| !document.is_null())
        .collect();
    for document in &documents {
        let kind = util::text(document.get("kind"));
        let name = util::text(document["metadata"].get("name"));
        match kind {
            "IngressClass" | "IngressClassParams" => {
                if name != format!("duckfleet-{namespace}") {
                    bail!("cluster-scoped {kind}/{name} is not named for {namespace}");
                }
            }
            _ if document["metadata"]
                .get("namespace")
                .and_then(Value::as_str)
                == Some(namespace) => {}
            _ => bail!("{kind}/{name} does not name namespace {namespace}"),
        }
    }
    Ok(documents)
}

/// The chart's `exposure` values from the operator's `duckfleet-exposure` ConfigMap.
pub fn exposure_values(config: &Value) -> Result<Value> {
    let data = &config["data"];
    let host = util::text(data.get("host"));
    let class = util::text(data.get("ingressClassName"));
    let sources: Vec<String> = serde_json::from_str(util::text(data.get("sourceCidrs")))
        .context("duckfleet-exposure sourceCidrs is not a JSON list")?;
    if host.is_empty() || class.is_empty() || sources.is_empty() {
        bail!(
            "the duckfleet-exposure ConfigMap is incomplete; re-run scripts/aws/apply-boundary.sh"
        );
    }
    Ok(json!({
        "enabled": true,
        "host": host,
        "ingressClassName": class,
        "loadBalancer": {"enabled": true, "sourceCidrs": sources},
    }))
}

pub fn boundary_documents(namespace: &str, node_class: &str) -> Result<Vec<Value>> {
    let boundary = util::root().join("infra/kubernetes/boundary");
    let mut paths: Vec<_> = std::fs::read_dir(&boundary)?
        .map(|entry| entry.unwrap().path())
        .filter(|path| {
            path.extension()
                .is_some_and(|extension| extension == "yaml")
        })
        .collect();
    paths.sort();
    if !node_class.is_empty() {
        let mut eks: Vec<_> = std::fs::read_dir(boundary.join("eks"))?
            .map(|entry| entry.unwrap().path())
            .filter(|path| {
                path.extension()
                    .is_some_and(|extension| extension == "yaml")
            })
            .collect();
        eks.sort();
        paths.extend(eks);
    }
    let mut documents = Vec::new();
    let placeholder = regex::Regex::new(r"\$\{[A-Za-z_][A-Za-z0-9_]*\}")?;
    for path in paths {
        let text = std::fs::read_to_string(path)?
            .replace("${namespace}", namespace)
            .replace("${node_class}", node_class);
        if placeholder.is_match(&text) {
            bail!("unresolved boundary placeholder");
        }
        documents.extend(
            util::yaml_documents(&text)?
                .into_iter()
                .filter(|document| !document.is_null()),
        );
    }
    for document in &documents {
        let kind = util::text(document.get("kind"));
        let metadata = document
            .get("metadata")
            .context("boundary object has no metadata")?;
        let name = util::text(metadata.get("name"));
        if kind == "Namespace" {
            if name != namespace {
                bail!("the Namespace is {name}, not {namespace}");
            }
            continue;
        }
        if matches!(kind, "Namespace" | "NodePool") {
            if !name.starts_with("duckfleet") {
                bail!("cluster-scoped {kind}/{name} is not named for DuckFleet");
            }
            continue;
        }
        if metadata.get("namespace").and_then(Value::as_str) != Some(namespace) {
            bail!("{kind}/{name} does not name namespace {namespace}");
        }
        for subject in util::array(document.get("subjects")) {
            if subject["kind"] == "ServiceAccount" && subject["namespace"] != namespace {
                bail!("{kind}/{name} binds a ServiceAccount outside {namespace}");
            }
        }
    }
    Ok(documents)
}

pub fn rotate(secret: &Value, password: &str) -> Result<Value> {
    let decode = |key: &str| -> Result<String> {
        if let Some(value) = secret["stringData"].get(key) {
            return Ok(value
                .as_str()
                .context("Secret value must be a string")?
                .to_owned());
        }
        if let Some(value) = secret["data"].get(key) {
            return util::decode_base64(value.as_str().context("Secret value must be a string")?);
        }
        bail!("the secret has no {key}; refusing to rotate a partial credential")
    };
    decode("DATABASE_CATALOG_PASSWORD")?;
    let dsn = decode("CATALOG_DSN")?;
    if !regex::Regex::new(r"password=\S+")?.is_match(&dsn) {
        bail!("CATALOG_DSN carries no password= to rotate");
    }
    let mut rotated = Map::new();
    for values in secret["stringData"]
        .as_object()
        .into_iter()
        .chain(secret["data"].as_object())
    {
        for name in values.keys() {
            rotated.insert(name.clone(), json!(decode(name)?));
        }
    }
    rotated.insert("DATABASE_CATALOG_PASSWORD".into(), json!(password));
    rotated.insert(
        "CATALOG_DSN".into(),
        json!(
            regex::Regex::new(r"password=\S+")?.replace_all(&dsn, format!("password={password}"))
        ),
    );
    Ok(json!({
        "apiVersion": "v1", "kind": "Secret",
        "metadata": {"name": secret["metadata"]["name"], "namespace": secret["metadata"]["namespace"]},
        "type": secret.get("type").cloned().unwrap_or_else(|| json!("Opaque")),
        "stringData": Value::Object(rotated),
    }))
}

pub fn rotate_secret(args: Vec<String>) -> Result<()> {
    util::reject_extra(&args, 0)?;
    let password = std::env::var("DUCKFLEET_NEW_PASSWORD")?;
    let mut input = String::new();
    std::io::Read::read_to_string(&mut std::io::stdin(), &mut input)?;
    let secret: Value = serde_json::from_str(&input)?;
    println!("{}", serde_json::to_string(&rotate(&secret, &password)?)?);
    Ok(())
}

pub fn wait_checks(args: Vec<String>) -> Result<()> {
    util::reject_extra(&args, 0)?;
    let revision = std::env::var("GITHUB_SHA")?;
    let repository = std::env::var("GITHUB_REPOSITORY")?;
    let expected = ["local-harness.yml", "infra.yml"];
    for _ in 0..280 {
        let mut passed = BTreeSet::new();
        for workflow in expected {
            let output = util::output_text([
                "gh",
                "run",
                "list",
                "--repo",
                &repository,
                "--workflow",
                workflow,
                "--commit",
                &revision,
                "--event",
                "push",
                "--branch",
                "main",
                "--limit",
                "1",
                "--json",
                "status,conclusion,databaseId",
            ])?;
            let runs: Value = serde_json::from_str(&output)?;
            if let Some(run) = runs.get(0).filter(|run| run["status"] == "completed") {
                if run["conclusion"] != "success" {
                    bail!("{workflow} failed: {run}");
                }
                passed.insert(workflow);
            }
        }
        if passed.len() == expected.len() {
            let head = util::output_text([
                "gh",
                "api",
                &format!("repos/{repository}/git/ref/heads/main"),
                "--jq",
                ".object.sha",
            ])?;
            if head != revision {
                bail!("Refusing deployment of a superseded main revision");
            }
            println!("Both main validation workflows passed on this revision");
            return Ok(());
        }
        println!("Waiting for main checks: {passed:?}");
        std::thread::sleep(std::time::Duration::from_secs(15));
    }
    bail!("Timed out waiting for main validation")
}

pub fn verify_aws(args: Vec<String>) -> Result<()> {
    util::reject_extra(&args, 0)?;
    let root = util::root().join("artifacts/aws");
    let log = std::fs::read_to_string(root.join("harness.log"))?;
    let records: Vec<_> = log
        .lines()
        .filter_map(|line| line.strip_prefix("DUCKFLEET_AWS_EVIDENCE="))
        .collect();
    if records.len() != 1 {
        bail!("Expected exactly one completed AWS harness evidence record");
    }
    let mut evidence: Value = serde_json::from_str(records[0])?;
    let pods = util::read_json(root.join("pods.json"))?;
    let after = util::read_json(root.join("pods-after.json"))?;
    let workers: Vec<_> = util::array(pods.get("items"))
        .iter()
        .filter(|pod| {
            pod["metadata"]["labels"]["app"] == "duckfleet-worker"
                && pod["metadata"]["labels"].get("duckfleet-compute").is_none()
                && pod["metadata"].get("deletionTimestamp").is_none()
        })
        .collect();
    if workers.len() != 1 {
        bail!("Expected exactly one persistent worker pod");
    }
    for pod in workers {
        let size = util::text(pod["metadata"]["labels"].get("size"));
        if size != "small" {
            bail!("unexpected persistent worker size: {size}");
        }
        let current = util::array(after.get("items"))
            .iter()
            .find(|current| current["metadata"]["uid"] == pod["metadata"]["uid"])
            .context("worker disappeared during the test")?;
        if current["metadata"].get("deletionTimestamp").is_some()
            || current["status"]["containerStatuses"] != pod["status"]["containerStatuses"]
        {
            bail!("Worker restarted or changed during the test");
        }
        if pod["metadata"]["namespace"] != "duckfleet-dev"
            || pod["status"]["phase"] != "Running"
            || pod["spec"]["serviceAccountName"] != "duckfleet-worker"
        {
            bail!("worker placement or identity changed");
        }
        let container = util::array(pod["spec"].get("containers"))
            .iter()
            .find(|container| container["name"] == "worker")
            .unwrap();
        crate::worker_resources::assert_worker_resources(
            &container["resources"],
            1,
            "live small worker",
            Some("1Gi"),
            Some("2Gi"),
        )?;
        let observed_index = util::array(evidence.get("workers"))
            .iter()
            .position(|worker| worker["worker_id"] == size)
            .context("AWS evidence is missing the small worker")?;
        let observed = &evidence["workers"][observed_index];
        if observed["cgroup_memory_max"] != 1024_u64.pow(3)
            || observed["cgroup_cpu_quota"] != observed["cgroup_cpu_period"]
            || observed["duckdb_threads"] != 1
        {
            bail!("worker-reported resources disagree with the pod");
        }
        evidence["workers"][observed_index]["pod_uid"] = pod["metadata"]["uid"].clone();
        evidence["workers"][observed_index]["node"] = pod["spec"]["nodeName"].clone();
    }
    let files = util::array(evidence.get("s3_files"));
    if files.is_empty()
        || !files
            .iter()
            .filter_map(Value::as_str)
            .all(|path| path.starts_with("s3://duckfleet-dev-797781631727-us-east-2/lake/"))
    {
        bail!("AWS evidence has invalid S3 files");
    }
    // The harness probe (#208): both out-of-lake reads refused by the engine
    // lockdown, and no request, signed or not, left the worker.
    let scope = &evidence["secret_scope"];
    if scope["out_of_lake_reads_refused"].as_u64() != Some(2)
        || scope["requests_leaving_worker"].as_u64() != Some(0)
        || scope["credentials_sent_outside_lake"] != false
    {
        bail!("secret-scope evidence is incomplete");
    }
    util::write_json(root.join("evidence.json"), &evidence)?;
    println!("PASS: AWS placement, worker cgroups, S3 Parquet, and secret-scope isolation agree");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plans_and_image_indexes_reject_unsafe_shapes() {
        let mut plan = json!({"complete":true,"errored":false,"resource_changes":[{"address":"aws_s3_bucket.lake","mode":"managed","change":{"actions":["create"]}}]});
        validate_plan(&plan).unwrap();
        plan["resource_changes"][0]["change"]["actions"] = json!(["delete"]);
        assert!(validate_plan(&plan).is_err());
        let index = json!({"manifests":[
            {"platform":{"os":"linux","architecture":"amd64"}},
            {"platform":{"os":"linux","architecture":"arm64"}},
            {"platform":{"os":"unknown","architecture":"unknown"},"annotations":{"vnd.docker.reference.type":"attestation-manifest"}}
        ]});
        assert!(
            image_reference("worker", &format!("sha256:{}", "a".repeat(64)), &index)
                .unwrap()
                .ends_with(&format!("worker@sha256:{}", "a".repeat(64)))
        );
    }

    #[test]
    fn exposure_is_namespaced_and_round_trips_into_values() {
        let documents = exposure_documents(
            "duckfleet-demo",
            "duckfleet-demo.example.com",
            "arn:aws:acm:us-east-2:123456789012:certificate/fff0813e-733e-4b06-a9a7-638623fb17bd",
            &["10.0.0.0/16".into()],
        )
        .unwrap();
        let kinds: Vec<&str> = documents
            .iter()
            .map(|d| util::text(d.get("kind")))
            .collect();
        assert_eq!(kinds, ["IngressClassParams", "IngressClass", "ConfigMap"]);
        assert_eq!(
            documents[0]["spec"]["namespaceSelector"]["matchLabels"]["kubernetes.io/metadata.name"],
            "duckfleet-demo"
        );
        let values = exposure_values(&documents[2]).unwrap();
        assert_eq!(values["host"], "duckfleet-demo.example.com");
        assert_eq!(values["ingressClassName"], "duckfleet-duckfleet-demo");
        assert_eq!(values["loadBalancer"]["sourceCidrs"][0], "10.0.0.0/16");
    }

    #[test]
    fn exposure_refuses_invented_or_public_facts() {
        let arn = "arn:aws:acm:us-east-2:123456789012:certificate/abc";
        let cidrs = ["10.0.0.0/16".to_string()];
        assert!(exposure_documents("ns", "", arn, &cidrs).is_err());
        assert!(exposure_documents("ns", "h.example.com", "not-an-arn", &cidrs).is_err());
        assert!(exposure_documents("ns", "h.example.com", arn, &[]).is_err());
        assert!(exposure_documents("ns", "h.example.com", arn, &["0.0.0.0/0".into()]).is_err());
    }

    #[test]
    fn overlay_is_sorted_and_boundary_is_namespaced() {
        let value = overlay(&["10.42.9.4/32".into(), "10.42.3.17/32".into()], &[443]);
        assert_eq!(
            value["isolatedCompute"]["apiServerCidrs"][0],
            "10.42.3.17/32"
        );
        assert_eq!(value["isolatedCompute"]["provisioningSeconds"], 240);
        let documents = boundary_documents("duckfleet-dev", "").unwrap();
        assert!(
            documents
                .iter()
                .any(|document| document["kind"] == "RoleBinding")
        );
    }
}
