//! On-demand compute: one pod per session, sized to the request, removed afterwards.
//!
//! The pod is the ordinary worker image. It is not a new kind of process — it reads
//! its own cgroup and refuses to start if it did not get the resources it was
//! declared with, exactly as a persistent worker does, so a pod that was rewritten
//! between the chart and the kubelet fails loudly instead of serving the wrong size
//! and billing as the size that was asked for.
//!
//! Removal does not rest on this code behaving well. A leaked pod is a leaked bill,
//! so there are four independent layers, and only the first is in the happy path:
//!
//! 1. the gateway deletes the pod when the session ends, however it ends;
//! 2. the pod is owned by the gateway's own pod, so losing the gateway has the
//!    garbage collector cascade the deletion;
//! 3. `activeDeadlineSeconds` has the kubelet stop it even if both of those fail;
//! 4. a startup sweep settles and removes anything a previous gateway left behind.
use crate::{ApiError, compute::Resources, failure, kube::Kube};
use serde_json::{Value, json};
use std::time::{Duration, Instant};

/// Marks every pod this module creates, so a sweep can find them without inferring
/// anything from names. Matches the label the chart puts on the template.
const LABEL: &str = "duckfleet-compute=isolated";
/// Session pods are named from this, and a pod's worker identity is its own name.
/// The prefix is what lets a sweep tell an isolated reservation from a persistent
/// worker's, whose funds are recovered by the worker itself when it restarts.
pub(crate) const PREFIX: &str = "duckfleet-session-";

/// The three template values the gateway replaces per session. Named here so a
/// template that stops carrying one fails at startup rather than running a pod that
/// claims another session's identity or a size nobody asked for.
const PATCHED: [&str; 3] = [
    "DUCKFLEET_WORKER_ID",
    "DUCKFLEET_WORKER_CPU",
    "DUCKFLEET_WORKER_MEMORY",
];

pub(crate) struct Isolation {
    kube: Kube,
    template: Value,
    owner: Value,
    ready: Duration,
    /// How long a session pod may sit Unschedulable before that is taken as terminal.
    ///
    /// A property of the cluster's provisioning model, so it is configured rather than
    /// guessed. On a fixed-node cluster (kind) a pod is either placeable at once or
    /// never, so Unschedulable is a verdict and this is left at zero — the gateway
    /// refuses immediately, naming the shortfall. On one that adds nodes on demand (EKS
    /// Auto Mode / Karpenter) the same condition is routine for the seconds between a
    /// pod's creation and a new node joining, so the deploy sets this generously: the
    /// gateway waits a new node out instead of deleting the pod before Karpenter can
    /// provision for it, which is why no isolated session ever placed there
    /// (duck-fleet#90).
    provisioning: Duration,
}

/// A running session pod.
pub(crate) struct Pod {
    pub name: String,
    pub endpoint: String,
}

impl Isolation {
    /// `Ok(None)` when this deployment is not wired for isolated compute, which is
    /// what the chart renders unless it is explicitly enabled.
    ///
    /// Being wired for it is not the same as being permitted: the grant lives outside
    /// the release, so `permitted` below is what settles whether this can actually
    /// create pods. See #39.
    pub fn configure() -> anyhow::Result<Option<Self>> {
        let Ok(path) = std::env::var("DUCKFLEET_ISOLATED_POD") else {
            return Ok(None);
        };
        let template: Value = serde_json::from_slice(
            &std::fs::read(&path)
                .map_err(|e| anyhow::anyhow!("reading the isolated pod template {path}: {e}"))?,
        )
        .map_err(|e| anyhow::anyhow!("the isolated pod template {path} is not valid JSON: {e}"))?;
        // Validated once here rather than per request: a template this code cannot
        // patch is a packaging mistake, and discovering it on the first session would
        // report it to a caller as though they had asked for something impossible.
        patchable(&template)?;
        let owner = match (
            std::env::var("DUCKFLEET_POD_NAME"),
            std::env::var("DUCKFLEET_POD_UID"),
        ) {
            (Ok(name), Ok(uid)) if !name.is_empty() && !uid.is_empty() => owner(&name, &uid),
            // Refused rather than run without. A gateway with no owner reference
            // leaves its pods to the deadline when it dies, which is half an hour of
            // compute that nobody is watching and every caller is paying for.
            _ => anyhow::bail!(
                "isolated compute needs DUCKFLEET_POD_NAME and DUCKFLEET_POD_UID so session \
                 pods are owned by this gateway and removed along with it"
            ),
        };
        Ok(Some(Self {
            kube: Kube::from_service_account()?,
            template,
            owner,
            ready: env_seconds("DUCKFLEET_ISOLATED_READY_SECONDS", 120),
            provisioning: env_seconds("DUCKFLEET_ISOLATED_PROVISIONING_SECONDS", 0),
        }))
    }

    /// Whether the cluster permits what this deployment is configured to do.
    ///
    /// The Role that allows pod creation is applied by an operator with cluster
    /// admin, separately from the release, because the deploy identity cannot create
    /// Roles and `scripts/aws/check-access.sh` fails the deploy if it ever can. So a
    /// chart value records an intention and this is the only statement of fact.
    ///
    /// Checked at startup rather than per session: a caller asking for isolation
    /// should be refused up front with a reason, not have a statement fail once a pod
    /// could not be created.
    pub async fn permitted(&self) -> anyhow::Result<()> {
        self.kube.assert_pod_grant().await
    }

    /// Creates a pod and waits for it to be ready, or removes it and says why.
    ///
    /// No path here falls back to a persistent worker. A caller asking for isolation
    /// is asking for a guarantee, and quietly serving it from shared compute would
    /// void that guarantee while looking like success.
    pub async fn start(&self, wanted: Resources) -> Result<Pod, ApiError> {
        let name = format!("{PREFIX}{}", uuid::Uuid::new_v4());
        let spec = specify(&self.template, &self.owner, &name, wanted).map_err(|error| {
            tracing::error!(%error, "could not build a session pod from the template");
            ApiError::internal()
        })?;
        self.kube.create_pod(&spec).await.map_err(|error| {
            tracing::error!(
                error = format!("{error:#}"),
                "creating a session pod failed"
            );
            if crate::kube::declined(&error) {
                // The API server refused before the pod existed — the namespace quota
                // or an admission webhook — so this provably left nothing behind and a
                // smaller request may succeed. Same answer as an unschedulable pod,
                // reached without creating one, and carrying the refusal's own words so
                // the ceiling that was hit is named rather than guessed.
                ApiError::new(
                    failure::NOT_PLACEABLE,
                    format!(
                        "this session could not be placed: {error:#}. No statement ran and \
                         the request was not downgraded to a shared worker"
                    ),
                )
            } else {
                // Deliberately not NOT_PLACEABLE: a call that timed out may still have
                // created a pod, so this cannot promise the request left nothing behind.
                ApiError::new(
                    failure::UNAVAILABLE,
                    format!("could not create a pod for this session; no statement ran: {error:#}"),
                )
            }
        })?;
        match self.await_ready(&name).await {
            Ok(endpoint) => Ok(Pod { name, endpoint }),
            Err(error) => {
                // The pod exists and is not usable. It does not outlive this call.
                self.stop(&name).await;
                Err(error)
            }
        }
    }

    async fn await_ready(&self, name: &str) -> Result<String, ApiError> {
        let deadline = Instant::now() + self.ready;
        // The grace never outlasts the ready budget: a shorter budget bounds everything.
        let provisioning = Instant::now() + self.provisioning.min(self.ready);
        loop {
            let pod = self
                .kube
                .pod(name)
                .await
                .map_err(|error| {
                    tracing::warn!(error = format!("{error:#}"), "reading a session pod failed");
                    ApiError::internal()
                })?
                .ok_or_else(|| {
                    ApiError::new(
                        failure::NOT_PLACEABLE,
                        "the session pod disappeared before it was ready; no statement ran",
                    )
                })?;
            // Checked before readiness: a pod that cannot be placed will never become
            // ready, and waiting out the full budget would turn a definite answer into
            // a timeout the caller cannot distinguish from a slow cluster. But an
            // Unschedulable condition is only terminal once a node has not been
            // nominated for the pod and the autoscaler has had time to act — until
            // then a new node may still be on its way (see PROVISIONING_GRACE).
            if let Some(reason) = terminally_unplaceable(&pod, Instant::now() >= provisioning) {
                return Err(ApiError::new(
                    failure::NOT_PLACEABLE,
                    format!(
                        "no node can run a pod with cpu={}, memory={}: {reason}. No statement \
                         ran and the request was not downgraded to a shared worker",
                        quantity(&pod, "cpu"),
                        quantity(&pod, "memory"),
                    ),
                ));
            }
            let phase = pod["status"]["phase"].as_str().unwrap_or("");
            if phase == "Failed" || phase == "Succeeded" {
                return Err(ApiError::new(
                    failure::NOT_PLACEABLE,
                    format!(
                        "the session pod stopped before it was ready ({phase}); no statement ran"
                    ),
                ));
            }
            if ready(&pod)
                && let Some(address) = pod["status"]["podIP"].as_str()
                && !address.is_empty()
            {
                return Ok(format!("http://{address}:8080"));
            }
            if Instant::now() >= deadline {
                return Err(ApiError::new(
                    failure::NOT_PLACEABLE,
                    format!(
                        "the session pod was not ready within {}s; no statement ran",
                        self.ready.as_secs()
                    ),
                ));
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }

    /// Best effort by design: layers 2 to 4 exist precisely because this can fail, so
    /// a failure is logged rather than returned to a caller whose statement already
    /// ran. The ledger is settled by the caller either way.
    pub async fn stop(&self, name: &str) {
        if let Err(error) = self.kube.delete_pod(name).await {
            tracing::error!(
                error = format!("{error:#}"),
                pod = name,
                "could not delete a session pod"
            );
        }
    }

    /// Removes session pods left behind by a previous gateway.
    ///
    /// `holding` comes from the ledger rather than from the cluster, because the two
    /// disagree in the direction that costs money: the owner reference may already
    /// have had a pod collected, leaving funds held by a worker identity that will
    /// never exist again. A persistent worker recovers its own on restart; an
    /// isolated pod has no restart to recover on. The caller settles `holding` once
    /// this returns, so a pod is always deleted before its funds are released.
    pub async fn sweep(&self, holding: &[String]) {
        for name in holding {
            self.stop(name).await;
        }
        // Anything still labelled at startup is orphaned by definition: this gateway
        // holds no route to it, and the chart runs a single gateway with `Recreate`,
        // so there is no sibling that could still be using one.
        match self.kube.pods_labelled(LABEL).await {
            Ok(pods) => {
                for pod in &pods {
                    if let Some(name) = pod["metadata"]["name"].as_str()
                        && !holding.iter().any(|held| held == name)
                    {
                        tracing::warn!(
                            pod = name,
                            "removing a session pod left by a gateway that is gone"
                        );
                        self.stop(name).await;
                    }
                }
            }
            Err(error) => tracing::error!(
                error = format!("{error:#}"),
                "could not list session pods to sweep"
            ),
        }
    }
}

/// A duration from an environment variable, falling back to `default` seconds when it
/// is unset or unparseable. The two session-timing budgets are read the same way.
fn env_seconds(name: &str, default: u64) -> Duration {
    Duration::from_secs(
        std::env::var(name)
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(default),
    )
}

fn owner(name: &str, uid: &str) -> Value {
    json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "name": name,
        "uid": uid,
        // Owns the pod for garbage collection only. Claiming to be its controller
        // would have Kubernetes expect this process to reconcile it.
        "controller": false,
        "blockOwnerDeletion": false,
    })
}

/// Builds the pod for one session from the chart's template.
///
/// A free function so the tests exercise the same code the gateway runs rather than
/// a copy of it.
fn specify(
    template: &Value,
    owner: &Value,
    name: &str,
    wanted: Resources,
) -> anyhow::Result<Value> {
    patchable(template)?;
    let mut pod = template.clone();
    pod["metadata"]["name"] = json!(name);
    pod["metadata"]["ownerReferences"] = json!([owner]);
    let declared = [
        (PATCHED[0], name.to_owned()),
        (PATCHED[1], wanted.cpu.to_string()),
        (PATCHED[2], wanted.memory()),
    ];
    let environment = pod["spec"]["containers"][0]["env"]
        .as_array_mut()
        .ok_or_else(|| anyhow::anyhow!("no container environment"))?;
    for entry in environment.iter_mut() {
        for (variable, value) in &declared {
            if entry["name"] == json!(variable) {
                entry["value"] = json!(value);
            }
        }
    }
    // Only cpu and memory are replaced; anything else the release asked for, such as
    // ephemeral storage, is left exactly as it was rendered.
    let quantities = &mut pod["spec"]["containers"][0]["resources"];
    for side in ["requests", "limits"] {
        quantities[side]["cpu"] = json!(wanted.cpu.to_string());
        quantities[side]["memory"] = json!(wanted.memory());
    }
    Ok(pod)
}

/// Rejects a template the gateway could not safely fill in. The container is
/// addressed positionally because the chart renders exactly one; a template of a
/// different shape is a packaging mistake and is refused rather than guessed at.
fn patchable(template: &Value) -> anyhow::Result<()> {
    let container = &template["spec"]["containers"][0];
    let environment = container["env"].as_array().ok_or_else(|| {
        anyhow::anyhow!("the isolated pod template declares no container environment")
    })?;
    for name in PATCHED {
        anyhow::ensure!(
            environment.iter().any(|e| e["name"] == json!(name)),
            "the isolated pod template declares no {name}; the gateway could not give a \
             session pod its identity or tell it what size it was meant to be"
        );
    }
    anyhow::ensure!(
        container["resources"]["requests"].is_object()
            && container["resources"]["limits"].is_object(),
        "the isolated pod template declares no resource requests and limits to size"
    );
    Ok(())
}

/// True once the kubelet reports the readiness probe passing, which for this image
/// means the catalog is attached and the engine will answer.
fn ready(pod: &Value) -> bool {
    conditions(pod).any(|c| c["type"] == json!("Ready") && c["status"] == json!("True"))
}

/// The scheduler's own explanation, which names the shortfall. Reusing its message
/// rather than composing one is what makes an unplaceable size actionable.
fn unschedulable(pod: &Value) -> Option<String> {
    conditions(pod)
        .find(|c| {
            c["type"] == json!("PodScheduled")
                && c["status"] == json!("False")
                && c["reason"] == json!("Unschedulable")
        })
        .map(|c| {
            c["message"]
                .as_str()
                .unwrap_or("no reason given")
                .to_owned()
        })
}

/// A node the autoscaler has committed to bringing up for this pod. Its presence is
/// positive evidence that an Unschedulable condition is transient — provisioning is
/// underway — so the pod is waited on however long the ready budget allows, not given
/// up on at the grace boundary.
fn nominated(pod: &Value) -> bool {
    pod["status"]["nominatedNodeName"]
        .as_str()
        .is_some_and(|name| !name.is_empty())
}

/// Whether an Unschedulable pod should be given up on now, carrying the scheduler's
/// reason when so. Transient while a node is being provisioned — one already
/// nominated, or the provisioning grace not yet elapsed — and terminal only once
/// neither holds, which is the difference between a fixed-node cluster and one that
/// adds nodes on demand.
fn terminally_unplaceable(pod: &Value, past_provisioning_grace: bool) -> Option<String> {
    match unschedulable(pod) {
        Some(reason) if past_provisioning_grace && !nominated(pod) => Some(reason),
        _ => None,
    }
}

fn conditions(pod: &Value) -> impl Iterator<Item = &Value> {
    pod["status"]["conditions"]
        .as_array()
        .map(Vec::as_slice)
        .unwrap_or_default()
        .iter()
}

fn quantity(pod: &Value, which: &str) -> String {
    pod["spec"]["containers"][0]["resources"]["limits"][which]
        .as_str()
        .unwrap_or("?")
        .to_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    const GIB: u64 = 1024 * 1024 * 1024;

    fn template() -> Value {
        json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"labels": {"app": "duckfleet-worker", "duckfleet-compute": "isolated"}},
            "spec": {
                "restartPolicy": "Never",
                "activeDeadlineSeconds": 1800,
                "automountServiceAccountToken": false,
                "containers": [{
                    "name": "worker",
                    "resources": {
                        "requests": {"cpu": "1", "memory": "1Gi", "ephemeral-storage": "2Gi"},
                        "limits": {"cpu": "1", "memory": "1Gi", "ephemeral-storage": "3Gi"},
                    },
                    "env": [
                        {"name": "DUCKFLEET_WORKER_ID", "value": "replaced-per-session"},
                        {"name": "DUCKFLEET_WORKER_CPU", "value": "1"},
                        {"name": "DUCKFLEET_WORKER_MEMORY", "value": "1Gi"},
                        {"name": "CATALOG_DSN", "valueFrom": {"secretKeyRef": {"name": "s", "key": "CATALOG_DSN"}}},
                    ],
                }],
            },
        })
    }

    fn specified(wanted: Resources) -> Value {
        specify(
            &template(),
            &owner("server-abc", "11111111-2222-3333-4444-555555555555"),
            "duckfleet-session-abc",
            wanted,
        )
        .expect("the rendered template should be patchable")
    }

    fn environment(pod: &Value) -> std::collections::HashMap<String, String> {
        pod["spec"]["containers"][0]["env"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|e| {
                Some((
                    e["name"].as_str()?.to_owned(),
                    e["value"].as_str()?.to_owned(),
                ))
            })
            .collect()
    }

    /// The ticket's first requirement. Requests below limits would let the scheduler
    /// place a pod the node cannot sustain, which is the opposite of a dedicated
    /// worker, so both sides carry the size that was asked for.
    #[test]
    fn a_session_pod_requests_exactly_what_was_asked_for() {
        let pod = specified(Resources {
            cpu: 4,
            memory_bytes: 8 * GIB,
        });
        let quantities = &pod["spec"]["containers"][0]["resources"];
        for side in ["requests", "limits"] {
            assert_eq!(quantities[side]["cpu"], json!("4"), "{side}");
            assert_eq!(quantities[side]["memory"], json!("8Gi"), "{side}");
        }
    }

    /// Sizes the old two classes never covered, which is the point of resolving a
    /// request to resources rather than to a name.
    #[test]
    fn any_resolvable_size_can_be_asked_for() {
        for (cpu, memory_bytes, expected) in [
            (1, 512 * 1024 * 1024, "512Mi"),
            (3, 6 * GIB, "6Gi"),
            (16, 64 * GIB, "64Gi"),
        ] {
            let pod = specified(Resources { cpu, memory_bytes });
            let quantities = &pod["spec"]["containers"][0]["resources"];
            assert_eq!(quantities["limits"]["cpu"], json!(cpu.to_string()));
            assert_eq!(quantities["limits"]["memory"], json!(expected));
            assert_eq!(environment(&pod)["DUCKFLEET_WORKER_MEMORY"], expected);
        }
    }

    /// Whatever else the release asked for survives being sized.
    #[test]
    fn sizing_leaves_the_rest_of_the_release_alone() {
        let pod = specified(Resources::default());
        let quantities = &pod["spec"]["containers"][0]["resources"];
        assert_eq!(quantities["requests"]["ephemeral-storage"], json!("2Gi"));
        assert_eq!(quantities["limits"]["ephemeral-storage"], json!("3Gi"));
        assert_eq!(pod["spec"]["restartPolicy"], json!("Never"));
        assert_eq!(pod["spec"]["activeDeadlineSeconds"], json!(1800));
        // An isolated pod runs user SQL and must not inherit the gateway's ability to
        // create more pods.
        assert_eq!(pod["spec"]["automountServiceAccountToken"], json!(false));
        let secret = pod["spec"]["containers"][0]["env"]
            .as_array()
            .unwrap()
            .iter()
            .find(|e| e["name"] == json!("CATALOG_DSN"))
            .expect("the catalog secret should survive patching");
        assert_eq!(
            secret["valueFrom"]["secretKeyRef"]["key"],
            json!("CATALOG_DSN")
        );
    }

    /// The pod tells the worker the same size the kubelet was asked for, so the
    /// worker's cgroup check has something to disagree with. Identity is the pod
    /// name, because reservation recovery keys on it.
    #[test]
    fn the_pod_declares_its_own_size_and_identity() {
        let pod = specified(Resources {
            cpu: 2,
            memory_bytes: 2 * GIB,
        });
        let env = environment(&pod);
        assert_eq!(env["DUCKFLEET_WORKER_ID"], "duckfleet-session-abc");
        let limits = &pod["spec"]["containers"][0]["resources"]["limits"];
        assert_eq!(env["DUCKFLEET_WORKER_CPU"], limits["cpu"].as_str().unwrap());
        assert_eq!(
            env["DUCKFLEET_WORKER_MEMORY"],
            limits["memory"].as_str().unwrap()
        );
    }

    /// Identity must also be unique, or two sessions would share a ledger key.
    #[test]
    fn every_session_pod_has_its_own_identity() {
        let template = template();
        let reference = owner("server-abc", "uid");
        let names: std::collections::HashSet<String> = (0..16)
            .map(|_| {
                let name = format!("{PREFIX}{}", uuid::Uuid::new_v4());
                let pod = specify(&template, &reference, &name, Resources::default()).unwrap();
                environment(&pod)["DUCKFLEET_WORKER_ID"].clone()
            })
            .collect();
        assert_eq!(names.len(), 16);
    }

    /// A session pod must be removed along with the gateway that forgot it.
    #[test]
    fn a_session_pod_is_owned_by_the_gateway() {
        let pod = specified(Resources::default());
        let reference = &pod["metadata"]["ownerReferences"][0];
        assert_eq!(reference["kind"], json!("Pod"));
        assert_eq!(reference["name"], json!("server-abc"));
        assert_eq!(
            reference["uid"],
            json!("11111111-2222-3333-4444-555555555555")
        );
        // Garbage collection only: this gateway does not reconcile the pod.
        assert_eq!(reference["controller"], json!(false));
    }

    /// A template the gateway cannot patch is refused rather than filled in with
    /// guesses. Accepting sessions and failing on the first would report a packaging
    /// mistake as a caller's error, and a pod with an unpatched identity would bill
    /// against the literal string in the chart.
    #[test]
    fn a_template_missing_a_patched_declaration_is_refused() {
        assert!(patchable(&template()).is_ok());
        for missing in PATCHED {
            let mut broken = template();
            let kept: Vec<Value> = broken["spec"]["containers"][0]["env"]
                .as_array()
                .unwrap()
                .iter()
                .filter(|e| e["name"] != json!(missing))
                .cloned()
                .collect();
            broken["spec"]["containers"][0]["env"] = json!(kept);
            let error = format!("{:#}", patchable(&broken).unwrap_err());
            assert!(error.contains(missing), "{error}");
            assert!(
                specify(
                    &broken,
                    &owner("g", "u"),
                    "duckfleet-session-x",
                    Resources::default()
                )
                .is_err()
            );
        }
        let mut sizeless = template();
        sizeless["spec"]["containers"][0]["resources"] = json!({});
        assert!(patchable(&sizeless).is_err());
        let mut empty = template();
        empty["spec"]["containers"] = json!([]);
        assert!(patchable(&empty).is_err());
    }

    #[test]
    fn an_unplaceable_pod_is_reported_with_the_schedulers_reason() {
        let pod = json!({"status": {"conditions": [{
            "type": "PodScheduled", "status": "False", "reason": "Unschedulable",
            "message": "0/1 nodes are available: 1 Insufficient cpu",
        }]}});
        assert_eq!(
            unschedulable(&pod).unwrap(),
            "0/1 nodes are available: 1 Insufficient cpu"
        );
        assert!(!ready(&pod));
    }

    #[test]
    fn an_unschedulable_pod_is_waited_on_while_a_node_is_coming() {
        let pending = json!({"status": {"conditions": [{
            "type": "PodScheduled", "status": "False", "reason": "Unschedulable",
            "message": "0/4 nodes are available: no new claims to deallocate",
        }]}});
        // Within the provisioning grace, Unschedulable is not yet a verdict: a node
        // may still be on its way, and bailing here deletes the pod before the
        // autoscaler can provision for it (duck-fleet#90).
        assert!(terminally_unplaceable(&pending, false).is_none());
        // Once the grace has elapsed with no node nominated, it is terminal.
        assert_eq!(
            terminally_unplaceable(&pending, true).unwrap(),
            "0/4 nodes are available: no new claims to deallocate"
        );
        // A node nominated for the pod is positive evidence provisioning is underway,
        // so it is waited on even past the grace.
        let mut nominated_pod = pending.clone();
        nominated_pod["status"]["nominatedNodeName"] = json!("ip-10-0-1-2.ec2.internal");
        assert!(nominated(&nominated_pod));
        assert!(terminally_unplaceable(&nominated_pod, true).is_none());
        // A pod that is not Unschedulable is never given up on, grace or not.
        let scheduling = json!({"status": {"conditions": [
            {"type": "PodScheduled", "status": "False", "reason": "SchedulerError"},
        ]}});
        assert!(terminally_unplaceable(&scheduling, true).is_none());
        assert!(!nominated(&json!({})));
    }

    #[test]
    fn a_ready_pod_is_distinguished_from_a_merely_scheduled_one() {
        let scheduled = json!({"status": {"conditions": [
            {"type": "PodScheduled", "status": "True"},
            {"type": "Ready", "status": "False"},
        ]}});
        assert!(!ready(&scheduled));
        assert!(unschedulable(&scheduled).is_none());
        let serving = json!({"status": {"conditions": [
            {"type": "PodScheduled", "status": "True"},
            {"type": "Ready", "status": "True"},
        ]}});
        assert!(ready(&serving));
        // A pod with no status yet is neither ready nor refused.
        assert!(!ready(&json!({})));
        assert!(unschedulable(&json!({})).is_none());
    }

    /// Identity carries the prefix a sweep selects on, and stays inside both the 63
    /// characters Kubernetes allows in a name and the worker's own identity limit.
    #[test]
    fn a_session_name_is_selectable_and_legal() {
        let name = format!("{PREFIX}{}", uuid::Uuid::new_v4());
        assert!(name.starts_with(PREFIX));
        assert!(name.len() <= 63, "{} characters", name.len());
        assert!(
            name.chars()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
        );
    }
}
