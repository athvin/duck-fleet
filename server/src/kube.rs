//! Just enough of the Kubernetes API to run one pod, over the identity the kubelet
//! mounts.
//!
//! Deliberately not the `kube` crate. This needs five calls against a single
//! namespace, and pinning the cluster CA as the only trusted root is tighter than
//! the usual alternative: with the public root set compiled in, any publicly trusted
//! certificate could answer for the API server.
use anyhow::{Context, Result};
use reqwest::{Client, Method, StatusCode};
use serde_json::Value;
use std::{fs, path::PathBuf, time::Duration};

/// Where the kubelet projects the pod's service account.
const ACCOUNT: &str = "/var/run/secrets/kubernetes.io/serviceaccount";

/// The verbs the gateway performs on pods, checked together at startup. A partial
/// grant is worse than none: `create` without `delete` leaks a pod, and a bill, per
/// session.
const REQUIRED: [&str; 4] = ["create", "get", "list", "delete"];

#[derive(Clone)]
pub(crate) struct Kube {
    client: Client,
    /// Namespaced collection URL; every call this makes is a pod in one namespace.
    pods: String,
    /// Where the API server answers questions about this identity's own access.
    reviews: String,
    namespace: String,
    account: PathBuf,
}

impl Kube {
    /// Builds a client from the mounted service account, or explains what is missing.
    ///
    /// The directory is overridable so the unit tests can exercise the failure paths
    /// without a cluster; nothing in a release sets it.
    pub fn from_service_account() -> Result<Self> {
        let account = PathBuf::from(
            std::env::var("DUCKFLEET_SERVICE_ACCOUNT").unwrap_or_else(|_| ACCOUNT.into()),
        );
        let namespace = fs::read_to_string(account.join("namespace"))
            .with_context(|| format!("reading the namespace from {}", account.display()))?;
        let namespace = namespace.trim();
        anyhow::ensure!(
            !namespace.is_empty(),
            "the mounted service account names no namespace"
        );
        let authority = fs::read(account.join("ca.crt"))
            .with_context(|| format!("reading the cluster CA from {}", account.display()))?;
        let host = std::env::var("KUBERNETES_SERVICE_HOST")
            .context("KUBERNETES_SERVICE_HOST is unset; this is not running in a cluster")?;
        let port = std::env::var("KUBERNETES_SERVICE_PORT").unwrap_or_else(|_| "443".to_owned());
        // A bundle rather than one certificate, because the CA file carries both
        // during a cluster CA rotation.
        let roots = reqwest::Certificate::from_pem_bundle(&authority)
            .context("the mounted cluster CA is not valid PEM")?;
        let client = Client::builder()
            // Short: a hung API server must not consume the caller's statement
            // deadline. Readiness is polled, so one slow call is not fatal.
            .timeout(Duration::from_secs(15))
            .use_rustls_tls()
            // `_only` is the point: it trusts these and drops the built-in roots, so
            // the cluster CA is the only certificate that may answer for the API
            // server. With the public set left in place, any certificate from any
            // public authority could.
            .tls_certs_only(roots)
            .build()?;
        Ok(Self {
            client,
            pods: format!("https://{host}:{port}/api/v1/namespaces/{namespace}/pods"),
            reviews: format!(
                "https://{host}:{port}/apis/authorization.k8s.io/v1/selfsubjectaccessreviews"
            ),
            namespace: namespace.to_owned(),
            account,
        })
    }

    /// Fails unless this identity may do everything to pods that session compute
    /// needs, naming the first verb it may not.
    ///
    /// Asked of the API server rather than inferred from configuration, because the
    /// grant is applied outside the release by an operator holding cluster admin — so
    /// a chart value records what was intended and only the cluster knows what is
    /// true. Creating a `SelfSubjectAccessReview` needs no permission of its own: the
    /// default `system:basic-user` binding lets every authenticated identity ask about
    /// itself, so this cannot fail for the very reason it is checking.
    pub async fn assert_pod_grant(&self) -> Result<()> {
        for verb in REQUIRED {
            let review = serde_json::json!({
                "apiVersion": "authorization.k8s.io/v1",
                "kind": "SelfSubjectAccessReview",
                "spec": {"resourceAttributes": {
                    "namespace": self.namespace,
                    "group": "",
                    "resource": "pods",
                    "verb": verb,
                }},
            });
            let (status, body) = self
                .send(Method::POST, &self.reviews.clone(), Some(&review))
                .await?;
            let answered =
                Self::expect_success(status, body, "asking the API server about our own access")?;
            if !allowed(&answered) {
                anyhow::bail!(
                    "this gateway may not {verb} pods in {}: apply infra/kubernetes/boundary \
                     with an administrative profile to grant it (#39)",
                    self.namespace
                );
            }
        }
        Ok(())
    }

    /// Read per call rather than cached. A projected token is rotated in place by the
    /// kubelet, so one read at startup stops authenticating after about an hour —
    /// which would present as a gateway that creates pods until it quietly cannot.
    fn token(&self) -> Result<String> {
        Ok(fs::read_to_string(self.account.join("token"))
            .context("reading the service account token")?
            .trim()
            .to_owned())
    }

    async fn send(
        &self,
        method: Method,
        url: &str,
        body: Option<&Value>,
    ) -> Result<(StatusCode, Value)> {
        let mut request = self
            .client
            .request(method, url)
            .bearer_auth(self.token()?)
            .header(reqwest::header::ACCEPT, "application/json");
        if let Some(body) = body {
            request = request.json(body);
        }
        let response = request.send().await.context("calling the Kubernetes API")?;
        let status = response.status();
        // The API server reports its own errors as a JSON Status object, so the body
        // is worth keeping on failure: it carries the admission or quota reason.
        let parsed = response.json::<Value>().await.unwrap_or(Value::Null);
        Ok((status, parsed))
    }

    /// Fails with the API server's own message, which names the admission webhook or
    /// quota that refused. A generic error here would hide the only useful detail.
    fn expect_success(status: StatusCode, body: Value, what: &str) -> Result<Value> {
        if status.is_success() {
            return Ok(body);
        }
        let message = body
            .get("message")
            .and_then(Value::as_str)
            .unwrap_or("no message")
            .to_owned();
        // The status travels with the message rather than only being formatted into
        // it, so a caller can tell a refusal from a failure without parsing prose.
        Err(anyhow::Error::new(Refused { status, message }).context(format!("{what} failed")))
    }

    pub async fn create_pod(&self, pod: &Value) -> Result<Value> {
        let (status, body) = self
            .send(Method::POST, &self.pods.clone(), Some(pod))
            .await?;
        Self::expect_success(status, body, "creating a session pod")
    }

    /// `None` when the pod does not exist, which is a normal answer while waiting for
    /// one to appear and after deleting one.
    pub async fn pod(&self, name: &str) -> Result<Option<Value>> {
        let (status, body) = self
            .send(Method::GET, &format!("{}/{name}", self.pods), None)
            .await?;
        if status == StatusCode::NOT_FOUND {
            return Ok(None);
        }
        Self::expect_success(status, body, "reading a session pod").map(Some)
    }

    pub async fn pod_log(&self, name: &str) -> Result<String> {
        let response = self
            .client
            .get(format!("{}/{name}/log?tailLines=200", self.pods))
            .bearer_auth(self.token()?)
            .send()
            .await
            .context("reading a failed session pod log")?;
        let status = response.status();
        let text = response.text().await?;
        anyhow::ensure!(
            status.is_success(),
            "reading a failed session pod log: {status}"
        );
        Ok(text)
    }

    /// Deletes a pod, treating an already-absent pod as success: both callers want
    /// the pod gone, and racing a garbage collector is not a failure.
    pub async fn delete_pod(&self, name: &str) -> Result<()> {
        let (status, body) = self
            .send(Method::DELETE, &format!("{}/{name}", self.pods), None)
            .await?;
        if status == StatusCode::NOT_FOUND {
            return Ok(());
        }
        Self::expect_success(status, body, "deleting a session pod").map(|_| ())
    }

    /// Delete only the observed pod incarnation, never a replacement with the
    /// same name created while cleanup was waiting on the API server.
    pub async fn delete_pod_uid(&self, name: &str, uid: &str) -> Result<()> {
        let options = serde_json::json!({"apiVersion":"v1","kind":"DeleteOptions","preconditions":{"uid":uid}});
        let (status, body) = self
            .send(
                Method::DELETE,
                &format!("{}/{name}", self.pods),
                Some(&options),
            )
            .await?;
        if status == StatusCode::NOT_FOUND {
            return Ok(());
        }
        Self::expect_success(status, body, "deleting a job pod").map(|_| ())
    }

    /// A DELETE acknowledgement only requests termination. Release durable work
    /// only after a separate GET proves absence; even a failed DELETE can race GC.
    /// The deadline bounds response bodies and every poll, not just connect time.
    pub async fn delete_pod_confirmed(&self, name: &str, wait: Duration) -> Result<bool> {
        tokio::time::timeout(wait, async {
            if let Err(error) = self.delete_pod(name).await {
                tracing::warn!(
                    pod = name,
                    error = format!("{error:#}"),
                    "pod deletion was not acknowledged; checking for independent collection"
                );
            }
            loop {
                if self.pod(name).await?.is_none() {
                    return Ok(true);
                }
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
        })
        .await
        .unwrap_or(Ok(false))
    }

    pub async fn pods_labelled(&self, selector: &str) -> Result<Vec<Value>> {
        let url = format!("{}?labelSelector={selector}", self.pods);
        let (status, body) = self.send(Method::GET, &url, None).await?;
        let listed = Self::expect_success(status, body, "listing session pods")?;
        Ok(listed
            .get("items")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default())
    }
}

/// An answer from the API server that was not a success, carrying the status so a
/// caller can act on what kind of answer it was.
#[derive(Debug)]
pub(crate) struct Refused {
    pub status: StatusCode,
    pub message: String,
}

impl std::fmt::Display for Refused {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.status, self.message)
    }
}

impl std::error::Error for Refused {}

/// Whether the API server declined before the pod existed, rather than failing in a
/// way that might still have created one.
///
/// This is the difference between "nothing was created, retry something smaller" and
/// "this may have been received" — a namespace quota or an admission webhook answers
/// `403` having changed nothing, which a timeout or a `500` cannot promise.
pub(crate) fn declined(error: &anyhow::Error) -> bool {
    error
        .downcast_ref::<Refused>()
        .is_some_and(|refused| refused.status == StatusCode::FORBIDDEN)
}

/// The API server's own words, when an error is a quota refusal — a `403` whose
/// message names the quota. `None` for every other failure, including admission
/// webhooks, whose refusals are verdicts rather than occupancy.
pub(crate) fn quota_message(error: &anyhow::Error) -> Option<&str> {
    error
        .downcast_ref::<Refused>()
        .filter(|refused| {
            refused.status == StatusCode::FORBIDDEN && refused.message.contains("exceeded quota")
        })
        .map(|refused| refused.message.as_str())
}

/// Whether a quota refusal is hopeless: some requested quantity alone exceeds its
/// limit, so the request cannot fit even an empty quota and waiting is pointless.
///
/// The message has a stable shape — `exceeded quota: <name>, requested: cpu=4,
/// used: cpu=14, limited: cpu=16` — and anything this cannot parse reads as
/// transient, because the wait is bounded and giving up on a request that would
/// fit is the costlier mistake.
pub(crate) fn quota_never_fits(message: &str) -> bool {
    let (Some(requested), Some(limited)) = (
        quota_section(message, "requested: "),
        quota_section(message, "limited: "),
    ) else {
        return false;
    };
    requested.iter().any(|(resource, asked)| {
        limited
            .iter()
            .find(|(limit_resource, _)| limit_resource == resource)
            .is_some_and(|(_, limit)| asked > limit)
    })
}

/// One `name: k=v,k=v` section of a quota message, with quantities normalized so
/// `2` and `2000m` compare equal. `None` when the section is absent or any entry
/// does not parse, which callers treat as "not provably hopeless".
fn quota_section(message: &str, marker: &str) -> Option<Vec<(String, u128)>> {
    let after = &message[message.find(marker)? + marker.len()..];
    let section = after.split(' ').next()?.trim_end_matches([',', '.']);
    section
        .split(',')
        .map(|entry| {
            let (resource, amount) = entry.split_once('=')?;
            Some((resource.to_owned(), parse_quantity(amount)?))
        })
        .collect()
}

/// A Kubernetes quantity in thousandths, covering the forms a quota message uses:
/// plain integers, `m` millicores, and binary or decimal byte suffixes. Fractional
/// or unrecognized forms return `None` rather than a guess.
fn parse_quantity(amount: &str) -> Option<u128> {
    const SUFFIXES: [(&str, u128); 9] = [
        ("Ki", 1024),
        ("Mi", 1024 * 1024),
        ("Gi", 1024 * 1024 * 1024),
        ("Ti", 1024 * 1024 * 1024 * 1024),
        ("k", 1_000),
        ("M", 1_000_000),
        ("G", 1_000_000_000),
        ("T", 1_000_000_000_000),
        ("m", 1), // millicores: already thousandths
    ];
    for (suffix, scale) in SUFFIXES {
        if let Some(number) = amount.strip_suffix(suffix) {
            let scale = if suffix == "m" { scale } else { scale * 1000 };
            return number.parse::<u128>().ok()?.checked_mul(scale);
        }
    }
    amount.parse::<u128>().ok()?.checked_mul(1000)
}

/// Whether a review came back allowed.
///
/// An absent or unparseable field reads as "no". The question is whether permission
/// was granted, and an answer this cannot read is not a grant — defaulting the other
/// way would have a gateway advertise isolated compute because it could not
/// understand the denial.
fn allowed(review: &Value) -> bool {
    review
        .pointer("/status/allowed")
        .and_then(Value::as_bool)
        .unwrap_or(false)
}

#[cfg(test)]
pub(crate) mod test_support {
    use super::*;

    pub(crate) struct FakeKube {
        pub kube: Kube,
        task: tokio::task::JoinHandle<()>,
        account: PathBuf,
    }

    impl FakeKube {
        pub fn at(endpoint: String, task: tokio::task::JoinHandle<()>) -> Self {
            let account =
                std::env::temp_dir().join(format!("duckfleet-kube-test-{}", uuid::Uuid::new_v4()));
            fs::create_dir(&account).unwrap();
            fs::write(account.join("token"), "fixture-token").unwrap();
            let _ = rustls::crypto::ring::default_provider().install_default();
            let kube = Kube {
                client: Client::builder()
                    .no_proxy()
                    .timeout(Duration::from_secs(3))
                    .build()
                    .unwrap(),
                pods: format!("{endpoint}/api/v1/namespaces/test/pods"),
                reviews: format!("{endpoint}/reviews"),
                namespace: "test".into(),
                account: account.clone(),
            };
            Self {
                kube,
                task,
                account,
            }
        }

        pub async fn serve(router: axum::Router) -> Self {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let endpoint = format!("http://{}", listener.local_addr().unwrap());
            let task = tokio::spawn(async move {
                axum::serve(listener, router).await.unwrap();
            });
            Self::at(endpoint, task)
        }
    }

    impl Drop for FakeKube {
        fn drop(&mut self) {
            self.task.abort();
            let _ = fs::remove_dir_all(&self.account);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn deletion_acknowledgement_and_terminating_status_never_prove_death() {
        for deleted in [
            StatusCode::OK,
            StatusCode::NOT_FOUND,
            StatusCode::INTERNAL_SERVER_ERROR,
        ] {
            let router = axum::Router::new().route(
                "/api/v1/namespaces/test/pods/session",
                axum::routing::delete(move || async move { deleted }).get(|| async {
                    axum::Json(
                        serde_json::json!({"metadata":{"deletionTimestamp":"2026-01-01T00:00:00Z"},
                        "status":{"phase":"Running"}}),
                    )
                }),
            );
            let api = test_support::FakeKube::serve(router).await;
            assert!(
                !api.kube
                    .delete_pod_confirmed("session", Duration::from_millis(80))
                    .await
                    .unwrap()
            );
        }
    }

    #[tokio::test]
    async fn only_a_read_proving_absence_can_release_work_even_when_delete_failed() {
        for deleted in [StatusCode::OK, StatusCode::NOT_FOUND, StatusCode::FORBIDDEN] {
            let router = axum::Router::new().route(
                "/api/v1/namespaces/test/pods/session",
                axum::routing::delete(move || async move { deleted })
                    .get(|| async { StatusCode::NOT_FOUND }),
            );
            let api = test_support::FakeKube::serve(router).await;
            assert!(
                api.kube
                    .delete_pod_confirmed("session", Duration::from_secs(1))
                    .await
                    .unwrap()
            );
        }
        let router = axum::Router::new().route(
            "/api/v1/namespaces/test/pods/session",
            axum::routing::delete(|| async { StatusCode::OK })
                .get(|| async { StatusCode::FORBIDDEN }),
        );
        let api = test_support::FakeKube::serve(router).await;
        assert!(
            api.kube
                .delete_pod_confirmed("session", Duration::from_secs(1))
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn confirmation_waits_for_collection_and_bounds_a_stalled_response_body() {
        use std::sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        };
        let reads = Arc::new(AtomicUsize::new(0));
        let observed = reads.clone();
        let router = axum::Router::new().route(
            "/api/v1/namespaces/test/pods/session",
            axum::routing::delete(|| async { StatusCode::OK }).get(move || {
                let reads = reads.clone();
                async move {
                    if reads.fetch_add(1, Ordering::SeqCst) == 0 {
                        StatusCode::OK
                    } else {
                        StatusCode::NOT_FOUND
                    }
                }
            }),
        );
        let api = test_support::FakeKube::serve(router).await;
        assert!(
            api.kube
                .delete_pod_confirmed("session", Duration::from_secs(2))
                .await
                .unwrap()
        );
        assert_eq!(observed.load(Ordering::SeqCst), 2);

        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let task = tokio::spawn(async move {
            for index in 0..2 {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut request = Vec::new();
                while !request.ends_with(b"\r\n\r\n") {
                    let mut byte = [0];
                    if socket.read_exact(&mut byte).await.is_err() {
                        return;
                    }
                    request.push(byte[0]);
                }
                if index == 0 {
                    socket
                        .write_all(
                            b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}",
                        )
                        .await
                        .unwrap();
                } else {
                    socket
                        .write_all(
                            b"HTTP/1.1 200 OK\r\nContent-Length: 100\r\nConnection: close\r\n\r\n",
                        )
                        .await
                        .unwrap();
                    std::future::pending::<()>().await;
                }
            }
        });
        let stalled = test_support::FakeKube::at(endpoint, task);
        let started = tokio::time::Instant::now();
        assert!(
            !stalled
                .kube
                .delete_pod_confirmed("session", Duration::from_millis(100))
                .await
                .unwrap()
        );
        assert!(started.elapsed() < Duration::from_secs(1));
    }

    /// A deployment mistake rather than a runtime condition, so it has to name what
    /// is missing: a gateway that started anyway would accept isolated sessions it
    /// could never place, and report its own misconfiguration as the caller's.
    #[test]
    fn a_missing_service_account_is_refused_by_name() {
        let empty = std::env::temp_dir().join(format!("duckfleet-kube-{}", uuid::Uuid::new_v4()));
        fs::create_dir_all(&empty).expect("should create a temporary directory");
        // SAFETY: set and removed around one call, which is the only reader.
        unsafe { std::env::set_var("DUCKFLEET_SERVICE_ACCOUNT", &empty) };
        let refused = Kube::from_service_account();
        unsafe { std::env::remove_var("DUCKFLEET_SERVICE_ACCOUNT") };
        let error = match refused {
            Ok(_) => panic!("a service account directory with no namespace should be refused"),
            Err(error) => format!("{error:#}"),
        };
        assert!(error.contains("namespace"), "{error}");
        assert!(error.contains(&empty.display().to_string()), "{error}");
    }

    #[test]
    fn an_api_error_keeps_the_servers_own_message() {
        let error = Kube::expect_success(
            StatusCode::FORBIDDEN,
            serde_json::json!({"message": "pods is forbidden"}),
            "creating a session pod",
        )
        .unwrap_err();
        let error = format!("{error:#}");
        assert!(error.contains("pods is forbidden"), "{error}");
        assert!(error.contains("creating a session pod"), "{error}");
    }

    /// A quota or admission refusal changed nothing, so the caller may retry something
    /// smaller; anything else may have been received and acted on. Getting this wrong
    /// either tells a caller a request is hopeless when it is not, or promises nothing
    /// was created when a pod may be running and billing.
    #[test]
    fn a_forbidden_answer_is_a_refusal_and_other_failures_are_not() {
        let quota = Kube::expect_success(
            StatusCode::FORBIDDEN,
            serde_json::json!({"message": "exceeded quota: duckfleet, requested: cpu=64"}),
            "creating a session pod",
        )
        .unwrap_err();
        assert!(declined(&quota));
        // The server's own words survive being wrapped, because the ceiling that was
        // hit is the only actionable part of the answer.
        let error = format!("{quota:#}");
        assert!(error.contains("exceeded quota"), "{error}");
        assert!(error.contains("creating a session pod"), "{error}");

        for (status, body) in [
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                serde_json::json!({"message": "etcd"}),
            ),
            (
                StatusCode::CONFLICT,
                serde_json::json!({"message": "already exists"}),
            ),
        ] {
            let other = Kube::expect_success(status, body, "creating a session pod").unwrap_err();
            assert!(!declined(&other), "{status} must not read as a refusal");
        }
        // A transport failure carries no status at all, and must not be mistaken for
        // one: a call that timed out may still have created the pod.
        assert!(!declined(&anyhow::anyhow!("connection reset")));
    }

    /// The API server answers a denial with `allowed: false` and a reason, not with an
    /// error, so a review that parses is not the same as a grant.
    #[test]
    fn only_an_explicit_allowance_counts_as_a_grant() {
        assert!(allowed(&serde_json::json!({"status": {"allowed": true}})));
        assert!(!allowed(&serde_json::json!(
            {"status": {"allowed": false, "reason": "no RBAC policy matched"}}
        )));
    }

    /// A gateway that could not read the answer must not conclude it has the grant:
    /// it would advertise isolated compute and then fail every session at pod-create
    /// time, which is the failure this check exists to prevent.
    #[test]
    fn an_unreadable_answer_is_not_a_grant() {
        for unreadable in [
            serde_json::json!({}),
            serde_json::json!({"status": {}}),
            serde_json::json!({"status": {"allowed": "true"}}),
            Value::Null,
        ] {
            assert!(!allowed(&unreadable), "{unreadable}");
        }
    }

    /// Occupancy clears when a Terminating pod finishes; a request larger than the
    /// quota itself never fits. Telling them apart is what lets the gateway wait out
    /// the first without ever waiting on the second.
    #[test]
    fn a_quota_refusal_is_hopeless_only_when_the_request_alone_exceeds_the_limit() {
        // The cold-start overlap (#94): one pod over, because a closed session's pod
        // is still Terminating. Clears within its termination grace — transient.
        assert!(!quota_never_fits(
            "exceeded quota: duckfleet, requested: count/pods=1, used: count/pods=24, \
             limited: count/pods=24"
        ));
        // The harness's deliberate ceiling probe: 64 CPUs against a 24-CPU limit can
        // never fit however empty the namespace is — hopeless, refuse at once.
        assert!(quota_never_fits(
            "exceeded quota: duckfleet, requested: limits.cpu=64,limits.memory=512Gi, \
             used: limits.cpu=14,limits.memory=18Gi, limited: limits.cpu=24,limits.memory=32Gi"
        ));
        // Millicores and binary quantities normalize before comparing.
        assert!(!quota_never_fits(
            "exceeded quota: duckfleet, requested: requests.cpu=500m, \
             used: requests.cpu=16, limited: requests.cpu=16"
        ));
        assert!(quota_never_fits(
            "exceeded quota: duckfleet, requested: requests.memory=48Gi, \
             used: requests.memory=0, limited: requests.memory=32Gi"
        ));
        // Anything unparseable reads as transient: the wait is bounded, and giving
        // up on a request that would fit is the costlier mistake.
        assert!(!quota_never_fits("exceeded quota: duckfleet"));
        assert!(!quota_never_fits(
            "exceeded quota: duckfleet, requested: cpu=1.5, used: cpu=0, limited: cpu=1"
        ));
    }

    #[test]
    fn only_a_quota_forbidden_carries_a_quota_message() {
        let quota = Kube::expect_success(
            StatusCode::FORBIDDEN,
            serde_json::json!({"message":
                "exceeded quota: duckfleet, requested: count/pods=1, used: count/pods=24, \
                 limited: count/pods=24"}),
            "creating a session pod",
        )
        .unwrap_err();
        assert!(quota_message(&quota).is_some_and(|m| m.contains("exceeded quota")));
        // An admission webhook's 403 is a verdict, not occupancy.
        let webhook = Kube::expect_success(
            StatusCode::FORBIDDEN,
            serde_json::json!({"message": "admission webhook \"deny-all\" denied the request"}),
            "creating a session pod",
        )
        .unwrap_err();
        assert!(quota_message(&webhook).is_none());
        // And a 500 mentioning quota text is still not a refusal.
        let broken = Kube::expect_success(
            StatusCode::INTERNAL_SERVER_ERROR,
            serde_json::json!({"message": "exceeded quota: etcd timeout"}),
            "creating a session pod",
        )
        .unwrap_err();
        assert!(quota_message(&broken).is_none());
    }

    #[test]
    fn a_successful_status_returns_the_body() {
        let body = serde_json::json!({"metadata": {"name": "session"}});
        assert_eq!(
            Kube::expect_success(StatusCode::CREATED, body.clone(), "creating").unwrap(),
            body
        );
    }
}
