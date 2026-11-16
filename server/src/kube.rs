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

pub(crate) struct Kube {
    client: Client,
    /// Namespaced collection URL; every call this makes is a pod in one namespace.
    pods: String,
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
            account,
        })
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
        anyhow::bail!("{what} failed: {status}: {message}")
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

#[cfg(test)]
mod tests {
    use super::*;

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

    #[test]
    fn a_successful_status_returns_the_body() {
        let body = serde_json::json!({"metadata": {"name": "session"}});
        assert_eq!(
            Kube::expect_success(StatusCode::CREATED, body.clone(), "creating").unwrap(),
            body
        );
    }
}
