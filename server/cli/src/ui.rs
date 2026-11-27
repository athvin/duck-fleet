//! `duckfleet ui`: the admin console, served on this machine.
//!
//! The CLI embeds the static web bundle from `web/admin` and serves it on
//! loopback. The page calls the gateway's own `/v1/...` paths. This process
//! forwards those calls to the configured gateway and attaches the CLI's bearer
//! token, so the credential never reaches the browser and the gateway still
//! authorizes every call against that credential. Only `GET` is forwarded: the
//! console is read-only until mutating actions get their own reviewed path.
//!
//! The threat model is other web pages in the same browser, not other local
//! processes (those can already read the CLI's credential from local state).
//! Three checks keep a hostile page out:
//!
//! - The listener binds 127.0.0.1 only, and the `Host` header must name it.
//!   That defeats DNS rebinding, where a page rebinds its own hostname to
//!   loopback and then reads responses as same-origin.
//! - API calls need a cookie holding a random key generated per launch. The
//!   key reaches the browser only through the launch URL printed on the
//!   terminal, which exchanges it for an `HttpOnly`, `SameSite=Strict` cookie
//!   and redirects it out of the address bar.
//! - Requests a browser marks as cross-site, or that carry a foreign `Origin`,
//!   are refused before any handler runs.

use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::extract::{Request, State};
use axum::http::{HeaderMap, HeaderValue, Method, StatusCode, header};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use serde_json::json;

/// Built by `npm run build` in `web/admin`. A binary built without it still
/// compiles, and `duckfleet ui` then explains how to build the bundle.
#[derive(rust_embed::Embed)]
// Relative to this crate's manifest directory (server/cli).
#[folder = "../../web/admin/out"]
#[allow_missing = true]
struct Bundle;

pub(crate) const DEFAULT_PORT: u16 = 7878;
const COOKIE: &str = "duckfleet_ui";
const LAUNCH_PATH: &str = "/_duckfleet/launch";
const META_PATH: &str = "/_duckfleet/meta";
const UPSTREAM_TIMEOUT: Duration = Duration::from_secs(30);

pub(crate) struct Options {
    pub url: String,
    pub token: String,
    pub port: u16,
}

#[derive(Clone)]
struct Ui(Arc<Inner>);

struct Inner {
    gateway: String,
    token: String,
    key: String,
    port: u16,
    http: reqwest::Client,
}

pub(crate) async fn run(options: Options) -> i32 {
    if Bundle::get("index.html").is_none() {
        eprintln!(
            "this duckfleet binary was built without the admin console; run `npm ci && npm run build` in web/admin, then rebuild the CLI"
        );
        return 2;
    }
    let listener = match tokio::net::TcpListener::bind((Ipv4Addr::LOCALHOST, options.port)).await {
        Ok(listener) => listener,
        Err(error) => {
            eprintln!(
                "could not listen on 127.0.0.1:{}: {error}; pass --port to choose another port",
                options.port
            );
            return 1;
        }
    };
    let address = match listener.local_addr() {
        Ok(address) => address,
        Err(error) => {
            eprintln!("could not read the listening address: {error}");
            return 1;
        }
    };
    let ui = match Ui::new(&options.url, &options.token, address) {
        Ok(ui) => ui,
        Err(message) => {
            eprintln!("{message}");
            return 1;
        }
    };
    println!("DuckFleet admin console for {}", ui.0.gateway);
    println!(
        "open http://127.0.0.1:{}{LAUNCH_PATH}?key={}",
        ui.0.port, ui.0.key
    );
    println!("press Ctrl-C to stop");
    let served = axum::serve(listener, router(ui))
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await;
    match served {
        Ok(()) => 0,
        Err(error) => {
            eprintln!("admin console stopped: {error}");
            1
        }
    }
}

impl Ui {
    fn new(gateway: &str, token: &str, address: SocketAddr) -> Result<Self, String> {
        // No redirects, so the bearer token can never follow a Location header
        // to another host. This matches the SDK client.
        let http = reqwest::Client::builder()
            .retry(reqwest::retry::never())
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|error| format!("could not build the gateway client: {error}"))?;
        Ok(Ui(Arc::new(Inner {
            gateway: gateway.trim_end_matches('/').to_owned(),
            token: token.to_owned(),
            key: random_key()?,
            port: address.port(),
            http,
        })))
    }

    fn authorized(&self, headers: &HeaderMap) -> bool {
        cookie(headers, COOKIE).is_some_and(|value| same(value.as_bytes(), self.0.key.as_bytes()))
    }
}

fn router(ui: Ui) -> Router {
    Router::new()
        .route(LAUNCH_PATH, get(launch))
        .route(META_PATH, get(meta))
        .route("/v1/{*rest}", get(proxy))
        .fallback(asset)
        .layer(middleware::from_fn_with_state(ui.clone(), guard))
        .with_state(ui)
}

/// Runs before every handler: refuse foreign hosts and cross-site requests,
/// then add the headers every response carries.
async fn guard(State(ui): State<Ui>, request: Request, next: Next) -> Response {
    let headers = request.headers();
    let host = headers.get(header::HOST).and_then(|v| v.to_str().ok());
    let allowed = [
        format!("127.0.0.1:{}", ui.0.port),
        format!("localhost:{}", ui.0.port),
    ];
    let Some(host) = host.filter(|host| allowed.iter().any(|a| a == host)) else {
        return refuse(StatusCode::FORBIDDEN, "unexpected Host header");
    };
    if let Some(origin) = headers.get(header::ORIGIN)
        && origin.to_str().ok() != Some(format!("http://{host}").as_str())
    {
        return refuse(StatusCode::FORBIDDEN, "cross-origin request");
    }
    if headers
        .get("sec-fetch-site")
        .is_some_and(|site| site == "cross-site")
    {
        return refuse(StatusCode::FORBIDDEN, "cross-site request");
    }
    let mut response = next.run(request).await;
    let headers = response.headers_mut();
    for (name, value) in [
        ("x-content-type-options", "nosniff"),
        ("x-frame-options", "DENY"),
        ("content-security-policy", "frame-ancestors 'none'"),
        ("referrer-policy", "no-referrer"),
    ] {
        headers.insert(name, HeaderValue::from_static(value));
    }
    response
}

/// Trades the key from the printed URL for a cookie, then drops the key from
/// the address bar so it does not land in browser history as the page URL.
async fn launch(State(ui): State<Ui>, request: Request) -> Response {
    let offered = request
        .uri()
        .query()
        .and_then(|query| query.split('&').find_map(|pair| pair.strip_prefix("key=")));
    if !offered.is_some_and(|key| same(key.as_bytes(), ui.0.key.as_bytes())) {
        return (
            StatusCode::FORBIDDEN,
            "This launch link is not valid for the running console. Use the link `duckfleet ui` printed.",
        )
            .into_response();
    }
    let cookie = format!("{COOKIE}={}; HttpOnly; SameSite=Strict; Path=/", ui.0.key);
    Response::builder()
        .status(StatusCode::SEE_OTHER)
        .header(header::LOCATION, "/")
        .header(header::SET_COOKIE, cookie)
        .header(header::CACHE_CONTROL, "no-store")
        .body(Body::empty())
        .expect("static response parts")
}

/// What the console shows about its own connection. Never the token.
async fn meta(State(ui): State<Ui>, headers: HeaderMap) -> Response {
    if !ui.authorized(&headers) {
        return unauthorized();
    }
    let body = json!({
        "gateway": ui.0.gateway,
        "cli_version": env!("CARGO_PKG_VERSION"),
        "proxy": "duckfleet-cli",
        "read_only": true,
    });
    let mut response = axum::Json(body).into_response();
    no_store(&mut response);
    response
}

async fn proxy(State(ui): State<Ui>, request: Request) -> Response {
    if !ui.authorized(request.headers()) {
        return unauthorized();
    }
    let path = request.uri().path();
    if !forwardable(path) {
        return refuse(StatusCode::BAD_REQUEST, "path is not forwarded");
    }
    let mut target = format!("{}{path}", ui.0.gateway);
    if let Some(query) = request.uri().query() {
        target.push('?');
        target.push_str(query);
    }
    let accept = request
        .headers()
        .get(header::ACCEPT)
        .cloned()
        .unwrap_or_else(|| HeaderValue::from_static("application/json"));
    let upstream =
        ui.0.http
            .get(target)
            .bearer_auth(&ui.0.token)
            .header(header::ACCEPT, accept)
            .timeout(UPSTREAM_TIMEOUT)
            .send()
            .await;
    let upstream = match upstream {
        Ok(upstream) => upstream,
        Err(error) => return gateway_unreachable(&error.without_url().to_string()),
    };
    let status =
        StatusCode::from_u16(upstream.status().as_u16()).unwrap_or(StatusCode::BAD_GATEWAY);
    let content_type = upstream.headers().get(header::CONTENT_TYPE).cloned();
    let body = match upstream.bytes().await {
        Ok(body) => body,
        Err(error) => return gateway_unreachable(&error.without_url().to_string()),
    };
    let mut response = (status, body).into_response();
    if let Some(content_type) = content_type {
        response
            .headers_mut()
            .insert(header::CONTENT_TYPE, content_type);
    }
    no_store(&mut response);
    response
}

async fn asset(method: Method, request: Request) -> Response {
    if method != Method::GET && method != Method::HEAD {
        return refuse(StatusCode::METHOD_NOT_ALLOWED, "the console is read-only");
    }
    for candidate in asset_candidates(request.uri().path()) {
        if let Some(file) = Bundle::get(&candidate) {
            return file_response(StatusCode::OK, &candidate, file);
        }
    }
    match Bundle::get("404.html") {
        Some(file) => file_response(StatusCode::NOT_FOUND, "404.html", file),
        None => (StatusCode::NOT_FOUND, "not found").into_response(),
    }
}

fn file_response(status: StatusCode, path: &str, file: rust_embed::EmbeddedFile) -> Response {
    // Next.js fingerprints everything under _next/static, so those never change.
    let cache = if path.starts_with("_next/static/") {
        "public, max-age=31536000, immutable"
    } else {
        "no-cache"
    };
    Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, file.metadata.mimetype())
        .header(header::CACHE_CONTROL, cache)
        .body(Body::from(file.data.into_owned()))
        .expect("static response parts")
}

/// The bundle files a request path can mean, most specific first. A static
/// export writes `/fleet` as `fleet.html`, and `/` as `index.html`.
fn asset_candidates(path: &str) -> Vec<String> {
    let path = path.trim_start_matches('/');
    if path.is_empty() {
        return vec!["index.html".into()];
    }
    if path
        .split('/')
        .any(|segment| segment == ".." || segment == ".")
    {
        return Vec::new();
    }
    let path = path.trim_end_matches('/');
    vec![
        path.to_owned(),
        format!("{path}.html"),
        format!("{path}/index.html"),
    ]
}

/// Gateway paths this proxy will forward. The gateway's ids are UUIDs and its
/// names are plain, so anything encoded or dot-segmented is refused rather than
/// normalized differently here and there.
fn forwardable(path: &str) -> bool {
    path.starts_with("/v1/")
        && !path.contains('%')
        && !path.contains("//")
        && !path
            .split('/')
            .any(|segment| segment == ".." || segment == ".")
}

fn cookie<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    headers
        .get_all(header::COOKIE)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(';'))
        .find_map(|pair| {
            let (key, value) = pair.trim().split_once('=')?;
            (key == name).then_some(value)
        })
}

/// Constant-time equality, so response timing does not reveal the key.
fn same(left: &[u8], right: &[u8]) -> bool {
    left.len() == right.len()
        && left
            .iter()
            .zip(right)
            .fold(0u8, |difference, (a, b)| difference | (a ^ b))
            == 0
}

fn random_key() -> Result<String, String> {
    use ring::rand::SecureRandom;
    let mut bytes = [0u8; 32];
    ring::rand::SystemRandom::new()
        .fill(&mut bytes)
        .map_err(|_| "could not generate a console key".to_owned())?;
    Ok(bytes.iter().map(|byte| format!("{byte:02x}")).collect())
}

fn no_store(response: &mut Response) {
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
}

fn refuse(status: StatusCode, message: &str) -> Response {
    (
        status,
        axum::Json(
            json!({"error": message, "failure_kind": "ui_request_refused", "retry_safe": false}),
        ),
    )
        .into_response()
}

fn unauthorized() -> Response {
    (
        StatusCode::UNAUTHORIZED,
        axum::Json(json!({
            "error": "open the console with the link `duckfleet ui` printed",
            "failure_kind": "ui_session_missing",
            "retry_safe": false,
        })),
    )
        .into_response()
}

fn gateway_unreachable(detail: &str) -> Response {
    (
        StatusCode::BAD_GATEWAY,
        axum::Json(json!({
            "error": format!("could not reach the gateway: {detail}"),
            "failure_kind": "transport_failure",
            "retry_safe": true,
        })),
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    const TOKEN: &str = "upstream-secret";

    /// A gateway that answers `/v1/workers` only for the expected bearer token
    /// and echoes back the query string it received.
    async fn fake_gateway() -> String {
        async fn workers(headers: HeaderMap, request: Request) -> Response {
            let bearer = format!("Bearer {TOKEN}");
            if headers
                .get(header::AUTHORIZATION)
                .and_then(|v| v.to_str().ok())
                != Some(bearer.as_str())
            {
                return StatusCode::UNAUTHORIZED.into_response();
            }
            axum::Json(json!({"workers": [], "query": request.uri().query()})).into_response()
        }
        let listener = tokio::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
            .await
            .unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, Router::new().route("/v1/workers", get(workers)))
                .await
                .unwrap();
        });
        format!("http://{address}/")
    }

    async fn console(gateway: &str) -> (Ui, String) {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let listener = tokio::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
            .await
            .unwrap();
        let address = listener.local_addr().unwrap();
        let ui = Ui::new(gateway, TOKEN, address).unwrap();
        let app = router(ui.clone());
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        (ui, format!("http://{address}"))
    }

    fn client() -> reqwest::Client {
        reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .unwrap()
    }

    #[tokio::test]
    async fn proxy_needs_the_launch_cookie_and_adds_the_bearer_token() {
        let (ui, base) = console(&fake_gateway().await).await;
        let http = client();

        let anonymous = http.get(format!("{base}/v1/workers")).send().await.unwrap();
        assert_eq!(anonymous.status(), 401);

        let wrong = http
            .get(format!("{base}{LAUNCH_PATH}?key=nope"))
            .send()
            .await
            .unwrap();
        assert_eq!(wrong.status(), 403);
        assert!(wrong.headers().get(header::SET_COOKIE).is_none());

        let launched = http
            .get(format!("{base}{LAUNCH_PATH}?key={}", ui.0.key))
            .send()
            .await
            .unwrap();
        assert_eq!(launched.status(), 303);
        assert_eq!(launched.headers()[header::LOCATION], "/");
        let set_cookie = launched.headers()[header::SET_COOKIE].to_str().unwrap();
        assert!(set_cookie.contains("HttpOnly"));
        assert!(set_cookie.contains("SameSite=Strict"));
        let cookie = set_cookie.split(';').next().unwrap().to_owned();

        let forwarded = http
            .get(format!("{base}/v1/workers?limit=5"))
            .header(header::COOKIE, &cookie)
            .send()
            .await
            .unwrap();
        assert_eq!(forwarded.status(), 200);
        assert_eq!(forwarded.headers()[header::CACHE_CONTROL], "no-store");
        assert_eq!(forwarded.headers()["referrer-policy"], "no-referrer");
        let body: serde_json::Value = forwarded.json().await.unwrap();
        assert_eq!(body["query"], "limit=5");

        let meta: serde_json::Value = http
            .get(format!("{base}{META_PATH}"))
            .header(header::COOKIE, &cookie)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(meta["read_only"], true);
        assert!(!meta.to_string().contains(TOKEN));

        let missing = http
            .get(format!("{base}/v1/nothing-here"))
            .header(header::COOKIE, &cookie)
            .send()
            .await
            .unwrap();
        assert_eq!(missing.status(), 404, "upstream status passes through");

        let write = http
            .post(format!("{base}/v1/workers"))
            .header(header::COOKIE, &cookie)
            .send()
            .await
            .unwrap();
        assert_eq!(write.status(), 405, "only GET is forwarded");
    }

    #[tokio::test]
    async fn foreign_hosts_and_cross_site_requests_are_refused() {
        let (ui, base) = console(&fake_gateway().await).await;
        let cookie = format!("{COOKIE}={}", ui.0.key);
        let http = client();
        let attempts = [
            (header::HOST, "attacker.example:7878".to_owned()),
            (header::ORIGIN, "https://attacker.example".to_owned()),
            (
                header::HeaderName::from_static("sec-fetch-site"),
                "cross-site".to_owned(),
            ),
        ];
        for (name, value) in attempts {
            let response = http
                .get(format!("{base}/v1/workers"))
                .header(header::COOKIE, &cookie)
                .header(name.clone(), value)
                .send()
                .await
                .unwrap();
            assert_eq!(response.status(), 403, "{name} should be refused");
        }
        let same_origin = http
            .get(format!("{base}/v1/workers"))
            .header(header::COOKIE, &cookie)
            .header(header::ORIGIN, base.clone())
            .send()
            .await
            .unwrap();
        assert_eq!(same_origin.status(), 200);
    }

    #[tokio::test]
    async fn an_unreachable_gateway_is_a_classified_transport_failure() {
        let (ui, base) = console("http://127.0.0.1:9").await;
        let response = client()
            .get(format!("{base}/v1/workers"))
            .header(header::COOKIE, format!("{COOKIE}={}", ui.0.key))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 502);
        let body: serde_json::Value = response.json().await.unwrap();
        assert_eq!(body["failure_kind"], "transport_failure");
    }

    #[test]
    fn only_plain_gateway_paths_are_forwarded() {
        assert!(forwardable("/v1/workers"));
        assert!(forwardable(
            "/v1/queries/0b6e3c1e-6a4e-4d0e-9a51-7a4f1f3b2c10"
        ));
        for path in [
            "/metrics",
            "/v1/../metrics",
            "/v1/./workers",
            "/v1//workers",
            "/v1/%2e%2e/metrics",
        ] {
            assert!(!forwardable(path), "{path}");
        }
    }

    #[test]
    fn asset_paths_resolve_like_a_static_export() {
        assert_eq!(asset_candidates("/"), ["index.html"]);
        assert_eq!(
            asset_candidates("/fleet/"),
            ["fleet", "fleet.html", "fleet/index.html"]
        );
        assert!(asset_candidates("/_next/../secret").is_empty());
    }

    #[test]
    fn cookie_lookup_and_key_comparison() {
        let mut headers = HeaderMap::new();
        headers.insert(
            header::COOKIE,
            HeaderValue::from_static("other=1; duckfleet_ui=abc"),
        );
        assert_eq!(cookie(&headers, COOKIE), Some("abc"));
        assert!(same(b"abc", b"abc"));
        assert!(!same(b"abc", b"abd"));
        assert!(!same(b"abc", b"abcd"));
        assert_eq!(random_key().unwrap().len(), 64);
    }
}
