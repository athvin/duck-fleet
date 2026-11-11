//! Pins each session to one configured worker. It never retries or reroutes SQL.
use crate::{
    ApiError, authorize,
    compute::{SessionRequest, Size},
    failure,
};
use axum::{
    Json, Router,
    body::Bytes,
    extract::{DefaultBodyLimit, Path, Query, State},
    http::header,
    middleware,
    response::{IntoResponse, Response},
    routing::{get, post},
};
use reqwest::{Client, Method};
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::{Mutex, OwnedSemaphorePermit, Semaphore};
use uuid::Uuid;

struct Route {
    endpoint: String,
    touched: Instant,
    _permit: OwnedSemaphorePermit,
}
#[derive(Clone)]
struct Gateway {
    client: Client,
    token: Arc<str>,
    small: String,
    medium: String,
    routes: Arc<Mutex<HashMap<Uuid, Route>>>,
    capacity: Arc<Semaphore>,
    history: crate::history::Reader,
}
impl Gateway {
    fn endpoint(&self, size: Size) -> &str {
        match size {
            Size::Small => &self.small,
            Size::Medium => &self.medium,
        }
    }
    async fn send(
        &self,
        endpoint: &str,
        method: Method,
        path: &str,
        body: Bytes,
    ) -> Result<reqwest::Response, ApiError> {
        self.client
            .request(method, format!("{endpoint}{path}"))
            .bearer_auth(&self.token)
            .header(header::CONTENT_TYPE, "application/json")
            .body(body)
            .send()
            .await
            .map_err(|_| ApiError::internal())
    }
    async fn route(&self, id: Uuid) -> Result<String, ApiError> {
        let mut routes = self.routes.lock().await;
        routes.retain(|_, r| r.touched.elapsed() < Duration::from_secs(300));
        let route = routes
            .get_mut(&id)
            .ok_or_else(|| ApiError::new(failure::UNKNOWN_SESSION, "unknown or expired session"))?;
        route.touched = Instant::now();
        Ok(route.endpoint.clone())
    }
}
async fn response(upstream: reqwest::Response) -> Result<Response, ApiError> {
    let status = upstream.status();
    let mut headers = axum::http::HeaderMap::new();
    for name in [
        header::CONTENT_TYPE,
        header::HeaderName::from_static("x-query-id"),
    ] {
        if let Some(value) = upstream.headers().get(&name) {
            headers.insert(name, value.clone());
        }
    }
    let body = upstream.bytes().await.map_err(|_| ApiError::internal())?;
    Ok((status, headers, body).into_response())
}
async fn open(
    State(app): State<Gateway>,
    request: Option<Json<SessionRequest>>,
) -> Result<Response, ApiError> {
    let mut request = request.map(|Json(r)| r).unwrap_or_default();
    let size = request.validate()?;
    request.compute = size.request();
    app.routes
        .lock()
        .await
        .retain(|_, r| r.touched.elapsed() < Duration::from_secs(300));
    let permit = app
        .capacity
        .clone()
        .try_acquire_owned()
        .map_err(|_| ApiError::new(failure::CAPACITY, "gateway session limit reached"))?;
    let endpoint = app.endpoint(size).to_owned();
    let body = serde_json::to_vec(&request).map_err(|_| ApiError::internal())?;
    let upstream = app
        .send(&endpoint, Method::POST, "/v1/sessions", body.into())
        .await?;
    if !upstream.status().is_success() {
        return response(upstream).await;
    }
    let info: Value = upstream.json().await.map_err(|_| ApiError::internal())?;
    let id = info
        .get("session_id")
        .and_then(Value::as_str)
        .and_then(|s| Uuid::parse_str(s).ok())
        .ok_or_else(ApiError::internal)?;
    app.routes.lock().await.insert(
        id,
        Route {
            endpoint,
            touched: Instant::now(),
            _permit: permit,
        },
    );
    Ok(Json(info).into_response())
}
async fn query(
    State(app): State<Gateway>,
    Path(id): Path<Uuid>,
    body: Bytes,
) -> Result<Response, ApiError> {
    let endpoint = app.route(id).await?;
    response(
        app.send(
            &endpoint,
            Method::POST,
            &format!("/v1/sessions/{id}/queries"),
            body,
        )
        .await?,
    )
    .await
}
async fn cancel(State(app): State<Gateway>, Path(id): Path<Uuid>) -> Result<Response, ApiError> {
    let endpoint = app.route(id).await?;
    response(
        app.send(
            &endpoint,
            Method::POST,
            &format!("/v1/sessions/{id}/cancel"),
            Bytes::new(),
        )
        .await?,
    )
    .await
}
async fn close(State(app): State<Gateway>, Path(id): Path<Uuid>) -> Result<Response, ApiError> {
    let endpoint = app.route(id).await?;
    let upstream = app
        .send(
            &endpoint,
            Method::DELETE,
            &format!("/v1/sessions/{id}"),
            Bytes::new(),
        )
        .await?;
    if upstream.status().is_success() {
        app.routes.lock().await.remove(&id);
    }
    response(upstream).await
}
async fn history(
    State(app): State<Gateway>,
    Query(filter): Query<crate::history::Filter>,
) -> Result<Json<Value>, ApiError> {
    app.history.list(filter).await.map(Json)
}
async fn query_detail(
    State(app): State<Gateway>,
    Path(id): Path<Uuid>,
) -> Result<Json<Value>, ApiError> {
    app.history.detail(id).await.map(Json)
}
async fn workers(State(app): State<Gateway>) -> Result<Json<Value>, ApiError> {
    async fn info(app: &Gateway, endpoint: &str) -> Result<Value, ApiError> {
        app.send(endpoint, Method::GET, "/v1/worker", Bytes::new())
            .await?
            .error_for_status()
            .map_err(|_| ApiError::internal())?
            .json()
            .await
            .map_err(|_| ApiError::internal())
    }
    let (small, medium) = tokio::try_join!(info(&app, &app.small), info(&app, &app.medium))?;
    Ok(Json(json!({"workers":[small,medium]})))
}

pub(crate) async fn serve(token: String) -> anyhow::Result<()> {
    let app = Gateway {
        client: Client::builder()
            .timeout(Duration::from_secs(75))
            .retry(reqwest::retry::never())
            .build()?,
        token: token.into(),
        small: std::env::var("WORKER_SMALL_URL")?,
        medium: std::env::var("WORKER_MEDIUM_URL")?,
        routes: Arc::new(Mutex::new(HashMap::new())),
        capacity: Arc::new(Semaphore::new(64)),
        history: crate::history::Reader::new(std::env::var("PLATFORM_DSN")?),
    };
    let api = Router::new()
        .route("/v1/sessions", post(open))
        .route("/v1/sessions/{id}", axum::routing::delete(close))
        .route("/v1/sessions/{id}/queries", post(query))
        .route("/v1/sessions/{id}/cancel", post(cancel))
        .route("/v1/queries", get(history))
        .route("/v1/queries/{id}", get(query_detail))
        .route("/v1/workers", get(workers))
        .route_layer(middleware::from_fn_with_state(app.token.clone(), authorize));
    let router = Router::new()
        .route(
            "/healthz",
            get(|| async { Json(json!({"status":"ok","role":"gateway"})) }),
        )
        .merge(api)
        .layer(DefaultBodyLimit::max(1_048_576))
        .with_state(app);
    tracing::info!("DuckFleet gateway ready");
    axum::serve(tokio::net::TcpListener::bind("0.0.0.0:8080").await?, router).await?;
    Ok(())
}
