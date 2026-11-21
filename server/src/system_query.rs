//! Worker-independent, bounded SQL over authorized operational metadata.
//! Every statement owns its async PostgreSQL driver and one read snapshot.
pub(crate) use crate::system_result::SystemResult;
use crate::{
    ApiError,
    authz::{self, Access, Action},
    credentials::{self, Identity},
    failure,
    metadata::{self, Relation, ResultType, StorageSchema, SystemType},
    system_admission::Permit,
    system_result::{self, Cell, Format, Snapshot, Table},
    system_sql::{self, Discovery, OutputColumn, Statement},
};
use axum::body::{Body, to_bytes};
use futures_util::TryStreamExt;
use serde::Deserialize;
use serde_json::{Value, json};
use std::{
    sync::Arc,
    time::{Duration, SystemTime},
};
use tokio::{
    sync::{OwnedSemaphorePermit, Semaphore},
    task::JoinHandle,
    time::{Instant, timeout_at},
};
use tokio_postgres::{Client, IsolationLevel, NoTls, Transaction};
use uuid::Uuid;

const DEADLINE: Duration = Duration::from_secs(3);
const MAX_BODY: usize = 256 * 1024;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Request {
    pub sql: String,
    #[serde(default)]
    pub parameters: Vec<Value>,
    pub bounds: Option<system_sql::BoundsEnvelope>,
    #[serde(default = "contract_version")]
    pub contract_version: u16,
    #[serde(default)]
    pub format: Format,
}
fn contract_version() -> u16 {
    metadata::SYSTEM_CONTRACT_VERSION
}
impl Request {
    pub fn sql(sql: String) -> Self {
        Self {
            sql,
            parameters: vec![],
            bounds: None,
            contract_version: contract_version(),
            format: Format::Arrow,
        }
    }
}
#[derive(Clone)]
pub(crate) struct Service {
    config: Arc<tokio_postgres::Config>,
    bootstrap: Arc<str>,
    schema: StorageSchema,
    queries: Arc<Semaphore>,
    authentication: Arc<Semaphore>,
}
struct Driver(JoinHandle<()>);
impl Drop for Driver {
    fn drop(&mut self) {
        self.0.abort();
    }
}
impl Driver {
    async fn close(mut self) {
        self.0.abort();
        let _ = (&mut self.0).await;
    }
}
enum Caller<'a> {
    Token(&'a str),
    Identity(&'a Identity),
}
fn unavailable() -> ApiError {
    ApiError::new(
        failure::SYSTEM_QUERY_UNAVAILABLE,
        "system metadata is temporarily unavailable",
    )
}
fn unauthorized() -> ApiError {
    ApiError::new(failure::UNAUTHORIZED, "invalid platform credential")
}
fn capacity() -> ApiError {
    ApiError::new(
        failure::CAPACITY,
        "system query capacity reached; finish reading an earlier result before submitting another",
    )
}
fn db_error(error: tokio_postgres::Error) -> ApiError {
    match error.code() {
        Some(code) if *code == tokio_postgres::error::SqlState::NUMERIC_VALUE_OUT_OF_RANGE => {
            system_result::unavailable_result()
        }
        Some(code)
            if *code == tokio_postgres::error::SqlState::QUERY_CANCELED
                || *code == tokio_postgres::error::SqlState::LOCK_NOT_AVAILABLE =>
        {
            system_result::timeout()
        }
        _ => unavailable(),
    }
}

impl Service {
    pub fn new(dsn: &str, bootstrap: Arc<str>, schema: StorageSchema) -> Result<Self, ApiError> {
        let mut config: tokio_postgres::Config = dsn.parse().map_err(|_| unavailable())?;
        config.connect_timeout(DEADLINE);
        config.application_name("duckfleet-system");
        // Replace caller-independent connection options with one trusted path.
        // The actual schema was read and validated at gateway startup.
        config.options(format!("-c search_path={} -c statement_timeout=3000 -c lock_timeout=500 -c idle_in_transaction_session_timeout=3000 -c work_mem=4MB -c max_parallel_workers_per_gather=0",schema.search_path()));
        Ok(Self {
            config: Arc::new(config),
            bootstrap,
            schema,
            queries: Arc::new(Semaphore::new(2)),
            authentication: Arc::new(Semaphore::new(8)),
        })
    }
    async fn connect(&self) -> Result<(Client, Driver), ApiError> {
        let (client, connection) = self.config.connect(NoTls).await.map_err(db_error)?;
        let driver = Driver(tokio::spawn(async move {
            let _ = connection.await;
        }));
        Ok((client, driver))
    }
    async fn access(
        &self,
        tx: &Transaction<'_>,
        caller: Caller<'_>,
    ) -> Result<(Identity, Access), ApiError> {
        let identity = match caller {
            Caller::Token(token) if credentials::tokens_match(token, &self.bootstrap) => {
                Identity::bootstrap()
            }
            Caller::Token(token) => credentials::resolve_async_in(tx, &credentials::digest(token))
                .await?
                .ok_or_else(unauthorized)?,
            Caller::Identity(identity) => identity.clone(),
        };
        if identity.expired() {
            return Err(unauthorized());
        }
        let access = authz::resolve_async_in(tx, &identity).await?;
        Ok((identity, access))
    }
    async fn authenticate_caller(&self, caller: Caller<'_>) -> Result<Identity, ApiError> {
        let deadline = Instant::now() + DEADLINE;
        timeout_at(deadline, async {
            let permit = self
                .authentication
                .clone()
                .acquire_owned()
                .await
                .map_err(|_| unavailable())?;
            let (mut client, driver) = self.connect().await?;
            let (_permit, completion) = Permit::new(permit, deadline);
            let mut denial = None;
            let result = async {
                let tx = client
                    .build_transaction()
                    .isolation_level(IsolationLevel::RepeatableRead)
                    .read_only(true)
                    .start()
                    .await
                    .map_err(db_error)?;
                let resolved = self
                    .access(&tx, caller)
                    .await
                    .and_then(|(identity, access)| {
                        if let Err(error) = access.require_metadata_access() {
                            denial =
                                Some(denial_event(&access, Action::HistoryRead, "system_entry"));
                            Err(error)
                        } else {
                            Ok(identity)
                        }
                    });
                match resolved {
                    Ok(identity) => {
                        tx.commit().await.map_err(db_error)?;
                        completion.confirmed_idle();
                        Ok(identity)
                    }
                    Err(error) => {
                        if tx.rollback().await.is_ok() {
                            completion.confirmed_idle();
                        }
                        Err(error)
                    }
                }
            }
            .await;
            drop(client);
            driver.close().await;
            if let Some(event) = denial {
                self.audit_denial(event);
            }
            result
        })
        .await
        .map_err(|_| system_result::timeout())?
    }
    pub async fn authenticate(&self, token: &str) -> Result<Identity, ApiError> {
        self.authenticate_caller(Caller::Token(token)).await
    }
    pub async fn revalidate(&self, identity: &Identity) -> Result<(), ApiError> {
        self.authenticate_caller(Caller::Identity(identity))
            .await
            .map(|_| ())
    }
    pub async fn query(
        &self,
        identity: &Identity,
        request: Request,
    ) -> Result<SystemResult, ApiError> {
        let deadline = Instant::now() + DEADLINE;
        let permit = self
            .queries
            .clone()
            .try_acquire_owned()
            .map_err(|_| capacity())?;
        timeout_at(
            deadline,
            self.execute(Caller::Identity(identity), request, permit, deadline, false),
        )
        .await
        .map_err(|_| system_result::timeout())?
    }
    /// Body collection is part of the same deadline/admission as authentication,
    /// compilation, probes, execution and serialization. No cached HTTP auth pass.
    pub async fn http(&self, token: &str, body: Body) -> Result<SystemResult, ApiError> {
        let deadline = Instant::now() + DEADLINE;
        let permit = self
            .queries
            .clone()
            .try_acquire_owned()
            .map_err(|_| capacity())?;
        timeout_at(deadline, async {
            let bytes = to_bytes(body, MAX_BODY)
                .await
                .map_err(|_| ApiError::bad("system request body must be at most 256 KiB"))?;
            let request: Request = serde_json::from_slice(&bytes)
                .map_err(|_| ApiError::bad("invalid system query request"))?;
            self.execute(Caller::Token(token), request, permit, deadline, false)
                .await
        })
        .await
        .map_err(|_| system_result::timeout())?
    }
    pub async fn schema(&self, token: &str) -> Result<SystemResult, ApiError> {
        let deadline = Instant::now() + DEADLINE;
        let permit = self
            .queries
            .clone()
            .try_acquire_owned()
            .map_err(|_| capacity())?;
        let mut request = Request::sql("SHOW TABLES FROM duckfleet_system".into());
        request.format = Format::Json;
        timeout_at(
            deadline,
            self.execute(Caller::Token(token), request, permit, deadline, true),
        )
        .await
        .map_err(|_| system_result::timeout())?
    }
    async fn execute(
        &self,
        caller: Caller<'_>,
        request: Request,
        permit: OwnedSemaphorePermit,
        deadline: Instant,
        full_schema: bool,
    ) -> Result<SystemResult, ApiError> {
        if request.contract_version != metadata::SYSTEM_CONTRACT_VERSION {
            return Err(ApiError::bad(
                "unsupported system contract version; this server supports version 1",
            ));
        }
        let (mut client, driver) = self.connect().await?;
        let (permit, completion) = Permit::new(permit, deadline);
        let mut denial = None;
        let result = async {
            let tx = client
                .build_transaction()
                .isolation_level(IsolationLevel::RepeatableRead)
                .read_only(true)
                .start()
                .await
                .map_err(db_error)?;
            let read_result = async {
            let (_, access) = self.access(&tx, caller).await?;
            if let Err(error) = access.require_metadata_access() {
                denial = Some(denial_event(&access, Action::HistoryRead, "system_entry"));
                return Err(error);
            }
            let at: SystemTime = tx
                .query_one("SELECT transaction_timestamp()", &[])
                .await
                .map_err(db_error)?
                .try_get(0)
                .map_err(|_| unavailable())?;
            let format = request.format;
            // A canceled waiter cannot free its slot while bounded parser work is
            // still queued/running on the blocking executor.
            let (statement, permit) = tokio::task::spawn_blocking(move || {
                let result =
                    system_sql::compile(&request.sql, &request.parameters, request.bounds.as_ref());
                (result, permit)
            })
            .await
            .map_err(|_| unavailable())?;
            let statement = statement?;
            let metadata = access.metadata();
            let requested_action = match &statement {
                Statement::Select(plan) if plan.required_relations() == [Relation::Usage] => {
                    Action::UsageRead
                }
                Statement::Discover(Discovery::Columns(Relation::Usage)) => Action::UsageRead,
                _ => Action::HistoryRead,
            };
            let table_result = async {
                match statement {
                    Statement::Discover(discovery) => {
                        Ok((discover(discovery, &metadata, full_schema)?, json!([])))
                    }
                    Statement::Select(plan) => {
                        let executable = system_sql::render(&plan, &metadata, &self.schema)?;
                        for probe in &executable.probes {
                            let params = probe.query.params();
                            let row = tx
                                .query_one(probe.query.sql(), &params)
                                .await
                                .map_err(db_error)?;
                            let rows: i64 =
                                row.try_get("eligible_rows").map_err(|_| unavailable())?;
                            let bytes: i64 = row
                                .try_get("selected_payload_bytes")
                                .map_err(|_| unavailable())?;
                            if rows < 0
                                || bytes < 0
                                || rows as u64 > u64::from(probe.row_limit)
                                || bytes as u64 > probe.payload_limit
                            {
                                return Err(ApiError::new(failure::SYSTEM_QUERY_TOO_WIDE,format!("{} exceeds its source row or payload limit; narrow its time window, request IDs, or projection",metadata::relation_def(probe.relation).name)));
                            }
                        }
                        let bounds = executable
                            .applied_bounds
                            .iter()
                            .map(|b| applied_bounds(b.relation, &b.bounds))
                            .collect::<Result<Vec<_>, _>>()?;
                        let params = executable.query.params();
                        let stream = tx
                            .query_raw(executable.query.sql(), params)
                            .await
                            .map_err(db_error)?;
                        tokio::pin!(stream);
                        let output_limit = executable.user_limit.map(|n|n as usize).unwrap_or(system_result::MAX_ROWS).min(system_result::MAX_ROWS);
                        let mut rows = Vec::new();
                        let mut bytes = 0usize;
                        while let Some(row) = stream.try_next().await.map_err(db_error)? {
                            if rows.len() == output_limit {
                                return Err(system_result::too_wide());
                            }
                            let values = system_result::row(&row, &executable.outputs)?;
                            bytes = bytes.saturating_add(
                                values
                                    .iter()
                                    .map(|v| match v {
                                        Cell::Text(s) => s.len() + 24,
                                        _ => 24,
                                    })
                                    .sum::<usize>(),
                            );
                            if bytes > system_result::MAX_BYTES {
                                return Err(system_result::too_wide());
                            }
                            rows.push(values);
                        }
                        Ok((
                            Table {
                                columns: executable.outputs,
                                rows,
                            },
                            json!(bounds),
                        ))
                    }
                }
            }
            .await;
            if table_result
                .as_ref()
                .is_err_and(|e: &ApiError| e.0 == failure::FORBIDDEN)
            {
                denial = Some(denial_event(&access, requested_action, "system_query"));
            }
            let (table,bounds) = table_result?;
            Ok((table,Snapshot {at,namespace:access.actor.namespace_id,bounds},format,permit))
            }.await;
            match read_result {
                Ok((table,snapshot,format,permit)) => {
                    tx.commit().await.map_err(db_error)?;
                    completion.confirmed_idle();
                    system_result::encode(table,snapshot,format,permit.into_owned(),deadline)
                }
                Err(error) => {
                    // Dropping a partial RowStream does not prove its query ended.
                    // ROLLBACK is queued behind it, and its acknowledgement does.
                    if tx.rollback().await.is_ok() { completion.confirmed_idle(); }
                    Err(error)
                }
            }
        }
        .await;
        drop(client);
        driver.close().await;
        // The read transaction has ended. An audit failure cannot change the
        // original denial or reveal SQL, raw database errors, or bearer values.
        if let Some(event) = denial {
            self.audit_denial(event);
        }
        result
    }
    fn audit_denial(&self, event: crate::audit::Event) {
        let Ok(permit) = self.authentication.clone().try_acquire_owned() else {
            tracing::warn!("system query authorization denied; audit capacity unavailable");
            return;
        };
        let service = self.clone();
        tokio::spawn(async move {
            let deadline = Instant::now() + Duration::from_millis(500);
            let result = timeout_at(deadline, async {
                let (mut client, driver) = service.connect().await?;
                let (_permit, completion) = Permit::new(permit, deadline);
                let result = async {
                    let tx = client.transaction().await.map_err(db_error)?;
                    crate::audit::append_async_in(&tx, &event)
                        .await
                        .map_err(|_| unavailable())?;
                    tx.commit().await.map_err(db_error)?;
                    completion.confirmed_idle();
                    Ok::<(), ApiError>(())
                }
                .await;
                drop(client);
                driver.close().await;
                result
            })
            .await;
            if !matches!(result, Ok(Ok(()))) {
                tracing::warn!("system query authorization denied; audit persistence unavailable");
            }
        });
    }
}

fn applied_bounds(relation: Relation, bounds: &metadata::SourceBounds) -> Result<Value, ApiError> {
    let time = |value| system_result::micros(value).and_then(system_result::timestamp);
    let value = match bounds {
        metadata::SourceBounds::TimeRange { from, until } => {
            json!({"from":time(*from)?,"until":time(*until)?})
        }
        metadata::SourceBounds::RequestIds(ids) => json!({"request_ids":ids}),
        metadata::SourceBounds::Both { from, until, ids } => {
            json!({"from":time(*from)?,"until":time(*until)?,"request_ids":ids})
        }
    };
    Ok(json!({"relation":metadata::relation_def(relation).name,"bounds":value}))
}
fn column(name: &str, ty: SystemType, nullable: bool) -> OutputColumn {
    OutputColumn {
        name: name.into(),
        ty: ResultType::Scalar(ty),
        nullable,
        units: None,
    }
}
fn text(value: impl Into<String>) -> Cell {
    Cell::Text(value.into())
}
fn access_label(value: metadata::ColumnAccess) -> &'static str {
    match value {
        metadata::ColumnAccess::Readable => "readable",
        metadata::ColumnAccess::PartiallyMasked => "partially_masked",
        metadata::ColumnAccess::Masked => "masked",
    }
}
fn discover(
    discovery: Discovery,
    access: &authz::MetadataAccess,
    full_schema: bool,
) -> Result<Table, ApiError> {
    if matches!(discovery, Discovery::Tables) && !full_schema {
        let rows = metadata::relations()
            .iter()
            .filter(|r| metadata::relation_plan(access, r.relation).is_ok())
            .map(|r| vec![text("duckfleet_system"), text(r.name), text(r.description)])
            .collect();
        return Ok(Table {
            columns: vec![
                column("schema", SystemType::Utf8, false),
                column("table", SystemType::Utf8, false),
                column("description", SystemType::Utf8, false),
            ],
            rows,
        });
    }
    let relations = match discovery {
        Discovery::Tables => metadata::relations()
            .iter()
            .filter(|r| metadata::relation_plan(access, r.relation).is_ok())
            .collect::<Vec<_>>(),
        Discovery::Columns(relation) => {
            metadata::relation_plan(access, relation)?;
            vec![metadata::relation_def(relation)]
        }
    };
    let mut rows = Vec::new();
    for relation in relations {
        for c in relation.columns {
            rows.push(vec![
                text(relation.name),
                text(c.name),
                text(system_result::type_name(ResultType::Scalar(c.ty))),
                Cell::Bool(c.nullable),
                c.units.map_or(Cell::Null, text),
                text(access_label(metadata::column_access(
                    access,
                    relation.relation,
                    c.id,
                )?)),
                text(c.description),
            ]);
        }
    }
    Ok(Table {
        columns: vec![
            column("table", SystemType::Utf8, false),
            column("column", SystemType::Utf8, false),
            column("type", SystemType::Utf8, false),
            column("nullable", SystemType::Bool, false),
            column("units", SystemType::Utf8, true),
            column("access", SystemType::Utf8, false),
            column("description", SystemType::Utf8, false),
        ],
        rows,
    })
}

fn denial_event(access: &Access, action: Action, target: &str) -> crate::audit::Event {
    crate::audit::Event::new(
        access,
        action,
        target,
        None,
        "denied",
        "permission_denied",
        Some(&Uuid::new_v4().to_string()),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    async fn silent_after_startup() -> (Service, JoinHandle<()>, tokio::sync::oneshot::Receiver<()>)
    {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (ready, received) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let size = socket.read_u32().await.unwrap();
            assert!((8..4096).contains(&size));
            let mut startup = vec![0; size as usize - 4];
            socket.read_exact(&mut startup).await.unwrap();
            socket
                .write_all(b"R\0\0\0\x08\0\0\0\0Z\0\0\0\x05I")
                .await
                .unwrap();
            assert_eq!(socket.read_u8().await.unwrap(), b'Q');
            let size = socket.read_u32().await.unwrap();
            assert!(size < 4096);
            let mut query = vec![0; size as usize - 4];
            socket.read_exact(&mut query).await.unwrap();
            ready.send(()).unwrap();
            // ACK TCP traffic but never answer the transaction command.
            let mut remaining = Vec::new();
            socket.read_to_end(&mut remaining).await.unwrap();
        });
        let service = Service::new(
            &format!(
                "host=127.0.0.1 port={} user=test dbname=test sslmode=disable",
                address.port()
            ),
            "bootstrap".into(),
            StorageSchema::validated("public").unwrap(),
        )
        .unwrap();
        (service, server, received)
    }

    #[tokio::test]
    async fn stalled_connected_driver_is_aborted_on_deadline() {
        let (service, server, ready) = silent_after_startup().await;
        let client_service = service.clone();
        let task = tokio::spawn(async move { client_service.authenticate("bootstrap").await });
        ready.await.unwrap();
        let result = tokio::time::timeout(Duration::from_secs(4), task)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(result.unwrap_err().0.kind, "system_query_timeout");
        tokio::time::timeout(Duration::from_secs(1), server)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(service.authentication.available_permits(), 7);
        tokio::time::pause();
        tokio::time::advance(DEADLINE + DEADLINE + Duration::from_secs(1)).await;
        tokio::task::yield_now().await;
        assert_eq!(service.authentication.available_permits(), 8);
    }

    #[tokio::test]
    async fn canceled_waiter_drops_connected_driver_and_its_admission() {
        let (service, server, ready) = silent_after_startup().await;
        let client_service = service.clone();
        let task = tokio::spawn(async move { client_service.authenticate("bootstrap").await });
        ready.await.unwrap();
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        tokio::time::timeout(Duration::from_secs(1), server)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(service.authentication.available_permits(), 7);
        tokio::time::pause();
        tokio::time::advance(DEADLINE + DEADLINE + Duration::from_secs(1)).await;
        tokio::task::yield_now().await;
        assert_eq!(service.authentication.available_permits(), 8);
    }
    #[test]
    fn version_one_manifest_matches_published_contract() {
        let relations = metadata::relations().iter().map(|r| json!({
            "name":r.name,"description":r.description,
            "time_column":metadata::column_def(r.relation,r.time_column).unwrap().name,
            "columns":r.columns.iter().map(|c|json!({"name":c.name,"type":system_result::type_name(ResultType::Scalar(c.ty)),"nullable":c.nullable,"sensitivity":format!("{:?}",c.sensitivity),"units":c.units,"description":c.description})).collect::<Vec<_>>()
        })).collect::<Vec<_>>();
        let actual = json!({"system_contract_version":metadata::SYSTEM_CONTRACT_VERSION,"relations":relations});
        let expected: Value =
            serde_json::from_str(include_str!("../../docs/system-schema-v1.json")).unwrap();
        assert_eq!(
            actual, expected,
            "review contract compatibility before updating the published manifest"
        );
    }
    #[tokio::test]
    async fn silent_startup_is_deadline_bounded_and_drops_socket() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut bytes = Vec::new();
            socket.read_to_end(&mut bytes).await.unwrap();
            assert!(!bytes.is_empty());
        });
        let service = Service::new(
            &format!(
                "host=127.0.0.1 port={} user=test dbname=test sslmode=disable",
                address.port()
            ),
            "bootstrap".into(),
            StorageSchema::validated("public").unwrap(),
        )
        .unwrap();
        let started = Instant::now();
        let result = service.authenticate("bootstrap").await;
        assert!(result.is_err());
        assert!(started.elapsed() < Duration::from_secs(4));
        tokio::time::timeout(Duration::from_secs(1), server)
            .await
            .unwrap()
            .unwrap();
        // No command was sent during the unfinished startup handshake.
        assert_eq!(service.authentication.available_permits(), 8);
    }
}
