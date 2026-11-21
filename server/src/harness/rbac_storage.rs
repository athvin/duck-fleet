//! Schema upgrade, metadata projection, and actual PostgreSQL role boundaries.
use crate::{
    ApiError,
    authz::{self, Action, PermissionGrant, PolicyUpdate, Role, Scope, TrustedOperation},
    credentials::{self, ActorKind, Identity},
    db_roles::{self, RoleNames, RuntimeRole},
    failure, history, metering, platform, pricing,
    query::{AdmissionLimits, Protocol, QueryRequest, WorkerLimits},
    queue::{self, SessionRegistration, SubmitResult, WorkerEpoch},
    requests, usage,
};
use anyhow::{Context, Result};
use postgres::{Client, Config, NoTls, error::SqlState};
use serde_json::{Value, json};
use std::time::{Duration, SystemTime};
use uuid::Uuid;

fn api<T>(result: Result<T, ApiError>) -> Result<T> {
    result.map_err(|e| anyhow::anyhow!("{}: {}", e.0.kind, e.1))
}
fn rate() -> pricing::RateCard {
    pricing::RateCard {
        version: "rbac-storage".into(),
        compute_micro_usd_per_unit_hour: 100_000,
    }
}
fn config(name: &str, schema: &str) -> Result<Config> {
    let mut config: Config = std::env::var(name)
        .with_context(|| format!("--rbac-storage requires {name}"))?
        .parse()?;
    config.connect_timeout(Duration::from_secs(3));
    config.options(&format!(
        "-c search_path={schema} -c statement_timeout=10000"
    ));
    Ok(config)
}
fn compute_grant() -> authz::ComputeGrant {
    authz::ComputeGrant {
        pool: Some("small".into()),
        mode: crate::compute::Mode::Persistent,
        cpu: 1,
        memory_bytes: 1 << 30,
        disk_bytes: 2 << 30,
    }
}
fn policy(
    client: &mut Client,
    name: &str,
    roles: Vec<Role>,
    permissions: Vec<PermissionGrant>,
) -> Result<()> {
    api(authz::set_policy(
        client,
        &Identity::bootstrap(),
        name,
        &PolicyUpdate {
            roles,
            permissions,
            compute: vec![compute_grant()],
            disabled: false,
        },
    ))?;
    Ok(())
}
fn identity(client: &mut Client, name: &str) -> Result<Identity> {
    let issued = credentials::issue(client, name, None)?;
    Ok(Identity {
        kind: ActorKind::Named,
        principal: name.into(),
        credential_id: Some(issued.credential_id),
        expires_at: None,
    })
}
fn session(
    client: &mut Client,
    epoch: &WorkerEpoch,
    actor: &Identity,
) -> Result<SessionRegistration> {
    let session = SessionRegistration {
        session_id: Uuid::new_v4(),
        epoch: epoch.clone(),
        principal: actor.principal.clone(),
        authority: actor.clone(),
        context: json!({"private":"sql-context-sentinel","requested_compute":{"pool":"small"}}),
        compute: json!({"mode":"persistent","pool":"small","cpu":1,"memory_bytes":1073741824_u64,"disk_bytes":2147483648_u64}),
        placement: json!({"worker_id":epoch.worker_id,"pool":"small","private":"placement-sentinel"}),
        scope: "persistent:small".into(),
        protocol: Protocol::Http,
        open_span_id: Uuid::new_v4(),
        operation: TrustedOperation::Sql,
    };
    api(queue::register_session(client, &session))?;
    Ok(session)
}
fn query() -> QueryRequest {
    QueryRequest {
        request_id: Uuid::new_v4(),
        retry_of: None,
        sql: "SELECT 'sql-text-sentinel'".into(),
        parameters: vec![],
        context: json!({}),
        timeout_seconds: 30,
        queue_timeout_seconds: 120,
    }
}
fn submit(
    client: &mut Client,
    session: &SessionRegistration,
    actor: &Identity,
    request: QueryRequest,
) -> Result<()> {
    let result = api(queue::submit(
        client,
        &session.epoch,
        session.session_id,
        request,
        actor,
        Protocol::Http,
    ))?;
    anyhow::ensure!(
        matches!(result, SubmitResult::Accepted { .. }),
        "query was not accepted: {result:?}"
    );
    Ok(())
}
fn finish(client: &mut Client, epoch: &WorkerEpoch, id: Uuid) -> Result<()> {
    let selected = api(queue::select_next(client, epoch))?.context("missing selected request")?;
    let prepared = api(queue::reserve(client, selected, &rate()))?;
    anyhow::ensure!(
        matches!(
            api(queue::grant(client, prepared))?,
            queue::GrantResult::Granted(_)
        ),
        "valid grant refused"
    );
    let mut tx = client.transaction()?;
    let changed = queue::finish_in(
        &mut tx,
        &queue::Finish {
            request_id: id,
            epoch,
            status: "succeeded",
            failure: None,
            outcome: requests::Outcome::StatementResult,
            elapsed_ms: 10,
            result_rows: Some(1),
            error: Some("error-text-sentinel"),
            engine_started_at: Some(SystemTime::now()),
            profile: Some(r#"{"query_name":"profile-sql-sentinel","latency":0.01}"#),
            profile_status: "captured",
            metrics: &json!({"latency":0.01,"cpu_time":0.005,"extra":"metric-secret-sentinel"}),
            source: "worker",
        },
    )?;
    anyhow::ensure!(changed, "terminal persistence did not own request");
    tx.commit()?;
    Ok(())
}
fn metadata(client: &mut Client, actor: &Identity, id: Uuid) -> Result<Value> {
    let mut tx = client.transaction()?;
    let access = api(authz::resolve_in(&mut tx, actor))?.metadata();
    let detail = api(history::detail_in(&mut tx, &access, id))?;
    tx.commit()?;
    Ok(detail)
}
fn denied(client: &mut Client, sql: &str) -> Result<()> {
    let error = client
        .batch_execute(sql)
        .expect_err("runtime database privilege unexpectedly allowed");
    anyhow::ensure!(
        error.code() == Some(&SqlState::INSUFFICIENT_PRIVILEGE),
        "expected PostgreSQL privilege denial, got {:?}",
        error.code()
    );
    Ok(())
}

pub(super) async fn checks() -> Result<()> {
    tokio::task::spawn_blocking(|| -> Result<()> {
        let schema = format!("df_rbac_{}", Uuid::new_v4().simple());
        let admin_config = config("DUCKFLEET_PLATFORM_TEST_DSN", &schema)?;
        let gateway_config = config("DUCKFLEET_GATEWAY_TEST_DSN", &schema)?;
        let worker_config = config("DUCKFLEET_WORKER_TEST_DSN", &schema)?;
        let maintainer_config = config("DUCKFLEET_MAINTAINER_TEST_DSN", &schema)?;
        let roles = RoleNames {
            gateway: gateway_config
                .get_user()
                .context("gateway role missing")?
                .into(),
            worker: worker_config
                .get_user()
                .context("worker role missing")?
                .into(),
            maintainer: maintainer_config
                .get_user()
                .context("maintainer role missing")?
                .into(),
        };
        let mut admin = admin_config.connect(NoTls)?;
        admin.batch_execute(&format!("CREATE SCHEMA {schema}"))?;
        let result = (|| -> Result<()> {
            upgrade(&mut admin)?;
            let mut tx = admin.transaction()?;
            db_roles::grant_runtime(&mut tx, &roles)?;
            tx.commit()?;
            let mut gateway = gateway_config.connect(NoTls)?;
            let mut worker = worker_config.connect(NoTls)?;
            let mut maintainer = maintainer_config.connect(NoTls)?;
            db_roles::validate_runtime(&mut gateway, RuntimeRole::Gateway)?;
            db_roles::validate_runtime(&mut worker, RuntimeRole::Worker)?;
            db_roles::validate_runtime(&mut maintainer, RuntimeRole::Maintainer)?;
            for client in [&mut gateway, &mut worker, &mut maintainer] {
                for table in ["usage_events", "audit_events"] {
                    denied(client, &format!("DELETE FROM {table}"))?;
                    denied(client, &format!("TRUNCATE {table}"))?;
                    denied(client, &format!("ALTER TABLE {table} DISABLE TRIGGER ALL"))?;
                }
                denied(client, "UPDATE audit_events SET reason='rewritten'")?;
                denied(client, "UPDATE usage_events SET quantity=0")?;
                denied(client, "CREATE TABLE unauthorized_ddl(id INTEGER)")?;
            }
            for client in [&mut worker, &mut maintainer] {
                denied(client, "SELECT token_sha256 FROM credentials")?;
                denied(client, "UPDATE principals SET disabled=true")?;
                denied(client, "DELETE FROM permission_grants")?;
                denied(client, "UPDATE budget SET prepaid_micro_usd=0")?;
            }
            fixtures(
                &mut admin,
                &mut gateway,
                &mut worker,
                &mut maintainer,
                &roles,
            )?;
            Ok(())
        })();
        let cleanup = admin.batch_execute(&format!("DROP SCHEMA {schema} CASCADE"));
        result?;
        cleanup?;
        Ok(())
    })
    .await
    .context("RBAC storage fixture thread")??;
    println!(
        "PASS: schema2 upgrade, stable owners, scoped metadata, policy revocation, immutable attribution, and actual runtime database privileges"
    );
    Ok(())
}

fn upgrade(admin: &mut Client) -> Result<()> {
    let seed = platform::Seed {
        prepaid_micro_usd: 10_000_000,
        max_job_micro_usd: 1_000_000,
    };
    platform::migrate(admin, &seed)?;
    super::platform::remove_authz_fixture_schema(admin)?;
    // Reconstruct a genuine schema2 credential dictionary before migrating3.
    admin.execute("INSERT INTO credentials(credential_id,principal,token_sha256) VALUES($1,'legacy-owner',$2)",&[&Uuid::new_v4().to_string(),&"a".repeat(64)])?;
    for (name, id) in [
        ("legacy-owner", "00000000-0000-4000-8000-000000000001"),
        ("direct", "00000000-0000-4000-8000-000000000002"),
    ] {
        admin.execute("INSERT INTO requests(request_id,principal,logical_request_id,state,status,outcome,worker_id) VALUES($1,$2,$1,'terminal','succeeded','statement_result','legacy')",&[&id,&name])?;
        admin.execute("INSERT INTO query_history(query_id,session_id,sql,context,status,worker_id,placement,principal) VALUES($1,'old','legacy-sql','{}','succeeded','legacy','{}',$2)",&[&id,&name])?;
    }
    let before: String = admin
        .query_one("SELECT to_jsonb(b)::text FROM budget b", &[])?
        .get(0);
    let report = platform::migrate(admin, &seed)?;
    anyhow::ensure!(
        report.applied && report.version == platform::SCHEMA_VERSION,
        "schema2 upgrade did not apply schema3"
    );
    anyhow::ensure!(
        admin
            .query_one("SELECT to_jsonb(b)::text FROM budget b", &[])?
            .get::<_, String>(0)
            == before,
        "RBAC upgrade changed budget"
    );
    anyhow::ensure!(
        admin
            .query_one(
                "SELECT COUNT(*)::BIGINT FROM authz_legacy_owners WHERE legacy_name='legacy-owner'",
                &[]
            )?
            .get::<_, i64>(0)
            == 1,
        "credible legacy owner was not mapped"
    );
    anyhow::ensure!(
        admin
            .query_one(
                "SELECT COUNT(*)::BIGINT FROM authz_legacy_owners WHERE legacy_name='direct'",
                &[]
            )?
            .get::<_, i64>(0)
            == 0,
        "unknown legacy direct was attributed"
    );
    let roles = admin
        .query_one("SELECT COUNT(*)::BIGINT FROM principal_roles", &[])?
        .get::<_, i64>(0);
    admin.execute("DELETE FROM principal_roles", &[])?;
    anyhow::ensure!(
        !platform::migrate(admin, &seed)?.applied,
        "repeat migration was not read-only"
    );
    anyhow::ensure!(
        roles > 0
            && admin
                .query_one("SELECT COUNT(*)::BIGINT FROM principal_roles", &[])?
                .get::<_, i64>(0)
                == 0,
        "repeat migration reseeded revoked grants"
    );
    Ok(())
}

fn fixtures(
    admin: &mut Client,
    gateway: &mut Client,
    worker: &mut Client,
    maintainer: &mut Client,
    roles: &RoleNames,
) -> Result<()> {
    let alice = identity(gateway, "alice")?;
    let bob = identity(gateway, "bob")?;
    let viewer = identity(gateway, "viewer")?;
    let direct = identity(gateway, "direct")?;
    for actor in [&alice, &bob, &direct] {
        policy(gateway, &actor.principal, vec![Role::Analyst], vec![])?;
    }
    policy(
        gateway,
        "viewer",
        vec![],
        vec![
            PermissionGrant {
                action: Action::HistoryRead,
                scope: Scope::All,
            },
            PermissionGrant {
                action: Action::UsageRead,
                scope: Scope::All,
            },
            PermissionGrant {
                action: Action::ProfileRead,
                scope: Scope::All,
            },
        ],
    )?;
    let epoch = WorkerEpoch {
        worker_id: "rbac-fixture".into(),
        boot_id: Uuid::new_v4(),
    };
    queue::publish_policies(
        gateway,
        &queue::PolicyPublication {
            publication_id: Uuid::new_v4(),
            digest: "rbac-fixture".into(),
            policies: vec![("persistent:small".into(), AdmissionLimits::default())],
        },
    )?;
    queue::register_worker(
        worker,
        &epoch,
        "persistent:small",
        WorkerLimits {
            max_queued_queries: 64,
            max_queued_bytes: 16 * 1024 * 1024,
        },
    )?;
    open_timing(worker, gateway, &epoch, &alice)?;
    let sa = session(worker, &epoch, &alice)?;
    let sb = session(worker, &epoch, &bob)?;
    let qa = query();
    submit(worker, &sa, &alice, qa.clone())?;
    finish(worker, &epoch, qa.request_id)?;
    let qb = query();
    submit(worker, &sb, &bob, qb.clone())?;
    finish(worker, &epoch, qb.request_id)?;
    let own = metadata(gateway, &alice, qa.request_id)?;
    anyhow::ensure!(
        own["sql"] == qa.sql && own["context"]["private"] == "sql-context-sentinel",
        "analyst lost own SQL projection"
    );
    let hidden = metadata(gateway, &viewer, qa.request_id)?;
    for sentinel in [
        "sql-text-sentinel",
        "sql-context-sentinel",
        "error-text-sentinel",
        "profile-sql-sentinel",
        "metric-secret-sentinel",
        "placement-sentinel",
    ] {
        anyhow::ensure!(
            !hidden.to_string().contains(sentinel),
            "metadata-only projection leaked {sentinel}"
        );
    }
    anyhow::ensure!(
        hidden["profile"].is_null() && hidden["metrics"]["latency"] == 0.01,
        "profile-only grant exposed raw SQL or lost numeric metric"
    );
    let mut tx = gateway.transaction()?;
    let access = api(authz::resolve_in(&mut tx, &alice))?.metadata();
    let list = api(history::list_in(
        &mut tx,
        &access,
        history::Filter {
            limit: 1,
            ..Default::default()
        },
    ))?;
    anyhow::ensure!(
        list["queries"][0]["query_id"] == qa.request_id.to_string()
            && list["next_before"].is_null(),
        "own filtering occurred after pagination"
    );
    anyhow::ensure!(
        history::detail_in(&mut tx, &access, qb.request_id)
            .is_err_and(|e| e.0 == failure::NOT_FOUND),
        "foreign history leaked"
    );
    anyhow::ensure!(
        requests::detail_in(&mut tx, &access, qb.request_id)
            .is_err_and(|e| e.0 == failure::NOT_FOUND),
        "foreign attempt leaked"
    );
    let events = api(usage::events_in(&mut tx, &access, usage::Filter::default()))?;
    anyhow::ensure!(
        events["events"].as_array().is_some_and(|v| v.len() == 1)
            && events["events"][0]["actor_credential_id"].as_str()
                == alice.credential_id.as_deref(),
        "owner usage or actor attribution incorrect"
    );
    let view = api(authz::resolve_in(&mut tx, &viewer))?.metadata();
    anyhow::ensure!(
        history::list_in(
            &mut tx,
            &view,
            history::Filter {
                model_id: Some("secret-guess".into()),
                ..Default::default()
            }
        )
        .is_err_and(|e| e.0 == failure::FORBIDDEN),
        "hidden context filter allowed an existence probe"
    );
    tx.commit()?;
    policy(
        gateway,
        "viewer",
        vec![],
        vec![
            PermissionGrant {
                action: Action::HistoryRead,
                scope: Scope::All,
            },
            PermissionGrant {
                action: Action::SqlRead,
                scope: Scope::All,
            },
        ],
    )?;
    let sql_only = metadata(gateway, &viewer, qa.request_id)?;
    anyhow::ensure!(
        sql_only["sql"] == qa.sql && sql_only["profile"].is_null(),
        "SQL grant implicitly granted profile"
    );
    policy(
        gateway,
        "viewer",
        vec![],
        vec![
            PermissionGrant {
                action: Action::HistoryRead,
                scope: Scope::All,
            },
            PermissionGrant {
                action: Action::SqlRead,
                scope: Scope::All,
            },
            PermissionGrant {
                action: Action::ProfileRead,
                scope: Scope::All,
            },
        ],
    )?;
    anyhow::ensure!(
        metadata(gateway, &viewer, qa.request_id)?["profile"]["query_name"]
            == "profile-sql-sentinel",
        "combined SQL/profile grant missing"
    );
    stable_owners(worker, gateway, &epoch, &direct, &sa, &alice, qa.request_id)?;
    revocation(worker, gateway, &epoch, &sa, &alice)?;
    revoked_credential_audit(admin, worker, gateway, &epoch, &alice, roles)?;
    audit_atomicity(admin, worker, &epoch, &sa, &alice, &bob, roles)?;
    retained_attribution(admin, worker, gateway, maintainer, &epoch, &sa, &alice)?;
    let report = platform::maintenance_pass(
        maintainer,
        Uuid::new_v4(),
        &platform::MaintenanceConfig::default(),
        true,
    )?;
    anyhow::ensure!(
        report.tasks.iter().all(|task| task.status == "succeeded"),
        "actual maintainer role cannot maintain platform"
    );
    Ok(())
}

fn open_timing(
    worker: &mut Client,
    gateway: &mut Client,
    epoch: &WorkerEpoch,
    actor: &Identity,
) -> Result<()> {
    // Worker registration can succeed even if gateway begin_open was unavailable.
    let missing_start = session(worker, epoch, actor)?;
    queue::finish_open(
        gateway,
        missing_start.open_span_id,
        Some(missing_start.session_id),
        999,
        Some("unavailable"),
    )?;
    let row=gateway.query_one("SELECT provisioning_started_at IS NULL,provisioning_ms IS NULL,finished_at IS NULL,opened_at IS NOT NULL,error FROM session_events WHERE open_span_id=$1", &[&missing_start.open_span_id.to_string()])?;
    anyhow::ensure!(
        row.get::<_, bool>(0)
            && row.get::<_, bool>(1)
            && row.get::<_, bool>(2)
            && row.get::<_, bool>(3)
            && row.get::<_, String>(4) == "unavailable",
        "missing gateway start fabricated provisioning time or lost backend observation"
    );

    let mut opened = missing_start.clone();
    opened.session_id = Uuid::new_v4();
    opened.open_span_id = Uuid::new_v4();
    queue::begin_open(
        gateway,
        opened.open_span_id,
        &actor.principal,
        &opened.scope,
    )?;
    api(queue::register_session(worker, &opened))?;
    queue::finish_open(
        gateway,
        opened.open_span_id,
        Some(opened.session_id),
        37,
        None,
    )?;
    let row=gateway.query_one("SELECT session_id,provisioning_started_at IS NOT NULL,provisioning_ms,finished_at IS NOT NULL,opened_at IS NOT NULL,error IS NULL FROM session_events WHERE open_span_id=$1", &[&opened.open_span_id.to_string()])?;
    anyhow::ensure!(
        row.get::<_, String>(0) == opened.session_id.to_string()
            && row.get::<_, bool>(1)
            && row.get::<_, i64>(2) == 37
            && row.get::<_, bool>(3)
            && row.get::<_, bool>(4)
            && row.get::<_, bool>(5),
        "actual gateway role did not persist successful provisioning duration"
    );

    let refused = Uuid::new_v4();
    queue::begin_open(gateway, refused, &actor.principal, &opened.scope)?;
    queue::finish_open(gateway, refused, None, 0, Some("unavailable"))?;
    let row=gateway.query_one("SELECT session_id IS NULL,provisioning_ms,finished_at IS NOT NULL,opened_at IS NULL,error FROM session_events WHERE open_span_id=$1", &[&refused.to_string()])?;
    anyhow::ensure!(
        row.get::<_, bool>(0)
            && row.get::<_, i64>(1) == 0
            && row.get::<_, bool>(2)
            && row.get::<_, bool>(3)
            && row.get::<_, String>(4) == "unavailable",
        "failed open lost zero elapsed timing or fabricated a backend session"
    );
    Ok(())
}

fn stable_owners(
    worker: &mut Client,
    gateway: &mut Client,
    epoch: &WorkerEpoch,
    direct: &Identity,
    alice_session: &SessionRegistration,
    alice: &Identity,
    alice_id: Uuid,
) -> Result<()> {
    let credible = identity(gateway, "legacy-owner")?;
    policy(gateway, "legacy-owner", vec![Role::Analyst], vec![])?;
    let legacy_session = session(worker, epoch, &credible)?;
    let mut credible_replay = query();
    credible_replay.request_id = "00000000-0000-4000-8000-000000000001".parse()?;
    anyhow::ensure!(
        matches!(
            api(queue::submit(
                worker,
                epoch,
                legacy_session.session_id,
                credible_replay,
                &credible,
                Protocol::Http
            ))?,
            SubmitResult::Duplicate(_)
        ),
        "credible pre-upgrade owner lost retained identity"
    );
    let sd = session(worker, epoch, direct)?;
    let mut old = query();
    old.request_id = "00000000-0000-4000-8000-000000000002".parse()?;
    for sql in ["legacy-sql", "different-payload"] {
        old.sql = sql.into();
        anyhow::ensure!(
            queue::submit(
                worker,
                epoch,
                sd.session_id,
                old.clone(),
                direct,
                Protocol::Http
            )
            .is_err_and(|e| e.0 == failure::NOT_FOUND),
            "named direct learned unknown legacy attempt"
        );
    }
    policy(
        gateway,
        "direct",
        vec![Role::Analyst],
        vec![
            PermissionGrant {
                action: Action::HistoryRead,
                scope: Scope::All,
            },
            PermissionGrant {
                action: Action::SqlRead,
                scope: Scope::All,
            },
        ],
    )?;
    old.request_id = alice_id;
    anyhow::ensure!(
        queue::submit(
            worker,
            epoch,
            sd.session_id,
            old.clone(),
            direct,
            Protocol::Http
        )
        .is_err_and(|e| e.0 == failure::NOT_FOUND),
        "all-history grant allowed foreign duplicate takeover"
    );
    old.request_id = Uuid::new_v4();
    old.retry_of = Some(alice_id);
    anyhow::ensure!(
        queue::submit(worker, epoch, sd.session_id, old, direct, Protocol::Http)
            .is_err_and(|e| e.0 == failure::NOT_FOUND),
        "foreign retry parent disclosed lineage"
    );
    let own = query();
    submit(worker, &sd, direct, own.clone())?;
    finish(worker, epoch, own.request_id)?;
    let mut replay = query();
    replay.request_id = alice_id;
    anyhow::ensure!(
        matches!(
            api(queue::submit(
                worker,
                epoch,
                alice_session.session_id,
                replay,
                alice,
                Protocol::Http
            ))?,
            SubmitResult::Duplicate(_)
        ),
        "stable own duplicate rejected"
    );
    Ok(())
}

fn revocation(
    worker: &mut Client,
    gateway: &mut Client,
    epoch: &WorkerEpoch,
    s: &SessionRegistration,
    alice: &Identity,
) -> Result<()> {
    let q = query();
    submit(worker, s, alice, q.clone())?;
    let selected = api(queue::select_next(worker, epoch))?.context("revocation selection")?;
    let prepared = api(queue::reserve(worker, selected, &rate()))?;
    policy(gateway, "alice", vec![Role::Viewer], vec![])?;
    anyhow::ensure!(
        matches!(
            api(queue::grant(worker, prepared))?,
            queue::GrantResult::Refused { .. }
        ),
        "revoked accepted work executed"
    );
    let row=worker.query_one("SELECT r.outcome,r.failure_kind,u.quantity,u.amount_micro_usd FROM requests r JOIN usage_events u ON u.execution_id=r.request_id WHERE r.request_id=$1",&[&q.request_id.to_string()])?;
    anyhow::ensure!(
        row.get::<_, String>(0) == "not_executed"
            && row.get::<_, String>(1) == "forbidden"
            && row.get::<_, i64>(2) == 0
            && row.get::<_, i64>(3) == 0,
        "policy refusal did not settle zero atomically"
    );
    let stale = queue::authorities(worker, epoch)?
        .into_iter()
        .find(|a| a.session_id == s.session_id)
        .context("authority missing")?;
    policy(gateway, "alice", vec![Role::Analyst], vec![])?;
    anyhow::ensure!(
        !api(queue::begin_close(
            worker,
            s.session_id,
            epoch,
            &queue::CloseIntent::Policy {
                authority_generation: stale.generation,
                policy_version: stale.authority_policy_version
            }
        ))?,
        "stale policy cleanup closed a regranted session"
    );
    Ok(())
}

fn revoked_credential_audit(
    admin: &mut Client,
    worker: &mut Client,
    gateway: &mut Client,
    epoch: &WorkerEpoch,
    opener: &Identity,
    roles: &RoleNames,
) -> Result<()> {
    let s = session(worker, epoch, opener)?;
    let rotated = identity(gateway, &opener.principal)?;
    let q = query();
    submit(worker, &s, &rotated, q.clone())?;
    let selected =
        api(queue::select_next(worker, epoch))?.context("revoked credential selection")?;
    let prepared = api(queue::reserve(worker, selected, &rate()))?;
    api(credentials::revoke_authorized(
        gateway,
        &Identity::bootstrap(),
        rotated.credential_id.as_deref().context("rotated ID")?,
    ))?;
    anyhow::ensure!(
        matches!(api(queue::grant(worker, prepared))?,queue::GrantResult::Refused{error,..} if error.0==failure::UNAUTHORIZED),
        "revoked accepted credential was not refused"
    );
    let rows = gateway.query("SELECT a.actor_principal_id,a.actor_kind,a.actor_principal,a.credential_id,a.namespace_id,a.policy_version,r.actor_principal_id,r.namespace_id,r.authorization_policy_version
        FROM audit_events a JOIN requests r ON r.request_id=a.target_id WHERE a.target_type='request' AND a.target_id=$1 AND a.decision='denied' AND a.reason='unauthorized' AND a.action='query.execute'", &[&q.request_id.to_string()])?;
    anyhow::ensure!(
        rows.len() == 1,
        "revoked accepted request must append one canonical denial audit"
    );
    let row = &rows[0];
    anyhow::ensure!(
        row.get::<_, Option<String>>(0) == row.get::<_, Option<String>>(6)
            && row.get::<_, String>(1) == "named"
            && row.get::<_, String>(2) == opener.principal
            && row.get::<_, Option<String>>(3) == rotated.credential_id
            && row.get::<_, Option<String>>(3) != opener.credential_id
            && row.get::<_, String>(4) == row.get::<_, String>(7)
            && row.get::<_, i64>(5) == 0
            && row.get::<_, i64>(8) > 0,
        "denial audit lost the accepted rotated actor/namespace or fabricated a current policy version"
    );
    let row=worker.query_one("SELECT r.outcome,u.quantity,u.amount_micro_usd,u.actor_credential_id FROM requests r JOIN usage_events u ON u.execution_id=r.request_id WHERE r.request_id=$1", &[&q.request_id.to_string()])?;
    anyhow::ensure!(
        row.get::<_, String>(0) == "not_executed"
            && row.get::<_, i64>(1) == 0
            && row.get::<_, i64>(2) == 0
            && row.get::<_, Option<String>>(3) == rotated.credential_id,
        "revoked queued actor did not retain zero-cost terminal attribution"
    );
    // A failed denial append must not abort known no-execution settlement.
    let unavailable_actor = identity(gateway, &opener.principal)?;
    let denied_query = query();
    submit(worker, &s, &unavailable_actor, denied_query.clone())?;
    let selected = api(queue::select_next(worker, epoch))?.context("audit failure selection")?;
    let prepared = api(queue::reserve(worker, selected, &rate()))?;
    api(credentials::revoke_authorized(
        gateway,
        &Identity::bootstrap(),
        unavailable_actor
            .credential_id
            .as_deref()
            .context("actor ID")?,
    ))?;
    admin.batch_execute(&format!(
        "REVOKE INSERT ON audit_events FROM {}",
        roles.worker
    ))?;
    let denied = queue::grant(worker, prepared);
    admin.batch_execute(&format!("GRANT INSERT ON audit_events TO {}", roles.worker))?;
    anyhow::ensure!(
        matches!(api(denied)?,queue::GrantResult::Refused{error,..} if error.0==failure::UNAUTHORIZED),
        "audit outage replaced the authoritative credential refusal"
    );
    let row=worker.query_one("SELECT r.outcome,u.quantity,u.amount_micro_usd FROM requests r JOIN usage_events u ON u.execution_id=r.request_id WHERE r.request_id=$1", &[&denied_query.request_id.to_string()])?;
    anyhow::ensure!(
        row.get::<_, String>(0) == "not_executed"
            && row.get::<_, i64>(1) == 0
            && row.get::<_, i64>(2) == 0,
        "audit outage prevented zero-cost terminal settlement"
    );
    Ok(())
}

fn audit_atomicity(
    admin: &mut Client,
    worker: &mut Client,
    epoch: &WorkerEpoch,
    s: &SessionRegistration,
    alice: &Identity,
    foreign: &Identity,
    roles: &RoleNames,
) -> Result<()> {
    let q = query();
    submit(worker, s, alice, q.clone())?;
    admin.batch_execute(&format!(
        "REVOKE INSERT ON audit_events FROM {}",
        roles.worker
    ))?;
    let hidden = queue::request_cancel(worker, q.request_id, foreign);
    let absent = queue::request_cancel(worker, Uuid::new_v4(), foreign);
    let refusal = queue::request_cancel(worker, q.request_id, alice);
    admin.batch_execute(&format!("GRANT INSERT ON audit_events TO {}", roles.worker))?;
    anyhow::ensure!(
        hidden.is_err_and(|e| e.0 == failure::NOT_FOUND)
            && absent.is_err_and(|e| e.0 == failure::NOT_FOUND),
        "unavailable denial audit disclosed foreign request existence"
    );
    anyhow::ensure!(
        refusal.is_err(),
        "cancel committed without required audit record"
    );
    anyhow::ensure!(
        !worker
            .query_one(
                "SELECT cancel_requested FROM query_controls WHERE request_id=$1",
                &[&q.request_id.to_string()]
            )?
            .get::<_, bool>(0),
        "failed audit left cancellation side effect"
    );
    api(queue::request_cancel(worker, q.request_id, alice))?;
    queue::poll_control(worker, epoch, false, 64)?;
    Ok(())
}

fn retained_attribution(
    admin: &mut Client,
    worker: &mut Client,
    gateway: &mut Client,
    maintainer: &mut Client,
    epoch: &WorkerEpoch,
    s: &SessionRegistration,
    alice: &Identity,
) -> Result<()> {
    let rotated = identity(gateway, &alice.principal)?;
    let q = query();
    submit(worker, s, &rotated, q.clone())?;
    let selected = api(queue::select_next(worker, epoch))?.context("rotation selection")?;
    let prepared = api(queue::reserve(worker, selected, &rate()))?;
    anyhow::ensure!(
        matches!(
            api(queue::grant(worker, prepared))?,
            queue::GrantResult::Granted(_)
        ),
        "rotated authority refused"
    );
    let authority = queue::authorities(worker, epoch)?
        .into_iter()
        .find(|a| a.session_id == s.session_id)
        .context("rotated authority missing")?;
    anyhow::ensure!(
        authority.identity.credential_id == rotated.credential_id,
        "queued rotation did not retain current actor"
    );
    // Recovery must use retained reservation attribution even if old diagnostics
    // are absent. This deliberately reconstructs a historical missing-row case.
    admin.execute(
        "DELETE FROM query_history WHERE query_id=$1",
        &[&q.request_id.to_string()],
    )?;
    let mut tx = maintainer.transaction()?;
    anyhow::ensure!(
        metering::recover(&mut tx, &epoch.worker_id, &rate())? == 1,
        "held recovery missing"
    );
    tx.commit()?;
    let row=admin.query_one("SELECT actor_credential_id,actor_principal_id,owner_principal_id,source FROM usage_events WHERE execution_id=$1",&[&q.request_id.to_string()])?;
    anyhow::ensure!(
        row.get::<_, Option<String>>(0) == rotated.credential_id
            && row.get::<_, Option<String>>(1) == row.get::<_, Option<String>>(2)
            && row.get::<_, String>(3) == "recovery",
        "history pruning changed actor or recovery attribution"
    );
    // This synthetic case cannot be fed back to ordinary queue recovery, which
    // correctly requires protected nonterminal history. Remove only its fixture
    // permit and mark its durable state terminal after validating the hold.
    admin.execute("UPDATE requests SET state='terminal',status='interrupted',outcome='unknown' WHERE request_id=$1",&[&q.request_id.to_string()])?;
    admin.execute(
        "DELETE FROM query_permits WHERE request_id=$1",
        &[&q.request_id.to_string()],
    )?;
    admin.execute(
        "UPDATE queue_sessions SET outstanding=NULL WHERE session_id=$1",
        &[&s.session_id.to_string()],
    )?;
    Ok(())
}
