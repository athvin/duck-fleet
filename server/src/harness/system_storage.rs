//! Real PostgreSQL migration, capture, shared projection, and auth-adapter checks.
use crate::{
    ApiError,
    authz::{self, Action, PermissionGrant, PolicyUpdate, Role, Scope, TrustedOperation},
    compute::{ComputeRequest, Mode, Placement, Resources},
    credentials::{self, ActorKind, Identity},
    db_roles::{self, RoleNames},
    history,
    metadata::{self, ColumnId, ColumnUses, Relation, SourceBounds, StorageSchema},
    platform, pricing,
    query::{Protocol, QueryRequest, WorkerLimits},
    queue::{self, SessionRegistration, WorkerEpoch},
    requests, system_sql, usage,
};
use anyhow::{Context, Result};
use postgres::{Client, Config, NoTls, Row};
use serde_json::json;
use std::{
    collections::BTreeSet,
    time::{Duration, SystemTime},
};
use uuid::Uuid;
#[path = "system_admission.rs"]
mod system_admission;
#[path = "system_auth.rs"]
mod system_auth;
fn api<T>(result: Result<T, ApiError>) -> Result<T> {
    result.map_err(|e| anyhow::anyhow!("{}: {}", e.0.kind, e.1))
}
fn config(dsn: &str, schema: &str) -> Result<Config> {
    let mut c: Config = dsn.parse()?;
    c.connect_timeout(Duration::from_secs(3));
    c.options(&format!(
        "-c search_path={schema} -c statement_timeout=10000"
    ));
    Ok(c)
}
fn rate() -> pricing::RateCard {
    pricing::RateCard {
        version: "system-storage".into(),
        compute_micro_usd_per_unit_hour: 100_000,
    }
}
fn identity(client: &mut Client, name: &str) -> Result<(Identity, String)> {
    let issued = credentials::issue(client, name, None)?;
    Ok((
        Identity {
            principal: name.into(),
            credential_id: Some(issued.credential_id),
            expires_at: None,
            kind: ActorKind::Named,
        },
        credentials::digest(&issued.token),
    ))
}
fn policy(
    client: &mut Client,
    who: &Identity,
    roles: Vec<Role>,
    permissions: Vec<PermissionGrant>,
    disabled: bool,
) -> Result<()> {
    api(authz::set_policy(
        client,
        &Identity::bootstrap(),
        &who.principal,
        &PolicyUpdate {
            roles,
            permissions,
            disabled,
            compute: vec![authz::ComputeGrant {
                job_limits: None,
                pool: Some("small".into()),
                mode: Mode::Persistent,
                cpu: 1,
                memory_bytes: 1 << 30,
                disk_bytes: 2 << 30,
            }],
        },
    ))?;
    Ok(())
}
fn access(client: &mut Client, identity: &Identity) -> Result<authz::MetadataAccess> {
    let mut tx = client.transaction()?;
    let access = api(authz::resolve_in(&mut tx, identity))?;
    tx.commit()?;
    Ok(access.metadata())
}
fn source(
    client: &mut Client,
    schema: &str,
    access: &authz::MetadataAccess,
    relation: Relation,
    ids: &[String],
    columns: &[ColumnId],
) -> Result<Vec<Row>> {
    let uses = ColumnUses {
        projected: columns.iter().copied().collect(),
        evaluated: BTreeSet::new(),
    };
    let source = api(metadata::source_plan(
        access,
        relation,
        &SourceBounds::RequestIds(ids.to_vec()),
        &uses,
        &api(StorageSchema::validated(schema))?,
    ))?;
    let probe = source.cap_probe(100);
    let cap = client.query_one(probe.sql(), &probe.params())?;
    anyhow::ensure!(
        cap.get::<_, i64>("eligible_rows") <= 100
            && cap.get::<_, i64>("selected_payload_bytes") <= 8 * 1024 * 1024,
        "source probe exceeded fixture bounds"
    );
    let mut bindings = metadata::Bindings::new();
    let cte = source.cte_sql(0, &mut bindings);
    let query = metadata::BoundSql::new(
        format!("WITH {cte} SELECT * FROM s0 ORDER BY request_id"),
        bindings,
    );
    Ok(client.query(query.sql(), &query.params())?)
}
fn session(
    client: &mut Client,
    epoch: &WorkerEpoch,
    actor: &Identity,
) -> Result<SessionRegistration> {
    let original: ComputeRequest = serde_json::from_value(json!({"pool":"small"}))?;
    let placement = Placement {
        mode: Mode::Persistent,
        pool: Some("small".into()),
        resources: Resources::default(),
        authority: crate::compute::Authority::Writer,
    };
    let dimensions = api(metadata::capture_session(
        &original,
        &placement,
        "v1.5.4",
        Some("fixture-revision"),
    ))?;
    let s = SessionRegistration {
        session_id: Uuid::new_v4(),
        epoch: epoch.clone(),
        principal: actor.principal.clone(),
        authority: actor.clone(),
        context: json!({"requested_compute":{"pool":"small"},"dbt":{"node_id":format!("model.{}",actor.principal),"invocation_id":"invocation-secret"}}),
        compute: json!({"mode":"persistent","pool":"small","cpu":1,"memory_bytes":1073741824_u64,"disk_bytes":2147483648_u64}),
        placement: json!({"worker_id":epoch.worker_id}),
        scope: "persistent:small".into(),
        protocol: Protocol::Http,
        open_span_id: Uuid::new_v4(),
        operation: TrustedOperation::Sql,
    };
    api(queue::register_session_captured(client, &s, &dimensions))?;
    Ok(s)
}
fn submit(client: &mut Client, s: &SessionRegistration, actor: &Identity) -> Result<Uuid> {
    let id = Uuid::new_v4();
    let q = QueryRequest {
        request_id: id,
        retry_of: None,
        sql: format!("SELECT '{}'", actor.principal),
        parameters: vec![],
        context: json!({}),
        timeout_seconds: 30,
        queue_timeout_seconds: 120,
    };
    anyhow::ensure!(
        matches!(
            api(queue::submit(
                client,
                &s.epoch,
                s.session_id,
                q,
                actor,
                Protocol::Http
            ))?,
            queue::SubmitResult::Accepted { .. }
        ),
        "fixture query was not accepted"
    );
    Ok(id)
}
fn complete(client: &mut Client, s: &SessionRegistration, actor: &Identity) -> Result<Uuid> {
    let id = submit(client, s, actor)?;
    let selected =
        api(queue::select_next(client, &s.epoch))?.context("fixture selection missing")?;
    let prepared = api(queue::reserve(client, selected, &rate()))?;
    anyhow::ensure!(
        matches!(
            api(queue::grant(client, prepared))?,
            queue::GrantResult::Granted(_)
        ),
        "fixture grant refused"
    );
    let mut tx = client.transaction()?;
    anyhow::ensure!(
        queue::finish_in(
            &mut tx,
            &queue::Finish {
                request_id: id,
                epoch: &s.epoch,
                status: "succeeded",
                failure: None,
                outcome: requests::Outcome::StatementResult,
                elapsed_ms: 500,
                result_rows: Some(1),
                error: None,
                engine_started_at: Some(SystemTime::now()),
                profile: Some("{\"query_name\":\"secret-profile\",\"latency\":0.5}"),
                profile_status: "captured",
                metrics: &json!({"latency":0.5,"cpu_time":0.25,"rows_returned":1,"total_bytes_read":64}),
                source: "worker"
            }
        )?,
        "terminal was not persisted"
    );
    tx.commit()?;
    Ok(id)
}
fn resource_bounds(
    admin: &mut Client,
    gateway: &mut Client,
    schema: &str,
    alice: &authz::MetadataAccess,
    bootstrap: &authz::MetadataAccess,
    alice_id: Uuid,
    bob_id: Uuid,
) -> Result<()> {
    let storage = api(StorageSchema::validated(schema))?;
    let probe_for = |access: &authz::MetadataAccess,
                     bounds: SourceBounds,
                     columns: &[ColumnId],
                     limit: u32|
     -> Result<metadata::BoundSql> {
        Ok(api(metadata::source_plan(
            access,
            Relation::History,
            &bounds,
            &ColumnUses {
                projected: columns.iter().copied().collect(),
                evaluated: BTreeSet::new(),
            },
            &storage,
        ))?
        .cap_probe(limit))
    };
    // Foreign raw payloads are not charged against an operator's materialization
    // budget: they are SQL NULL in both the probe and the materialized source.
    let masked = probe_for(
        alice,
        SourceBounds::RequestIds(vec![bob_id.to_string()]),
        &[
            ColumnId::SqlText,
            ColumnId::ProfileJson,
            ColumnId::ApplicationContext,
        ],
        100,
    )?;
    let masked = gateway.query_one(masked.sql(), &masked.params())?;
    anyhow::ensure!(
        masked.get::<_, i64>("eligible_rows") == 1
            && masked.get::<_, i64>("selected_payload_bytes") == 0,
        "masked SQL/profile bytes entered a visible source budget"
    );

    // Repeating one legal payload must fail its preflight certificate. Do not
    // execute plan.query here: passing the probes is the only path to that SQL.
    let saved = admin.query_one(
        "SELECT sql,profile FROM query_history WHERE query_id=$1",
        &[&alice_id.to_string()],
    )?;
    let original_sql: String = saved.get(0);
    let original_profile: Option<String> = saved.get(1);
    let large_sql = format!("SELECT 1 /*{}*/", "x".repeat(250 * 1024));
    let large_profile =
        json!({"query_name":"private", "padding":"x".repeat(1000 * 1024)}).to_string();
    admin.execute(
        "UPDATE query_history SET sql=$2,profile=$3 WHERE query_id=$1",
        &[&alice_id.to_string(), &large_sql, &large_profile],
    )?;
    for column in ["sql_text", "profile_json"] {
        let outputs = (0..32)
            .map(|i| format!("{column} AS repeated_{i}"))
            .collect::<Vec<_>>()
            .join(",");
        let statement = api(system_sql::compile(
            &format!("SELECT {outputs} FROM query_history WHERE request_id=$1"),
            &[json!(alice_id.to_string())],
            None,
        ))?;
        let system_sql::Statement::Select(plan) = statement else {
            anyhow::bail!("repeated-payload SELECT parsed as discovery")
        };
        let executable = api(system_sql::render(&plan, bootstrap, &storage))?;
        let probe = &executable.probes[0];
        let row = gateway.query_one(probe.query.sql(), &probe.query.params())?;
        anyhow::ensure!(
            row.get::<_, i64>("eligible_rows") == 1
                && row.get::<_, i64>("selected_payload_bytes") > probe.payload_limit as i64,
            "repeated {column} escaped its pre-materialization byte certificate"
        );
    }
    admin.execute(
        "UPDATE query_history SET sql=$2,profile=$3 WHERE query_id=$1",
        &[&alice_id.to_string(), &original_sql, &original_profile],
    )?;

    // More than eight MiB of individually legal profiles also fails before
    // materialization, even though the eligible-row count is below 100.
    let mut profile_ids = Vec::new();
    for i in 0..9 {
        let id = format!("system-profile-{i}");
        admin.execute(
            "INSERT INTO query_history(query_id,session_id,sql,context,status,principal,namespace_id,owner_principal_id,profile,accepted_at) VALUES($1,'fixture','SELECT 1','{}','succeeded','system-alice',$2,$3,$4,'2022-01-01T00:00:00Z')",
            &[&id,&alice.actor.namespace_id,&alice.actor.principal_id,&large_profile],
        )?;
        profile_ids.push(id);
    }
    let profiles = probe_for(
        bootstrap,
        SourceBounds::RequestIds(profile_ids),
        &[ColumnId::ProfileJson],
        100,
    )?;
    let profiles = gateway.query_one(profiles.sql(), &profiles.params())?;
    anyhow::ensure!(
        profiles.get::<_, i64>("eligible_rows") == 9
            && profiles.get::<_, i64>("selected_payload_bytes") > 8 * 1024 * 1024,
        "visible profile bytes were not counted"
    );

    // Representative history cardinality: a broad bounded request stops at
    // cap+1, while a seven-row window can use a time index without examining
    // the 120,010 unrelated records. No planner flags force this outcome.
    admin.execute(
        "INSERT INTO query_history(query_id,session_id,sql,context,status,principal,namespace_id,owner_principal_id,accepted_at) SELECT 'system-window-'||n,'fixture','SELECT 1','{}','succeeded','system-alice',$1,$2,CASE WHEN n<=60010 THEN '2020-01-01T00:00:00Z'::timestamptz ELSE '2019-01-01T00:00:00Z'::timestamptz END FROM generate_series(1,120010) n",
        &[&alice.actor.namespace_id,&alice.actor.principal_id],
    )?;
    admin.execute(
        "INSERT INTO query_history(query_id,session_id,sql,context,status,principal,namespace_id,owner_principal_id,accepted_at) SELECT 'system-narrow-'||n,'fixture','SELECT 1','{}','succeeded','system-alice',$1,$2,'2021-01-01T00:00:00Z'::timestamptz FROM generate_series(1,7) n",
        &[&alice.actor.namespace_id,&alice.actor.principal_id],
    )?;
    admin.batch_execute("ANALYZE query_history; ANALYZE requests; ANALYZE authz_legacy_owners")?;
    let range = |epoch| SourceBounds::TimeRange {
        from: SystemTime::UNIX_EPOCH + Duration::from_secs(epoch),
        until: SystemTime::UNIX_EPOCH + Duration::from_secs(epoch + 86400),
    };
    let broad = probe_for(bootstrap, range(1_577_836_800), &[], 50_000)?;
    let broad = gateway.query_one(broad.sql(), &broad.params())?;
    anyhow::ensure!(
        broad.get::<_, i64>("eligible_rows") == 50_001,
        "large source was silently truncated instead of signaling cap+1"
    );
    let narrow = probe_for(alice, range(1_609_459_200), &[ColumnId::RequestId], 50_000)?;
    let count = gateway.query_one(narrow.sql(), &narrow.params())?;
    anyhow::ensure!(count.get::<_, i64>("eligible_rows") == 7, "window widened");
    let explain = gateway
        .query(
            &format!("EXPLAIN (ANALYZE, BUFFERS) {}", narrow.sql()),
            &narrow.params(),
        )?
        .iter()
        .map(|row| row.get::<_, String>(0))
        .collect::<Vec<_>>()
        .join("\n");
    anyhow::ensure!(
        (explain.contains("history_system_window")
            || explain.contains("history_system_own_window")
            || explain.contains("query_history_expired"))
            && !explain.contains("Seq Scan on query_history"),
        "bounded history window did not use a time index:\n{explain}"
    );
    println!(
        "PASS: system source cap+1, masked/visible bytes, repeated-output preflight, and indexed seven-row window over 120k rows\n{explain}"
    );
    Ok(())
}
struct FixtureAuth {
    cases: Vec<Identity>,
    digests: Vec<String>,
}
fn fixtures(admin: &mut Client, dsn: &str, schema: &str) -> Result<FixtureAuth> {
    let seed = platform::Seed {
        prepaid_micro_usd: 10_000_000,
        max_job_micro_usd: 1_000_000,
    };
    platform::migrate(admin, &seed)?;
    super::platform::remove_system_fixture_schema(admin)?;
    let legacy = Uuid::new_v4().to_string();
    let hash = "b".repeat(64);
    admin.execute("INSERT INTO requests(request_id,principal,logical_request_id,fingerprint_version,payload_hash,state,status,outcome,worker_id,accepted_at) VALUES($1,'direct',$1,3,$2,'terminal','succeeded','statement_result','legacy','1965-01-02T03:04:05Z')",&[&legacy,&hash])?;
    admin.execute("INSERT INTO query_history(query_id,session_id,sql,context,status,worker_id,placement,principal,accepted_at) VALUES($1,'legacy','old-secret','{}','succeeded','legacy','{\"engine_started_at\":\"not-a-timestamp\"}','direct','1965-01-02T03:04:05Z')",&[&legacy])?;
    let budget: String = admin
        .query_one("SELECT to_jsonb(b)::text FROM budget b", &[])?
        .get(0);
    anyhow::ensure!(
        platform::migrate(admin, &seed)?.applied,
        "schema 3 upgrade did not apply"
    );
    anyhow::ensure!(
        !platform::migrate(admin, &seed)?.applied,
        "current schema migration rewrote state"
    );
    anyhow::ensure!(
        admin
            .query_one("SELECT to_jsonb(b)::text FROM budget b", &[])?
            .get::<_, String>(0)
            == budget,
        "upgrade repriced budget"
    );
    let old = admin.query_one(
        "SELECT payload_hash,query_fingerprint,engine_version FROM requests WHERE request_id=$1",
        &[&legacy],
    )?;
    anyhow::ensure!(
        old.get::<_, String>(0) == hash
            && old.get::<_, Option<String>>(1).is_none()
            && old.get::<_, Option<String>>(2).is_none(),
        "upgrade changed identity or invented legacy dimensions"
    );
    let worker_dsn = std::env::var("DUCKFLEET_WORKER_TEST_DSN")
        .context("--system-storage requires worker test DSN")?;
    let gateway_dsn = std::env::var("DUCKFLEET_GATEWAY_TEST_DSN")
        .context("--system-storage requires gateway test DSN")?;
    let maintainer_dsn = std::env::var("DUCKFLEET_MAINTAINER_TEST_DSN")
        .context("--system-storage requires maintainer test DSN")?;
    let mut worker = config(&worker_dsn, schema)?.connect(NoTls)?;
    let mut gateway = config(&gateway_dsn, schema)?.connect(NoTls)?;
    let mut maintainer = config(&maintainer_dsn, schema)?.connect(NoTls)?;
    let roles = RoleNames {
        worker: worker.query_one("SELECT current_user", &[])?.get(0),
        gateway: gateway.query_one("SELECT current_user", &[])?.get(0),
        maintainer: maintainer.query_one("SELECT current_user", &[])?.get(0),
    };
    let mut tx = admin.transaction()?;
    db_roles::grant_runtime(&mut tx, &roles)?;
    tx.commit()?;
    let (alice, alice_digest) = identity(admin, "system-alice")?;
    let (bob, bob_digest) = identity(admin, "system-bob")?;
    let (direct, direct_digest) = identity(admin, "direct")?;
    for actor in [&alice, &bob, &direct] {
        policy(admin, actor, vec![Role::Analyst], vec![], false)?;
    }
    let epoch = WorkerEpoch {
        worker_id: "system-fixture".into(),
        boot_id: Uuid::new_v4(),
    };
    anyhow::ensure!(
        queue::register_worker(
            &mut worker,
            &epoch,
            "persistent:small",
            WorkerLimits {
                max_queued_queries: 64,
                max_queued_bytes: 16 * 1024 * 1024
            }
        )?,
        "worker policy missing"
    );
    let alice_session = session(&mut worker, &epoch, &alice)?;
    let bob_session = session(&mut worker, &epoch, &bob)?;
    let alice_id = complete(&mut worker, &alice_session, &alice)?;
    let bob_id = complete(&mut worker, &bob_session, &bob)?;
    policy(
        admin,
        &alice,
        vec![Role::Analyst],
        vec![
            PermissionGrant {
                action: Action::HistoryRead,
                scope: Scope::All,
            },
            PermissionGrant {
                action: Action::UsageRead,
                scope: Scope::All,
            },
        ],
        false,
    )?;
    let alice_access = access(&mut gateway, &alice)?;
    let bootstrap = access(&mut gateway, &Identity::bootstrap())?;
    let direct_access = access(&mut gateway, &direct)?;
    let ids = vec![alice_id.to_string(), bob_id.to_string()];
    let columns = [
        ColumnId::RequestId,
        ColumnId::SqlText,
        ColumnId::ProfileJson,
        ColumnId::QueryFingerprint,
        ColumnId::QueryFingerprintVersion,
        ColumnId::SelectedCpu,
        ColumnId::RequestedCpu,
        ColumnId::EngineVersion,
        ColumnId::AcceptedAt,
        ColumnId::ProfileRowsReturned,
    ];
    let rows = source(
        &mut gateway,
        schema,
        &alice_access,
        Relation::History,
        &ids,
        &columns,
    )?;
    anyhow::ensure!(rows.len() == 2, "all-history lost visible rows");
    for row in &rows {
        let own = row.get::<_, String>("request_id") == alice_id.to_string();
        anyhow::ensure!(
            row.get::<_, Option<String>>("sql_text").is_some() == own
                && row.get::<_, Option<String>>("profile_json").is_some() == own
                && row.get::<_, Option<String>>("query_fingerprint").is_some() == own,
            "mixed SQL/profile masking differs from ownership"
        );
        anyhow::ensure!(
            row.get::<_, Option<i64>>("selected_cpu") == Some(1)
                && row.get::<_, Option<i64>>("requested_cpu").is_none()
                && row.get::<_, Option<i64>>("profile_rows_returned") == Some(1),
            "typed dimensions/metrics incorrect"
        );
        let _: SystemTime = row.get("accepted_at");
    }
    let uses = ColumnUses {
        projected: BTreeSet::new(),
        evaluated: [ColumnId::QueryFingerprint].into_iter().collect(),
    };
    let error = metadata::source_plan(
        &alice_access,
        Relation::History,
        &SourceBounds::RequestIds(ids.clone()),
        &uses,
        &api(StorageSchema::validated(schema))?,
    )
    .unwrap_err();
    anyhow::ensure!(
        error.0 == crate::failure::FORBIDDEN,
        "hidden aggregate/filter was permitted"
    );
    let mut tx = gateway.transaction()?;
    let http = api(history::detail_in(&mut tx, &alice_access, alice_id))?;
    tx.commit()?;
    anyhow::ensure!(
        http["selected_cpu"] == 1
            && http["requested_cpu"].is_null()
            && http["query_fingerprint"].is_string(),
        "HTTP capture differs from system source"
    );
    let mut tx = gateway.transaction()?;
    let hidden = api(history::detail_in(&mut tx, &alice_access, bob_id))?;
    tx.commit()?;
    anyhow::ensure!(
        hidden["sql"].is_null()
            && hidden["query_fingerprint"].is_null()
            && hidden["profile"].is_null(),
        "HTTP projection disclosed foreign diagnostics"
    );
    let visible = source(
        &mut gateway,
        schema,
        &bootstrap,
        Relation::History,
        std::slice::from_ref(&legacy),
        &[
            ColumnId::AcceptedAt,
            ColumnId::EngineStartedAt,
            ColumnId::EngineVersion,
            ColumnId::QueryFingerprint,
        ],
    )?;
    anyhow::ensure!(
        visible[0].get::<_, SystemTime>("accepted_at") < SystemTime::UNIX_EPOCH
            && visible[0]
                .get::<_, Option<SystemTime>>("engine_started_at")
                .is_none()
            && visible[0]
                .get::<_, Option<String>>("engine_version")
                .is_none(),
        "legacy timestamp/dimensions fabricated"
    );
    anyhow::ensure!(
        source(
            &mut gateway,
            schema,
            &direct_access,
            Relation::History,
            std::slice::from_ref(&legacy),
            &[ColumnId::RequestId]
        )?
        .is_empty(),
        "new named direct claimed unknown legacy owner"
    );
    // Every public descriptor must match a real PostgreSQL column/type, including empty nullable fields.
    for relation in [Relation::History, Relation::Attempts, Relation::Usage] {
        let cols: Vec<_> = metadata::relation_def(relation)
            .columns
            .iter()
            .map(|c| c.id)
            .collect();
        let rows = source(&mut gateway, schema, &bootstrap, relation, &ids, &cols)?;
        anyhow::ensure!(!rows.is_empty(), "manifest fixture relation empty");
        for def in metadata::relation_def(relation).columns {
            let actual = rows[0]
                .columns()
                .iter()
                .find(|c| c.name() == def.name)
                .context("manifest column missing")?
                .type_();
            let expected = match def.ty {
                metadata::SystemType::Bool => postgres::types::Type::BOOL,
                metadata::SystemType::Int64 => postgres::types::Type::INT8,
                metadata::SystemType::Float64 => postgres::types::Type::FLOAT8,
                metadata::SystemType::TimestampUtc => postgres::types::Type::TIMESTAMPTZ,
                _ => postgres::types::Type::TEXT,
            };
            anyhow::ensure!(
                *actual == expected,
                "manifest type mismatch for {}",
                def.name
            );
        }
    }
    resource_bounds(
        admin,
        &mut gateway,
        schema,
        &alice_access,
        &bootstrap,
        alice_id,
        bob_id,
    )?;
    let mut tx = gateway.transaction()?;
    let events = api(usage::events_in(
        &mut tx,
        &alice_access,
        usage::Filter {
            execution_id: Some(alice_id.to_string()),
            ..Default::default()
        },
    ))?;
    tx.commit()?;
    anyhow::ensure!(
        events["events"][0]["selected_pool"] == "small"
            && events["events"][0]["model_id"] == "model.system-alice"
            && events["events"][0]["rate_micro_usd_per_unit_hour"] == 100_000,
        "usage snapshot missing"
    );
    admin.execute(
        "DELETE FROM query_history WHERE query_id=$1",
        &[&alice_id.to_string()],
    )?;
    let retained = source(
        &mut gateway,
        schema,
        &alice_access,
        Relation::Attempts,
        &[alice_id.to_string()],
        &[ColumnId::QueryFingerprint, ColumnId::SelectedPool],
    )?;
    anyhow::ensure!(
        retained[0]
            .get::<_, Option<String>>("query_fingerprint")
            .is_some(),
        "history pruning removed retained fingerprint"
    );
    let saved = source(
        &mut gateway,
        schema,
        &alice_access,
        Relation::Usage,
        &[alice_id.to_string()],
        &[
            ColumnId::ModelId,
            ColumnId::OwnerPrincipalName,
            ColumnId::RateMicroUsdPerUnitHour,
        ],
    )?;
    anyhow::ensure!(
        saved[0]
            .get::<_, Option<String>>("owner_principal_name")
            .as_deref()
            == Some("system-alice"),
        "history pruning removed usage attribution"
    );
    let pending = submit(&mut worker, &alice_session, &alice)?;
    let selected = api(queue::select_next(&mut worker, &epoch))?.context("recovery selection")?;
    api(queue::reserve(&mut worker, selected, &rate()))?;
    admin.execute(
        "DELETE FROM query_history WHERE query_id=$1",
        &[&pending.to_string()],
    )?;
    // Legacy recovery is allowed to settle from the retained reservation alone.
    let mut tx = admin.transaction()?;
    crate::metering::recover(&mut tx, &epoch.worker_id, &rate())?;
    tx.commit()?;
    let recovered = source(
        &mut gateway,
        schema,
        &alice_access,
        Relation::Usage,
        &[pending.to_string()],
        &[ColumnId::ModelId, ColumnId::RateMicroUsdPerUnitHour],
    )?;
    anyhow::ensure!(
        recovered[0].get::<_, Option<String>>("model_id").as_deref() == Some("model.system-alice")
            && recovered[0].get::<_, Option<i64>>("rate_micro_usd_per_unit_hour") == Some(100_000),
        "recovery lost saved dimensions/rate"
    );
    let (disabled, disabled_digest) = identity(admin, "system-disabled")?;
    policy(admin, &disabled, vec![Role::Viewer], vec![], true)?;
    let (revoked, revoked_digest) = identity(admin, "system-revoked")?;
    admin.execute(
        "UPDATE credentials SET revoked_at=clock_timestamp() WHERE credential_id=$1",
        &[&revoked.credential_id],
    )?;
    let (expired, expired_digest) = identity(admin, "system-expired")?;
    admin.execute("UPDATE credentials SET expires_at=clock_timestamp()-interval '1 second' WHERE credential_id=$1",&[&expired.credential_id])?;
    let mut spoofed = alice.clone();
    spoofed.principal = bob.principal.clone();
    let _ = dsn;
    Ok(FixtureAuth {
        cases: vec![
            Identity::bootstrap(),
            alice,
            bob,
            direct,
            disabled,
            revoked,
            expired,
            spoofed,
        ],
        digests: vec![
            alice_digest,
            bob_digest,
            direct_digest,
            disabled_digest,
            revoked_digest,
            expired_digest,
            "0".repeat(64),
        ],
    })
}
pub(super) async fn checks() -> Result<()> {
    let dsn = std::env::var("DUCKFLEET_PLATFORM_TEST_DSN")
        .context("--system-storage requires DUCKFLEET_PLATFORM_TEST_DSN")?;
    let schema = format!("df_system_{}", Uuid::new_v4().simple());
    let setup_dsn = dsn.clone();
    let setup_schema = schema.clone();
    let setup = tokio::task::spawn_blocking(move || -> Result<FixtureAuth> {
        let mut db = config(&setup_dsn, &setup_schema)?.connect(NoTls)?;
        db.batch_execute(&format!("CREATE SCHEMA {setup_schema}"))?;
        fixtures(&mut db, &setup_dsn, &setup_schema)
    })
    .await
    .context("system storage fixture thread")?;
    let result = match setup {
        Ok(auth) => {
            async {
                system_auth::parity(&dsn, &schema, &auth.cases, &auth.digests).await?;
                let identity = auth
                    .cases
                    .iter()
                    .find(|identity| identity.principal == "system-alice")
                    .context("system admission fixture identity missing")?;
                system_admission::canceled_callers(&dsn, &schema, identity).await
            }
            .await
        }
        Err(e) => Err(e),
    };
    let cleanup = tokio::task::spawn_blocking(move || -> Result<()> {
        let mut db = config(&dsn, &schema)?.connect(NoTls)?;
        db.batch_execute(&format!("DROP SCHEMA {schema} CASCADE"))?;
        Ok(())
    })
    .await
    .context("system fixture cleanup")?;
    result?;
    cleanup?;
    println!(
        "PASS: system schema 3 upgrade, typed capture, scoped shared projections, legacy timestamps, retained usage, and async authorization parity"
    );
    Ok(())
}
