//! Standalone job storage tests use real transactions and actual runtime roles.
use crate::{
    ApiError, authz,
    credentials::{self, Identity},
    db_roles, failure, job_artifacts as artifacts,
    job_policy::{JobLimits, JobRequest},
    jobs, metadata, platform,
    pools::PoolRegistry,
    pricing,
    query::{Protocol, QueryRequest, WorkerLimits},
    queue, requests,
};
use anyhow::{Context, Result, ensure};
use postgres::{Client, Config, NoTls};
use serde_json::{Value, json};
use std::time::{Duration, SystemTime};
use uuid::Uuid;
fn api<T>(r: Result<T, ApiError>) -> Result<T> {
    r.map_err(|e| anyhow::anyhow!("{}: {}", e.0.kind, e.1))
}
fn cfg(env: &str, schema: &str) -> Result<Config> {
    let mut c: Config = std::env::var(env)
        .with_context(|| format!("job storage requires {env}"))?
        .parse()?;
    c.options(&format!(
        "-c search_path={schema} -c statement_timeout=10000"
    ));
    c.connect_timeout(Duration::from_secs(3));
    Ok(c)
}
fn rate() -> pricing::RateCard {
    pricing::RateCard {
        version: "job-fixture".into(),
        compute_micro_usd_per_unit_hour: 100_000,
    }
}
fn request() -> JobRequest {
    serde_json::from_value(json!({"request_id":Uuid::new_v4(),"sql":"SELECT 'job-sql-sentinel', ?","parameters":[7],"context":{"secret":"job-context-sentinel"},"compute":{"pool":"small"}})).expect("static job input")
}
fn submit(
    db: &mut Client,
    actor: &Identity,
    r: &JobRequest,
    registry: &PoolRegistry,
) -> Result<jobs::SubmitResult> {
    api(jobs::submit(
        db,
        actor,
        api(r.clone().validate())?,
        registry,
        true,
    ))
}
fn accepted(r: jobs::SubmitResult) -> Result<()> {
    ensure!(
        matches!(r, jobs::SubmitResult::Accepted(_)),
        "job not accepted"
    );
    Ok(())
}
fn actor(db: &mut Client, name: &str) -> Result<Identity> {
    let issued = credentials::issue(db, name, None)?;
    api(authz::set_policy(
        db,
        &Identity::bootstrap(),
        name,
        &authz::PolicyUpdate {
            roles: vec![authz::Role::Analyst],
            permissions: vec![],
            compute: vec![authz::ComputeGrant {
                pool: Some("small".into()),
                mode: crate::compute::Mode::Persistent,
                cpu: 1,
                memory_bytes: 1 << 30,
                disk_bytes: 2 << 30,
                job_limits: Some(JobLimits::HARD),
            }],
            disabled: false,
        },
    ))?;
    Ok(Identity {
        principal: name.into(),
        credential_id: Some(issued.credential_id),
        expires_at: None,
        kind: credentials::ActorKind::Named,
    })
}
fn worker(db: &mut Client, name: &str) -> Result<queue::WorkerEpoch> {
    let w = queue::WorkerEpoch {
        worker_id: name.into(),
        boot_id: Uuid::new_v4(),
    };
    ensure!(
        queue::register_worker(
            db,
            &w,
            "persistent:small",
            WorkerLimits {
                max_queued_queries: 8,
                max_queued_bytes: 16 * 1024 * 1024
            }
        )?,
        "fixture policy missing"
    );
    Ok(w)
}
fn bind(
    gateway: &mut Client,
    worker: &mut Client,
    epoch: &queue::WorkerEpoch,
    id: Uuid,
) -> Result<jobs::BindingToken> {
    let claim = jobs::claim_binding(gateway, Uuid::new_v4())?.context("binding claim missing")?;
    ensure!(claim.token.request_id == id, "wrong binding candidate");
    jobs::record_binding_target(
        gateway,
        &claim.token,
        jobs::BindingTarget {
            endpoint: Some("http://fixture.invalid:8080".into()),
            pod_name: None,
        },
    )?;
    let dimensions = api(metadata::capture_session(
        &Default::default(),
        &claim.placement,
        "fixture-engine",
        None,
    ))?;
    api(jobs::bind_session(
        worker,
        &claim.token,
        epoch,
        &claim.placement,
        &dimensions,
    ))?;
    Ok(claim.token)
}
fn grant(worker: &mut Client, epoch: &queue::WorkerEpoch) -> Result<queue::ExecutionGrant> {
    let selected = api(queue::select_next(worker, epoch))?.context("job not selected")?;
    let prepared = api(queue::reserve(worker, selected, &rate()))?;
    match api(queue::grant(worker, prepared))? {
        queue::GrantResult::Granted(g) => Ok(*g),
        queue::GrantResult::Refused { error, .. } => {
            Err(anyhow::anyhow!("grant refused {}", error.0.kind))
        }
    }
}
fn finish(
    db: &mut Client,
    g: &queue::ExecutionGrant,
    result: jobs::ResultDisposition,
    elapsed: i64,
) -> Result<()> {
    let mut tx = db.transaction()?;
    jobs::bounded(&mut tx)?;
    let p = &g.prepared.selected.payload;
    ensure!(
        jobs::finish_in(
            &mut tx,
            &queue::Finish {
                request_id: p.request.request_id,
                epoch: &p.epoch,
                status: "succeeded",
                failure: None,
                outcome: requests::Outcome::StatementResult,
                elapsed_ms: elapsed,
                result_rows: Some(1),
                error: None,
                engine_started_at: Some(SystemTime::now()),
                profile: None,
                profile_status: "not_recorded",
                metrics: &json!({}),
                source: "fixture"
            },
            result
        )?,
        "finish did not transition"
    );
    tx.commit()?;
    Ok(())
}
fn cancel(db: &mut Client, admin: &mut Client, id: Uuid, actor: &Identity) -> Result<()> {
    api(queue::request_cancel(db, id, actor))?;
    queue::expire_batch(admin, 64, Duration::from_secs(3))?;
    Ok(())
}

pub(super) async fn checks() -> Result<()> {
    tokio::task::spawn_blocking(||->Result<()>{
    let schema=format!("df_jobs_{}",Uuid::new_v4().simple());let ac=cfg("DUCKFLEET_PLATFORM_TEST_DSN",&schema)?;let gc=cfg("DUCKFLEET_GATEWAY_TEST_DSN",&schema)?;let wc=cfg("DUCKFLEET_WORKER_TEST_DSN",&schema)?;let mc=cfg("DUCKFLEET_MAINTAINER_TEST_DSN",&schema)?;
    let mut admin=ac.connect(NoTls)?;admin.batch_execute(&format!("CREATE SCHEMA {schema}"))?;
    let result=(||->Result<()>{
        let seed=platform::Seed{prepaid_micro_usd:100_000_000,max_job_micro_usd:1_000_000};platform::migrate(&mut admin,&seed)?;
        // A genuine4→5 upgrade preserves identities and never revisits old hashes.
        super::platform::remove_job_fixture_schema(&mut admin)?;
        let legacy=Uuid::new_v4().to_string();admin.execute("INSERT INTO requests(request_id,logical_request_id,fingerprint_version,payload_hash,state,status,outcome,worker_id) VALUES($1,$1,3,$2,'terminal','succeeded','statement_result','old')",&[&legacy,&"a".repeat(64)])?;
        let before:String=admin.query_one("SELECT to_jsonb(r)::text FROM requests r WHERE request_id=$1",&[&legacy])?.get(0);
        ensure!(platform::migrate(&mut admin,&seed)?.applied,"schema8 not applied");let after:Value=serde_json::from_str(admin.query_one("SELECT to_jsonb(r)::text FROM requests r WHERE request_id=$1",&[&legacy])?.get::<_,&str>(0))?;let old:Value=serde_json::from_str(&before)?;for(k,v)in old.as_object().context("legacy object")?{ensure!(after[k]==*v,"migration rewrote legacy {k}");}
        ensure!(!platform::migrate(&mut admin,&seed)?.applied,"current migration mutated schema");
        let roles=db_roles::RoleNames{gateway:gc.get_user().context("gateway user")?.into(),worker:wc.get_user().context("worker user")?.into(),maintainer:mc.get_user().context("maintenance user")?.into()};let mut tx=admin.transaction()?;db_roles::grant_runtime(&mut tx,&roles)?;tx.commit()?;
        let(mut gateway,mut engine,mut maintenance)=(gc.connect(NoTls)?,wc.connect(NoTls)?,mc.connect(NoTls)?);
        let mut registry=PoolRegistry::from_json(&json!({"default_pool":"small","pools":{"small":{"cpu":1,"memory":"1Gi","disk":"2Gi","endpoints":["http://fixture.invalid:8080"],"max_sessions_per_worker":8}}}).to_string())?;registry.job_defaults.limits.runtime_seconds=60;
        queue::publish_policies_with_jobs(&mut gateway,&queue::PolicyPublication{publication_id:Uuid::new_v4(),digest:"jobs-fixture".into(),policies:registry.admission_policies()},&registry.job_policies())?;
        let alice=actor(&mut gateway,"job-alice")?;let bob=actor(&mut gateway,"job-bob")?;
        let w=worker(&mut engine,"job-fixture-worker")?;
        acceptance(&mut admin,&mut gateway,&mut engine,&alice,&bob,&registry,&w)?;
        publication(&mut admin,&mut gateway,&mut engine,&mut maintenance,&alice,&registry,&w)?;
        recovery(&mut admin,&mut gateway,&mut engine,&alice,&registry)?;
        budget_retry(&mut admin,&mut gateway,&mut engine,&alice,&registry)?;
        zero_unexecuted(&mut admin,&mut gateway,&mut engine,&alice,&registry)?;
        final_denial(&mut admin,&mut gateway,&mut engine,&alice,&registry)?;
        println!("PASS: actual-role standalone job acceptance, immutable replay, shared queue, generation fencing, result publication, expiry and crash recovery");Ok(())
    })();let cleanup=admin.batch_execute(&format!("DROP SCHEMA {schema} CASCADE"));match(result,cleanup){(Err(e),_)=>Err(e),(Ok(()),c)=>c.context("cleanup jobs fixture")}
 }).await??;
    Ok(())
}
fn acceptance(
    admin: &mut Client,
    gateway: &mut Client,
    engine: &mut Client,
    alice: &Identity,
    bob: &Identity,
    registry: &PoolRegistry,
    w: &queue::WorkerEpoch,
) -> Result<()> {
    let q = request();
    accepted(submit(gateway, alice, &q, registry)?)?;
    let row=admin.query_one("SELECT r.worker_id,r.session_id,j.intent_options_json,(SELECT COUNT(*)::BIGINT FROM query_permits WHERE request_id=r.request_id) FROM requests r JOIN query_jobs j USING(request_id) WHERE r.request_id=$1",&[&q.request_id.to_string()])?;
    ensure!(
        row.get::<_, Option<String>>(0).is_none()
            && row.get::<_, Option<String>>(1).is_none()
            && row.get::<_, i64>(3) == 1,
        "unbound acceptance fabricated assignment or lost permit"
    );
    let options: String = row.get(2);
    ensure!(
        !options.contains("sentinel")
            && !options.contains("SELECT")
            && !options.contains("parameters"),
        "retained options copied payload"
    );
    let mut changed = registry.clone();
    changed.job_defaults.limits.runtime_seconds = 300;
    ensure!(
        matches!(
            api(jobs::submit(
                gateway,
                alice,
                api(q.clone().validate())?,
                &changed,
                false
            ))?,
            jobs::SubmitResult::Duplicate(_)
        ),
        "duplicate re-resolved defaults or required enabled store"
    );
    let mut mismatch = q.clone();
    mismatch.queue_timeout_seconds = Some(121);
    ensure!(
        jobs::submit(gateway, alice, api(mismatch.validate())?, registry, true)
            .err()
            .context("changed queue option accepted")?
            .0
            == failure::REQUEST_ID_MISMATCH,
        "wrong replay mismatch"
    );
    ensure!(
        jobs::submit(gateway, bob, api(q.clone().validate())?, registry, true)
            .err()
            .context("foreign duplicate accepted")?
            .0
            == failure::NOT_FOUND,
        "foreign ID leaked payload comparison"
    );
    let claim = jobs::claim_binding(gateway, Uuid::new_v4())?.context("missing binding")?;
    jobs::record_binding_target(
        gateway,
        &claim.token,
        jobs::BindingTarget {
            endpoint: Some("http://fixture.invalid:8080".into()),
            pod_name: None,
        },
    )?;
    jobs::binding_failed(gateway, &claim.token, jobs::BindingFailure::Unknown)?;
    ensure!(
        jobs::inspect_binding(gateway, &claim.token)?.state == "opening",
        "ambiguous open was replayable"
    );
    admin.execute("UPDATE query_jobs SET dispatcher_lease_until=clock_timestamp()-interval '1 second' WHERE request_id=$1",&[&q.request_id.to_string()])?;
    let resumed = jobs::claim_binding(gateway, Uuid::new_v4())?.context("resume missing")?;
    ensure!(
        resumed.token.generation == claim.token.generation
            && resumed.token.session_id == claim.token.session_id
            && resumed.target
                == claim.target.or(Some(jobs::BindingTarget {
                    endpoint: Some("http://fixture.invalid:8080".into()),
                    pod_name: None
                })),
        "restart amplified binding generation"
    );
    ensure!(
        !jobs::renew_binding(gateway, &claim.token)?,
        "stale dispatcher renewed"
    );
    let dimensions = api(metadata::capture_session(
        &q.compute,
        &resumed.placement,
        "fixture",
        None,
    ))?;
    api(jobs::bind_session(
        engine,
        &resumed.token,
        w,
        &resumed.placement,
        &dimensions,
    ))?;
    ensure!(
        jobs::binding_for_worker(engine, &resumed.token)
            .map_err(|e| anyhow::anyhow!("binding {}", e.0.kind))?
            .bound,
        "worker binding was not committed"
    );
    api(queue::request_cancel(gateway, q.request_id, alice))?;
    queue::poll_control(engine, w, false, 64)?;
    ensure!(
        admin
            .query_one(
                "SELECT COUNT(*)::BIGINT FROM query_permits WHERE request_id=$1",
                &[&q.request_id.to_string()]
            )?
            .get::<_, i64>(0)
            == 0,
        "cancel leaked permit"
    );
    let expired = request();
    accepted(submit(gateway, alice, &expired, registry)?)?;
    admin.execute("UPDATE requests SET queue_deadline_at=clock_timestamp()-interval '1 second' WHERE request_id=$1",&[&expired.request_id.to_string()])?;
    queue::expire_batch(admin, 64, Duration::from_secs(3))?;
    ensure!(
        api(jobs::detail(gateway, alice, expired.request_id))?["outcome"] == "not_executed",
        "unbound expiry outcome wrong"
    );
    let expired_head = request();
    accepted(submit(gateway, alice, &expired_head, registry)?)?;
    admin.execute("UPDATE requests SET queue_deadline_at=clock_timestamp()-interval '1 second' WHERE request_id=$1",&[&expired_head.request_id.to_string()])?;
    let never_opened = request();
    accepted(submit(gateway, alice, &never_opened, registry)?)?;
    let never_claim = jobs::claim_binding(gateway, Uuid::new_v4())?.context("unopened claim")?;
    ensure!(
        never_claim.token.request_id == never_opened.request_id,
        "expired head hid an eligible standalone job"
    );
    jobs::binding_failed(gateway, &never_claim.token, jobs::BindingFailure::NotOpened)?;
    ensure!(
        jobs::inspect_binding(gateway, &never_claim.token)?.state == "unbound",
        "conclusive unopened intent was not released"
    );
    cancel(gateway, admin, never_opened.request_id, alice)?;
    // Interactive and unbound jobs spend the same pool/principal counters.
    admin.execute("UPDATE admission_policies SET max_queued=1,principal_queued=1 WHERE scope='persistent:small'",&[])?;
    let s = queue::SessionRegistration {
        session_id: Uuid::new_v4(),
        epoch: w.clone(),
        principal: alice.principal.clone(),
        authority: alice.clone(),
        context: json!({}),
        compute: jobs::flat_compute(
            &registry
                .resolve(&q.compute)
                .map_err(|e| anyhow::anyhow!("resolve {}", e.0.kind))?,
        ),
        placement: json!({}),
        scope: "persistent:small".into(),
        protocol: Protocol::Http,
        open_span_id: Uuid::new_v4(),
        operation: authz::TrustedOperation::Sql,
    };
    api(queue::register_session(engine, &s))?;
    let iq = QueryRequest {
        request_id: Uuid::new_v4(),
        retry_of: None,
        sql: "SELECT 1".into(),
        parameters: vec![],
        context: json!({}),
        timeout_seconds: 30,
        queue_timeout_seconds: 120,
    };
    ensure!(
        matches!(
            api(queue::submit(
                engine,
                w,
                s.session_id,
                iq.clone(),
                alice,
                Protocol::Http
            ))?,
            queue::SubmitResult::Accepted { .. }
        ),
        "interactive fixture admission failed"
    );
    let capped = request();
    ensure!(
        matches!(
            submit(gateway, alice, &capped, registry)?,
            jobs::SubmitResult::Refused(_)
        ),
        "unbound job bypassed common queue cap"
    );
    api(queue::request_cancel(gateway, iq.request_id, alice))?;
    queue::poll_control(engine, w, false, 64)?;
    admin.execute("UPDATE admission_policies SET max_queued=256,principal_queued=16 WHERE scope='persistent:small'",&[])?;
    Ok(())
}
fn publication(
    admin: &mut Client,
    gateway: &mut Client,
    engine: &mut Client,
    maintenance: &mut Client,
    alice: &Identity,
    registry: &PoolRegistry,
    w: &queue::WorkerEpoch,
) -> Result<()> {
    let q = request();
    accepted(submit(gateway, alice, &q, registry)?)?;
    bind(gateway, engine, w, q.request_id)?;
    let g = grant(engine, w)?;
    let e = g
        .prepared
        .selected
        .payload
        .job
        .as_ref()
        .context("job payload")?;
    api(jobs::authorize_execution(engine, e))?;
    jobs::mark_sql_complete(engine, &e.token, jobs::SqlCompletion::Succeeded, 10)?;
    let intent = artifacts::intend_chunk(
        engine,
        &e.token,
        &artifacts::BlobDescriptor {
            sequence: 0,
            rows: 1,
            bytes: 128,
            sha256: "a".repeat(64),
        },
    )?;
    artifacts::ack_upload(engine, &e.token, &intent)?;
    let parts = artifacts::manifest_parts(engine, &e.token)?;
    ensure!(
        parts.rows == 1 && parts.bytes == 128 && parts.chunks.len() == 1,
        "manifest totals wrong"
    );
    let manifest = artifacts::intend_manifest(engine, &e.token, 100, &"b".repeat(64))?;
    artifacts::ack_upload(engine, &e.token, &manifest)?;
    let verified = artifacts::verified_manifest(engine, &e.token, &manifest)?;
    finish(
        engine,
        &g,
        jobs::ResultDisposition::Ready(verified),
        600_000,
    )?;
    let detail = api(jobs::detail(gateway, alice, q.request_id))?;
    ensure!(
        detail["status"] == "succeeded" && detail["result_state"] == "ready",
        "ready SQL outcome wrong: {detail}"
    );
    let mut tx = gateway.transaction()?;
    let access = api(authz::resolve_in(&mut tx, alice))?;
    let object = api(artifacts::authorize_in(
        &mut tx,
        &access,
        q.request_id,
        None,
    ))?;
    ensure!(
        object.generation == e.token.artifact_generation && object.request_id == q.request_id,
        "authorized object lost identity"
    );
    tx.commit()?;
    let row = admin.query_one(
        "SELECT COUNT(*)::BIGINT,MAX(quantity) FROM usage_events WHERE execution_id=$1",
        &[&q.request_id.to_string()],
    )?;
    ensure!(
        row.get::<_, i64>(0) == 1 && row.get::<_, Option<i64>>(1) == Some(60_000),
        "job charged beyond saved admission deadline"
    );
    admin.execute("UPDATE query_jobs SET result_expires_at=clock_timestamp()-interval '1 second' WHERE request_id=$1",&[&q.request_id.to_string()])?;
    let mut tx = gateway.transaction()?;
    let access = api(authz::resolve_in(&mut tx, alice))?;
    ensure!(
        artifacts::authorize_in(&mut tx, &access, q.request_id, None)
            .err()
            .context("expired result readable")?
            .0
            == failure::RESULT_EXPIRED,
        "expiry not authoritative"
    );
    tx.rollback()?;
    let config = platform::MaintenanceConfig::default();
    let lease = platform::claim_maintenance(maintenance, Uuid::new_v4(), &config)?
        .context("cleanup lease")?;
    let candidates = artifacts::deletion_candidates(maintenance, &lease, &config, 8)?;
    ensure!(
        candidates.len() == 2,
        "expiry missed chunk or manifest intents"
    );
    artifacts::ack_delete(maintenance, &lease, &config, &candidates[0])?;
    admin.execute("UPDATE job_artifacts SET delete_after=clock_timestamp()-interval '1 second' WHERE request_id=$1",&[&q.request_id.to_string()])?;
    let failed_first = artifacts::deletion_candidates(maintenance, &lease, &config, 1)?;
    // Simulate an unavailable object store: no ACK. The next pass must progress.
    let later = artifacts::deletion_candidates(maintenance, &lease, &config, 1)?;
    ensure!(
        failed_first.len() == 1
            && later.len() == 1
            && failed_first[0].sequence != later[0].sequence,
        "failed first deletion starved another object"
    );
    platform::release_maintenance(maintenance, &lease, &config)?;
    ensure!(
        artifacts::ack_delete(maintenance, &lease, &config, &candidates[1]).is_err(),
        "expired cleanup lease acknowledged deletion"
    );
    ensure!(
        admin
            .query_one(
                "SELECT COUNT(*)::BIGINT FROM job_artifacts WHERE request_id=$1",
                &[&q.request_id.to_string()]
            )?
            .get::<_, i64>(0)
            == 2,
        "cleanup discarded late-PUT tombstone"
    );
    let short = request();
    accepted(submit(gateway, alice, &short, registry)?)?;
    bind(gateway, engine, w, short.request_id)?;
    let short_grant = grant(engine, w)?;
    let short_execution = short_grant
        .prepared
        .selected
        .payload
        .job
        .as_ref()
        .context("expiry execution")?;
    jobs::mark_sql_complete(
        engine,
        &short_execution.token,
        jobs::SqlCompletion::Succeeded,
        1,
    )?;
    admin.execute("UPDATE query_jobs SET result_expires_at=clock_timestamp()-interval '1 second' WHERE request_id=$1",&[&short.request_id.to_string()])?;
    let expired = artifacts::intend_manifest(engine, &short_execution.token, 10, &"c".repeat(64))
        .err()
        .context("expired manifest accepted")?;
    ensure!(
        expired.is::<artifacts::ArtifactExpired>(),
        "ordinary expiry was classified as uncertain metadata"
    );
    finish(
        engine,
        &short_grant,
        jobs::ResultDisposition::Failed("result_expired"),
        1,
    )?;
    let detail = api(jobs::detail(gateway, alice, short.request_id))?;
    ensure!(
        detail["status"] == "succeeded" && detail["result_state"] == "expired",
        "expired delivery erased SQL success"
    );
    Ok(())
}
fn recovery(
    admin: &mut Client,
    gateway: &mut Client,
    engine: &mut Client,
    alice: &Identity,
    registry: &PoolRegistry,
) -> Result<()> {
    for (n, mode) in ["queued", "preparing", "unknown", "completed", "failed"]
        .iter()
        .enumerate()
    {
        let w = worker(engine, &format!("job-recovery-{n}"))?;
        let q = request();
        accepted(submit(gateway, alice, &q, registry)?)?;
        bind(gateway, engine, &w, q.request_id)?;
        if *mode == "preparing" {
            let s = api(queue::select_next(engine, &w))?.context("select")?;
            api(queue::reserve(engine, s, &rate()))?;
        } else if matches!(*mode, "unknown" | "completed" | "failed") {
            let g = grant(engine, &w)?;
            if matches!(*mode, "completed" | "failed") {
                jobs::mark_sql_complete(
                    engine,
                    &g.prepared.selected.payload.job.context("job")?.token,
                    if *mode == "completed" {
                        jobs::SqlCompletion::Succeeded
                    } else {
                        jobs::SqlCompletion::Failed
                    },
                    1000,
                )?;
            }
        }
        let replacement = queue::WorkerEpoch {
            worker_id: w.worker_id.clone(),
            boot_id: Uuid::new_v4(),
        };
        queue::recover_worker(engine, &replacement, &rate())?;
        let detail = api(jobs::detail(gateway, alice, q.request_id))?;
        match *mode {
            "queued" => {
                ensure!(
                    detail["state"] == "queued" && detail["worker_id"].is_null(),
                    "unstarted death did not unbind"
                );
                cancel(gateway, admin, q.request_id, alice)?;
            }
            "preparing" => ensure!(
                detail["outcome"] == "not_executed" && detail["retry_safe"] == false,
                "prepared death was replayable"
            ),
            "unknown" => ensure!(
                detail["outcome"] == "unknown" && detail["retry_safe"] == false,
                "executing death certified outcome"
            ),
            "failed" => ensure!(
                detail["status"] == "failed"
                    && detail["outcome"] == "statement_result"
                    && detail["sql_completion"] == "failed",
                "recovery erased observed native failure"
            ),
            _ => ensure!(
                detail["status"] == "succeeded"
                    && detail["sql_completion"] == "succeeded"
                    && detail["result_state"] == "failed",
                "recovery erased checked SQL success"
            ),
        }
        ensure!(
            matches!(
                submit(gateway, alice, &q, registry)?,
                jobs::SubmitResult::Duplicate(_)
            ),
            "recovery replayed SQL"
        );
    }
    Ok(())
}
fn budget_retry(
    admin: &mut Client,
    gateway: &mut Client,
    engine: &mut Client,
    alice: &Identity,
    registry: &PoolRegistry,
) -> Result<()> {
    let w = worker(engine, "job-budget")?;
    let q = request();
    accepted(submit(gateway, alice, &q, registry)?)?;
    bind(gateway, engine, &w, q.request_id)?;
    let old: i64 = admin
        .query_one("SELECT prepaid_micro_usd FROM budget", &[])?
        .get(0);
    admin.execute(
        "UPDATE budget SET prepaid_micro_usd=spent_micro_usd+held_micro_usd",
        &[],
    )?;
    let selected = api(queue::select_next(engine, &w))?.context("budget selected")?;
    ensure!(
        queue::reserve(engine, selected, &rate())
            .err()
            .context("empty budget accepted")?
            .0
            == failure::BUDGET_EXHAUSTED,
        "wrong budget refusal"
    );
    admin.execute("UPDATE budget SET prepaid_micro_usd=$1", &[&old])?;
    let mut child = q.clone();
    child.request_id = Uuid::new_v4();
    child.retry_of = Some(q.request_id);
    let mut changed = registry.clone();
    changed.job_defaults.limits.runtime_seconds = 300;
    accepted(submit(gateway, alice, &child, &changed)?)?;
    let policy: String = admin
        .query_one(
            "SELECT policy_json FROM query_jobs WHERE request_id=$1",
            &[&child.request_id.to_string()],
        )?
        .get(0);
    ensure!(
        serde_json::from_str::<Value>(&policy)?["limits"]["runtime_seconds"] == 60,
        "retry re-resolved default runtime"
    );
    let mut sibling = child.clone();
    sibling.request_id = Uuid::new_v4();
    ensure!(
        jobs::submit(gateway, alice, api(sibling.validate())?, &changed, true)
            .err()
            .context("retry branched")?
            .0
            == failure::INVALID_RETRY,
        "wrong retry branch refusal"
    );
    cancel(gateway, admin, child.request_id, alice)?;
    Ok(())
}

fn final_denial(
    admin: &mut Client,
    gateway: &mut Client,
    engine: &mut Client,
    alice: &Identity,
    registry: &PoolRegistry,
) -> Result<()> {
    let w = worker(engine, "job-final-denial")?;
    let q = request();
    accepted(submit(gateway, alice, &q, registry)?)?;
    bind(gateway, engine, &w, q.request_id)?;
    let granted = grant(engine, &w)?;
    api(authz::set_policy(
        gateway,
        &Identity::bootstrap(),
        &alice.principal,
        &authz::PolicyUpdate {
            roles: vec![authz::Role::Analyst],
            permissions: vec![],
            compute: vec![authz::ComputeGrant {
                pool: Some("small".into()),
                mode: crate::compute::Mode::Persistent,
                cpu: 1,
                memory_bytes: 1 << 30,
                disk_bytes: 2 << 30,
                job_limits: Some(JobLimits {
                    runtime_seconds: 1,
                    ..JobLimits::HARD
                }),
            }],
            disabled: false,
        },
    ))?;
    let error = jobs::authorize_execution(
        engine,
        granted
            .prepared
            .selected
            .payload
            .job
            .as_ref()
            .context("denial execution")?,
    )
    .err()
    .context("removed job envelope accepted")?;
    ensure!(
        error.0 == failure::FORBIDDEN,
        "final job denial classification changed"
    );
    let evidence=admin.query_one("SELECT COUNT(*)::BIGINT,MAX(policy_version),MAX(credential_id) FROM audit_events WHERE target_id=$1 AND action='query.execute' AND decision='denied' AND reason='job_bounds_denied'",&[&q.request_id.to_string()])?;
    ensure!(
        evidence.get::<_, i64>(0) == 1
            && evidence.get::<_, Option<i64>>(1).is_some_and(|v| v > 0)
            && evidence.get::<_, Option<String>>(2) == alice.credential_id,
        "final envelope denial lost current policy/accepted actor audit"
    );
    let mut tx = engine.transaction()?;
    jobs::bounded(&mut tx)?;
    ensure!(
        queue::finish_in(
            &mut tx,
            &queue::Finish {
                request_id: q.request_id,
                epoch: &w,
                status: "refused",
                failure: Some(failure::FORBIDDEN),
                outcome: requests::Outcome::NotExecuted,
                elapsed_ms: 0,
                result_rows: None,
                error: Some("job bounds denied"),
                engine_started_at: None,
                profile: None,
                profile_status: "not_recorded",
                metrics: &json!({}),
                source: "fixture"
            }
        )?,
        "denial not terminal"
    );
    tx.commit()?;
    ensure!(admin.query_one("SELECT COUNT(*)::BIGINT,MAX(amount_micro_usd) FROM usage_events WHERE execution_id=$1",&[&q.request_id.to_string()])?.get::<_,Option<i64>>(1)==Some(0),"denied engine entry charged execution");
    Ok(())
}

fn zero_unexecuted(
    admin: &mut Client,
    gateway: &mut Client,
    engine: &mut Client,
    alice: &Identity,
    registry: &PoolRegistry,
) -> Result<()> {
    let w = worker(engine, "job-never-executed")?;
    for known in [false, true] {
        let q = request();
        accepted(submit(gateway, alice, &q, registry)?)?;
        bind(gateway, engine, &w, q.request_id)?;
        let granted = grant(engine, &w)?;
        let before: i64 = admin
            .query_one("SELECT spent_micro_usd FROM budget", &[])?
            .get(0);
        admin.execute("UPDATE requests SET execution_granted_at=clock_timestamp()-interval '5 seconds' WHERE request_id=$1",&[&q.request_id.to_string()])?;
        jobs::checkpoint_worker(engine, &w)?;
        ensure!(
            admin
                .query_one(
                    "SELECT occupied_checkpoint_ms FROM query_jobs WHERE request_id=$1",
                    &[&q.request_id.to_string()]
                )?
                .get::<_, i64>(0)
                >= 5000,
            "fixture did not checkpoint occupied preparation"
        );
        if known {
            jobs::mark_sql_complete(
                engine,
                &granted
                    .prepared
                    .selected
                    .payload
                    .job
                    .context("execution")?
                    .token,
                jobs::SqlCompletion::Succeeded,
                10,
            )?;
        }
        let mut tx = engine.transaction()?;
        jobs::bounded(&mut tx)?;
        ensure!(
            queue::finish_in(
                &mut tx,
                &queue::Finish {
                    request_id: q.request_id,
                    epoch: &w,
                    status: "refused",
                    failure: Some(failure::JOB_PREPARE_REFUSED),
                    outcome: requests::Outcome::NotExecuted,
                    elapsed_ms: 0,
                    result_rows: None,
                    error: Some("preparation refused"),
                    engine_started_at: None,
                    profile: None,
                    profile_status: "not_recorded",
                    metrics: &json!({}),
                    source: "fixture"
                }
            )?,
            "preparation finalization absent"
        );
        tx.commit()?;
        let row=admin.query_one("SELECT COUNT(*)::BIGINT,MAX(quantity),MAX(amount_micro_usd) FROM usage_events WHERE execution_id=$1",&[&q.request_id.to_string()])?;
        ensure!(
            row.get::<_, i64>(0) == 1,
            "preparation settled more than once"
        );
        let detail = api(jobs::detail(gateway, alice, q.request_id))?;
        if known {
            ensure!(
                detail["status"] == "succeeded"
                    && detail["outcome"] == "statement_result"
                    && row.get::<_, Option<i64>>(1).is_some_and(|v| v >= 5000),
                "stale no-entry evidence erased known SQL completion"
            );
        } else {
            ensure!(
                detail["outcome"] == "not_executed"
                    && detail["sql_completion"] == "not_executed"
                    && row.get::<_, Option<i64>>(1) == Some(0)
                    && row.get::<_, Option<i64>>(2) == Some(0),
                "occupied preparation was billed as execution"
            );
            ensure!(
                admin
                    .query_one("SELECT spent_micro_usd FROM budget", &[])?
                    .get::<_, i64>(0)
                    == before,
                "preparation changed spent budget"
            );
        }
        ensure!(
            admin
                .query_one(
                    "SELECT state FROM reservations WHERE execution_id=$1",
                    &[&q.request_id.to_string()]
                )?
                .get::<_, String>(0)
                == "settled",
            "preparation retained held funds"
        );
    }
    Ok(())
}
