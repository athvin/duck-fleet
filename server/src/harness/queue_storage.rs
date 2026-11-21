//! Real PostgreSQL queue invariants, isolated from running workers and budgets.
use crate::{
    ApiError, authz,
    credentials::{self, Identity},
    failure, platform, pricing,
    query::{AdmissionLimits, Protocol, QueryRequest, WorkerLimits},
    queue::{self, SessionRegistration, SubmitResult, WorkerEpoch},
    requests,
};
use anyhow::{Context, Result};
use postgres::{Client, NoTls};
use serde_json::json;
use std::time::{Duration, Instant};
use uuid::Uuid;

fn rate() -> pricing::RateCard {
    pricing::RateCard {
        version: "queue-storage-fixture".into(),
        compute_micro_usd_per_unit_hour: 100_000,
    }
}

fn api<T>(result: Result<T, ApiError>) -> Result<T> {
    result.map_err(|e| anyhow::anyhow!("{}: {}", e.0.kind, e.1))
}
fn actor(client: &mut Client, name: &str) -> Result<Identity> {
    let issued = credentials::issue(client, name, None)?;
    api(authz::set_policy(
        client,
        &Identity::bootstrap(),
        name,
        &authz::PolicyUpdate {
            roles: vec![authz::Role::Analyst],
            permissions: vec![],
            compute: vec![authz::ComputeGrant {
                pool: Some("fixture".into()),
                mode: crate::compute::Mode::Persistent,
                cpu: 1,
                memory_bytes: 1 << 30,
                disk_bytes: 2 << 30,
            }],
            disabled: false,
        },
    ))?;
    Ok(Identity {
        kind: credentials::ActorKind::Named,
        principal: name.to_owned(),
        credential_id: Some(issued.credential_id),
        expires_at: None,
    })
}
fn session(
    client: &mut Client,
    epoch: &WorkerEpoch,
    actor: &Identity,
) -> Result<SessionRegistration> {
    let s = SessionRegistration {
        session_id: Uuid::new_v4(),
        epoch: epoch.clone(),
        principal: actor.principal.clone(),
        authority: actor.clone(),
        context: json!({"suite":"queue-storage","requested_compute":{"pool":"fixture"}}),
        compute: json!({"mode":"persistent","pool":"fixture","cpu":1,"memory_bytes":1073741824_u64,"disk_bytes":2147483648_u64}),
        placement: json!({"worker_id":epoch.worker_id,"pool":"fixture"}),
        scope: "persistent:fixture".into(),
        protocol: Protocol::Http,
        open_span_id: Uuid::new_v4(),
        operation: authz::TrustedOperation::Sql,
    };
    api(queue::register_session(client, &s))?;
    Ok(s)
}
fn query() -> QueryRequest {
    QueryRequest {
        request_id: Uuid::new_v4(),
        retry_of: None,
        sql: "SELECT ?".into(),
        parameters: vec![json!(7)],
        context: json!({}),
        timeout_seconds: 30,
        queue_timeout_seconds: 120,
    }
}
fn submit(
    client: &mut Client,
    s: &SessionRegistration,
    q: QueryRequest,
    a: &Identity,
) -> Result<SubmitResult> {
    api(queue::submit(
        client,
        &s.epoch,
        s.session_id,
        q,
        a,
        Protocol::Http,
    ))
}
fn accepted(result: SubmitResult) -> Result<()> {
    anyhow::ensure!(
        matches!(result, SubmitResult::Accepted { .. }),
        "expected accepted query, got {result:?}"
    );
    Ok(())
}
fn cancel(client: &mut Client, epoch: &WorkerEpoch, id: Uuid, actor: &Identity) -> Result<()> {
    api(queue::request_cancel(client, id, actor))?;
    queue::poll_control(client, epoch, false, 256)?;
    Ok(())
}
fn outcome(client: &mut Client, id: Uuid) -> Result<(String, String)> {
    let r = client.query_one(
        "SELECT status,outcome FROM requests WHERE request_id=$1",
        &[&id.to_string()],
    )?;
    Ok((r.get(0), r.get(1)))
}

pub(super) async fn checks() -> Result<()> {
    let dsn = std::env::var("DUCKFLEET_PLATFORM_TEST_DSN")
        .context("--queue-storage requires DUCKFLEET_PLATFORM_TEST_DSN")?;
    tokio::task::spawn_blocking(move || -> Result<()> {
        let schema = format!("df_queue_{}", Uuid::new_v4().simple());
        let mut config: postgres::Config = dsn.parse()?;
        config.connect_timeout(Duration::from_secs(3));
        config.options(&format!(
            "-c search_path={schema} -c statement_timeout=10000"
        ));
        let mut client = config.connect(NoTls)?;
        client.batch_execute(&format!("CREATE SCHEMA {schema}"))?;
        let result = fixtures(&mut client, &config);
        let cleanup = client.batch_execute(&format!("DROP SCHEMA {schema} CASCADE"));
        result?;
        cleanup?;
        Ok(())
    })
    .await
    .context("queue storage fixture thread")??;
    println!(
        "PASS: durable queue claims, owner privacy, scoped limits, fairness, expiry, rotation, and recovery"
    );
    println!(
        "PASS: budget contention preserves visible queued identity and independently committed cancellation"
    );
    Ok(())
}

fn fixtures(client: &mut Client, config: &postgres::Config) -> Result<()> {
    let seed = platform::Seed {
        prepaid_micro_usd: 10_000_000,
        max_job_micro_usd: 1_000_000,
    };
    anyhow::ensure!(
        platform::migrate(client, &seed)?.version == platform::SCHEMA_VERSION,
        "current schema migration missing"
    );
    anyhow::ensure!(
        !platform::migrate(client, &seed)?.applied,
        "repeat migration rewrote schema"
    );
    let limits = AdmissionLimits {
        max_queued_queries: 4,
        max_running_queries: 1,
        max_queued_per_principal: 2,
        max_running_per_principal: 1,
    };
    queue::publish_policies(
        client,
        &queue::PolicyPublication {
            publication_id: Uuid::new_v4(),
            digest: "fixture".into(),
            policies: vec![("persistent:fixture".into(), limits)],
        },
    )?;
    let w1 = WorkerEpoch {
        worker_id: "fixture-one".into(),
        boot_id: Uuid::new_v4(),
    };
    let w2 = WorkerEpoch {
        worker_id: "fixture-two".into(),
        boot_id: Uuid::new_v4(),
    };
    for worker in [&w1, &w2] {
        anyhow::ensure!(
            queue::register_worker(
                client,
                worker,
                "persistent:fixture",
                WorkerLimits {
                    max_queued_queries: 4,
                    max_queued_bytes: 1_048_576
                }
            )?,
            "policy missing"
        );
    }
    let alice = actor(client, "alice")?;
    let bob = actor(client, "bob")?;
    let rotated = actor(client, "alice")?;
    let a1 = session(client, &w1, &alice)?;
    let a2 = session(client, &w1, &alice)?;
    let a3 = session(client, &w2, &alice)?;
    let b1 = session(client, &w1, &bob)?;
    let q1 = query();
    let q2 = query();
    let qb = query();
    accepted(submit(client, &a1, q1.clone(), &alice)?)?;
    anyhow::ensure!(
        matches!(
            submit(client, &a1, q1.clone(), &alice)?,
            SubmitResult::Duplicate(_)
        ),
        "duplicate consumed another queue slot"
    );
    let mut changed = q1.clone();
    changed.queue_timeout_seconds = 119;
    let error =
        queue::submit(client, &w1, a1.session_id, changed, &alice, Protocol::Http).unwrap_err();
    anyhow::ensure!(
        error.0 == failure::REQUEST_ID_MISMATCH,
        "v3 queue deadline did not participate in identity"
    );
    let rejected = submit(client, &a1, query(), &rotated)?;
    anyhow::ensure!(
        matches!(
            rejected,
            SubmitResult::Refused {
                error: ApiError(failure::SESSION_BUSY, _),
                ..
            }
        ),
        "same-session work was not refused"
    );
    let authority = queue::authorities(client, &w1)?
        .into_iter()
        .find(|a| a.session_id == a1.session_id)
        .context("session authority")?;
    anyhow::ensure!(
        authority.identity.credential_id == alice.credential_id,
        "rejected submission rotated authority"
    );
    let denied = queue::request_cancel(client, q1.request_id, &bob).unwrap_err();
    anyhow::ensure!(
        denied.0 == failure::NOT_FOUND,
        "cross-principal cancel exposed an attempt"
    );
    accepted(submit(client, &a2, q2.clone(), &alice)?)?;
    anyhow::ensure!(
        matches!(
            submit(client, &a3, query(), &alice)?,
            SubmitResult::Refused {
                error: ApiError(failure::QUEUE_CAPACITY, _),
                ..
            }
        ),
        "principal queued cap multiplied across workers"
    );
    accepted(submit(client, &b1, qb.clone(), &bob)?)?;
    let first = api(queue::select_next(client, &w1))?.context("first selection")?;
    anyhow::ensure!(
        first.payload.request.request_id == q1.request_id,
        "FIFO first turn changed"
    );
    let qa3 = query();
    accepted(submit(client, &a3, qa3.clone(), &alice)?)?;
    anyhow::ensure!(
        api(queue::select_next(client, &w2))?.is_none(),
        "pool running cap multiplied across workers"
    );
    let _prepared = api(queue::reserve(client, first, &rate()))?;
    cancel(client, &w1, q1.request_id, &alice)?;
    let second = api(queue::select_next(client, &w1))?.context("second selection")?;
    anyhow::ensure!(
        second.payload.principal == "bob",
        "many Alice sessions bought consecutive turns ahead of Bob"
    );
    cancel(client, &w1, qb.request_id, &bob)?;
    let other = api(queue::select_next(client, &w2))?.context("released global running permit")?;
    anyhow::ensure!(
        other.payload.request.request_id == qa3.request_id,
        "released permit failed to reach sibling"
    );
    cancel(client, &w2, qa3.request_id, &alice)?;
    cancel(client, &w1, q2.request_id, &alice)?;
    let usage=client.query_one("SELECT COUNT(*)::BIGINT,COALESCE(SUM(amount_micro_usd),0)::BIGINT FROM usage_events WHERE execution_id=$1",&[&q1.request_id.to_string()])?;
    anyhow::ensure!(
        usage.get::<_, i64>(0) == 1 && usage.get::<_, i64>(1) == 0,
        "prepared cancellation failed zero settlement"
    );

    // Durable B acceptance, not a header/local cache update, protects against a
    // housekeeping decision made earlier against opener A.
    let rotation = session(client, &w1, &alice)?;
    let stale = queue::authorities(client, &w1)?
        .into_iter()
        .find(|a| a.session_id == rotation.session_id)
        .context("rotation authority")?;
    let qr = query();
    accepted(submit(client, &rotation, qr.clone(), &rotated)?)?;
    client.execute(
        "UPDATE credentials SET revoked_at=clock_timestamp() WHERE credential_id=$1",
        &[&alice.credential_id],
    )?;
    anyhow::ensure!(
        !api(queue::begin_close(
            client,
            rotation.session_id,
            &w1,
            &queue::CloseIntent::Credential {
                credential_id: stale.identity.credential_id,
                authority_generation: stale.generation
            }
        ))?,
        "stale opener cleanup closed accepted B"
    );
    let current = queue::authorities(client, &w1)?
        .into_iter()
        .find(|a| a.session_id == rotation.session_id)
        .context("new authority")?;
    anyhow::ensure!(
        current.identity.credential_id == rotated.credential_id && !current.closing,
        "accepted replacement not authoritative"
    );
    cancel(client, &w1, qr.request_id, &rotated)?;

    let expiring = session(client, &w1, &bob)?;
    let qe = query();
    accepted(submit(client, &expiring, qe.clone(), &bob)?)?;
    client.execute("UPDATE requests SET queue_deadline_at=clock_timestamp()-interval '1 second' WHERE request_id=$1",&[&qe.request_id.to_string()])?;
    anyhow::ensure!(
        queue::expire_batch(client, 64, Duration::ZERO)? == 0,
        "exhausted maintenance budget started expiry work"
    );
    anyhow::ensure!(
        client
            .query_one(
                "SELECT state FROM requests WHERE request_id=$1",
                &[&qe.request_id.to_string()]
            )?
            .get::<_, String>(0)
            == "queued",
        "zero-budget expiry changed an attempt"
    );
    queue::poll_control(client, &w1, false, 64)?;
    anyhow::ensure!(
        outcome(client, qe.request_id)? == ("expired".into(), "not_executed".into()),
        "queued expiry missing"
    );

    fairness_fixture(client, &w1, &rotated, &bob)?;
    limit_fixtures(client, config, &w1, &w2, &rotated, &bob)?;
    budget_lock_fixture(client, config, &w1, &bob)?;
    cancel_completion_fixture(client, config, &w1, &bob)?;
    legacy_v2_fixture(client, &w1, &bob)?;
    recovery_fixture(client, &w1, &bob)?;
    anyhow::ensure!(
        client
            .query_one("SELECT COUNT(*)::BIGINT FROM query_permits", &[])?
            .get::<_, i64>(0)
            == 0,
        "live permits leaked"
    );
    anyhow::ensure!(
        client
            .query_one("SELECT COUNT(*)::BIGINT FROM query_payloads", &[])?
            .get::<_, i64>(0)
            == 0,
        "terminal payloads leaked"
    );
    Ok(())
}

fn fairness_fixture(
    client: &mut Client,
    epoch: &WorkerEpoch,
    noisy: &Identity,
    quiet: &Identity,
) -> Result<()> {
    let a1 = session(client, epoch, noisy)?;
    let a2 = session(client, epoch, noisy)?;
    let b = session(client, epoch, quiet)?;
    let qa1 = query();
    let qa2 = query();
    let qb = query();
    accepted(submit(client, &a1, qa1, noisy)?)?;
    accepted(submit(client, &a2, qa2, noisy)?)?;
    accepted(submit(client, &b, qb, quiet)?)?;
    let mut turns = Vec::new();
    // Alice continuously replenishes two independent sessions. Bob remains
    // eligible with one: session count must not buy consecutive principal turns.
    for _ in 0..12 {
        let selected = api(queue::select_next(client, epoch))?.context("fairness selection")?;
        let principal = selected.payload.principal.clone();
        let actor = if principal == noisy.principal {
            noisy
        } else {
            quiet
        };
        let s = if selected.payload.session_id == a1.session_id {
            &a1
        } else if selected.payload.session_id == a2.session_id {
            &a2
        } else {
            &b
        };
        cancel(client, epoch, selected.payload.request.request_id, actor)?;
        accepted(submit(client, s, query(), actor)?)?;
        turns.push(principal);
    }
    anyhow::ensure!(
        turns.windows(2).all(|pair| pair[0] != pair[1]),
        "sustained noisy principal bypassed turn fairness: {turns:?}"
    );
    for s in [&a1, &a2, &b] {
        let id = client
            .query_one(
                "SELECT outstanding FROM queue_sessions WHERE session_id=$1",
                &[&s.session_id.to_string()],
            )?
            .get::<_, String>(0);
        cancel(
            client,
            epoch,
            Uuid::parse_str(&id)?,
            if s.principal == noisy.principal {
                noisy
            } else {
                quiet
            },
        )?;
    }
    Ok(())
}

fn publish(client: &mut Client, limits: AdmissionLimits) -> Result<()> {
    queue::publish_policies(
        client,
        &queue::PolicyPublication {
            publication_id: Uuid::new_v4(),
            digest: serde_json::to_string(&limits)?,
            policies: vec![("persistent:fixture".into(), limits)],
        },
    )?;
    Ok(())
}
fn limit_fixtures(
    client: &mut Client,
    config: &postgres::Config,
    w1: &WorkerEpoch,
    w2: &WorkerEpoch,
    alice: &Identity,
    bob: &Identity,
) -> Result<()> {
    let limits = AdmissionLimits {
        max_queued_queries: 4,
        max_running_queries: 2,
        max_queued_per_principal: 2,
        max_running_per_principal: 1,
    };
    publish(client, limits)?;
    let w3 = WorkerEpoch {
        worker_id: "fixture-three".into(),
        boot_id: Uuid::new_v4(),
    };
    anyhow::ensure!(
        queue::register_worker(
            client,
            &w3,
            "persistent:fixture",
            WorkerLimits {
                max_queued_queries: 4,
                max_queued_bytes: 1_048_576
            }
        )?,
        "fixture policy missing"
    );
    let a1 = session(client, w1, alice)?;
    let a2 = session(client, w2, alice)?;
    let b = session(client, &w3, bob)?;
    let qa1 = query();
    let qa2 = query();
    let qb = query();
    accepted(submit(client, &a1, qa1.clone(), alice)?)?;
    accepted(submit(client, &a2, qa2.clone(), alice)?)?;
    accepted(submit(client, &b, qb.clone(), bob)?)?;
    anyhow::ensure!(
        api(queue::select_next(client, w1))?.is_some(),
        "first principal turn missing"
    );
    anyhow::ensure!(
        api(queue::select_next(client, w2))?.is_none(),
        "principal running cap multiplied across workers"
    );
    anyhow::ensure!(
        api(queue::select_next(client, &w3))?.is_some(),
        "principal cap incorrectly blocked other principals below pool cap"
    );
    cancel(client, w1, qa1.request_id, alice)?;
    cancel(client, w2, qa2.request_id, alice)?;
    cancel(client, &w3, qb.request_id, bob)?;

    // Requests fit the per-query envelope but exceed this worker's aggregate
    // queued byte allowance; a different session cannot bypass that limit.
    let a = session(client, &w3, alice)?;
    let b = session(client, &w3, bob)?;
    let mut large = query();
    large.parameters = vec![json!("x".repeat(600_000))];
    accepted(submit(client, &a, large.clone(), alice)?)?;
    let mut second = large.clone();
    second.request_id = Uuid::new_v4();
    anyhow::ensure!(
        matches!(
            submit(client, &b, second, bob)?,
            SubmitResult::Refused {
                error: ApiError(failure::QUEUE_CAPACITY, _),
                ..
            }
        ),
        "aggregate queued bytes exceeded worker cap"
    );
    cancel(client, &w3, large.request_id, alice)?;

    // Independent connections race different sessions/workers for the same
    // authoritative pool slot. Exactly one admission can commit.
    publish(
        client,
        AdmissionLimits {
            max_queued_queries: 1,
            max_queued_per_principal: 1,
            ..limits
        },
    )?;
    let left = session(client, w1, alice)?;
    let right = session(client, w2, bob)?;
    let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
    let handles = [(left, alice.clone()), (right, bob.clone())]
        .into_iter()
        .map(|(s, actor)| {
            let config = config.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || -> Result<_> {
                let mut connection = config.connect(NoTls)?;
                let q = query();
                let id = q.request_id;
                barrier.wait();
                let result = submit(&mut connection, &s, q, &actor)?;
                Ok((s, actor, id, result))
            })
        })
        .collect::<Vec<_>>();
    let mut admitted = 0;
    let mut refused = 0;
    let mut outcomes = Vec::new();
    for thread in handles {
        outcomes.push(
            thread
                .join()
                .map_err(|_| anyhow::anyhow!("concurrent admission thread panicked"))??,
        );
    }
    for (s, actor, id, result) in outcomes {
        match result {
            SubmitResult::Accepted { .. } => {
                admitted += 1;
                cancel(client, &s.epoch, id, &actor)?;
            }
            SubmitResult::Refused {
                error: ApiError(failure::QUEUE_CAPACITY, _),
                ..
            } => refused += 1,
            other => anyhow::bail!("unexpected concurrent admission result: {other:?}"),
        }
    }
    anyhow::ensure!(
        admitted == 1 && refused == 1,
        "shared cap admitted{admitted} refused{refused}"
    );
    publish(
        client,
        AdmissionLimits {
            max_running_queries: 1,
            ..limits
        },
    )?;
    Ok(())
}

fn cancel_completion_fixture(
    client: &mut Client,
    config: &postgres::Config,
    epoch: &WorkerEpoch,
    actor: &Identity,
) -> Result<()> {
    let s = session(client, epoch, actor)?;
    let q = query();
    accepted(submit(client, &s, q.clone(), actor)?)?;
    let selected = api(queue::select_next(client, epoch))?.context("cancel race selection")?;
    let prepared = api(queue::reserve(client, selected, &rate()))?;
    anyhow::ensure!(
        matches!(
            api(queue::grant(client, prepared))?,
            queue::GrantResult::Granted(_)
        ),
        "cancel race grant refused"
    );
    let mut completer = config.connect(NoTls)?;
    let mut tx = completer.transaction()?;
    anyhow::ensure!(
        queue::finish_in(
            &mut tx,
            &queue::Finish {
                request_id: q.request_id,
                epoch,
                status: "succeeded",
                failure: None,
                outcome: requests::Outcome::StatementResult,
                elapsed_ms: 1,
                result_rows: Some(1),
                error: None,
                engine_started_at: Some(std::time::SystemTime::now()),
                profile: None,
                profile_status: "not_recorded",
                metrics: &json!({}),
                source: "fixture",
            }
        )?,
        "terminal race did not persist completion"
    );
    let app = format!("queue-cancel-{}", Uuid::new_v4().simple());
    let mut cancel_config = config.clone();
    cancel_config.application_name(&app);
    let actor = actor.clone();
    let id = q.request_id;
    let waiting = std::thread::spawn(move || -> Result<_> {
        let mut client = cancel_config.connect(NoTls)?;
        api(queue::request_cancel(&mut client, id, &actor))
    });
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        if client.query_one("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE application_name=$1 AND wait_event_type='Lock')",&[&app])?.get::<_,bool>(0) {break}
        anyhow::ensure!(
            Instant::now() < deadline,
            "cancel did not race terminal control lock"
        );
        std::thread::sleep(Duration::from_millis(5));
    }
    tx.commit()?;
    let receipt = waiting
        .join()
        .map_err(|_| anyhow::anyhow!("cancel race thread panicked"))??;
    anyhow::ensure!(
        receipt["state"] == "terminal"
            && receipt["status"] == "succeeded"
            && receipt["cancel_requested"] == false,
        "cancel overwrote an already committed terminal outcome: {receipt}"
    );
    anyhow::ensure!(
        !client
            .query_one(
                "SELECT cancel_requested FROM query_controls WHERE request_id=$1",
                &[&id.to_string()]
            )?
            .get::<_, bool>(0),
        "terminal control row was mutated"
    );
    Ok(())
}

fn budget_lock_fixture(
    client: &mut Client,
    config: &postgres::Config,
    epoch: &WorkerEpoch,
    actor: &Identity,
) -> Result<()> {
    let s = session(client, epoch, actor)?;
    let q = query();
    accepted(submit(client, &s, q.clone(), actor)?)?;
    let selected = api(queue::select_next(client, epoch))?.context("budget candidate")?;
    let mut locker = config.connect(NoTls)?;
    let mut lock = locker.transaction()?;
    lock.query_one(
        "SELECT budget_id FROM budget WHERE budget_id='local' FOR UPDATE",
        &[],
    )?;
    let app = format!("queue-lock-{}", Uuid::new_v4().simple());
    let mut waiting = config.clone();
    waiting.application_name(&app);
    let thread = std::thread::spawn(move || -> Result<_> {
        let mut connection = waiting.connect(NoTls)?;
        Ok(queue::reserve(&mut connection, selected, &rate()))
    });
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        let blocked:bool=client.query_one("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE application_name=$1 AND wait_event_type='Lock')",&[&app])?.get(0);
        if blocked {
            break;
        }
        anyhow::ensure!(
            Instant::now() < deadline,
            "budget contender never waited for held row"
        );
        std::thread::sleep(Duration::from_millis(5));
    }
    anyhow::ensure!(
        client
            .query_one(
                "SELECT status FROM query_history WHERE query_id=$1",
                &[&q.request_id.to_string()]
            )?
            .get::<_, String>(0)
            == "preparing",
        "queued identity hidden by budget transaction"
    );
    let receipt = api(queue::request_cancel(client, q.request_id, actor))?;
    anyhow::ensure!(
        receipt["cancel_requested"] == true,
        "cancellation did not commit behind held budget"
    );
    lock.rollback()?;
    let reserved = thread
        .join()
        .map_err(|_| anyhow::anyhow!("budget fixture thread panicked"))??;
    if let Ok(prepared) = reserved {
        anyhow::ensure!(
            matches!(
                api(queue::grant(client, prepared))?,
                queue::GrantResult::Refused { .. }
            ),
            "canceled preparing query received execution grant"
        );
    }
    queue::poll_control(client, epoch, false, 64)?;
    anyhow::ensure!(
        outcome(client, q.request_id)?.1 == "not_executed",
        "budget-wait canceled request lost definite no-entry outcome"
    );
    Ok(())
}

fn legacy_v2_fixture(client: &mut Client, epoch: &WorkerEpoch, actor: &Identity) -> Result<()> {
    // Independently frozen from the #101 v2 canonical JSON, not recomputed by
    // the current helper under test.
    const V2: &str = "28721fd97f39352eadabfbf471615fe922c6b2ab769ac0ede3579bda694b11aa";
    let s = session(client, epoch, actor)?;
    let mut old = query();
    old.queue_timeout_seconds = 90;
    client.execute("INSERT INTO requests(request_id,principal,logical_request_id,fingerprint_version,payload_hash,state,status,outcome,failure_kind,retry_safe,worker_id)
        VALUES($1,$2,$1,2,$3,'terminal','refused','not_executed','budget_exhausted',true,$4)",&[&old.request_id.to_string(),&actor.principal,&V2,&epoch.worker_id])?;
    // A v2 attempt with a proven owner keeps its original hash across schema3.
    client.execute("UPDATE requests SET namespace_id=(SELECT id FROM platform_namespace),owner_principal_id=(SELECT principal_id FROM principals WHERE name=$2) WHERE request_id=$1", &[&old.request_id.to_string(),&actor.principal])?;
    anyhow::ensure!(
        matches!(
            submit(client, &s, old.clone(), actor)?,
            SubmitResult::Duplicate(_)
        ),
        "v2 payload rejected after queue upgrade"
    );
    let mut child = old.clone();
    child.request_id = Uuid::new_v4();
    child.retry_of = Some(old.request_id);
    accepted(submit(client, &s, child.clone(), actor)?)?;
    let row = client.query_one(
        "SELECT fingerprint_version,retry_of FROM requests WHERE request_id=$1",
        &[&child.request_id.to_string()],
    )?;
    anyhow::ensure!(
        row.get::<_, i32>(0) == 3 && row.get::<_, String>(1) == old.request_id.to_string(),
        "v2 parent did not create a v3 child"
    );
    cancel(client, epoch, child.request_id, actor)?;
    Ok(())
}

fn recovery_fixture(client: &mut Client, epoch: &WorkerEpoch, actor: &Identity) -> Result<()> {
    let a = session(client, epoch, actor)?;
    let qa = query();
    accepted(submit(client, &a, qa.clone(), actor)?)?;
    let selected = api(queue::select_next(client, epoch))?.context("recover candidate")?;
    let stale_selected = selected.clone();
    let prepared = api(queue::reserve(client, selected, &rate()))?;
    let stale_prepared = queue::Prepared {
        selected: prepared.selected.clone(),
        admission: prepared.admission.clone(),
    };
    anyhow::ensure!(
        matches!(
            api(queue::grant(client, prepared))?,
            queue::GrantResult::Granted(_)
        ),
        "recovery grant refused"
    );
    anyhow::ensure!(
        queue::reserve(client, stale_selected, &rate()).is_err(),
        "stale selection acquired another reservation"
    );
    anyhow::ensure!(
        matches!(
            api(queue::grant(client, stale_prepared))?,
            queue::GrantResult::Refused { .. }
        ),
        "stale preparation acquired another execution grant"
    );
    let state = client
        .query_one(
            "SELECT state FROM requests WHERE request_id=$1",
            &[&qa.request_id.to_string()],
        )?
        .get::<_, String>(0);
    anyhow::ensure!(
        state == "executing",
        "stale preparation terminalized a granted execution"
    );
    anyhow::ensure!(
        client
            .query_one(
                "SELECT COUNT(*)::BIGINT FROM query_permits WHERE request_id=$1",
                &[&qa.request_id.to_string()]
            )?
            .get::<_, i64>(0)
            == 1,
        "stale preparation released an executing permit"
    );
    let b = session(client, epoch, actor)?;
    let qb = query();
    accepted(submit(client, &b, qb.clone(), actor)?)?;
    let replacement = WorkerEpoch {
        worker_id: epoch.worker_id.clone(),
        boot_id: Uuid::new_v4(),
    };
    queue::recover_worker(client, &replacement, &rate())?;
    anyhow::ensure!(
        outcome(client, qa.request_id)? == ("interrupted".into(), "unknown".into()),
        "granted crash replay safety changed"
    );
    anyhow::ensure!(
        outcome(client, qb.request_id)? == ("refused".into(), "not_executed".into()),
        "queued crash was mislabeled uncertain execution"
    );
    anyhow::ensure!(
        queue::recover_worker(client, &replacement, &rate())? == 0,
        "repeated recovery did additional work"
    );
    let events = client
        .query_one(
            "SELECT COUNT(*)::BIGINT FROM usage_events WHERE execution_id=$1",
            &[&qa.request_id.to_string()],
        )?
        .get::<_, i64>(0);
    anyhow::ensure!(events == 1, "recovery charged an attempt more than once");
    Ok(())
}
