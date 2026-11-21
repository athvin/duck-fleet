//! Isolated PostgreSQL fixtures for durable-request upgrades and crash recovery.
//! These exercise the real migration/accounting helpers without touching the
//! running workers' rows or assuming a successful charge proves SQL committed.
use std::time::Duration;

use anyhow::{Context, Result};
use postgres::{Client, NoTls, Transaction};
use serde_json::json;

use super::{Ctx, require};
use crate::{credentials, metering, pricing::RateCard, requests};

const WORKER: &str = "request-upgrade-fixture";
const PRINCIPAL: &str = "upgrade-analyst";

pub(super) async fn checks(_ctx: &Ctx) -> Result<()> {
    let Ok(dsn) = std::env::var("DUCKFLEET_PLATFORM_TEST_DSN") else {
        println!("SKIP: request upgrade fixtures require DUCKFLEET_PLATFORM_TEST_DSN");
        return Ok(());
    };
    // Connecting, querying, rolling back, and dropping the synchronous client all
    // happen on the blocking pool. The generated search_path has no public fallback.
    tokio::task::spawn_blocking(move || -> Result<()> {
        let schema = format!("df_request_upgrade_{}", uuid::Uuid::new_v4().simple());
        let mut config: postgres::Config = dsn.parse().context("fixture PostgreSQL config")?;
        config.connect_timeout(Duration::from_secs(3));
        config.options(&format!(
            "-c search_path={schema} -c statement_timeout=10000"
        ));
        ownership_checks(&config, &schema)?;
        let mut client = config
            .connect(NoTls)
            .context("connecting upgrade fixture")?;
        client.batch_execute(&format!("CREATE SCHEMA {schema}"))?;
        let result = fixtures(&mut client, &schema);
        let cleanup = client.batch_execute(&format!("DROP SCHEMA {schema} CASCADE"));
        match (result, cleanup) {
            (Err(error), Err(cleanup)) => Err(error.context(format!(
                "also failed to remove isolated fixture schema {schema}: {cleanup}"
            ))),
            (Err(error), Ok(())) => Err(error),
            (Ok(()), cleanup) => cleanup.context("removing isolated upgrade fixture"),
        }
    })
    .await
    .context("request upgrade fixture task")??;
    println!("PASS: legacy request migration preserves ownership and unknown fingerprints");
    println!("PASS: crash recovery preserves uncertain outcomes and charges each attempt once");
    println!("PASS: retained request timing settles held funds after diagnostic history is pruned");
    Ok(())
}

fn ownership_checks(config: &postgres::Config, worker_id: &str) -> Result<()> {
    use crate::worker_ownership::Guard;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    let mut owner =
        Guard::try_claim(config.connect(NoTls)?, worker_id)?.context("first worker owner")?;
    let normal_callbacks = Arc::new(AtomicUsize::new(0));
    let notified = normal_callbacks.clone();
    owner.on_loss(move || {
        notified.fetch_add(1, Ordering::SeqCst);
    });
    require(
        Guard::try_claim(config.connect(NoTls)?, worker_id)?.is_none(),
        "replacement worker recovered while the previous process still owned its identity",
    )?;
    let sibling = Guard::try_claim(config.connect(NoTls)?, &format!("{worker_id}-sibling"))?;
    require(
        sibling.is_some(),
        "another worker identity was blocked by ownership",
    )?;
    require(
        !owner.is_closed(),
        "live worker ownership reported disconnected",
    )?;
    // No calls to check while this thread is busy: the independent heartbeat
    // must keep a healthy worker's lock alive beyond the server's 10s idle limit.
    std::thread::sleep(Duration::from_secs(11));
    require(owner.check(), "ownership expired while its worker was busy")?;
    require(
        Guard::try_claim(config.connect(NoTls)?, worker_id)?.is_none(),
        "replacement acquired an identity whose independent heartbeat was healthy",
    )?;
    drop(owner);
    require(
        normal_callbacks.load(Ordering::SeqCst) == 0,
        "normal ownership drop invoked the terminal-loss callback",
    )?;
    let deadline = std::time::Instant::now() + Duration::from_secs(3);
    loop {
        if Guard::try_claim(config.connect(NoTls)?, worker_id)?.is_some() {
            break;
        }
        require(
            std::time::Instant::now() < deadline,
            "released identity was never available for recovery",
        )?;
        std::thread::sleep(Duration::from_millis(20));
    }
    let mut guarded_config = config.clone();
    guarded_config.application_name(worker_id);
    let mut lost = Guard::try_claim(guarded_config.connect(NoTls)?, worker_id)?
        .context("worker owner before forced disconnect")?;
    let loss_callbacks = Arc::new(AtomicUsize::new(0));
    let notified = loss_callbacks.clone();
    lost.on_loss(move || {
        notified.fetch_add(1, Ordering::SeqCst);
    });
    let mut admin = config.connect(NoTls)?;
    let pid: i32 = admin
        .query_one(
            "SELECT pid FROM pg_stat_activity WHERE application_name=$1",
            &[&worker_id],
        )?
        .get(0);
    admin.query_one("SELECT pg_terminate_backend($1)", &[&pid])?;
    require(
        !lost.check() && lost.is_closed() && !lost.check(),
        "disconnected ownership was not permanently refused",
    )?;
    let callback_deadline = std::time::Instant::now() + Duration::from_secs(3);
    while loss_callbacks.load(Ordering::SeqCst) == 0 {
        require(
            std::time::Instant::now() < callback_deadline,
            "ownership loss did not notify the worker",
        )?;
        std::thread::sleep(Duration::from_millis(10));
    }
    drop(lost);
    require(
        loss_callbacks.load(Ordering::SeqCst) == 1,
        "ownership loss callback ran more than once",
    )?;
    require(
        Guard::try_claim(config.connect(NoTls)?, worker_id)?.is_some(),
        "replacement could not acquire a terminated backend's identity",
    )?;

    // Expire a server session without dropping or polling its client object.
    // This reproduces a dead pod whose TCP connection has not been reaped yet.
    let orphan_id = format!("{worker_id}-orphan");
    let mut orphan = config.connect(NoTls)?;
    orphan.batch_execute("SET statement_timeout=3000; SET idle_session_timeout=500")?;
    require(
        orphan
            .query_one(
                "SELECT pg_try_advisory_lock(4283122,hashtext($1))",
                &[&orphan_id],
            )?
            .get(0),
        "orphan fixture could not claim its identity",
    )?;
    let deadline = std::time::Instant::now() + Duration::from_secs(3);
    let replacement = loop {
        if let Some(guard) = Guard::try_claim(config.connect(NoTls)?, &orphan_id)? {
            break guard;
        }
        require(
            std::time::Instant::now() < deadline,
            "server idle expiry did not release the orphaned advisory lock",
        )?;
        std::thread::sleep(Duration::from_millis(20));
    };
    require(
        orphan.query_one("SELECT 1", &[]).is_err(),
        "orphan was replaced before its old server session expired",
    )?;
    drop(replacement);

    // A connection can fail between acquiring ownership and installing the
    // worker callback. Late registration must immediately observe sticky loss.
    let late_id = format!("{worker_id}-late");
    guarded_config.application_name(&late_id);
    let mut late = Guard::try_claim(guarded_config.connect(NoTls)?, &late_id)?
        .context("owner before late callback registration")?;
    let pid: i32 = admin
        .query_one(
            "SELECT pid FROM pg_stat_activity WHERE application_name=$1",
            &[&late_id],
        )?
        .get(0);
    admin.query_one("SELECT pg_terminate_backend($1)", &[&pid])?;
    require(!late.check(), "terminated owner passed a fresh probe")?;
    let late_callbacks = Arc::new(AtomicUsize::new(0));
    let notified = late_callbacks.clone();
    late.on_loss(move || {
        notified.fetch_add(1, Ordering::SeqCst);
    });
    require(
        late_callbacks.load(Ordering::SeqCst) == 1 && !late.check(),
        "already-lost ownership did not notify a late callback exactly once",
    )?;
    drop(late);
    require(
        late_callbacks.load(Ordering::SeqCst) == 1,
        "dropping an already-lost owner repeated its notification",
    )?;
    println!(
        "PASS: independent ownership heartbeats survive long work, orphan sessions expire, and loss callbacks fire exactly once"
    );
    Ok(())
}

fn submit(
    client: &mut Client,
    id: &str,
    principal: &str,
    rate: &RateCard,
) -> Result<requests::AdmissionResult, crate::ApiError> {
    requests::admit(
        client,
        requests::Submission {
            request_id: id,
            retry_of: None,
            session_id: "upgrade-fixture-session",
            sql: "SELECT $1",
            parameters: &[json!(1)],
            context: &json!({"fixture": "upgrade"}),
            timeout_seconds: 60,
            queue_timeout_seconds: 10,
            compute: &json!({"mode":"persistent","cpu":1,"memory_bytes":1073741824_u64,"disk_bytes":1073741824_u64}),
            worker_id: WORKER,
            placement: &json!({"worker_id": WORKER}),
            principal,
        },
        1,
        rate,
    )
}

fn admit(client: &mut Client, id: &str, rate: &RateCard) -> Result<metering::Admission> {
    match submit(client, id, PRINCIPAL, rate)
        .map_err(|error| anyhow::anyhow!("fixture admission: {}: {}", error.0.kind, error.1))?
    {
        requests::AdmissionResult::Admitted(admission) => Ok(admission),
        requests::AdmissionResult::Duplicate(_) => anyhow::bail!("fresh fixture ID was duplicate"),
    }
}

fn finish(
    transaction: &mut Transaction<'_>,
    admission: &metering::Admission,
    outcome: requests::Outcome,
    elapsed_ms: i64,
    rate: &RateCard,
) -> Result<()> {
    let refused = outcome == requests::Outcome::NotExecuted;
    let status = if refused { "refused" } else { "succeeded" };
    requests::finish_in(
        transaction,
        &admission.execution_id,
        status,
        refused.then_some("unauthorized"),
        refused.then_some(false),
        elapsed_ms,
        (!refused).then_some(1),
        outcome,
    )?;
    transaction.execute(
        "UPDATE query_history SET status=$2,elapsed_ms=$3,finished_at=now() WHERE query_id=$1",
        &[&admission.execution_id, &status, &elapsed_ms],
    )?;
    metering::settle_in(transaction, admission, elapsed_ms, WORKER, "{}", rate)?;
    Ok(())
}

fn assert_outcome(client: &mut Client, id: &str, status: &str, outcome: &str) -> Result<()> {
    let row = client.query_one(
        "SELECT state,status,outcome,retry_safe FROM requests WHERE request_id=$1",
        &[&id],
    )?;
    require(
        row.get::<_, String>("state") == "terminal"
            && row.get::<_, String>("status") == status
            && row.get::<_, String>("outcome") == outcome,
        format!("recovery changed the durable outcome of {id}"),
    )?;
    if outcome == "unknown" {
        require(
            row.get::<_, Option<bool>>("retry_safe") == Some(false),
            format!("uncertain request {id} was marked safe to retry"),
        )?;
    }
    Ok(())
}

fn assert_accounting(client: &mut Client) -> Result<(i64, i64, i64)> {
    let row = client.query_one(
        "SELECT spent_micro_usd,held_micro_usd,
            (SELECT COALESCE(SUM(amount_micro_usd),0)::BIGINT FROM usage_events) AS spent,
            (SELECT COALESCE(SUM(reserved_micro_usd),0)::BIGINT FROM reservations WHERE state='held') AS held,
            (SELECT COUNT(*) FROM usage_events) AS events
         FROM budget WHERE budget_id='local'",
        &[],
    )?;
    let spent: i64 = row.get("spent");
    let held: i64 = row.get("held");
    require(
        row.get::<_, i64>("spent_micro_usd") == spent
            && row.get::<_, i64>("held_micro_usd") == held,
        "budget accumulators diverged from reservations and append-only usage",
    )?;
    Ok((spent, held, row.get("events")))
}

fn fixtures(client: &mut Client, schema: &str) -> Result<()> {
    require(
        client
            .query_one("SELECT current_schema()", &[])?
            .get::<_, String>(0)
            == schema,
        "upgrade fixture must stay inside its isolated schema",
    )?;
    let rate = RateCard {
        version: "upgrade-fixture-1".into(),
        compute_micro_usd_per_unit_hour: 100_000,
    };
    let mut transaction = client.transaction()?;
    // This is the pre-upgrade history shape; the requests module consumes only
    // these persisted fields, and cannot reconstruct old bound parameters.
    transaction.batch_execute(
        "CREATE TABLE query_history (
            query_id TEXT PRIMARY KEY,session_id TEXT NOT NULL,sql TEXT NOT NULL,
            context TEXT NOT NULL,status TEXT NOT NULL,
            accepted_at TIMESTAMPTZ NOT NULL DEFAULT now(),finished_at TIMESTAMPTZ,
            elapsed_ms BIGINT,result_rows BIGINT,error TEXT,
            worker_id TEXT NOT NULL,placement TEXT NOT NULL DEFAULT '{}',
            principal TEXT NOT NULL DEFAULT 'direct',failure_kind TEXT,retry_safe BOOLEAN
        );",
    )?;
    metering::migrate(&mut transaction, 10_000_000, 1_000_000)?;
    transaction.execute(
        "INSERT INTO query_history(query_id,session_id,sql,context,status,worker_id,
            principal,elapsed_ms,finished_at,result_rows)
         VALUES ('legacy-history','old-session','SELECT $1','{}','succeeded',$1,$2,36000,now(),1),
            ('legacy-pruned','old-session','SELECT $1','{}','succeeded',$1,$2,36000,now(),1)",
        &[&WORKER, &PRINCIPAL],
    )?;
    transaction.execute(
        "INSERT INTO reservations(execution_id,reserved_micro_usd,resource_units,state,
            worker_id,settled_micro_usd,settled_at,rate_micro_usd_per_unit_hour,rate_card_version)
         VALUES ('legacy-reservation',2000,1,'settled',$1,0,now(),$2,$3),
            ('legacy-pruned',2000,1,'held',$1,NULL,NULL,$2,$3)",
        &[
            &WORKER,
            &rate.compute_micro_usd_per_unit_hour,
            &rate.version,
        ],
    )?;
    transaction.execute(
        "INSERT INTO usage_events(event_id,execution_id,meter_name,quantity,unit,resource_units,
            amount_micro_usd,rate_card_version,worker_id,context,source,started_at,ended_at)
         VALUES ('legacy-usage:compute','legacy-usage','compute',0,'millisecond',1,
            0,$1,$2,'{}','worker',now(),now())",
        &[&rate.version, &WORKER],
    )?;
    requests::migrate(&mut transaction)?;
    metering::reconcile(&mut transaction)?;
    transaction.commit()?;

    let history = client.query_one(
        "SELECT principal,payload_hash,fingerprint_version,status,outcome FROM requests WHERE request_id='legacy-history'",
        &[],
    )?;
    require(
        history.get::<_, Option<String>>("principal").as_deref() == Some(PRINCIPAL)
            && history.get::<_, Option<String>>("payload_hash").is_none()
            && history
                .get::<_, Option<i32>>("fingerprint_version")
                .is_none()
            && history.get::<_, String>("status") == "succeeded"
            && history.get::<_, String>("outcome") == "statement_result",
        "legacy history must retain its known owner without inventing a fingerprint",
    )?;
    for id in ["legacy-reservation", "legacy-usage"] {
        let row = client.query_one(
            "SELECT principal,payload_hash,outcome FROM requests WHERE request_id=$1",
            &[&id],
        )?;
        require(
            row.get::<_, Option<String>>("principal").is_none()
                && row.get::<_, Option<String>>("payload_hash").is_none()
                && row.get::<_, String>("outcome") == "unknown",
            "reservation/usage-only evidence must not invent ownership or SQL success",
        )?;
        let refused = submit(client, id, PRINCIPAL, &rate);
        require(
            matches!(refused, Err(error) if error.0.kind == "not_found"),
            "a replay claimed an ownerless legacy request",
        )?;
        require(
            matches!(
                submit(client, id, credentials::BOOTSTRAP_PRINCIPAL, &rate),
                Ok(requests::AdmissionResult::Duplicate(_))
            ),
            "bootstrap replay did not preserve the unknown legacy attempt",
        )?;
    }
    require(
        matches!(
            submit(client, "legacy-history", PRINCIPAL, &rate),
            Ok(requests::AdmissionResult::Duplicate(_))
        ),
        "legacy history was re-admitted from a caller-supplied fingerprint",
    )?;

    let accepted = admit(client, "crash-accepted", &rate)?;
    let executing = admit(client, "crash-executing", &rate)?;
    let rollback = admit(client, "crash-terminal-rollback", &rate)?;
    let settled = admit(client, "settled-success", &rate)?;
    let refused = admit(client, "settled-not-executed", &rate)?;
    for id in [&executing.execution_id, &rollback.execution_id] {
        requests::mark_executing(client, id)
            .map_err(|error| anyhow::anyhow!("fixture execution transition: {}", error.1))?;
    }
    let fingerprint: String = client
        .query_one(
            "SELECT payload_hash FROM requests WHERE request_id=$1",
            &[&accepted.execution_id],
        )?
        .get(0);
    let mut transaction = client.transaction()?;
    requests::migrate(&mut transaction)?;
    requests::migrate(&mut transaction)?;
    transaction.commit()?;
    let preserved = client.query_one(
        "SELECT payload_hash,principal,fingerprint_version FROM requests WHERE request_id=$1",
        &[&accepted.execution_id],
    )?;
    require(
        preserved.get::<_, String>("payload_hash") == fingerprint
            && preserved.get::<_, String>("principal") == PRINCIPAL
            && preserved.get::<_, i32>("fingerprint_version") == 3,
        "repeated migration rewrote a fingerprinted request",
    )?;
    require(
        client.query_one("SELECT COUNT(*) FROM requests WHERE request_id LIKE 'legacy-%' AND payload_hash IS NOT NULL", &[])?.get::<_, i64>(0) == 0,
        "migration or duplicate submission certified an unknown legacy fingerprint",
    )?;

    // Losing the terminal transaction must lose all three changes together.
    let before_rollback = assert_accounting(client)?;
    let mut transaction = client.transaction()?;
    finish(
        &mut transaction,
        &rollback,
        requests::Outcome::StatementResult,
        36_000,
        &rate,
    )?;
    transaction.rollback()?;
    let state = client.query_one(
        "SELECT r.state,h.status,s.state AS reservation_state
         FROM requests r JOIN query_history h ON h.query_id=r.request_id
         JOIN reservations s ON s.execution_id=r.request_id WHERE r.request_id=$1",
        &[&rollback.execution_id],
    )?;
    require(
        state.get::<_, String>("state") == "executing"
            && state.get::<_, String>("status") == "running"
            && state.get::<_, String>("reservation_state") == "held"
            && assert_accounting(client)? == before_rollback,
        "rolled-back terminal transaction leaked outcome, history, or billing changes",
    )?;
    let mut transaction = client.transaction()?;
    finish(
        &mut transaction,
        &settled,
        requests::Outcome::StatementResult,
        36_000,
        &rate,
    )?;
    finish(
        &mut transaction,
        &refused,
        requests::Outcome::NotExecuted,
        0,
        &rate,
    )?;
    transaction.commit()?;
    client.execute(
        "DELETE FROM query_history WHERE query_id='legacy-pruned'",
        &[],
    )?;

    // A changed runtime price must not reprice an old held reservation. Legacy
    // split settlement can leave terminal timing plus a hold; the migration's
    // durable copy remains useful even after the diagnostic row is deleted.
    let recovery_rate = RateCard {
        version: "upgrade-fixture-2".into(),
        compute_micro_usd_per_unit_hour: 900_000,
    };
    let mut transaction = client.transaction()?;
    metering::interrupt_abandoned_history(&mut transaction, WORKER)?;
    let recovered = metering::recover(&mut transaction, WORKER, &recovery_rate)?;
    transaction.commit()?;
    require(
        recovered == 4,
        "recovery did not settle exactly the four held attempts",
    )?;
    for id in [
        &accepted.execution_id,
        &executing.execution_id,
        &rollback.execution_id,
    ] {
        assert_outcome(client, id, "interrupted", "unknown")?;
        let event = client.query_one(
            "SELECT quantity,amount_micro_usd,source FROM usage_events WHERE execution_id=$1",
            &[&id],
        )?;
        require(
            event.get::<_, i64>("quantity") == 0
                && event.get::<_, i64>("amount_micro_usd") == 0
                && event.get::<_, String>("source") == "recovery",
            "unknown abandoned execution acquired invented elapsed time or charges",
        )?;
    }
    assert_outcome(
        client,
        &settled.execution_id,
        "succeeded",
        "statement_result",
    )?;
    assert_outcome(client, &refused.execution_id, "refused", "not_executed")?;
    assert_outcome(client, "legacy-pruned", "succeeded", "statement_result")?;
    let pruned = client.query_one(
        "SELECT quantity,amount_micro_usd,rate_card_version FROM usage_events WHERE execution_id='legacy-pruned'",
        &[],
    )?;
    require(
        pruned.get::<_, i64>("quantity") == 36_000
            && pruned.get::<_, i64>("amount_micro_usd") == 1_000
            && pruned.get::<_, String>("rate_card_version") == rate.version,
        "pruned history erased durable timing or recovery repriced the old reservation",
    )?;
    let after_recovery = assert_accounting(client)?;
    require(
        after_recovery == (2_000, 0, 7),
        "recovery produced extra charges or retained holds",
    )?;
    let mut transaction = client.transaction()?;
    metering::interrupt_abandoned_prefix(&mut transaction, "request-upgrade-")?;
    require(
        metering::recover(&mut transaction, WORKER, &recovery_rate)? == 0,
        "repeated recovery found an already settled hold",
    )?;
    metering::reconcile(&mut transaction)?;
    transaction.commit()?;
    require(
        assert_accounting(client)? == after_recovery,
        "repeated recovery or reconciliation charged an attempt twice",
    )?;
    assert_outcome(client, &refused.execution_id, "refused", "not_executed")?;
    assert_outcome(
        client,
        &settled.execution_id,
        "succeeded",
        "statement_result",
    )?;
    policy_upgrade_fixtures(client, &rate)?;
    Ok(())
}

fn policy_upgrade_fixtures(client: &mut Client, rate: &RateCard) -> Result<()> {
    // Independently frozen pre-pools hashes. No current fingerprint helper is
    // used to manufacture historical identity, and no original history is needed.
    let cases = [
        (
            "direct",
            "a249ad7c77cc1abc9c6a2e6f479e1df1d58c229218bb9cee1ceaa4aa708e7103",
            false,
        ),
        (
            "gateway",
            "227f543c6262a7fcf36ddf2ebf4acdc823512cff439e9fd60bfab229503866bc",
            false,
        ),
        (
            "small",
            "969f5d969855167364657a532132f703ba79a3a5dc3d3d7024927ef3d0ae5f2f",
            true,
        ),
    ];
    let before = assert_accounting(client)?;
    for (name, hash, persistent) in cases {
        let parent = format!("v1-policy-{name}");
        let child = format!("v2-policy-{name}");
        client.execute(
            "INSERT INTO requests(request_id,principal,logical_request_id,fingerprint_version,
                payload_hash,state,status,outcome,failure_kind,retry_safe,worker_id,finished_at)
             VALUES ($1,$2,$1,1,$3,'terminal','refused','not_executed','budget_exhausted',true,$4,now())",
            &[&parent, &PRINCIPAL, &hash, &WORKER],
        )?;
        let compute = json!({"mode":if persistent { "persistent" } else { "isolated" },
            "pool":if persistent { Some("small") } else { None },
            "cpu":if persistent {1} else {2},"memory_bytes":1073741824_u64,"disk_bytes":2147483648_u64});
        let context = json!({"connection_name":"legacy-policy","requested_compute":
            if persistent { json!({"pool":"small"}) } else { json!({"cpu":2}) }});
        let parameters = [json!(7)];
        let placement = json!({"worker_id":WORKER});
        let input = |id, retry_of| requests::Submission {
            request_id: id,
            retry_of,
            session_id: "new-policy-session",
            sql: "INSERT INTO events VALUES (?)",
            parameters: &parameters,
            context: &context,
            timeout_seconds: 30,
            queue_timeout_seconds: 10,
            compute: &compute,
            worker_id: WORKER,
            placement: &placement,
            principal: PRINCIPAL,
        };
        let replay = requests::admit(client, input(&parent, None), 2, rate)
            .map_err(|error| anyhow::anyhow!("v1 {name} replay: {}", error.1))?;
        require(
            matches!(replay, requests::AdmissionResult::Duplicate(value) if value["failure_kind"] == "duplicate_request"),
            "v1 replay executed again",
        )?;
        let mut foreign = input(&parent, None);
        foreign.principal = "another-principal";
        require(
            matches!(requests::admit(client, foreign, 2, rate),
            Err(crate::ApiError(failure, _)) if failure.kind == "not_found"),
            "v1 replay exposed another principal's identity",
        )?;
        let mut changed = input(&parent, None);
        changed.sql = "INSERT INTO events VALUES (?);";
        require(
            matches!(requests::admit(client, changed, 2, rate),
            Err(crate::ApiError(failure, _)) if failure.kind == "request_id_mismatch"),
            "v1 replay accepted changed SQL",
        )?;
        let admission = match requests::admit(client, input(&child, Some(&parent)), 2, rate)
            .map_err(|error| anyhow::anyhow!("v1 {name} retry: {}", error.1))?
        {
            requests::AdmissionResult::Admitted(admission) => admission,
            requests::AdmissionResult::Duplicate(_) => {
                anyhow::bail!("fresh v3 child was duplicate")
            }
        };
        let identities = client.query_one(
            "SELECT p.fingerprint_version,p.payload_hash,c.fingerprint_version AS child_version,
                c.logical_request_id,c.retry_of FROM requests p JOIN requests c ON c.retry_of=p.request_id
             WHERE p.request_id=$1", &[&parent],
        )?;
        require(
            identities.get::<_, i32>("fingerprint_version") == 1
                && identities.get::<_, String>("payload_hash") == hash
                && identities.get::<_, i32>("child_version") == 3
                && identities.get::<_, String>("logical_request_id") == parent
                && identities.get::<_, String>("retry_of") == parent,
            "v1 parent -> v3 child changed historical evidence or lost lineage",
        )?;
        requests::mark_executing(client, &child)
            .map_err(|error| anyhow::anyhow!("v3 execution grant: {}", error.1))?;
        let mut transaction = client.transaction()?;
        finish(
            &mut transaction,
            &admission,
            requests::Outcome::StatementResult,
            1000,
            rate,
        )?;
        transaction.commit()?;
        let replay = requests::admit(client, input(&child, Some(&parent)), 2, rate)
            .map_err(|error| anyhow::anyhow!("v2 child replay: {}", error.1))?;
        require(
            matches!(replay, requests::AdmissionResult::Duplicate(_)),
            "v2 child replay executed again",
        )?;
        let count: i64 = client
            .query_one(
                "SELECT COUNT(*) FROM usage_events WHERE execution_id=$1 OR execution_id=$2",
                &[&parent, &child],
            )?
            .get(0);
        require(count == 1, "v1 parent/v2 child replay produced extra usage")?;
        let sibling = format!("forbidden-sibling-{name}");
        require(
            matches!(requests::admit(client, input(&sibling, Some(&parent)), 2, rate),
            Err(crate::ApiError(failure, _)) if failure.kind == "invalid_retry"),
            "v1 parent allowed a second retry branch",
        )?;
    }
    let after = assert_accounting(client)?;
    require(
        after.1 == 0 && after.2 == before.2 + 3,
        "fingerprint upgrade left held funds or duplicate events",
    )?;
    println!(
        "PASS: frozen v1 direct/gateway/default identities survive pruning and produce one v3 retry child without duplicate charges"
    );
    Ok(())
}
