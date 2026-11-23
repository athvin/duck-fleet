//! Real PostgreSQL migration, fencing, retention and startup-volume fixtures.
//! Every mutation is confined to a generated schema which is removed afterwards.
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use postgres::{Client, NoTls};
use serde_json::json;
use uuid::Uuid;

use super::require;
use crate::{metering, platform, pricing::RateCard, requests};

pub(super) async fn checks() -> Result<()> {
    let dsn = std::env::var("DUCKFLEET_PLATFORM_TEST_DSN")
        .context("platform fixtures require DUCKFLEET_PLATFORM_TEST_DSN")?;
    tokio::task::spawn_blocking(move || fixture(&dsn)).await??;
    Ok(())
}

fn fixture(dsn: &str) -> Result<()> {
    let schema = format!("df_platform_{}", Uuid::new_v4().simple());
    let mut config: postgres::Config = dsn.parse()?;
    config.connect_timeout(Duration::from_secs(3));
    config.options(&format!(
        "-c search_path={schema} -c statement_timeout=10000"
    ));
    let mut client = config.connect(NoTls)?;
    client.batch_execute(&format!("CREATE SCHEMA {schema}"))?;
    let result = (|| -> Result<()> {
        checks_in_schema(&mut client, &config)?;
        let invalid = platform::wait_schema("not_a_postgres_option=1", Duration::from_secs(5))
            .err()
            .context("malformed platform DSN was accepted")?;
        require(
            invalid
                .to_string()
                .contains("invalid platform connection settings"),
            "malformed DSN was hidden behind a startup retry timeout",
        )?;
        let missing_database = format!("df_absent_{}", Uuid::new_v4().simple());
        let missing_dsn = if dsn.starts_with("postgres://") || dsn.starts_with("postgresql://") {
            let mut url = reqwest::Url::parse(dsn)?;
            url.set_path(&format!("/{missing_database}"));
            url.to_string()
        } else {
            format!("{dsn} dbname={missing_database}")
        };
        let refused = platform::wait_schema(&missing_dsn, Duration::from_secs(5))
            .err()
            .context("nonexistent platform database was accepted")?;
        require(
            refused.to_string().contains("platform startup refused"),
            "fatal database configuration error was hidden behind a startup retry timeout",
        )?;
        // The real connection helper must preserve a custom search_path rather
        // than silently inspecting or migrating the deployment's public schema.
        let isolated_dsn = isolated_dsn(dsn, &schema)?;
        let mut connected = platform::wait_schema(&isolated_dsn, Duration::from_secs(3))?;
        require(
            connected
                .query_one("SELECT current_schema()", &[])?
                .get::<_, String>(0)
                == schema,
            "platform connection dropped the configured search_path",
        )?;
        platform::worker_startup(&mut connected, "policy-fixture-no-orphans", &rate())?;
        let settings = connected.query_one(
            "SELECT current_setting('statement_timeout'),
            current_setting('idle_in_transaction_session_timeout')",
            &[],
        )?;
        require(
            settings.get::<_, String>(0) == "10s" && settings.get::<_, String>(1) == "17s",
            "schema validation or local recovery changed the foreground ledger timeout policy",
        )?;
        println!(
            "PASS: schema startup preserves DSN options and foreground timeouts while failing fatal configuration promptly"
        );
        Ok(())
    })();
    let cleanup = client.batch_execute(&format!("DROP SCHEMA {schema} CASCADE"));
    match (result, cleanup) {
        (Err(error), Err(cleanup)) => Err(error.context(format!("fixture cleanup: {cleanup}"))),
        (Err(error), _) => Err(error),
        (Ok(()), cleanup) => cleanup.context("remove platform fixture"),
    }
}

fn isolated_dsn(dsn: &str, schema: &str) -> Result<String> {
    let options = format!(
        "-c search_path={schema} -c statement_timeout=10000 -c idle_in_transaction_session_timeout=17000"
    );
    if dsn.starts_with("postgres://") || dsn.starts_with("postgresql://") {
        let mut url = reqwest::Url::parse(dsn)?;
        let pairs: Vec<_> = url
            .query_pairs()
            .filter(|(key, _)| key != "options")
            .map(|(key, value)| (key.into_owned(), value.into_owned()))
            .collect();
        url.query_pairs_mut()
            .clear()
            .extend_pairs(pairs)
            .append_pair("options", &options);
        Ok(url.to_string())
    } else {
        // The appended value contains only generated identifier characters and
        // fixed settings, and a repeated libpq keyword intentionally replaces it.
        Ok(format!("{dsn} options='{options}'"))
    }
}

fn seed() -> platform::Seed {
    platform::Seed {
        prepaid_micro_usd: 100_000_000,
        max_job_micro_usd: 1_000_000,
    }
}

fn rate() -> RateCard {
    RateCard {
        version: "platform-fixture-1".into(),
        compute_micro_usd_per_unit_hour: 100_000,
    }
}

fn admit(client: &mut Client, id: &str, worker: &str) -> Result<metering::Admission> {
    match requests::admit(
        client,
        requests::Submission {
            request_id: id,
            retry_of: None,
            session_id: "platform-fixture",
            sql: "SELECT 1",
            parameters: &[],
            context: &json!({}),
            timeout_seconds: 30,
            queue_timeout_seconds: 10,
            compute: &json!({"mode":"persistent","cpu":1}),
            worker_id: worker,
            placement: &json!({"worker_id":worker}),
            principal: "platform-fixture-user",
        },
        1,
        &rate(),
    )
    .map_err(|error| anyhow::anyhow!("{}: {}", error.0.kind, error.1))?
    {
        requests::AdmissionResult::Admitted(admission) => Ok(admission),
        requests::AdmissionResult::Duplicate(_) => anyhow::bail!("fixture ID unexpectedly reused"),
    }
}

fn finish(client: &mut Client, admission: &metering::Admission, worker: &str) -> Result<()> {
    let mut tx = client.transaction()?;
    requests::finish_in(
        &mut tx,
        &admission.execution_id,
        "succeeded",
        None,
        None,
        1000,
        Some(1),
        requests::Outcome::StatementResult,
    )?;
    tx.execute("UPDATE query_history SET status='succeeded',elapsed_ms=1000,finished_at=now() WHERE query_id=$1",
        &[&admission.execution_id])?;
    metering::settle_in(&mut tx, admission, 1000, worker, "{}", &rate())?;
    tx.commit()?;
    Ok(())
}

fn accounting(client: &mut Client) -> Result<()> {
    let row = client.query_one("SELECT spent_micro_usd,held_micro_usd,
        (SELECT COALESCE(SUM(amount_micro_usd),0)::BIGINT FROM usage_events) AS spent,
        (SELECT COALESCE(SUM(reserved_micro_usd),0)::BIGINT FROM reservations WHERE state='held') AS held
        FROM budget WHERE budget_id='local'", &[])?;
    require(
        row.get::<_, i64>("spent_micro_usd") == row.get::<_, i64>("spent")
            && row.get::<_, i64>("held_micro_usd") == row.get::<_, i64>("held"),
        "maintenance lost a concurrent budget delta",
    )
}

fn migration_failure(client: &mut Client) -> Result<()> {
    for bad in [
        platform::Seed {
            prepaid_micro_usd: 0,
            max_job_micro_usd: 1,
        },
        platform::Seed {
            prepaid_micro_usd: 1,
            max_job_micro_usd: 0,
        },
    ] {
        require(
            platform::migrate(client, &bad).is_err(),
            "migration accepted a zero budget seed",
        )?;
    }
    // Fail after history, credentials and accounting DDL have run, proving the
    // baseline marker and every preceding change share the same transaction.
    client.batch_execute("CREATE TABLE requests(request_id TEXT PRIMARY KEY)")?;
    require(
        platform::migrate(client, &seed()).is_err(),
        "malformed requests schema unexpectedly migrated",
    )?;
    let state = client.query_one(
        "SELECT to_regclass('query_history') IS NULL
            AND to_regclass('credentials') IS NULL
            AND to_regclass('budget') IS NULL
            AND to_regclass('usage_events') IS NULL
            AND to_regclass('requests_schema_migrations') IS NULL
            AND to_regclass('platform_schema_migrations') IS NULL
            AND to_regclass('platform_maintenance_lease') IS NULL",
        &[],
    )?;
    require(
        state.get(0),
        "failed baseline left partially committed schema or an advanced version",
    )?;
    client.batch_execute("DROP TABLE requests")?;
    println!("PASS: invalid seeds and a mid-migration schema failure leave no partial baseline");
    Ok(())
}

/// This generated-schema fixture reconstructs the real schema-1 surface before
/// testing upgrades. Merely deleting version markers leaves schema-2 DDL behind
/// and does not represent any supported historical installation.
pub(super) fn remove_job_fixture_schema(client: &mut Client) -> Result<()> {
    let current: i32 = client
        .query_one("SELECT MAX(version) FROM platform_schema_migrations", &[])?
        .get(0);
    if current >= 7 {
        client.batch_execute("ALTER TABLE query_history DROP COLUMN sql_status; ALTER TABLE query_history ALTER COLUMN sql SET NOT NULL; DELETE FROM platform_schema_migrations WHERE version=7;")?;
    }
    if current >= 6 {
        client.batch_execute("DROP TABLE compute_lifecycles,compute_reservations,compute_policies; DELETE FROM platform_schema_migrations WHERE version=6;")?;
    }
    if current < 5 {
        return Ok(());
    }
    client.batch_execute("DROP TABLE job_artifacts,job_bindings,query_jobs,job_dispatch_fairness; DROP SEQUENCE job_dispatch_turn;
        ALTER TABLE requests DROP COLUMN work_kind; ALTER TABLE requests ALTER COLUMN worker_id SET NOT NULL;
        ALTER TABLE query_history DROP COLUMN work_kind; ALTER TABLE query_history ALTER COLUMN worker_id SET NOT NULL; ALTER TABLE query_history ALTER COLUMN session_id SET NOT NULL;
        ALTER TABLE query_permits ALTER COLUMN worker_id SET NOT NULL; ALTER TABLE query_permits ALTER COLUMN boot_id SET NOT NULL;
        ALTER TABLE query_permits DROP CONSTRAINT query_permits_check,DROP CONSTRAINT query_permits_check1;
        ALTER TABLE admission_policies DROP COLUMN job_limits;ALTER TABLE reservations DROP COLUMN deadline_seconds;
        DELETE FROM platform_maintenance_tasks WHERE task='artifacts';DELETE FROM platform_schema_migrations WHERE version=5;")?;
    Ok(())
}
pub(super) fn remove_system_fixture_schema(client: &mut Client) -> Result<()> {
    remove_job_fixture_schema(client)?;
    let current: i32 = client
        .query_one("SELECT MAX(version) FROM platform_schema_migrations", &[])?
        .get(0);
    if current < 4 {
        return Ok(());
    }
    client.batch_execute("DROP INDEX history_system_window,history_system_own_window,requests_system_window,usage_system_window,usage_system_own_window")?;
    for (table, columns) in [
        ("queue_sessions", "system_dimensions"),
        (
            "requests",
            "open_span_id execution_timeout_seconds requested_mode requested_pool requested_cpu requested_memory_bytes requested_disk_bytes selected_mode selected_pool selected_cpu selected_memory_bytes selected_disk_bytes engine_version registry_revision query_fingerprint query_fingerprint_version normalization_status model_id invocation_id model_run_id",
        ),
        (
            "query_history",
            "profile_capture_version profile_latency_seconds profile_cpu_seconds profile_rows_returned profile_rows_scanned peak_buffer_memory_bytes peak_temp_dir_bytes bytes_read bytes_written",
        ),
        (
            "reservations",
            "selected_mode selected_pool owner_principal_name model_id invocation_id model_run_id",
        ),
        (
            "usage_events",
            "selected_mode selected_pool owner_principal_name model_id invocation_id model_run_id rate_micro_usd_per_unit_hour",
        ),
    ] {
        for column in columns.split_whitespace() {
            client.batch_execute(&format!("ALTER TABLE {table} DROP COLUMN {column}"))?;
        }
    }
    client.execute(
        "DELETE FROM platform_schema_migrations WHERE version=4",
        &[],
    )?;
    Ok(())
}

pub(super) fn remove_authz_fixture_schema(client: &mut Client) -> Result<()> {
    remove_system_fixture_schema(client)?;
    for table in [
        "requests",
        "query_history",
        "queue_sessions",
        "reservations",
        "usage_events",
    ] {
        for column in [
            "namespace_id",
            "owner_principal_id",
            "actor_principal_id",
            "actor_kind",
            "actor_credential_id",
        ] {
            client.batch_execute(&format!("ALTER TABLE {table} DROP COLUMN {column}"))?;
        }
        if table != "requests" {
            client.batch_execute(&format!("ALTER TABLE {table} DROP COLUMN actor_principal"))?;
        }
    }
    client.batch_execute("ALTER TABLE requests DROP COLUMN authorization_policy_version;
        ALTER TABLE queue_sessions DROP COLUMN authority_kind,DROP COLUMN authority_principal_id,DROP COLUMN authority_policy_version,DROP COLUMN operation;
        ALTER TABLE credentials DROP COLUMN principal_id;
        DROP TABLE audit_events,authz_legacy_owners,principal_roles,permission_grants,compute_grants,principals,authz_defaults,platform_namespace;
        DROP FUNCTION immutable_audit_event();
        DELETE FROM platform_schema_migrations WHERE version=3;")?;
    Ok(())
}

fn remove_queue_fixture_schema(client: &mut Client) -> Result<()> {
    remove_authz_fixture_schema(client)?;
    client.batch_execute(
        "DROP TABLE admission_policies,admission_publication,admission_scopes,
        queue_workers,queue_sessions,query_payloads,query_controls,query_permits,
        queue_fairness,queue_notifications,session_events",
    )?;
    for column in [
        "session_id",
        "session_sequence",
        "worker_boot_id",
        "pool_scope",
        "protocol",
        "actor_principal",
        "credential_id",
        "queued_at",
        "selected_at",
        "execution_granted_at",
        "engine_started_at",
        "queue_deadline_at",
        "queue_timeout_seconds",
        "policy_generation",
        "state_version",
        "delivery_state",
    ] {
        client.batch_execute(&format!("ALTER TABLE requests DROP COLUMN {column}"))?;
    }
    for column in [
        "queued_at",
        "selected_at",
        "execution_granted_at",
        "engine_started_at",
        "queue_deadline_at",
        "session_sequence",
        "worker_boot_id",
        "pool_scope",
        "protocol",
        "open_span_id",
    ] {
        client.batch_execute(&format!("ALTER TABLE query_history DROP COLUMN {column}"))?;
    }
    client.batch_execute(
        "ALTER TABLE requests DROP CONSTRAINT requests_state_check;
        ALTER TABLE requests ADD CHECK(state IN ('accepted','executing','terminal'))",
    )?;
    Ok(())
}

fn versioned_queue_upgrade(client: &mut Client) -> Result<()> {
    let baseline: String = client
        .query_one(
            "SELECT applied_at::TEXT FROM platform_schema_migrations WHERE version=1",
            &[],
        )?
        .get(0);
    let before: i64 = client
        .query_one(
            "SELECT spent_micro_usd FROM budget WHERE budget_id='local'",
            &[],
        )?
        .get(0);
    remove_queue_fixture_schema(client)?;
    client.execute(
        "DELETE FROM platform_schema_migrations WHERE version=2",
        &[],
    )?;
    // A version-1 upgrade must not rerun the baseline's global reconciliation.
    client.execute(
        "UPDATE budget SET spent_micro_usd=spent_micro_usd+19 WHERE budget_id='local'",
        &[],
    )?;
    let report = platform::migrate(client, &seed())?;
    require(
        report.applied && report.version == platform::SCHEMA_VERSION,
        "ordered queue migration missing",
    )?;
    require(
        client
            .query_one(
                "SELECT spent_micro_usd FROM budget WHERE budget_id='local'",
                &[],
            )?
            .get::<_, i64>(0)
            == before + 19,
        "versioned queue upgrade repeated global accounting reconciliation",
    )?;
    require(
        client
            .query_one(
                "SELECT applied_at::TEXT FROM platform_schema_migrations WHERE version=1",
                &[],
            )?
            .get::<_, String>(0)
            == baseline,
        "versioned queue upgrade rewrote the baseline marker",
    )?;
    client.execute(
        "UPDATE budget SET spent_micro_usd=$1 WHERE budget_id='local'",
        &[&before],
    )?;
    println!(
        "PASS: schema1 upgrades through queue and authorization without repeating baseline scans or budget reconciliation"
    );
    Ok(())
}

fn adoption_and_future_schema(client: &mut Client, credential_id: &str) -> Result<()> {
    let credential: String = client
        .query_one(
            "SELECT (to_jsonb(c)-'principal_id')::TEXT FROM credentials c WHERE credential_id=$1",
            &[&credential_id],
        )?
        .get(0);
    let fingerprint: String = client
        .query_one(
            "SELECT to_jsonb(r)::TEXT FROM requests r WHERE request_id='preserved-request'",
            &[],
        )?
        .get(0);
    let budget: String = client.query_one(
        "SELECT jsonb_build_array(prepaid_micro_usd,max_job_micro_usd,spent_micro_usd,held_micro_usd)::TEXT FROM budget", &[],
    )?.get(0);
    let usage: String = client
        .query_one(
            "SELECT jsonb_agg(to_jsonb(u) ORDER BY event_id)::TEXT FROM usage_events u",
            &[],
        )?
        .get(0);
    remove_queue_fixture_schema(client)?;
    // Reconstruct the supported pre-#100 deployment: #99 request identities and
    // its one-time marker already exist, but platform version/maintenance tables
    // do not. No durable customer or billing table is dropped.
    client.batch_execute("DROP TABLE platform_maintenance_runs,platform_maintenance_tasks,
        platform_maintenance_lease,platform_schema_migrations;
        INSERT INTO requests(request_id,principal,logical_request_id,state,status,outcome,worker_id,elapsed_ms)
        VALUES ('pre100-legacy','legacy-analyst','pre100-legacy','terminal','succeeded','statement_result','legacy-worker',1000);
        INSERT INTO query_history(query_id,session_id,sql,context,status,worker_id,principal,accepted_at,finished_at,elapsed_ms)
        VALUES ('pre100-retention-audit','platform','-- history retention sweep','{}','succeeded',
            'old-pruner','platform',now()-interval '2 days',now()-interval '2 days',0);
        UPDATE budget SET spent_micro_usd=spent_micro_usd+19,held_micro_usd=held_micro_usd+23;")?;
    require(
        platform::check_schema(client).is_err(),
        "unversioned platform was treated as ready",
    )?;
    let report = platform::migrate(
        client,
        &platform::Seed {
            prepaid_micro_usd: 1,
            max_job_micro_usd: 1,
        },
    )?;
    require(
        report.applied && report.version == platform::SCHEMA_VERSION,
        "unversioned adoption did not apply its baseline",
    )?;
    require(
        client
            .query_one(
                "SELECT (to_jsonb(c)-'principal_id')::TEXT FROM credentials c WHERE credential_id=$1",
                &[&credential_id],
            )?
            .get::<_, String>(0)
            == credential,
        "baseline adoption changed an existing credential",
    )?;
    require(
        client
            .query_one(
                "SELECT to_jsonb(r)::TEXT FROM requests r WHERE request_id='preserved-request'",
                &[],
            )?
            .get::<_, String>(0)
            == fingerprint,
        "baseline adoption changed request ownership, fingerprint or outcome",
    )?;
    require(client.query_one("SELECT jsonb_build_array(prepaid_micro_usd,max_job_micro_usd,spent_micro_usd,held_micro_usd)::TEXT FROM budget", &[])?.get::<_, String>(0) == budget,
        "baseline adoption changed budget limits or failed to repair stale accumulators")?;
    require(
        client
            .query_one(
                "SELECT jsonb_agg(to_jsonb(u) ORDER BY event_id)::TEXT FROM usage_events u",
                &[],
            )?
            .get::<_, String>(0)
            == usage,
        "baseline adoption changed immutable usage evidence",
    )?;
    require(client.query_one("SELECT principal='legacy-analyst' AND payload_hash IS NULL AND fingerprint_version IS NULL
        AND outcome='statement_result' FROM requests WHERE request_id='pre100-legacy'", &[])?.get::<_, bool>(0),
        "baseline adoption invented a legacy fingerprint or erased its known owner")?;
    require(
        client
            .query_one(
                "SELECT COUNT(*) FROM requests_schema_migrations WHERE version=1",
                &[],
            )?
            .get::<_, i64>(0)
            == 1,
        "baseline adoption discarded the completed request-backfill marker",
    )?;
    require(
        client.execute("DELETE FROM usage_events", &[]).is_err(),
        "baseline adoption lost append-only usage protection",
    )?;
    require(
        client
            .query_one(
                "SELECT principal IS NULL AND payload_hash IS NULL
        AND fingerprint_version IS NULL AND state='terminal' AND outcome='statement_result'
        FROM requests WHERE request_id='pre100-retention-audit'",
                &[],
            )?
            .get::<_, bool>(0),
        "baseline did not preserve the identity of audit history written after the old backfill marker",
    )?;
    let audit_retention = platform::MaintenanceConfig {
        retention_days: Some(1),
        retention_batch_size: 1,
        retention_max_batches: 1,
        ..platform::MaintenanceConfig::default()
    };
    platform::maintenance_pass(client, Uuid::new_v4(), &audit_retention, true)?;
    require(client.query_one("SELECT NOT EXISTS(SELECT 1 FROM query_history WHERE query_id='pre100-retention-audit')
        AND EXISTS(SELECT 1 FROM requests WHERE request_id='pre100-retention-audit')", &[])?.get::<_, bool>(0),
        "adopted synthetic audit history could not be pruned without erasing its identity")?;
    accounting(client)?;

    let future = platform::SCHEMA_VERSION + 1;
    client.execute(
        "INSERT INTO platform_schema_migrations(version,name) VALUES ($1,'fixture-future')",
        &[&future],
    )?;
    require(
        platform::check_schema(client).is_err(),
        "worker accepted a future schema",
    )?;
    require(
        platform::migrate(client, &seed()).is_err(),
        "migration accepted or downgraded a future schema",
    )?;
    require(
        client
            .query_one("SELECT MAX(version) FROM platform_schema_migrations", &[])?
            .get::<_, i32>(0)
            == future,
        "refused migration rewrote the future schema marker",
    )?;
    require(client.query_one("SELECT jsonb_build_array(prepaid_micro_usd,max_job_micro_usd,spent_micro_usd,held_micro_usd)::TEXT FROM budget", &[])?.get::<_, String>(0) == budget,
        "future-schema refusal changed accounting")?;
    client.execute(
        "DELETE FROM platform_schema_migrations WHERE version=$1",
        &[&future],
    )?;
    platform::check_schema(client)?;
    println!(
        "PASS: unversioned adoption preserves credentials, fingerprints, legacy identities and usage while refusing future schemas"
    );
    Ok(())
}

fn checks_in_schema(client: &mut Client, config: &postgres::Config) -> Result<()> {
    require(
        platform::check_schema(client).is_err(),
        "missing schema accepted",
    )?;
    migration_failure(client)?;
    platform::migrate(client, &seed())?;
    platform::check_schema(client)?;
    let credential = crate::credentials::issue(client, "fixture-operator", None)?;
    let request = admit(client, "preserved-request", "preserved-worker")?;
    finish(client, &request, "preserved-worker")?;
    let budget_before: i64 = client
        .query_one("SELECT prepaid_micro_usd FROM budget", &[])?
        .get(0);
    platform::migrate(
        client,
        &platform::Seed {
            prepaid_micro_usd: 1,
            max_job_micro_usd: 1,
        },
    )?;
    require(
        client
            .query_one("SELECT prepaid_micro_usd FROM budget", &[])?
            .get::<_, i64>(0)
            == budget_before,
        "repeat migration refilled or reset the budget",
    )?;
    require(
        client
            .query_one(
                "SELECT COUNT(*) FROM credentials WHERE credential_id=$1",
                &[&credential.credential_id],
            )?
            .get::<_, i64>(0)
            == 1,
        "repeat migration lost credentials",
    )?;
    require(client.query_one("SELECT payload_hash IS NOT NULL FROM requests WHERE request_id='preserved-request'", &[])?
        .get::<_, bool>(0), "repeat migration lost a request fingerprint")?;
    require(
        client.execute("DELETE FROM usage_events", &[]).is_err(),
        "migration removed append-only protection",
    )?;
    accounting(client)?;
    println!(
        "PASS: fresh and repeat migration preserve budget, credentials, fingerprints and immutable usage"
    );
    versioned_queue_upgrade(client)?;
    adoption_and_future_schema(client, &credential.credential_id)?;

    let cfg = platform::MaintenanceConfig {
        retention_days: Some(1),
        retention_batch_size: 2,
        retention_max_batches: 1,
        ..platform::MaintenanceConfig::default()
    };
    lease_checks(client, config, &cfg)?;
    abandoned_transaction(client, config)?;
    maintenance_lock_failure(client, config, &cfg)?;
    retention_checks(client, &cfg)?;
    artifact_retention_checks(client)?;
    maintenance_fairness(client, config)?;
    compute_lifecycle_checks(client, config)?;
    concurrent_accounting(client, config, &cfg)?;
    startup_scaling(client, config)?;
    Ok(())
}

fn artifact_retention_checks(client: &mut Client) -> Result<()> {
    let admission = admit(client, "artifact-expired", "artifact-worker")?;
    finish(client, &admission, "artifact-worker")?;
    client.execute(
        "UPDATE query_history SET sql='select sensitive_literal',profile='{\"latency\":1}',profile_status='captured',accepted_at=now()-interval '2 days' WHERE query_id='artifact-expired'",
        &[],
    )?;
    let running = admit(client, "artifact-running", "artifact-worker")?;
    client.execute(
        "UPDATE query_history SET sql='select still_running',accepted_at=now()-interval '2 days' WHERE query_id=$1",
        &[&running.execution_id],
    )?;
    let usage: i64 = client
        .query_one("SELECT count(*) FROM usage_events", &[])?
        .get(0);
    let config = platform::MaintenanceConfig {
        sql_retention_days: Some(1),
        profile_retention_days: Some(1),
        retention_batch_size: 1,
        retention_max_batches: 1,
        ..platform::MaintenanceConfig::default()
    };
    for expected in [2, 0] {
        let report = platform::maintenance_pass(client, Uuid::new_v4(), &config, true)?;
        let retention = report
            .tasks
            .iter()
            .find(|task| task.task == "retention")
            .unwrap();
        require(
            retention.rows_pruned == expected,
            "artifact cleanup count or repeat settlement drifted",
        )?;
    }
    let expired = client.query_one(
        "SELECT sql IS NULL,sql_status,profile IS NULL,profile_status,metrics FROM query_history WHERE query_id='artifact-expired'",
        &[],
    )?;
    require(
        expired.get::<_, bool>(0)
            && expired.get::<_, String>(1) == "expired"
            && expired.get::<_, bool>(2)
            && expired.get::<_, String>(3) == "expired"
            && expired.get::<_, String>(4) == "{}",
        "independent artifact expiry did not preserve the compact summary",
    )?;
    require(
        client
            .query_one(
                "SELECT sql FROM query_history WHERE query_id=$1",
                &[&running.execution_id],
            )?
            .get::<_, Option<String>>(0)
            .as_deref()
            == Some("select still_running"),
        "artifact retention expired a running statement",
    )?;
    require(
        client
            .query_one("SELECT count(*) FROM usage_events", &[])?
            .get::<_, i64>(0)
            == usage,
        "artifact retention changed immutable usage evidence",
    )?;
    println!(
        "PASS: independent SQL/profile retention preserves running statements, metrics and usage"
    );
    Ok(())
}

fn compute_lifecycle_checks(client: &mut Client, config: &postgres::Config) -> Result<()> {
    client.batch_execute("DELETE FROM compute_reservations; DELETE FROM compute_lifecycles")?;
    crate::compute_lifecycle::publish_policy(
        client,
        &crate::compute_lifecycle::Policy {
            max_cpu: Some(1),
            max_memory_bytes: Some(1 << 30),
            max_disk_bytes: Some(2 << 30),
            max_principal_cpu: Some(1),
            max_principal_memory_bytes: Some(1 << 30),
            max_principal_disk_bytes: Some(2 << 30),
            idle_timeout_seconds: Some(1),
            max_lifetime_seconds: Some(60),
        },
    )
    .map_err(|error| anyhow::anyhow!(error.1))?;
    let resources = crate::compute::Resources::default();
    let first_id = format!("lifecycle-{}", Uuid::new_v4());
    let mut first = client.transaction()?;
    crate::compute_lifecycle::reserve(
        &mut first,
        &first_id,
        Some("principal-a"),
        "isolated",
        resources,
    )
    .map_err(|error| anyhow::anyhow!(error.1))?;
    first.commit()?;
    let mut repeated = client.transaction()?;
    crate::compute_lifecycle::reserve(
        &mut repeated,
        &first_id,
        Some("principal-a"),
        "isolated",
        resources,
    )
    .map_err(|error| anyhow::anyhow!(error.1))?;
    repeated.commit()?;
    require(
        client.query_one(
            "SELECT count(*) FROM compute_reservations WHERE lifecycle_id=$1 AND released_at IS NULL",
            &[&first_id],
        )?.get::<_, i64>(0) == 1,
        "a dispatcher retry duplicated its lifecycle reservation",
    )?;
    let mut other = config.connect(NoTls)?;
    let mut competing = other.transaction()?;
    let second_id = format!("lifecycle-{}", Uuid::new_v4());
    let refused = crate::compute_lifecycle::reserve(
        &mut competing,
        &second_id,
        Some("principal-b"),
        "isolated",
        resources,
    )
    .expect_err("a concurrent open must not oversubscribe the one-CPU cap");
    require(
        refused.0.kind == crate::failure::CAPACITY.kind,
        "wrong resource refusal kind",
    )?;
    drop(competing);
    crate::compute_lifecycle::ready(client, &first_id, "isolated-fixture", "boot-fixture")
        .map_err(|error| anyhow::anyhow!(error.1))?;
    crate::compute_lifecycle::activity(client, &first_id, true)
        .map_err(|error| anyhow::anyhow!(error.1))?;
    crate::compute_lifecycle::activity(client, &first_id, false)
        .map_err(|error| anyhow::anyhow!(error.1))?;
    crate::compute_lifecycle::finish(client, &first_id, Some("failed_provisioning"))
        .map_err(|error| anyhow::anyhow!(error.1))?;
    crate::compute_lifecycle::finish(client, &first_id, None)
        .map_err(|error| anyhow::anyhow!(error.1))?;
    let mut retry = other.transaction()?;
    crate::compute_lifecycle::reserve(
        &mut retry,
        &second_id,
        Some("principal-b"),
        "isolated",
        resources,
    )
    .map_err(|error| anyhow::anyhow!(error.1))?;
    retry.commit()?;
    crate::compute_lifecycle::ready(client, &second_id, "restart-fixture", "boot-before-restart")
        .map_err(|error| anyhow::anyhow!(error.1))?;
    crate::compute_lifecycle::finish_worker(client, "restart-fixture")
        .map_err(|error| anyhow::anyhow!(error.1))?;
    crate::compute_lifecycle::finish_worker(client, "restart-fixture")
        .map_err(|error| anyhow::anyhow!(error.1))?;
    let restarted = client.query_one(
        "SELECT state,failure_kind FROM compute_lifecycles WHERE lifecycle_id=$1",
        &[&second_id],
    )?;
    require(
        restarted.get::<_, String>(0) == "terminated"
            && restarted.get::<_, Option<String>>(1).as_deref() == Some("worker_restarted"),
        "restart recovery did not settle the previous boot exactly once",
    )?;
    let active: i64 = client
        .query_one(
            "SELECT count(*) FROM compute_reservations WHERE released_at IS NULL",
            &[],
        )?
        .get(0);
    require(active == 0, "lifecycle settlement leaked a reservation")?;
    let reports = crate::compute_lifecycle::report(client, &rate())
        .map_err(|error| anyhow::anyhow!(error.1))?;
    let first = reports
        .iter()
        .find(|report| report["lifecycle_id"] == first_id)
        .context("missing lifecycle report")?;
    require(
        first["failure_kind"] == "failed_provisioning",
        "failure evidence missing",
    )?;
    require(
        first["measured_usage"]["cpu_seconds"].is_number(),
        "missing measured CPU-seconds",
    )?;
    require(
        first["measured_usage"]["gib_seconds"].is_number(),
        "missing measured GiB-seconds",
    )?;
    require(
        first["measured_usage"]["startup_ms"].is_number(),
        "missing startup duration",
    )?;
    require(
        first["measured_usage"]["idle_ms"].is_number(),
        "missing idle duration",
    )?;
    require(
        first["execution_ms"].is_number(),
        "missing execution duration",
    )?;
    require(
        first["measured_usage"]["utilization"].is_number(),
        "missing utilization",
    )?;
    require(
        first["estimated_provider_cost"]["basis"]
            == "estimate_only_not_reconciled_provider_billing",
        "estimated cost was presented as reconciled billing",
    )?;
    println!(
        "PASS: competing compute admission, failed provisioning, idle deadlines, idempotent settlement and lifecycle reporting"
    );
    Ok(())
}

fn lease_checks(
    client: &mut Client,
    config: &postgres::Config,
    cfg: &platform::MaintenanceConfig,
) -> Result<()> {
    let first = platform::claim_maintenance(client, Uuid::new_v4(), cfg)?.context("first lease")?;
    let mut other = config.connect(NoTls)?;
    require(
        platform::claim_maintenance(&mut other, Uuid::new_v4(), cfg)?.is_none(),
        "second coordinator stole a live lease",
    )?;
    // A transaction from a vanished owner must roll back, then a later generation
    // can take over. Explicit expiry keeps this test fast without wall-clock races.
    {
        let mut tx = platform::fenced_transaction(client, &first, cfg)?;
        tx.execute("UPDATE budget SET prepaid_micro_usd=1", &[])?;
    }
    client.execute(
        "UPDATE platform_maintenance_lease SET lease_until=now()-interval '1 second'",
        &[],
    )?;
    let next =
        platform::claim_maintenance(&mut other, Uuid::new_v4(), cfg)?.context("takeover lease")?;
    require(
        next.generation > first.generation,
        "takeover did not advance generation",
    )?;
    require(
        platform::fenced_transaction(client, &first, cfg).is_err(),
        "expired owner could mutate",
    )?;
    require(
        client
            .query_one("SELECT prepaid_micro_usd FROM budget", &[])?
            .get::<_, i64>(0)
            == seed().prepaid_micro_usd,
        "interrupted owner committed partial work",
    )?;
    platform::run_claimed(&mut other, &next, cfg, true)?;
    platform::release_maintenance(&mut other, &next, cfg)?;
    println!(
        "PASS: maintenance ownership excludes competitors, rolls back interrupted work and fences stale owners"
    );
    Ok(())
}

fn abandoned_transaction(client: &mut Client, config: &postgres::Config) -> Result<()> {
    let cfg = platform::MaintenanceConfig {
        lease_duration: Duration::from_secs(1),
        pass_budget: Duration::from_millis(400),
        statement_timeout: Duration::from_millis(200),
        lock_timeout: Duration::from_millis(50),
        ..platform::MaintenanceConfig::default()
    };
    let mut vanished = config.connect(NoTls)?;
    let lease = platform::claim_maintenance(&mut vanished, Uuid::new_v4(), &cfg)?
        .context("abandoned lease")?;
    let run_id = Uuid::new_v4().to_string();
    vanished.execute(
        "INSERT INTO platform_maintenance_runs(run_id,owner,generation,task,status)
        VALUES ($1,$2,$3,'retention','running')",
        &[&run_id, &lease.owner.to_string(), &lease.generation],
    )?;
    let mut transaction = platform::fenced_transaction(&mut vanished, &lease, &cfg)?;
    transaction.execute("UPDATE budget SET prepaid_micro_usd=1", &[])?;
    // Simulate an owner that stopped progressing while retaining an open socket.
    // PostgreSQL, rather than a cooperative client rollback, must release its lock.
    std::thread::sleep(Duration::from_millis(1300));
    let successor =
        platform::claim_maintenance(client, Uuid::new_v4(), &cfg)?.context("timed takeover")?;
    require(
        transaction.commit().is_err(),
        "expired idle transaction committed after takeover",
    )?;
    require(
        client
            .query_one("SELECT prepaid_micro_usd FROM budget", &[])?
            .get::<_, i64>(0)
            == seed().prepaid_micro_usd,
        "abandoned owner's mutation survived server-side rollback",
    )?;
    require(
        client
            .query_one(
                "SELECT status FROM platform_maintenance_runs WHERE run_id=$1",
                &[&run_id],
            )?
            .get::<_, String>(0)
            == "interrupted",
        "takeover did not record interrupted maintenance",
    )?;
    require(
        platform::fenced_transaction(client, &lease, &cfg).is_err(),
        "old owner remained authorized after takeover",
    )?;
    platform::release_maintenance(client, &successor, &cfg)?;
    println!(
        "PASS: server-side idle timeout releases an abandoned transaction and lease expiry permits fenced takeover"
    );
    Ok(())
}

fn maintenance_lock_failure(
    client: &mut Client,
    config: &postgres::Config,
    cfg: &platform::MaintenanceConfig,
) -> Result<()> {
    let mut holder = config.connect(NoTls)?;
    let mut locked = holder.transaction()?;
    locked.batch_execute("LOCK TABLE query_history IN ACCESS EXCLUSIVE MODE")?;
    let report = platform::maintenance_pass(client, Uuid::new_v4(), cfg, true)?;
    require(
        report
            .tasks
            .iter()
            .any(|task| task.task == "retention" && task.status == "failed")
            && report
                .tasks
                .iter()
                .any(|task| task.task == "reconcile" && task.status == "succeeded"),
        "a locked retention table did not report failure independently of reconciliation",
    )?;
    locked.rollback()?;
    let report = platform::maintenance_pass(client, Uuid::new_v4(), cfg, true)?;
    require(
        report.tasks.iter().all(|task| task.status == "succeeded"),
        "maintenance did not recover after lock release",
    )?;
    println!(
        "PASS: metadata lock contention is bounded, recorded and retried without preventing independent tasks"
    );
    Ok(())
}

fn retention_checks(client: &mut Client, cfg: &platform::MaintenanceConfig) -> Result<()> {
    for i in 0..5 {
        let id = format!("expired-{i}");
        let admission = admit(client, &id, "retention-worker")?;
        finish(client, &admission, "retention-worker")?;
    }
    let _running = admit(client, "protected-running", "retention-worker")?;
    let _held = admit(client, "protected-held", "retention-worker")?;
    client.batch_execute("UPDATE query_history SET accepted_at=now()-interval '2 days'
        WHERE query_id LIKE 'expired-%' OR query_id LIKE 'protected-%';
        UPDATE query_history SET status='succeeded' WHERE query_id='protected-held';
        INSERT INTO query_history(query_id,session_id,sql,context,status,accepted_at,worker_id)
        VALUES ('protected-legacy','legacy','SELECT 1','{}','succeeded',now()-interval '2 days','legacy');")?;
    let events: i64 = client
        .query_one("SELECT COUNT(*) FROM usage_events", &[])?
        .get(0);
    platform::maintenance_pass(client, Uuid::new_v4(), cfg, true)?;
    require(
        client
            .query_one(
                "SELECT COUNT(*) FROM query_history WHERE query_id LIKE 'expired-%'",
                &[],
            )?
            .get::<_, i64>(0)
            == 3,
        "retention exceeded its row budget or failed to prune eligible history",
    )?;
    require(client.query_one("SELECT backlog_remaining FROM platform_maintenance_runs WHERE task='retention' ORDER BY started_at DESC LIMIT 1", &[])?.get::<_, bool>(0),
        "retention did not expose remaining backlog")?;
    for _ in 0..3 {
        platform::maintenance_pass(client, Uuid::new_v4(), cfg, true)?;
    }
    require(
        client
            .query_one(
                "SELECT COUNT(*) FROM query_history WHERE query_id LIKE 'expired-%'",
                &[],
            )?
            .get::<_, i64>(0)
            == 0,
        "bounded passes did not drain retention backlog",
    )?;
    require(
        client
            .query_one(
                "SELECT COUNT(*) FROM query_history WHERE query_id LIKE 'protected-%'",
                &[],
            )?
            .get::<_, i64>(0)
            == 3,
        "retention removed running, held or unmigrated evidence",
    )?;
    require(
        client
            .query_one(
                "SELECT COUNT(*) FROM requests WHERE request_id LIKE 'expired-%'",
                &[],
            )?
            .get::<_, i64>(0)
            == 5,
        "retention removed durable request identity",
    )?;
    require(
        client
            .query_one("SELECT COUNT(*) FROM usage_events", &[])?
            .get::<_, i64>(0)
            == events,
        "retention changed billing evidence",
    )?;
    println!(
        "PASS: bounded retention drains backlog while preserving running/recovery evidence, requests and usage"
    );
    Ok(())
}

fn maintenance_fairness(client: &mut Client, config: &postgres::Config) -> Result<()> {
    for i in 0..6 {
        let id = format!("fairness-expired-{i}");
        let admission = admit(client, &id, "fairness-worker")?;
        finish(client, &admission, "fairness-worker")?;
    }
    client.batch_execute(
        "UPDATE query_history SET accepted_at=now()-interval '2 days'
         WHERE query_id LIKE 'fairness-expired-%';
         UPDATE platform_maintenance_tasks SET next_due_at=now()-
            CASE task WHEN 'retention' THEN interval '2 hours' ELSE interval '1 hour' END;
         UPDATE budget SET spent_micro_usd=spent_micro_usd+19;",
    )?;
    let spent_before: i64 = client
        .query_one("SELECT spent_micro_usd FROM budget", &[])?
        .get(0);
    let bounded = platform::MaintenanceConfig {
        retention_days: Some(1),
        retention_batch_size: 1,
        retention_max_batches: 1,
        lease_duration: Duration::from_secs(5),
        pass_budget: Duration::from_millis(500),
        statement_timeout: Duration::from_millis(500),
        lock_timeout: Duration::from_millis(500),
        ..platform::MaintenanceConfig::default()
    };
    let mut holder = config.connect(NoTls)?;
    let mut locked = holder.transaction()?;
    locked.batch_execute("LOCK TABLE query_history IN ACCESS EXCLUSIVE MODE")?;
    // A real lock wait consumes the first pass's work budget. There is no sleep
    // or race against a scheduled unlock: PostgreSQL's timeout ends the wait.
    let report = platform::maintenance_pass(client, Uuid::new_v4(), &bounded, false)?;
    require(
        report
            .tasks
            .first()
            .is_some_and(|task| task.task == "retention" && task.status == "failed")
            && !report
                .tasks
                .iter()
                .any(|task| task.task == "reconcile" && task.status == "succeeded"),
        "lock-held retention did not leave reconciliation unfinished after exhausting its pass",
    )?;
    require(
        client
            .query_one("SELECT spent_micro_usd FROM budget", &[])?
            .get::<_, i64>(0)
            == spent_before,
        "reconciliation unexpectedly repaired the budget in the exhausted pass",
    )?;
    require(
        client
            .query_one(
                "SELECT next_due_at <= clock_timestamp() FROM platform_maintenance_tasks
                 WHERE task='reconcile'",
                &[],
            )?
            .get(0),
        "the exhausted pass postponed reconciliation instead of leaving it overdue",
    )?;
    locked.rollback()?;
    let normal = platform::MaintenanceConfig {
        retention_days: Some(1),
        retention_batch_size: 1,
        retention_max_batches: 1,
        ..platform::MaintenanceConfig::default()
    };
    for (attempt, force) in [false, false, true].into_iter().enumerate() {
        // Model the next tick without waiting a minute. Both tasks remain due,
        // but reconcile has been waiting longer. For a forced pass neither is
        // due yet: force must still preserve the same fairness ordering.
        let schedule = if attempt == 0 {
            "UPDATE platform_maintenance_tasks SET next_due_at=now()-interval '1 second'
             WHERE task='retention'"
        } else if force {
            "UPDATE platform_maintenance_tasks SET next_due_at=now()+
                CASE task WHEN 'reconcile' THEN interval '1 hour' ELSE interval '2 hours' END"
        } else {
            "UPDATE platform_maintenance_tasks SET next_due_at=now()-
                CASE task WHEN 'reconcile' THEN interval '1 hour' ELSE interval '1 second' END"
        };
        client.batch_execute(schedule)?;
        client.execute("UPDATE budget SET spent_micro_usd=spent_micro_usd+19", &[])?;
        let report = platform::maintenance_pass(client, Uuid::new_v4(), &normal, force)?;
        require(
            report
                .tasks
                .first()
                .is_some_and(|task| task.task == "reconcile" && task.status == "succeeded")
                && report.tasks.iter().any(|task| {
                    task.task == "retention"
                        && task.status == "succeeded"
                        && task.rows_pruned == 1
                        && task.backlog_remaining == Some(true)
                }),
            "a recurring retention backlog ran ahead of overdue reconciliation",
        )?;
        accounting(client)?;
    }
    client.batch_execute(
        "UPDATE platform_maintenance_tasks SET next_due_at=now()-interval '1 hour'",
    )?;
    let report = platform::maintenance_pass(client, Uuid::new_v4(), &normal, false)?;
    require(
        report
            .tasks
            .first()
            .is_some_and(|task| task.task == "retention")
            && report.tasks.iter().all(|task| task.status == "succeeded"),
        "equal maintenance due times lost their stable retention-first tie break",
    )?;
    accounting(client)?;
    println!(
        "PASS: overdue reconciliation precedes recurring retention backlog after an exhausted pass, including forced runs and stable ties"
    );
    Ok(())
}

fn concurrent_accounting(
    client: &mut Client,
    config: &postgres::Config,
    cfg: &platform::MaintenanceConfig,
) -> Result<()> {
    // Separate real PostgreSQL connections race admission, settlement, own-worker
    // crash recovery and the budget-locked aggregate overwrite.
    std::thread::scope(|scope| -> Result<()> {
        let mut handles = Vec::new();
        for worker in 0..3 {
            let config = config.clone();
            handles.push(scope.spawn(move || -> Result<()> {
                let mut client = config.connect(NoTls)?;
                let worker = format!("concurrent-{worker}");
                for i in 0..12 {
                    let id = format!("{worker}-{i}");
                    let admission = admit(&mut client, &id, &worker)?;
                    if i % 3 == 0 {
                        platform::worker_startup(&mut client, &worker, &rate())?;
                    } else {
                        finish(&mut client, &admission, &worker)?;
                    }
                }
                Ok(())
            }));
        }
        for _ in 0..12 {
            platform::maintenance_pass(client, Uuid::new_v4(), cfg, true)?;
        }
        for handle in handles {
            handle
                .join()
                .map_err(|_| anyhow::anyhow!("accounting fixture thread panicked"))??;
        }
        Ok(())
    })?;
    accounting(client)?;
    require(
        client
            .query_one(
                "SELECT COUNT(*) FROM usage_events WHERE worker_id LIKE 'concurrent-%'",
                &[],
            )?
            .get::<_, i64>(0)
            == 36,
        "concurrent maintenance duplicated or lost a usage event",
    )?;
    println!(
        "PASS: concurrent admissions, settlements and recovery preserve exact reconciled budget accumulators"
    );
    Ok(())
}

fn startup_samples(client: &mut Client) -> Result<Vec<f64>> {
    let mut values = Vec::new();
    for _ in 0..9 {
        let begin = Instant::now();
        platform::worker_startup(client, "worker-with-no-orphans", &rate())?;
        values.push(begin.elapsed().as_secs_f64() * 1000.0);
    }
    Ok(values)
}

fn startup_scaling(client: &mut Client, config: &postgres::Config) -> Result<()> {
    let small = startup_samples(client)?;
    client.batch_execute("INSERT INTO query_history(query_id,session_id,sql,context,status,worker_id,elapsed_ms)
        SELECT 'volume-'||i,'fixture','SELECT 1','{}','succeeded','volume-worker',1 FROM generate_series(1,50000) i;
        INSERT INTO usage_events(event_id,execution_id,meter_name,quantity,unit,resource_units,amount_micro_usd,
            rate_card_version,worker_id,context,source,started_at,ended_at)
        SELECT 'volume-'||i,'volume-'||i,'compute',1,'millisecond',1,0,'fixture','volume-worker','{}','worker',now(),now()
        FROM generate_series(1,50000) i;
        ANALYZE query_history; ANALYZE usage_events;")?;
    // An O(ledger) reconciliation would wait on this budget row; local startup
    // with no abandoned holds must finish without ever needing that lock.
    let mut other = config.connect(NoTls)?;
    let mut locked = other.transaction()?;
    locked.execute(
        "SELECT 1 FROM budget WHERE budget_id='local' FOR UPDATE",
        &[],
    )?;
    let large = startup_samples(client)?;
    platform::migrate(client, &seed())?;
    locked.rollback()?;
    let evidence = json!({"phase":"platform schema validation and own-worker recovery",
        "large_fixture_added_history_rows":50000,"large_fixture_added_usage_events":50000,
        "small_ms":small,"large_ms":large,
        "large_budget_row_locked":true,"duckdb_attach_included":false});
    std::fs::write(
        "/artifacts/platform-startup-volume.json",
        serde_json::to_vec_pretty(&evidence)?,
    )?;
    println!(
        "PASS: metadata startup and repeat migrations avoid global budget locking at +50,000 history and ledger rows: {evidence}"
    );
    Ok(())
}
