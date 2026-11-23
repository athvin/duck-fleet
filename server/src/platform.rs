//! Deployment-owned platform schema and bounded metadata maintenance.
//!
//! Workers only validate this schema and recover their own exclusive identity.
//! Maintenance ownership is fenced by a PostgreSQL lease generation; every work
//! transaction locks and rechecks that lease before touching diagnostic/accounting
//! state. No lease or age heuristic establishes that a worker has died.
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use postgres::{Client, GenericClient, NoTls, Transaction};
use serde::Serialize;
use uuid::Uuid;

use crate::{credentials, metering, pricing::RateCard, requests};

pub(crate) const SCHEMA_VERSION: i32 = 7;
const MIGRATION_LOCK: i64 = 4_283_121;
const POLL: Duration = Duration::from_millis(250);
const MAINTENANCE_TICK: Duration = Duration::from_secs(60);
const RETRY_SECONDS: i64 = 60;

pub(crate) struct Seed {
    pub prepaid_micro_usd: i64,
    pub max_job_micro_usd: i64,
}
impl Seed {
    pub fn from_env() -> Result<Self> {
        Ok(Self {
            prepaid_micro_usd: positive_env("DUCKFLEET_PREPAID_MICRO_USD", 10_000_000)?,
            max_job_micro_usd: positive_env("DUCKFLEET_MAX_JOB_MICRO_USD", 1_000_000)?,
        })
    }
}

fn positive_env(name: &str, default: i64) -> Result<i64> {
    let value = nonnegative_env(name, default)?;
    anyhow::ensure!(value > 0, "{name} must be a positive whole number");
    Ok(value)
}

fn nonnegative_env(name: &str, default: i64) -> Result<i64> {
    match std::env::var(name) {
        Ok(value) => value
            .trim()
            .parse::<i64>()
            .ok()
            .filter(|value| *value >= 0)
            .with_context(|| format!("{name} must be a nonnegative whole number")),
        Err(std::env::VarError::NotPresent) => Ok(default),
        Err(_) => anyhow::bail!("{name} must contain valid text"),
    }
}

fn seconds_env(name: &str, default: i64, maximum: i64) -> Result<Duration> {
    let seconds = nonnegative_env(name, default)?;
    anyhow::ensure!(
        (1..=maximum).contains(&seconds),
        "{name} must be between 1 and {maximum}"
    );
    Ok(Duration::from_secs(seconds as u64))
}

pub(crate) fn schema_wait() -> Result<Duration> {
    seconds_env("DUCKFLEET_PLATFORM_WAIT_SECONDS", 120, 600)
}

fn millis(duration: Duration) -> Result<i64> {
    let milliseconds = i64::try_from(duration.as_millis())?;
    anyhow::ensure!(
        milliseconds > 0,
        "database timeout must be at least one millisecond"
    );
    Ok(milliseconds)
}

fn timeouts(transaction: &mut Transaction<'_>, statement: Duration, lock: Duration) -> Result<()> {
    let statement = millis(statement)?.to_string();
    let lock = millis(lock)?.to_string();
    transaction.query_one(
        "SELECT set_config('statement_timeout',$1,true),
            set_config('lock_timeout',$2,true),
            set_config('idle_in_transaction_session_timeout',$1,true)",
        &[&statement, &lock],
    )?;
    Ok(())
}

fn connection_config(dsn: &str) -> Result<postgres::Config> {
    let mut config: postgres::Config = dsn
        .parse()
        .map_err(|_| anyhow::anyhow!("invalid platform connection settings"))?;
    config.tcp_user_timeout(Duration::from_secs(3));
    config.keepalives(true);
    config.keepalives_idle(Duration::from_secs(3));
    config.keepalives_interval(Duration::from_secs(1));
    config.keepalives_retries(2);
    Ok(config)
}

fn connect_config(
    config: &postgres::Config,
    connect_timeout: Duration,
    statement_timeout: Option<Duration>,
) -> Result<Client> {
    let mut config = config.clone();
    config.connect_timeout(connect_timeout);
    let mut client = config
        .connect(NoTls)
        .context("connecting to platform PostgreSQL")?;
    // Dedicated migration/maintenance clients receive session limits. The client
    // returned to a worker keeps its original DSN policy: schema checks below
    // use only transaction-local limits, never a new foreground query timeout.
    if let Some(statement_timeout) = statement_timeout {
        let timeout = millis(statement_timeout)?.to_string();
        client.query_one(
            "SELECT set_config('statement_timeout',$1,false),
            set_config('idle_in_transaction_session_timeout',$1,false)",
            &[&timeout],
        )?;
    }
    Ok(client)
}

fn connect(dsn: &str, connect_timeout: Duration, statement_timeout: Duration) -> Result<Client> {
    connect_config(
        &connection_config(dsn)?,
        connect_timeout,
        Some(statement_timeout),
    )
}

fn fatal_startup(error: &anyhow::Error) -> bool {
    error
        .downcast_ref::<postgres::Error>()
        .and_then(postgres::Error::code)
        .is_some_and(|code| {
            code.code().starts_with("28") || matches!(code.code(), "3D000" | "42501")
        })
}

fn startup_error(error: &anyhow::Error) -> anyhow::Error {
    anyhow::anyhow!(
        "platform startup refused: {}",
        crate::secrets::Redactor::from_env().scrub(&format!("{error:#}"))
    )
}

fn version(client: &mut impl GenericClient) -> Result<Option<i32>> {
    let exists: bool = client
        .query_one(
            "SELECT to_regclass('platform_schema_migrations') IS NOT NULL",
            &[],
        )?
        .get(0);
    if !exists {
        return Ok(None);
    }
    Ok(client
        .query_one("SELECT MAX(version) FROM platform_schema_migrations", &[])?
        .get(0))
}

fn supported(version: i32) -> Result<()> {
    anyhow::ensure!(
        version == SCHEMA_VERSION,
        "unsupported platform schema version {version}; this binary requires {SCHEMA_VERSION}; run the matching migration image before starting compute"
    );
    Ok(())
}

/// Indexed version reads only: no schema creation, advisory lock, or aggregation.
pub(crate) fn check_schema(client: &mut Client) -> Result<()> {
    let mut transaction = client.transaction()?;
    timeouts(
        &mut transaction,
        Duration::from_secs(3),
        Duration::from_secs(3),
    )?;
    let version = version(&mut transaction)?.context(
        "platform schema is not initialized; run DUCKFLEET_ROLE=migrate before starting compute",
    )?;
    transaction.commit()?;
    supported(version)
}

/// Called on a blocking thread, returning the connection for worker-local recovery.
/// Missing schema/backend availability waits only within the caller's budget;
/// an incompatible committed version fails immediately rather than looking ready.
pub(crate) fn wait_schema(dsn: &str, wait: Duration) -> Result<Client> {
    // Parse once, before the retry loop: malformed configuration and known fatal
    // authentication/database/permission failures cannot heal by waiting.
    let config = connection_config(dsn)?;
    let started = Instant::now();
    while let Some(remaining) = wait.checked_sub(started.elapsed()) {
        if remaining < Duration::from_millis(1) {
            break;
        }
        let timeout = remaining.min(Duration::from_secs(3));
        let attempt = (|| -> Result<(Client, Option<i32>)> {
            let mut client = connect_config(&config, timeout, None)?;
            let mut transaction = client.transaction()?;
            timeouts(&mut transaction, timeout, timeout)?;
            let current = version(&mut transaction)?;
            transaction.commit()?;
            Ok((client, current))
        })();
        match attempt {
            Ok((client, Some(current))) if current == SCHEMA_VERSION => return Ok(client),
            Ok((_, Some(1..=6))) => {}
            Ok((_, Some(current))) => {
                supported(current)?;
            }
            Err(error) if fatal_startup(&error) => return Err(startup_error(&error)),
            Ok((_, None)) | Err(_) => {}
        }
        std::thread::sleep(POLL.min(wait.saturating_sub(started.elapsed())));
    }
    anyhow::bail!(
        "platform schema was unavailable within {}s; check PostgreSQL and run DUCKFLEET_ROLE=migrate",
        wait.as_secs()
    );
}

#[derive(Debug, Serialize)]
pub(crate) struct MigrationReport {
    pub version: i32,
    pub applied: bool,
    pub elapsed_ms: u64,
}

const HISTORY_SCHEMA: &str = "CREATE TABLE IF NOT EXISTS query_history (
    query_id TEXT PRIMARY KEY,session_id TEXT NOT NULL,sql TEXT,
    context TEXT NOT NULL,status TEXT NOT NULL,
    accepted_at TIMESTAMPTZ NOT NULL DEFAULT now(),finished_at TIMESTAMPTZ,
    elapsed_ms BIGINT,result_rows BIGINT,error TEXT);
    ALTER TABLE query_history ADD COLUMN IF NOT EXISTS worker_id TEXT NOT NULL DEFAULT 'legacy';
    ALTER TABLE query_history ADD COLUMN IF NOT EXISTS placement TEXT NOT NULL DEFAULT '{}';
    ALTER TABLE query_history ADD COLUMN IF NOT EXISTS history_id BIGSERIAL;
    ALTER TABLE query_history ADD COLUMN IF NOT EXISTS profile_status TEXT NOT NULL DEFAULT 'not_recorded';
    ALTER TABLE query_history ADD COLUMN IF NOT EXISTS sql_status TEXT NOT NULL DEFAULT 'available';
    UPDATE query_history SET sql_status='not_recorded' WHERE sql IS NULL AND sql_status='available';
    ALTER TABLE query_history ADD COLUMN IF NOT EXISTS profile TEXT;
    ALTER TABLE query_history ADD COLUMN IF NOT EXISTS metrics TEXT NOT NULL DEFAULT '{}';
    ALTER TABLE query_history ADD COLUMN IF NOT EXISTS failure_kind TEXT;
    ALTER TABLE query_history ADD COLUMN IF NOT EXISTS retry_safe BOOLEAN;
    ALTER TABLE query_history ADD COLUMN IF NOT EXISTS principal TEXT NOT NULL DEFAULT 'direct';
    CREATE UNIQUE INDEX IF NOT EXISTS query_history_order ON query_history(history_id DESC);
    CREATE INDEX IF NOT EXISTS query_history_failure ON query_history(failure_kind,history_id DESC);
    CREATE INDEX IF NOT EXISTS query_history_model ON query_history((context::jsonb #>> '{dbt,node_id}'),history_id DESC);
    CREATE INDEX IF NOT EXISTS query_history_invocation ON query_history((context::jsonb #>> '{dbt,invocation_id}'),history_id DESC);
    CREATE INDEX IF NOT EXISTS query_history_model_run ON query_history((context::jsonb ->> 'model_run_id'),history_id DESC);
    CREATE INDEX IF NOT EXISTS query_history_running ON query_history(worker_id) WHERE status='running';
    CREATE INDEX IF NOT EXISTS query_history_expired ON query_history(accepted_at,query_id) WHERE status <> 'running';";

const MAINTENANCE_SCHEMA: &str = "CREATE TABLE platform_maintenance_lease (
    name TEXT PRIMARY KEY,owner TEXT,generation BIGINT NOT NULL DEFAULT 0,lease_until TIMESTAMPTZ);
    INSERT INTO platform_maintenance_lease(name) VALUES ('metadata');
    CREATE TABLE platform_maintenance_tasks (
        task TEXT PRIMARY KEY,next_due_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        last_success_at TIMESTAMPTZ,last_failure_at TIMESTAMPTZ,last_error TEXT);
    INSERT INTO platform_maintenance_tasks(task) VALUES ('retention'),('reconcile');
    CREATE TABLE platform_maintenance_runs (
        run_id TEXT PRIMARY KEY,owner TEXT NOT NULL,generation BIGINT NOT NULL,task TEXT NOT NULL,
        started_at TIMESTAMPTZ NOT NULL DEFAULT now(),finished_at TIMESTAMPTZ,
        status TEXT NOT NULL CHECK(status IN ('running','succeeded','failed','interrupted')),
        rows_pruned BIGINT NOT NULL DEFAULT 0,backlog_remaining BOOLEAN,
        oldest_eligible_at TIMESTAMPTZ,error TEXT);
    CREATE INDEX platform_maintenance_runs_recent ON platform_maintenance_runs(started_at DESC);
    CREATE INDEX platform_maintenance_runs_active ON platform_maintenance_runs(generation) WHERE status='running';";

fn elapsed_ms(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX)
}

/// Baseline adoption supports existing unversioned installations and fresh ones.
/// Repeats skip all baseline DDL, request backfill, and lifetime reconciliation.
/// The caller bounds migration statements; deployment uses a separate, longer
/// bound than foreground work because a one-time backfill can be substantial.
pub(crate) fn migrate(client: &mut Client, seed: &Seed) -> Result<MigrationReport> {
    migrate_with_roles(client, seed, None)
}
fn migrate_with_roles(
    client: &mut Client,
    seed: &Seed,
    roles: Option<&crate::db_roles::RoleNames>,
) -> Result<MigrationReport> {
    let started = Instant::now();
    anyhow::ensure!(
        seed.prepaid_micro_usd > 0 && seed.max_job_micro_usd > 0,
        "budget seeds must be positive"
    );
    if let Some(current) = version(client)? {
        if current == SCHEMA_VERSION {
            return Ok(MigrationReport {
                version: current,
                applied: false,
                elapsed_ms: elapsed_ms(started),
            });
        }
        anyhow::ensure!(
            matches!(current, 1..=6),
            "unsupported migration source version {current}"
        );
    }
    let mut transaction = client.transaction()?;
    transaction.batch_execute("SET LOCAL lock_timeout='3s'")?;
    // Retain the preceding worker binary's lock during the supported quiesced
    // upgrade. This lock serializes migrations, never routine worker startup.
    transaction.query_one("SELECT pg_advisory_xact_lock($1)", &[&MIGRATION_LOCK])?;
    let current = version(&mut transaction)?;
    if current == Some(SCHEMA_VERSION) {
        transaction.commit()?;
        return Ok(MigrationReport {
            version: SCHEMA_VERSION,
            applied: false,
            elapsed_ms: elapsed_ms(started),
        });
    }
    anyhow::ensure!(
        current.is_none() || matches!(current, Some(1..=6)),
        "unsupported migration source version"
    );
    if current.is_none() {
        transaction.batch_execute(HISTORY_SCHEMA)?;
        credentials::migrate(&mut transaction)?;
        metering::migrate(
            &mut transaction,
            seed.prepaid_micro_usd,
            seed.max_job_micro_usd,
        )?;
        requests::migrate(&mut transaction)?;
        // Older workers could emit synthetic retention audit history after #99's
        // backfill marker committed. Adopt those remaining identities once here,
        // preserving known owners/outcomes and never inventing old fingerprints.
        requests::backfill_missing_history(&mut transaction)?;
        // Adoption repairs old accumulator drift once. No worker identities are
        // interrupted here: schema ownership is not evidence of worker death.
        metering::reconcile(&mut transaction)?;
        transaction.batch_execute(MAINTENANCE_SCHEMA)?;
        transaction.batch_execute(
            "CREATE TABLE IF NOT EXISTS platform_schema_migrations (
        version INTEGER PRIMARY KEY CHECK(version > 0),name TEXT NOT NULL,
        applied_at TIMESTAMPTZ NOT NULL DEFAULT now())",
        )?;
        transaction.execute(
            "INSERT INTO platform_schema_migrations(version,name) VALUES (1,'platform-baseline')",
            &[],
        )?;
    }
    if current.is_none() || current == Some(1) {
        crate::queue::migrate(&mut transaction)?;
        transaction.execute(
            "INSERT INTO platform_schema_migrations(version,name) VALUES (2,'durable-query-queue')",
            &[],
        )?;
    }
    if current.is_none() || current.is_some_and(|v| v < 3) {
        crate::authz::migrate(&mut transaction, &crate::authz::Seed::from_env()?)?;
        crate::audit::migrate(&mut transaction)?;
        crate::metadata::migrate(&mut transaction)?;
        transaction.execute(
        "INSERT INTO platform_schema_migrations(version,name) VALUES(3,'platform-authorization')",
        &[],
    )?;
    }
    if current.is_none() || current.is_some_and(|v| v < 4) {
        crate::metadata::migrate_system(&mut transaction)?;
        transaction.execute("INSERT INTO platform_schema_migrations(version,name) VALUES(4,'typed-system-metadata')",&[])?;
    }
    if current.is_none() || current.is_some_and(|v| v < 5) {
        crate::jobs::migrate(&mut transaction)?;
        transaction.execute(
            "INSERT INTO platform_schema_migrations(version,name) VALUES(5,'durable-jobs')",
            &[],
        )?;
    }
    if current.is_none() || current.is_some_and(|v| v < 6) {
        crate::compute_lifecycle::migrate(&mut transaction)?;
        transaction.execute(
            "INSERT INTO platform_schema_migrations(version,name) VALUES(6,'compute-lifecycle')",
            &[],
        )?;
    }
    if current.is_none() || current.is_some_and(|v| v < 7) {
        transaction.batch_execute("ALTER TABLE query_history ADD COLUMN IF NOT EXISTS sql_status TEXT NOT NULL DEFAULT 'available';
            ALTER TABLE query_history ALTER COLUMN sql DROP NOT NULL;
            UPDATE query_history SET sql_status='not_recorded' WHERE sql IS NULL AND sql_status='available';")?;
        transaction.execute(
            "INSERT INTO platform_schema_migrations(version,name) VALUES(7,'artifact-retention')",
            &[],
        )?;
    }
    if let Some(roles) = roles {
        crate::db_roles::grant_runtime(&mut transaction, roles)?;
    }
    transaction.commit()?;
    Ok(MigrationReport {
        version: SCHEMA_VERSION,
        applied: true,
        elapsed_ms: elapsed_ms(started),
    })
}

#[derive(Debug, Serialize)]
pub(crate) struct WorkerStartup {
    pub recovered: u64,
    pub elapsed_ms: u64,
}

/// Only the exclusively owned stable worker identity is recovered. A worker with
/// no held reservations never locks the shared budget or scans lifetime history.
/// The caller must already hold its lifetime worker-ownership guard; this helper
/// does not establish process fencing or infer abandonment from elapsed time.
pub(crate) fn worker_startup(
    client: &mut Client,
    worker_id: &str,
    rate: &RateCard,
) -> Result<WorkerStartup> {
    let started = Instant::now();
    check_schema(client)?;
    crate::compute_lifecycle::finish_worker(client, worker_id)
        .map_err(|error| anyhow::anyhow!(error.1))?;
    let queued_recovered = crate::queue::recover_worker(
        client,
        &crate::queue::WorkerEpoch {
            worker_id: worker_id.to_owned(),
            boot_id: Uuid::nil(),
        },
        rate,
    )?;

    let mut transaction = client.transaction()?;
    timeouts(
        &mut transaction,
        Duration::from_secs(3),
        Duration::from_secs(3),
    )?;
    metering::interrupt_abandoned_history(&mut transaction, worker_id)?;
    let recovered = queued_recovered + metering::recover(&mut transaction, worker_id, rate)?;
    transaction.commit()?;
    let report = WorkerStartup {
        recovered,
        elapsed_ms: elapsed_ms(started),
    };
    tracing::info!(
        worker_id,
        recovered,
        elapsed_ms = report.elapsed_ms,
        "worker platform recovery complete"
    );
    Ok(report)
}

#[derive(Clone, Debug)]
pub(crate) struct MaintenanceConfig {
    pub retention_days: Option<i32>,
    pub sql_retention_days: Option<i32>,
    pub profile_retention_days: Option<i32>,
    pub retention_batch_size: i64,
    pub retention_max_batches: u32,
    pub retention_interval: Duration,
    pub reconcile_interval: Duration,
    pub lease_duration: Duration,
    pub pass_budget: Duration,
    pub statement_timeout: Duration,
    pub lock_timeout: Duration,
}
impl Default for MaintenanceConfig {
    fn default() -> Self {
        Self {
            retention_days: None,
            sql_retention_days: None,
            profile_retention_days: None,
            retention_batch_size: 5000,
            retention_max_batches: 2,
            retention_interval: Duration::from_secs(3600),
            reconcile_interval: Duration::from_secs(21600),
            lease_duration: Duration::from_secs(30),
            pass_budget: Duration::from_secs(10),
            statement_timeout: Duration::from_secs(3),
            lock_timeout: Duration::from_millis(250),
        }
    }
}
impl MaintenanceConfig {
    pub fn from_env() -> Result<Self> {
        let mut config = Self::default();
        let days = nonnegative_env("DUCKFLEET_HISTORY_RETENTION_DAYS", 0)?;
        anyhow::ensure!(
            days <= 3650,
            "DUCKFLEET_HISTORY_RETENTION_DAYS must be from 0 (keep forever) to 3650"
        );
        config.retention_days = (days > 0).then_some(days as i32);
        let sql_days = nonnegative_env("DUCKFLEET_SQL_RETENTION_DAYS", 0)?;
        let profile_days = nonnegative_env("DUCKFLEET_PROFILE_RETENTION_DAYS", 0)?;
        anyhow::ensure!(
            sql_days <= 3650 && profile_days <= 3650,
            "SQL and profile retention must be from 0 (keep forever) to 3650 days"
        );
        config.sql_retention_days = (sql_days > 0).then_some(sql_days as i32);
        config.profile_retention_days = (profile_days > 0).then_some(profile_days as i32);
        // Deliberate one-shot repair can receive a larger bound, without making
        // the recurring foreground default an unbounded lifetime scan.
        config.statement_timeout = seconds_env("DUCKFLEET_MAINTENANCE_STATEMENT_SECONDS", 3, 600)?;
        config.pass_budget = seconds_env("DUCKFLEET_MAINTENANCE_PASS_SECONDS", 10, 1800)?;
        config.lease_duration = seconds_env("DUCKFLEET_MAINTENANCE_LEASE_SECONDS", 30, 3600)?;
        config.validate()?;
        Ok(config)
    }

    fn validate(&self) -> Result<()> {
        anyhow::ensure!(
            self.retention_days
                .is_none_or(|days| (1..=3650).contains(&days))
                && self
                    .sql_retention_days
                    .is_none_or(|days| (1..=3650).contains(&days))
                && self
                    .profile_retention_days
                    .is_none_or(|days| (1..=3650).contains(&days)),
            "summary, SQL and profile retention days must be 1..3650 or absent"
        );
        anyhow::ensure!(
            (1..=6000).contains(&self.retention_batch_size)
                && (1..=100).contains(&self.retention_max_batches),
            "retention batches must contain 1..5000 rows with 1..100 batches"
        );
        for timeout in [
            self.retention_interval,
            self.reconcile_interval,
            self.lease_duration,
            self.pass_budget,
            self.statement_timeout,
            self.lock_timeout,
        ] {
            millis(timeout)?;
        }
        anyhow::ensure!(
            self.lease_duration > self.pass_budget
                && self.statement_timeout <= self.pass_budget
                && self.lock_timeout <= self.statement_timeout,
            "maintenance requires lease > pass budget >= statement timeout >= lock timeout"
        );
        Ok(())
    }

    fn interval(&self, task: &str) -> Duration {
        if task == "artifacts" {
            Duration::from_secs(60)
        } else if task == "retention" {
            self.retention_interval
        } else {
            self.reconcile_interval
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct Lease {
    pub owner: Uuid,
    pub generation: i64,
}

/// Claiming increments the fencing generation atomically. Database time decides
/// expiry; caller clocks never grant authority. Unfinished prior runs are reported
/// as interrupted once a new generation proves their owner lost its lease.
pub(crate) fn claim_maintenance(
    client: &mut Client,
    owner: Uuid,
    config: &MaintenanceConfig,
) -> Result<Option<Lease>> {
    config.validate()?;
    let mut transaction = client.transaction()?;
    timeouts(
        &mut transaction,
        config.statement_timeout,
        config.lock_timeout,
    )?;
    let row = transaction.query_opt(
        "UPDATE platform_maintenance_lease SET owner=$1,generation=generation+1,
            lease_until=clock_timestamp()+($2::BIGINT * interval '1 millisecond')
         WHERE name='metadata' AND (lease_until IS NULL OR lease_until <= clock_timestamp())
         RETURNING generation",
        &[&owner.to_string(), &millis(config.lease_duration)?],
    )?;
    let Some(row) = row else { return Ok(None) };
    let generation: i64 = row.get(0);
    let interrupted = transaction.query(
        "UPDATE platform_maintenance_runs SET status='interrupted',finished_at=now(),
            error='maintenance owner lease expired before completion'
         WHERE status='running' AND generation < $1 RETURNING task",
        &[&generation],
    )?;
    for row in interrupted {
        let task: String = row.get(0);
        transaction.execute(
            "UPDATE platform_maintenance_tasks SET last_failure_at=now(),
            last_error='maintenance owner lease expired before completion',
            next_due_at=now()+($2::BIGINT * interval '1 second') WHERE task=$1",
            &[&task, &RETRY_SECONDS],
        )?;
    }
    transaction.commit()?;
    Ok(Some(Lease { owner, generation }))
}

/// Locks the lease before validating it, then validates using database wall time
/// after any lock wait. A stale owner cannot resume work after expiry/takeover.
/// The transaction's server-side idle/statement bounds also cap a lost owner's
/// ability to hold this row past its lease deadline.
pub(crate) fn fenced_transaction<'a>(
    client: &'a mut Client,
    lease: &Lease,
    config: &MaintenanceConfig,
) -> Result<Transaction<'a>> {
    let mut transaction = client.transaction()?;
    timeouts(
        &mut transaction,
        config.statement_timeout,
        config.lock_timeout,
    )?;
    transaction.query_one(
        "SELECT name FROM platform_maintenance_lease WHERE name='metadata' FOR UPDATE",
        &[],
    )?;
    let valid: bool = transaction
        .query_one(
            "SELECT owner=$1 AND generation=$2 AND lease_until > clock_timestamp()
         FROM platform_maintenance_lease WHERE name='metadata'",
            &[&lease.owner.to_string(), &lease.generation],
        )?
        .get::<_, Option<bool>>(0)
        .unwrap_or(false);
    anyhow::ensure!(valid, "maintenance lease expired or was replaced");
    Ok(transaction)
}

pub(crate) fn release_maintenance(
    client: &mut Client,
    lease: &Lease,
    config: &MaintenanceConfig,
) -> Result<()> {
    let mut transaction = client.transaction()?;
    timeouts(
        &mut transaction,
        config.statement_timeout,
        config.lock_timeout,
    )?;
    // Never release a replacement owner's lease, even if this process resumes
    // long after its own pass budget or after a connection interruption.
    transaction.execute(
        "UPDATE platform_maintenance_lease SET owner=NULL,lease_until=NULL
        WHERE name='metadata' AND owner=$1 AND generation=$2",
        &[&lease.owner.to_string(), &lease.generation],
    )?;
    transaction.commit()?;
    Ok(())
}

#[derive(Debug, Serialize)]
pub(crate) struct TaskReport {
    pub task: String,
    pub status: String,
    pub rows_pruned: u64,
    pub backlog_remaining: Option<bool>,
    pub error: Option<String>,
}
#[derive(Debug, Serialize)]
pub(crate) struct PassReport {
    pub acquired: bool,
    pub generation: Option<i64>,
    pub tasks: Vec<TaskReport>,
}

fn safe_error(error: &anyhow::Error) -> String {
    // Error fields never store a DSN, SQL text, bound parameters, or PostgreSQL's
    // potentially sensitive message/detail. Logs and records share this wording.
    match error
        .downcast_ref::<postgres::Error>()
        .and_then(postgres::Error::code)
    {
        Some(code) => format!(
            "platform maintenance database failure (SQLSTATE {})",
            code.code()
        ),
        None => "platform maintenance connection or ownership failure".into(),
    }
}

const ELIGIBLE: &str =
    "h.accepted_at < now()-($1::INT * interval '1 day') AND h.status <> 'running'
    AND EXISTS(SELECT 1 FROM requests r WHERE r.request_id=h.query_id AND r.state='terminal')
    AND NOT EXISTS(SELECT 1 FROM reservations s WHERE s.execution_id=h.query_id AND s.state='held')
    AND NOT EXISTS(SELECT 1 FROM query_permits p WHERE p.request_id=h.query_id)";

fn retention(
    client: &mut Client,
    lease: &Lease,
    config: &MaintenanceConfig,
    run_id: &str,
    started: Instant,
) -> Result<()> {
    if config.retention_days.is_none()
        && config.sql_retention_days.is_none()
        && config.profile_retention_days.is_none()
    {
        return Ok(());
    }
    for _ in 0..config.retention_max_batches {
        let Some(remaining) = config.pass_budget.checked_sub(started.elapsed()) else {
            break;
        };
        if remaining < Duration::from_millis(1) {
            break;
        }
        let mut bounded = config.clone();
        bounded.statement_timeout = config.statement_timeout.min(remaining);
        bounded.lock_timeout = config.lock_timeout.min(bounded.statement_timeout);
        let mut transaction = fenced_transaction(client, lease, &bounded)?;
        let sql_removed = match config.sql_retention_days {
            Some(days) => transaction.execute(
                &format!(
                    "UPDATE query_history SET sql=NULL,sql_status='expired' WHERE query_id IN (
                    SELECT h.query_id FROM query_history h WHERE {ELIGIBLE} AND h.sql IS NOT NULL
                    ORDER BY h.accepted_at,h.query_id LIMIT $2 FOR UPDATE OF h SKIP LOCKED)"
                ),
                &[&days, &config.retention_batch_size],
            )?,
            None => 0,
        };
        let profile_removed = match config.profile_retention_days {
            Some(days) => transaction.execute(
                &format!("UPDATE query_history SET profile=NULL,profile_status='expired' WHERE query_id IN (
                    SELECT h.query_id FROM query_history h WHERE {ELIGIBLE} AND h.profile IS NOT NULL
                    ORDER BY h.accepted_at,h.query_id LIMIT $2 FOR UPDATE OF h SKIP LOCKED)"),
                &[&days, &config.retention_batch_size],
            )?,
            None => 0,
        };
        let removed = match config.retention_days {
            Some(days) => {
                let removed = transaction.execute(
                    &format!(
                        "DELETE FROM query_history WHERE query_id IN (
                        SELECT h.query_id FROM query_history h WHERE {ELIGIBLE}
                        ORDER BY h.accepted_at,h.query_id LIMIT $2 FOR UPDATE OF h SKIP LOCKED)"
                    ),
                    &[&days, &config.retention_batch_size],
                )?;
                transaction.execute("DELETE FROM session_events WHERE open_span_id IN (
                    SELECT open_span_id FROM session_events WHERE COALESCE(closed_at,finished_at,provisioning_started_at)<now()-($1::INT*interval '1 day')
                    AND NOT EXISTS(SELECT 1 FROM queue_sessions s WHERE s.open_span_id=session_events.open_span_id AND s.closed_at IS NULL)
                    ORDER BY COALESCE(closed_at,finished_at,provisioning_started_at),open_span_id LIMIT $2 FOR UPDATE SKIP LOCKED)", &[&days,&config.retention_batch_size])?;
                transaction.execute("DELETE FROM queue_sessions WHERE session_id IN (SELECT session_id FROM queue_sessions
                    WHERE closed_at<now()-($1::INT*interval '1 day') AND outstanding IS NULL ORDER BY closed_at,session_id LIMIT $2 FOR UPDATE SKIP LOCKED)", &[&days,&config.retention_batch_size])?;
                removed
            }
            None => 0,
        };
        let artifacts_removed = removed + sql_removed + profile_removed;
        transaction.execute(
            "UPDATE platform_maintenance_runs SET rows_pruned=rows_pruned+$2 WHERE run_id=$1",
            &[&run_id, &(artifacts_removed as i64)],
        )?;
        transaction.commit()?;
        if [removed, sql_removed, profile_removed]
            .into_iter()
            .all(|count| count < config.retention_batch_size as u64)
        {
            break;
        }
    }
    Ok(())
}

fn finish_task(
    client: &mut Client,
    lease: &Lease,
    config: &MaintenanceConfig,
    run_id: &str,
    task: &str,
    error: Option<&str>,
) -> Result<TaskReport> {
    let mut transaction = fenced_transaction(client, lease, config)?;
    // Failure recording must not retry the query that just timed out (for
    // example, inspecting a locked history table to calculate its backlog).
    let oldest: Option<String> = if task == "retention" && error.is_none() {
        match config.retention_days {
            Some(days) => transaction.query_opt(&format!(
                "SELECT h.accepted_at::TEXT FROM query_history h WHERE {ELIGIBLE} ORDER BY h.accepted_at,h.query_id LIMIT 1"), &[&days])?.map(|row| row.get(0)),
            None => None,
        }
    } else {
        None
    };
    let artifact_backlog = if task == "retention" && error.is_none() {
        let mut found = false;
        for (days, column) in [
            (config.sql_retention_days, "sql"),
            (config.profile_retention_days, "profile"),
        ] {
            if let Some(days) = days {
                found |= transaction.query_one(
                    &format!("SELECT EXISTS(SELECT 1 FROM query_history h WHERE {ELIGIBLE} AND h.{column} IS NOT NULL)"),
                    &[&days],
                )?.get::<_, bool>(0);
            }
        }
        found
    } else {
        false
    };
    let backlog =
        (task == "retention" && error.is_none()).then_some(oldest.is_some() || artifact_backlog);
    let status = if error.is_some() {
        "failed"
    } else {
        "succeeded"
    };
    let row = transaction.query_one(
        "UPDATE platform_maintenance_runs SET finished_at=now(),status=$2,error=$3,
            backlog_remaining=$4,oldest_eligible_at=$5::TEXT::TIMESTAMPTZ WHERE run_id=$1 RETURNING rows_pruned",
        &[&run_id, &status, &error, &backlog, &oldest],
    )?;
    if let Some(error) = error {
        transaction.execute(
            "UPDATE platform_maintenance_tasks SET last_failure_at=now(),last_error=$2,
            next_due_at=now()+($3::BIGINT * interval '1 second') WHERE task=$1",
            &[&task, &error, &RETRY_SECONDS],
        )?;
    } else {
        let next = if backlog == Some(true) {
            Duration::from_secs(RETRY_SECONDS as u64).min(config.interval(task))
        } else {
            config.interval(task)
        };
        transaction.execute(
            "UPDATE platform_maintenance_tasks SET last_success_at=now(),last_error=NULL,
            next_due_at=now()+($2::BIGINT * interval '1 millisecond') WHERE task=$1",
            &[&task, &millis(next)?],
        )?;
    }
    transaction.commit()?;
    Ok(TaskReport {
        task: task.into(),
        status: status.into(),
        rows_pruned: u64::try_from(row.get::<_, i64>(0))?,
        backlog_remaining: backlog,
        error: error.map(str::to_owned),
    })
}

/// Work transactions are independently fenced and bounded. A failed reconcile
/// does not roll back a completed prune or prevent retention's later schedule.
/// A process crash leaves a running record that the next owner marks interrupted.
/// `pass_budget` stops starting more work; the current bounded transaction and
/// at most two finish-record transactions plus lease release can finish afterward.
/// This is a work budget, not a claim that bookkeeping obeys a wall-clock deadline.
/// Oldest due work runs first, including forced passes, so a recurring retention
/// backlog cannot repeatedly consume the budget ahead of overdue reconciliation.
pub(crate) fn run_claimed(
    client: &mut Client,
    lease: &Lease,
    config: &MaintenanceConfig,
    force: bool,
) -> Result<PassReport> {
    run_claimed_with_store(client, lease, config, force, None)
}
fn run_claimed_with_store(
    client: &mut Client,
    lease: &Lease,
    config: &MaintenanceConfig,
    force: bool,
    store: Option<&crate::artifacts::Store>,
) -> Result<PassReport> {
    config.validate()?;
    let started = Instant::now();
    let mut report = PassReport {
        acquired: true,
        generation: Some(lease.generation),
        tasks: Vec::new(),
    };
    let tasks = {
        let mut transaction = fenced_transaction(client, lease, config)?;
        let tasks: Vec<String> = transaction
            .query(
                "SELECT task FROM platform_maintenance_tasks
                 WHERE task IN ('retention','reconcile') OR ($1 AND task='artifacts')
                 ORDER BY next_due_at, CASE task WHEN 'retention' THEN 0 ELSE 1 END",
                &[&store.is_some()],
            )?
            .iter()
            .map(|row| row.get(0))
            .collect();
        transaction.commit()?;
        tasks
    };
    // Queue expiry is a conditional row transition, safe alongside live worker
    // control lanes. Charge its work to this pass and cap its slice so an expiry
    // backlog cannot consume every reconciliation/retention opportunity.
    let expiry_budget = config
        .pass_budget
        .saturating_sub(started.elapsed())
        .min(Duration::from_secs(1));
    if let Err(error) = crate::queue::expire_batch(client, 64, expiry_budget) {
        tracing::warn!(
            cause = safe_error(&error),
            "bounded queue expiry pass failed; live control or next maintenance pass will retry"
        );
    }
    for task in &tasks {
        let task = task.as_str();
        if started.elapsed() >= config.pass_budget {
            break;
        }
        let mut transaction = fenced_transaction(client, lease, config)?;
        let due: bool = transaction.query_one("SELECT next_due_at <= clock_timestamp() OR $2 FROM platform_maintenance_tasks WHERE task=$1", &[&task, &force])?.get(0);
        if !due {
            transaction.commit()?;
            continue;
        }
        let run_id = Uuid::new_v4().to_string();
        transaction.execute("INSERT INTO platform_maintenance_runs(run_id,owner,generation,task,status) VALUES ($1,$2,$3,$4,'running')", &[&run_id, &lease.owner.to_string(), &lease.generation, &task])?;
        transaction.commit()?;
        tracing::info!(
            task,
            generation = lease.generation,
            run_id,
            "platform maintenance started"
        );
        let result = if task == "retention" {
            retention(client, lease, config, &run_id, started)
        } else if task == "artifacts" {
            (|| -> Result<()> {
                let store = store.context("artifact store unavailable")?;
                let candidates =
                    crate::job_artifacts::deletion_candidates(client, lease, config, 8)?;
                let mut failures = 0_u32;
                for object in candidates {
                    let remaining = config.pass_budget.saturating_sub(started.elapsed());
                    if remaining < Duration::from_millis(1) {
                        break;
                    }
                    // No SQL transaction or lease-row lock is held during IO.
                    if tokio::runtime::Handle::current()
                        .block_on(store.delete(
                            &object.relative_key,
                            tokio::time::Instant::now() + remaining,
                        ))
                        .is_err()
                    {
                        failures += 1;
                        continue;
                    }
                    let mut bounded = config.clone();
                    bounded.statement_timeout = config.statement_timeout.min(
                        config
                            .pass_budget
                            .saturating_sub(started.elapsed())
                            .max(Duration::from_millis(1)),
                    );
                    bounded.lock_timeout = config.lock_timeout.min(bounded.statement_timeout);
                    crate::job_artifacts::ack_delete(client, lease, &bounded, &object)?;
                }
                anyhow::ensure!(
                    failures == 0,
                    "{failures} artifact deletions failed; durable retries remain scheduled"
                );
                Ok(())
            })()
        } else {
            (|| -> Result<()> {
                let remaining = config.pass_budget.saturating_sub(started.elapsed());
                anyhow::ensure!(
                    remaining >= Duration::from_millis(1),
                    "maintenance pass budget exhausted"
                );
                let mut bounded = config.clone();
                bounded.statement_timeout = config.statement_timeout.min(remaining);
                bounded.lock_timeout = config.lock_timeout.min(bounded.statement_timeout);
                let mut transaction = fenced_transaction(client, lease, &bounded)?;
                metering::reconcile(&mut transaction)?;
                transaction.commit()?;
                Ok(())
            })()
        };
        let error = result.as_ref().err().map(safe_error);
        let task_report = match finish_task(client, lease, config, &run_id, task, error.as_deref())
        {
            Ok(report) => report,
            Err(error) => {
                let failure = safe_error(&error);
                finish_task(client, lease, config, &run_id, task, Some(&failure))?
            }
        };
        tracing::info!(task, generation=lease.generation, run_id, status=task_report.status,
            rows_pruned=task_report.rows_pruned, backlog_remaining=?task_report.backlog_remaining,
            error=?task_report.error, "platform maintenance finished");
        report.tasks.push(task_report);
    }
    Ok(report)
}

pub(crate) fn maintenance_pass(
    client: &mut Client,
    owner: Uuid,
    config: &MaintenanceConfig,
    force: bool,
) -> Result<PassReport> {
    maintenance_with_store(client, owner, config, force, None)
}
fn maintenance_with_store(
    client: &mut Client,
    owner: Uuid,
    config: &MaintenanceConfig,
    force: bool,
    store: Option<&crate::artifacts::Store>,
) -> Result<PassReport> {
    check_schema(client)?;

    let Some(lease) = claim_maintenance(client, owner, config)? else {
        return Ok(PassReport {
            acquired: false,
            generation: None,
            tasks: Vec::new(),
        });
    };
    let result = match store {
        Some(store) => run_claimed_with_store(client, &lease, config, force, Some(store)),
        None => run_claimed(client, &lease, config, force),
    };
    let release = release_maintenance(client, &lease, config);
    match result {
        Ok(report) => {
            release?;
            Ok(report)
        }
        Err(error) => {
            let _ = release;
            Err(error)
        }
    }
}

pub(crate) async fn migrate_command() -> Result<()> {
    let dsn = std::env::var("PLATFORM_DSN")?;
    let config = connection_config(&dsn)?;
    let wait = schema_wait()?;
    let timeout = seconds_env("DUCKFLEET_MIGRATION_TIMEOUT_SECONDS", 240, 3600)?;
    let seed = Seed::from_env()?;
    let roles = crate::db_roles::RoleNames::from_env()?;
    let report = tokio::task::spawn_blocking(move || -> Result<MigrationReport> {
        let started = Instant::now();
        loop {
            let remaining = wait.saturating_sub(started.elapsed());
            anyhow::ensure!(
                remaining >= Duration::from_millis(1),
                "platform PostgreSQL was unavailable before migration connection deadline"
            );
            match connect_config(
                &config,
                remaining.min(Duration::from_secs(3)),
                Some(timeout),
            ) {
                Ok(mut client) => return migrate_with_roles(&mut client, &seed, Some(&roles)),
                Err(error) if fatal_startup(&error) => return Err(startup_error(&error)),
                Err(_) => {}
            }
            std::thread::sleep(POLL.min(wait.saturating_sub(started.elapsed())));
        }
    })
    .await
    .context("platform migration task")??;
    tracing::info!(
        version = report.version,
        applied = report.applied,
        elapsed_ms = report.elapsed_ms,
        "platform migration complete"
    );
    println!("{}", serde_json::to_string(&report)?);
    Ok(())
}

pub(crate) async fn grants_command() -> Result<()> {
    let dsn = std::env::var("PLATFORM_DSN")?;
    let roles = crate::db_roles::RoleNames::from_env()?;
    tokio::task::spawn_blocking(move || -> Result<()> {
        let mut client = connect(&dsn, Duration::from_secs(3), Duration::from_secs(30))?;
        check_schema(&mut client)?;
        let mut tx = client.transaction()?;
        crate::db_roles::grant_runtime(&mut tx, &roles)?;
        tx.commit()?;
        Ok(())
    })
    .await??;
    println!("platform runtime privileges repaired");
    Ok(())
}

pub(crate) async fn maintain_command() -> Result<()> {
    let dsn = std::env::var("PLATFORM_DSN")?;
    let config = MaintenanceConfig::from_env()?;
    let store = crate::artifacts::StoreConfig::from_env()?
        .map(crate::artifacts::StoreConfig::build)
        .transpose()?;
    let report = tokio::task::spawn_blocking(move || -> Result<PassReport> {
        let mut client = connect(&dsn, Duration::from_secs(3), config.statement_timeout)?;
        crate::db_roles::validate_runtime(&mut client, crate::db_roles::RuntimeRole::Maintainer)?;
        maintenance_with_store(&mut client, Uuid::new_v4(), &config, true, store.as_ref())
    })
    .await
    .context("platform maintenance task")??;
    println!("{}", serde_json::to_string(&report)?);
    anyhow::ensure!(
        report.tasks.iter().all(|task| task.status == "succeeded"),
        "one or more platform maintenance tasks failed; inspect platform_maintenance_runs"
    );
    Ok(())
}

pub(crate) fn spawn_maintenance(
    dsn: String,
    config: MaintenanceConfig,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let store = match crate::artifacts::StoreConfig::from_env()
            .and_then(|config| config.map(crate::artifacts::StoreConfig::build).transpose())
        {
            Ok(store) => store,
            Err(_) => {
                tracing::error!(
                    "artifact cleanup configuration unavailable; metadata maintenance continues"
                );
                None
            }
        };
        let owner = Uuid::new_v4();
        let mut interval = tokio::time::interval(MAINTENANCE_TICK);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
            let dsn = dsn.clone();
            let config = config.clone();
            let store = store.clone();
            let result = tokio::task::spawn_blocking(move || -> Result<PassReport> {
                let mut client = connect(&dsn, Duration::from_secs(3), config.statement_timeout)?;
                crate::db_roles::validate_runtime(
                    &mut client,
                    crate::db_roles::RuntimeRole::Maintainer,
                )?;
                maintenance_with_store(&mut client, owner, &config, false, store.as_ref())
            })
            .await;
            match result {
                Ok(Ok(_)) => {}
                Ok(Err(error)) => tracing::warn!(
                    error = safe_error(&error),
                    "platform maintenance pass failed; retrying next tick"
                ),
                Err(_) => tracing::error!("platform maintenance task failed; retrying next tick"),
            }
        }
    })
}
