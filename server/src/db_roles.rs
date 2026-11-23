//! Deployment database privileges. Application authorization remains a separate
//! boundary: these roles protect retained records from runtime edits and DDL.
use anyhow::{Context, Result, ensure};
use postgres::{Client, Transaction};

use crate::compute::Authority;
use serde::Deserialize;

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RoleNames {
    pub gateway: String,
    pub worker: String,
    pub maintainer: String,
}

impl RoleNames {
    pub fn from_env() -> Result<Self> {
        let value = std::env::var("DUCKFLEET_DATABASE_ROLES_JSON")
            .context("set DUCKFLEET_DATABASE_ROLES_JSON after provisioning separate runtime PostgreSQL roles")?;
        let roles: Self = serde_json::from_str(&value).context(
            "DUCKFLEET_DATABASE_ROLES_JSON must name gateway, worker, and maintainer roles",
        )?;
        roles.validate()?;
        Ok(roles)
    }

    fn validate(&self) -> Result<()> {
        let names = [&self.gateway, &self.worker, &self.maintainer];
        for name in names {
            ensure!(
                !name.is_empty()
                    && name.len() <= 63
                    && name.bytes().enumerate().all(|(i, b)| b == b'_'
                        || b.is_ascii_lowercase()
                        || (i > 0 && b.is_ascii_digit())),
                "runtime role names must be lowercase PostgreSQL identifiers of at most 63 bytes"
            );
        }
        ensure!(
            self.gateway != self.worker
                && self.gateway != self.maintainer
                && self.worker != self.maintainer,
            "runtime PostgreSQL roles must be distinct"
        );
        Ok(())
    }
}

fn identifier(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

const QUEUE_TABLES: &[&str] = &[
    "requests",
    "query_history",
    "reservations",
    "admission_scopes",
    "queue_workers",
    "queue_sessions",
    "query_payloads",
    "query_controls",
    "query_permits",
    "queue_fairness",
    "queue_notifications",
    "session_events",
    "query_jobs",
    "job_bindings",
    "job_dispatch_fairness",
    "job_artifacts",
];
const POLICY_TABLES: &[&str] = &[
    "platform_namespace",
    "principals",
    "principal_roles",
    "permission_grants",
    "compute_grants",
    "authz_defaults",
    "authz_legacy_owners",
];
const MAINTENANCE_TABLES: &[&str] = &[
    "platform_maintenance_lease",
    "platform_maintenance_tasks",
    "platform_maintenance_runs",
];
const CREDENTIAL_COLUMNS: &str = "credential_id,principal,principal_id,expires_at,revoked_at";

/// Run as the platform object owner in the same transaction as schema migration.
/// This grants no membership and creates no roles. Repeating it repairs grants
/// after a restore without recreating policy grants or changing retained records.
pub(crate) fn grant_runtime(tx: &mut Transaction<'_>, roles: &RoleNames) -> Result<()> {
    roles.validate()?;
    let schema: String = tx.query_one("SELECT current_schema()", &[])?.get(0);
    let database: String = tx.query_one("SELECT current_database()", &[])?.get(0);
    let qualified = |table: &str| format!("{}.{}", identifier(&schema), identifier(table));
    for role in [&roles.gateway, &roles.worker, &roles.maintainer] {
        let row = tx.query_opt("SELECT rolcanlogin AND NOT (rolsuper OR rolcreaterole OR rolcreatedb OR rolreplication OR rolbypassrls),
            oid<>current_user::regrole::oid AND NOT EXISTS(SELECT 1 FROM pg_auth_members WHERE member=pg_roles.oid)
            FROM pg_roles WHERE rolname=$1", &[role])?
            .context("runtime database role is missing; run scripts/provision-database.sh with a PostgreSQL administrator first")?;
        ensure!(
            row.get::<_, bool>(0) && row.get::<_, bool>(1),
            "runtime database roles must be separate constrained LOGIN roles without memberships"
        );
    }
    let all = [&roles.gateway, &roles.worker, &roles.maintainer]
        .map(|r| identifier(r))
        .join(",");
    tx.batch_execute(&format!(
        "REVOKE ALL ON DATABASE {} FROM PUBLIC;
         GRANT CONNECT ON DATABASE {} TO {all};
         REVOKE CREATE ON SCHEMA {} FROM PUBLIC,{all};
         GRANT USAGE ON SCHEMA {} TO {all};
         REVOKE ALL ON ALL TABLES IN SCHEMA {} FROM PUBLIC,{all};
         REVOKE ALL ON ALL SEQUENCES IN SCHEMA {} FROM PUBLIC,{all};
         REVOKE ALL ON ALL FUNCTIONS IN SCHEMA {} FROM PUBLIC,{all};
         ALTER DEFAULT PRIVILEGES IN SCHEMA {} REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC;",
        identifier(&database),
        identifier(&database),
        identifier(&schema),
        identifier(&schema),
        identifier(&schema),
        identifier(&schema),
        identifier(&schema),
        identifier(&schema)
    ))?;
    // Table-level REVOKE does not remove old column grants. Clear those too before
    // installing the reviewed matrix, including legacy password/hash access.
    for row in tx.query(
        "SELECT c.relname, string_agg(quote_ident(a.attname),',' ORDER BY a.attnum)
        FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
        JOIN pg_attribute a ON a.attrelid=c.oid AND a.attnum>0 AND NOT a.attisdropped
        WHERE n.nspname=$1 AND c.relkind IN ('r','p','v') GROUP BY c.relname",
        &[&schema],
    )? {
        let table: String = row.get(0);
        let columns: String = row.get(1);
        tx.batch_execute(&format!(
            "REVOKE ALL ({columns}) ON {} FROM PUBLIC,{all}",
            qualified(&table)
        ))?;
    }
    let gateway = identifier(&roles.gateway);
    let worker = identifier(&roles.worker);
    let maintainer = identifier(&roles.maintainer);
    for table in QUEUE_TABLES {
        tx.batch_execute(&format!(
            "GRANT SELECT,INSERT,UPDATE,DELETE ON {} TO {all}",
            qualified(table)
        ))?;
    }
    for table in POLICY_TABLES {
        tx.batch_execute(&format!("GRANT SELECT ON {} TO {all}", qualified(table)))?;
    }
    for table in ["principal_roles", "permission_grants", "compute_grants"] {
        tx.batch_execute(&format!(
            "GRANT INSERT,UPDATE,DELETE ON {} TO {gateway}",
            qualified(table)
        ))?;
    }
    tx.batch_execute(&format!(
        "GRANT INSERT ON {} TO {gateway};
         GRANT UPDATE(disabled,policy_version) ON {} TO {gateway};
         GRANT SELECT,INSERT ON {} TO {gateway};
         GRANT UPDATE(revoked_at) ON {} TO {gateway};
         GRANT SELECT({CREDENTIAL_COLUMNS}) ON {} TO {worker},{maintainer};
         GRANT SELECT ON {},{},{},{} TO {all};
         GRANT SELECT,INSERT ON {} TO {all};
         GRANT INSERT ON {} TO {all};
         GRANT SELECT ON {} TO {gateway};
         GRANT UPDATE(spent_micro_usd,held_micro_usd) ON {} TO {all};
         GRANT UPDATE(reconciled_at) ON {} TO {maintainer};
         GRANT UPDATE(prepaid_micro_usd,max_job_micro_usd,updated_at) ON {} TO {gateway};",
        qualified("principals"),
        qualified("principals"),
        qualified("credentials"),
        qualified("credentials"),
        qualified("credentials"),
        qualified("platform_schema_migrations"),
        qualified("requests_schema_migrations"),
        qualified("budget"),
        qualified("admission_policies"),
        qualified("usage_events"),
        qualified("audit_events"),
        qualified("audit_events"),
        qualified("budget"),
        qualified("budget"),
        qualified("budget")
    ))?;
    for table in ["admission_policies", "admission_publication"] {
        tx.batch_execute(&format!(
            "GRANT SELECT,INSERT,UPDATE ON {} TO {gateway}",
            qualified(table)
        ))?;
    }
    for table in MAINTENANCE_TABLES {
        tx.batch_execute(&format!(
            "GRANT SELECT ON {} TO {gateway}; GRANT SELECT,INSERT,UPDATE ON {} TO {maintainer}",
            qualified(table),
            qualified(table)
        ))?;
    }
    // BIGSERIAL allocations are necessary for appends, never sequence rewrites.
    for row in tx.query(
        "SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
        WHERE n.nspname=$1 AND c.relkind='S'",
        &[&schema],
    )? {
        let sequence: String = row.get(0);
        tx.batch_execute(&format!(
            "GRANT USAGE ON SEQUENCE {} TO {all}",
            qualified(&sequence)
        ))?;
    }
    Ok(())
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum RuntimeRole {
    Gateway,
    Worker,
    Maintainer,
}

/// Fail startup for owner/superuser credentials or dangerous inherited grants.
/// These catalog checks are bounded by the fixed application relations; this is
/// privilege validation, not a name-based assurance or end-user PostgreSQL RLS.
pub(crate) fn validate_runtime(client: &mut Client, role: RuntimeRole) -> Result<()> {
    let row = client.query_one(
        "SELECT NOT EXISTS(SELECT 1 FROM pg_roles r WHERE
        (r.rolsuper OR r.rolcreaterole OR r.rolcreatedb OR r.rolreplication OR r.rolbypassrls)
        AND pg_has_role(current_user,r.oid,'MEMBER')),
        NOT has_database_privilege(current_user,current_database(),'CREATE'),
        NOT has_schema_privilege(current_user,current_schema(),'CREATE'),
        NOT EXISTS(SELECT 1 FROM pg_namespace n WHERE n.nspname=current_schema()
            AND pg_has_role(current_user,n.nspowner,'MEMBER')),
        session_user=current_user,
        NOT EXISTS(SELECT 1 FROM pg_auth_members WHERE member=current_user::regrole::oid)",
        &[],
    )?;
    ensure!(
        (0..6).all(|i| row.get::<_, bool>(i)),
        "unsafe platform database credential: connect directly as the provisioned nonowner {role:?} role, without schema CREATE or role memberships"
    );
    for table in ["audit_events", "usage_events"] {
        let row = client.query_one("SELECT has_table_privilege(current_user,$1,'UPDATE,DELETE,TRUNCATE,TRIGGER'),
            has_any_column_privilege(current_user,$1,'UPDATE'),
            pg_has_role(current_user,(SELECT relowner FROM pg_class WHERE oid=to_regclass($1)),'MEMBER')", &[&table])?;
        ensure!(
            !(0..3).any(|i| row.get::<_, bool>(i)),
            "unsafe {role:?} database privileges on append-only {table}; rerun migration grants using the migration credential"
        );
    }
    let hash_access: bool = client
        .query_one(
            "SELECT has_column_privilege(current_user,'credentials','token_sha256','SELECT')",
            &[],
        )?
        .get(0);
    if !matches!(role, RuntimeRole::Gateway) {
        ensure!(
            !hash_access,
            "worker/maintainer database roles must not read credential token hashes"
        );
        for table in [
            "credentials",
            "principals",
            "principal_roles",
            "permission_grants",
            "compute_grants",
            "authz_defaults",
        ] {
            let writable: bool = client.query_one("SELECT has_table_privilege(current_user,$1,'INSERT,UPDATE,DELETE,TRUNCATE,TRIGGER')
                OR has_any_column_privilege(current_user,$1,'INSERT,UPDATE')", &[&table])?.get(0);
            ensure!(
                !writable,
                "worker/maintainer database roles must not manage {table}"
            );
        }
        for column in ["prepaid_micro_usd", "max_job_micro_usd"] {
            let writable: bool = client
                .query_one(
                    "SELECT has_column_privilege(current_user,'budget',$1,'UPDATE')",
                    &[&column],
                )?
                .get(0);
            ensure!(
                !writable,
                "worker/maintainer database roles must not change budget limits"
            );
        }
    } else {
        ensure!(
            hash_access,
            "gateway database role requires credential authentication privileges"
        );
        let spill_kib: i64 = client
            .query_one(
                "SELECT setting::bigint FROM pg_settings WHERE name='temp_file_limit'",
                &[],
            )?
            .get(0);
        ensure!(
            (1..=16_384).contains(&spill_kib),
            "gateway requires a finite temp_file_limit of at most 16MB; rerun administrator database provisioning before starting schema 4 compute"
        );
    }
    let expected = match role {
        RuntimeRole::Gateway => ("credentials", "INSERT"),
        RuntimeRole::Worker => ("query_history", "INSERT"),
        RuntimeRole::Maintainer => ("platform_maintenance_lease", "UPDATE"),
    };
    let allowed: bool = client
        .query_one(
            "SELECT has_table_privilege(current_user,$1,$2)
        AND has_table_privilege(current_user,'audit_events','INSERT')
        AND has_table_privilege(current_user,'platform_schema_migrations','SELECT')",
            &[&expected.0, &expected.1],
        )?
        .get(0);
    ensure!(
        allowed,
        "missing required {role:?} PostgreSQL privileges; rerun the matching migration grants"
    );
    Ok(())
}

/// The engine may own its dedicated lake schema, but must not carry cluster-wide
/// authority or be able to connect to the protected platform database. Catalog
/// and platform can use separate PostgreSQL servers; absence of the platform
/// database on the catalog server is therefore an ordinary supported case.
pub(crate) fn validate_catalog(
    client: &mut Client,
    platform_database: &str,
    authority: Authority,
) -> Result<()> {
    let safe: bool = client
        .query_one(
            "SELECT current_user=session_user
            AND NOT (rolsuper OR rolcreaterole OR rolcreatedb OR rolreplication OR rolbypassrls)
            AND NOT EXISTS(SELECT 1 FROM pg_auth_members WHERE member=pg_roles.oid)
            AND current_database()<>$1
            AND NOT EXISTS(SELECT 1 FROM pg_database WHERE datname=$1
                AND has_database_privilege(current_user,oid,'CONNECT'))
         FROM pg_roles WHERE rolname=current_user",
            &[&platform_database],
        )?
        .get(0);
    ensure!(
        safe,
        "unsafe catalog database credential: use a dedicated nonsuperuser lake role without memberships or platform database access"
    );
    if authority == Authority::Reader {
        let write_free: bool = client
            .query_one(
                "SELECT NOT has_schema_privilege(current_user,current_schema(),'CREATE')
            AND NOT has_database_privilege(current_user,current_database(),'CREATE')
            AND NOT EXISTS (
                SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
                WHERE n.nspname=current_schema() AND c.relkind IN ('r','p','v','m')
                AND (has_table_privilege(current_user,c.oid,'INSERT,UPDATE,DELETE,TRUNCATE,TRIGGER')
                    OR has_any_column_privilege(current_user,c.oid,'INSERT,UPDATE')
                    OR pg_has_role(current_user,c.relowner,'MEMBER'))
            )",
                &[],
            )?
            .get(0);
        ensure!(
            write_free,
            "reader catalog credential must have USAGE/SELECT only, without DDL, DML, or object ownership"
        );
    }
    Ok(())
}
