use crate::util;
use anyhow::{Result, bail, ensure};
use serde_json::Value;
use std::{
    collections::BTreeMap,
    process::{Command, Output},
};

struct Fixture {
    base: Vec<String>,
    environment: BTreeMap<String, String>,
    settings: Value,
}

impl Fixture {
    fn new() -> Result<Self> {
        let project = format!(
            "duckfleet-provision-test-{}",
            &uuid::Uuid::new_v4().simple().to_string()[..12]
        );
        let base = vec![
            "docker".into(),
            "compose".into(),
            "-f".into(),
            util::root()
                .join("compose.yaml")
                .to_string_lossy()
                .into_owned(),
            "-p".into(),
            project,
        ];
        let mut environment: BTreeMap<String, String> = std::env::vars().collect();
        environment.insert(
            "DUCKFLEET_POSTGRES_ADMIN_PASSWORD".into(),
            "local-administrator-password".into(),
        );
        environment.insert("DUCKFLEET_ADOPT_DATABASE_OWNER".into(), "no".into());
        let output = Self::execute(
            &base,
            &environment,
            &["config".into(), "--format".into(), "json".into()],
            true,
        )?;
        let resolved: Value = serde_json::from_slice(&output.stdout)?;
        let settings = resolved["services"]["platform-provision"]["environment"].clone();
        Ok(Self {
            base,
            environment,
            settings,
        })
    }

    fn execute(
        base: &[String],
        environment: &BTreeMap<String, String>,
        arguments: &[String],
        succeeds: bool,
    ) -> Result<Output> {
        let output = Command::new(&base[0])
            .args(&base[1..])
            .args(arguments)
            .envs(environment)
            .current_dir(util::root())
            .output()?;
        if succeeds && !output.status.success() {
            bail!(
                "database fixture command failed: {}",
                util::stderr_text(&output)
            );
        }
        Ok(output)
    }

    fn passwords(&self) -> Vec<String> {
        ["MIGRATOR", "GATEWAY", "WORKER", "MAINTAINER", "CATALOG"]
            .iter()
            .map(|role| {
                util::text(self.settings.get(format!("DUCKFLEET_DB_{role}_PASSWORD"))).to_owned()
            })
            .collect()
    }

    fn redacted(&self, text: &str) -> String {
        self.passwords()
            .iter()
            .fold(text.to_owned(), |text, password| {
                text.replace(password, "[redacted]")
            })
    }

    fn command(&self, arguments: &[String], succeeds: bool) -> Result<Output> {
        let output = Self::execute(&self.base, &self.environment, arguments, false)?;
        if succeeds && !output.status.success() {
            let error = self.redacted(&format!(
                "{}{}",
                util::stdout_text(&output),
                util::stderr_text(&output)
            ));
            bail!(
                "database fixture command failed: {}",
                error.chars().take(1200).collect::<String>()
            );
        }
        Ok(output)
    }

    fn query(
        &self,
        role: &str,
        database: &str,
        sql: &str,
        allowed: bool,
        password: Option<&str>,
    ) -> Result<String> {
        let key = role.to_uppercase();
        let password = password.map(str::to_owned).unwrap_or_else(|| {
            util::text(self.settings.get(format!("DUCKFLEET_DB_{key}_PASSWORD"))).to_owned()
        });
        let name = util::text(self.settings.get(format!("DUCKFLEET_DB_{key}_ROLE")));
        let output = self.command(
            &[
                "exec".into(),
                "-T".into(),
                "-e".into(),
                format!("PGPASSWORD={password}"),
                "postgres".into(),
                "psql".into(),
                "-X".into(),
                "-h".into(),
                "postgres".into(),
                "-U".into(),
                name.into(),
                "-d".into(),
                database.into(),
                "-v".into(),
                "ON_ERROR_STOP=1".into(),
                "-tAc".into(),
                sql.into(),
            ],
            false,
        )?;
        if output.status.success() != allowed {
            bail!(
                "{role}/{database} expected allowed={allowed}: {}",
                self.redacted(&util::stderr_text(&output))
            );
        }
        Ok(util::stdout_text(&output).trim().to_owned())
    }

    fn admin(&self, sql: &str, database: &str) -> Result<String> {
        let output = self.command(
            &[
                "exec".into(),
                "-T".into(),
                "postgres".into(),
                "psql".into(),
                "-X".into(),
                "-U".into(),
                "duckfleet".into(),
                "-d".into(),
                database.into(),
                "-v".into(),
                "ON_ERROR_STOP=1".into(),
                "-tAc".into(),
                sql.into(),
            ],
            true,
        )?;
        Ok(util::stdout_text(&output).trim().to_owned())
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = Self::execute(
            &self.base,
            &self.environment,
            &["down".into(), "--volumes".into(), "--remove-orphans".into()],
            false,
        );
    }
}

pub fn run(args: Vec<String>) -> Result<()> {
    util::reject_extra(&args, 0)?;
    let fixture = Fixture::new()?;
    fixture.command(
        &[
            "up".into(),
            "-d".into(),
            "--wait".into(),
            "--wait-timeout".into(),
            "60".into(),
            "postgres".into(),
        ],
        true,
    )?;
    fixture.command(
        &[
            "run".into(),
            "--rm".into(),
            "--no-deps".into(),
            "platform-provision".into(),
        ],
        true,
    )?;
    let identities =
        "SELECT rolname,oid FROM pg_roles WHERE rolname LIKE 'duckfleet_%' ORDER BY rolname";
    let before = fixture.admin(identities, "postgres")?;
    fixture.command(
        &[
            "run".into(),
            "--rm".into(),
            "--no-deps".into(),
            "platform-provision".into(),
        ],
        true,
    )?;
    ensure!(
        before == fixture.admin(identities, "postgres")?,
        "repeat provisioning changed role identities"
    );
    println!("PASS: fresh/repeated provisioning preserves five separate role identities");
    for role in ["migrator", "gateway", "worker", "maintainer", "catalog"] {
        let own = if role == "catalog" {
            "lake"
        } else {
            "platform"
        };
        let other = if role == "catalog" {
            "platform"
        } else {
            "lake"
        };
        ensure!(fixture.query(role, own, "SELECT current_user=session_user AND NOT (rolsuper OR rolcreatedb OR rolcreaterole OR rolreplication OR rolbypassrls) AND NOT EXISTS(SELECT 1 FROM pg_auth_members WHERE member=pg_roles.oid) FROM pg_roles WHERE rolname=current_user", true, None)? == "t");
        fixture.query(role, other, "SELECT 1", false, None)?;
        fixture.query(role, own, "CREATE ROLE forbidden_role", false, None)?;
    }
    for role in ["gateway", "worker", "maintainer"] {
        fixture.query(
            role,
            "platform",
            "CREATE TABLE public.forbidden_table(id int)",
            false,
            None,
        )?;
        fixture.query(role, "platform", "SET ROLE duckfleet_migrator", false, None)?;
    }
    let worker_password =
        util::text(fixture.settings.get("DUCKFLEET_DB_WORKER_PASSWORD")).to_owned();
    fixture.query(
        "migrator",
        "platform",
        "SELECT 1",
        false,
        Some(&worker_password),
    )?;
    println!(
        "PASS: real network logins reject cross-database access, DDL, role escalation, and password reuse"
    );
    ensure!(
        fixture.admin(
            "SELECT pg_get_userbyid(datdba) FROM pg_database WHERE datname='platform'",
            "postgres"
        )? == "duckfleet_migrator"
    );
    ensure!(
        fixture.admin(
            "SELECT pg_get_userbyid(datdba) FROM pg_database WHERE datname='lake'",
            "postgres"
        )? == "duckfleet_catalog"
    );
    fixture.admin("ALTER DATABASE platform OWNER TO duckfleet", "postgres")?;
    fixture.admin(
        "CREATE TABLE public.legacy_owner_fixture(id int)",
        "platform",
    )?;
    let refused = fixture.command(
        &[
            "run".into(),
            "--rm".into(),
            "--no-deps".into(),
            "platform-provision".into(),
        ],
        false,
    )?;
    let message = format!(
        "{}{}",
        util::stdout_text(&refused),
        util::stderr_text(&refused)
    );
    ensure!(
        !refused.status.success() && message.contains("owner differs"),
        "owner refusal must return a failing exit status"
    );
    let owner =
        "SELECT pg_get_userbyid(relowner) FROM pg_class WHERE oid='legacy_owner_fixture'::regclass";
    ensure!(fixture.admin(owner, "platform")? == "duckfleet");
    fixture.command(
        &[
            "run".into(),
            "--rm".into(),
            "--no-deps".into(),
            "-e".into(),
            "DUCKFLEET_ADOPT_DATABASE_OWNER=yes".into(),
            "platform-provision".into(),
        ],
        true,
    )?;
    ensure!(fixture.admin(owner, "platform")? == "duckfleet_migrator");
    ensure!(
        fixture.admin(
            "SELECT pg_get_userbyid(datdba) FROM pg_database WHERE datname='lake'",
            "postgres"
        )? == "duckfleet_catalog"
    );
    println!("PASS: legacy-owner adoption is explicit and does not reassign the other database");
    Ok(())
}
