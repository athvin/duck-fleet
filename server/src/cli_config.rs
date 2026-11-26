use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::fs;
use std::path::{Path, PathBuf};

const PROJECT_FILE: &str = "duckfleet.json";
const STATE_DIR: &str = ".duckfleet";
const LOCAL_FILE: &str = "config.json";

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProjectConfig {
    version: u32,
    url: String,
}

#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct LocalConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    token: Option<String>,
}

#[derive(Debug)]
pub struct Resolved {
    pub url: Option<String>,
    pub token: Option<String>,
    project_path: Option<PathBuf>,
    url_source: &'static str,
    token_source: &'static str,
}

impl Resolved {
    pub fn redacted(&self) -> Value {
        json!({
            "url": self.url,
            "url_source": self.url_source,
            "token": self.token.as_ref().map(|_| "<redacted>"),
            "token_source": self.token_source,
            "project": self.project_path,
        })
    }
}

pub fn resolve(flag_url: Option<&str>, flag_token: Option<&str>) -> Result<Resolved, String> {
    let directory = std::env::current_dir()
        .map_err(|_| "cannot determine the current directory; use --url and --token".to_owned())?;
    resolve_from(
        &directory,
        flag_url,
        flag_token,
        std::env::var("DUCKFLEET_URL").ok().as_deref(),
        std::env::var("DUCKFLEET_TOKEN").ok().as_deref(),
    )
}

fn resolve_from(
    directory: &Path,
    flag_url: Option<&str>,
    flag_token: Option<&str>,
    environment_url: Option<&str>,
    environment_token: Option<&str>,
) -> Result<Resolved, String> {
    let project_path = directory
        .ancestors()
        .map(|parent| parent.join(PROJECT_FILE))
        .find(|path| path.exists());
    let project = project_path
        .as_ref()
        .map(|path| read_project(path))
        .transpose()?;
    let local_path = project_path
        .as_ref()
        .and_then(|path| path.parent())
        .map(|path| path.join(STATE_DIR).join(LOCAL_FILE));
    let local = local_path
        .as_ref()
        .filter(|path| path.exists())
        .map(|path| read_local(path))
        .transpose()?
        .unwrap_or_default();
    let project_url = project.as_ref().map(|config| config.url.as_str());
    let local_token = local.token.as_deref();
    let url = flag_url
        .or(environment_url)
        .or(project_url)
        .map(str::to_owned);
    let token = flag_token
        .or(environment_token)
        .or(local_token)
        .map(str::to_owned);
    let url_source = if flag_url.is_some() {
        "flag"
    } else if environment_url.is_some() {
        "environment"
    } else if project_url.is_some() {
        "project"
    } else {
        "missing"
    };
    let token_source = if flag_token.is_some() {
        "flag"
    } else if environment_token.is_some() {
        "environment"
    } else if local_token.is_some() {
        "local"
    } else {
        "missing"
    };
    Ok(Resolved {
        url,
        token,
        project_path,
        url_source,
        token_source,
    })
}

fn read_project(path: &Path) -> Result<ProjectConfig, String> {
    let bytes = bounded_read(path, 64 * 1024)?;
    let config: ProjectConfig = serde_json::from_slice(&bytes).map_err(|_| {
        format!(
            "{} is malformed; fix it or run duckfleet init --force",
            path.display()
        )
    })?;
    if config.version != 1 || config.url.trim().is_empty() {
        return Err(format!(
            "{} must contain version 1 and a non-empty url",
            path.display()
        ));
    }
    Ok(config)
}

fn read_local(path: &Path) -> Result<LocalConfig, String> {
    let bytes = bounded_read(path, 64 * 1024)?;
    let config: LocalConfig = serde_json::from_slice(&bytes).map_err(|_| {
        format!(
            "{} is malformed; fix or remove the local configuration",
            path.display()
        )
    })?;
    if config.token.as_ref().is_some_and(|token| token.is_empty()) {
        return Err(format!("{} contains an empty token", path.display()));
    }
    Ok(config)
}

fn bounded_read(path: &Path, maximum: u64) -> Result<Vec<u8>, String> {
    let metadata = fs::metadata(path)
        .map_err(|_| format!("cannot read configuration at {}", path.display()))?;
    if !metadata.is_file() || metadata.len() > maximum {
        return Err(format!(
            "configuration at {} is not a bounded regular file",
            path.display()
        ));
    }
    fs::read(path).map_err(|_| format!("cannot read configuration at {}", path.display()))
}

pub fn init(
    flag_url: Option<&str>,
    flag_token: Option<&str>,
    force: bool,
) -> Result<String, String> {
    let directory = std::env::current_dir()
        .map_err(|_| "cannot determine the current directory for duckfleet init".to_owned())?;
    init_in(
        &directory,
        flag_url,
        flag_token,
        std::env::var("DUCKFLEET_URL").ok().as_deref(),
        std::env::var("DUCKFLEET_TOKEN").ok().as_deref(),
        force,
    )
}

fn init_in(
    directory: &Path,
    flag_url: Option<&str>,
    flag_token: Option<&str>,
    environment_url: Option<&str>,
    environment_token: Option<&str>,
    force: bool,
) -> Result<String, String> {
    let project_path = directory.join(PROJECT_FILE);
    if project_path.exists() && !force {
        return Err("duckfleet.json already exists; use --force to replace it".into());
    }
    let url = flag_url
        .or(environment_url)
        .unwrap_or("http://127.0.0.1:18080");
    if url.trim().is_empty() {
        return Err("the project URL must not be empty".into());
    }
    if project_path
        .symlink_metadata()
        .ok()
        .is_some_and(|metadata| !metadata.file_type().is_file())
    {
        return Err("duckfleet.json must be a regular file, not a symlink or directory".into());
    }
    let project = ProjectConfig {
        version: 1,
        url: url.to_owned(),
    };
    let encoded = serde_json::to_vec_pretty(&project)
        .map_err(|_| "cannot encode project configuration".to_owned())?;
    fs::write(&project_path, [encoded, b"\n".to_vec()].concat())
        .map_err(|_| "cannot write duckfleet.json".to_owned())?;

    let state = directory.join(STATE_DIR);
    fs::create_dir_all(&state).map_err(|_| "cannot create .duckfleet".to_owned())?;
    set_private_directory(&state)?;
    let local_path = state.join(LOCAL_FILE);
    if local_path.exists() && !force {
        update_gitignore(directory)?;
        return Ok(format!(
            "initialized {}; preserved existing local credentials\nnext: duckfleet sql 'select 42'",
            project_path.display()
        ));
    }
    if local_path
        .symlink_metadata()
        .ok()
        .is_some_and(|metadata| !metadata.file_type().is_file())
    {
        return Err(
            ".duckfleet/config.json must be a regular file, not a symlink or directory".into(),
        );
    }
    let local = LocalConfig {
        token: flag_token.or(environment_token).map(str::to_owned),
    };
    let encoded = serde_json::to_vec_pretty(&local)
        .map_err(|_| "cannot encode local configuration".to_owned())?;
    write_private(&local_path, &[encoded, b"\n".to_vec()].concat())?;
    update_gitignore(directory)?;
    Ok(format!(
        "initialized {}; local state is {}/\nnext: duckfleet sql 'select 42'",
        project_path.display(),
        STATE_DIR
    ))
}

fn update_gitignore(directory: &Path) -> Result<(), String> {
    let path = directory.join(".gitignore");
    let existing = fs::read_to_string(&path).unwrap_or_default();
    if existing
        .lines()
        .any(|line| matches!(line.trim(), ".duckfleet" | ".duckfleet/"))
    {
        return Ok(());
    }
    let mut updated = existing;
    if !updated.is_empty() && !updated.ends_with('\n') {
        updated.push('\n');
    }
    updated.push_str(".duckfleet/\n");
    fs::write(path, updated).map_err(|_| "cannot update .gitignore".to_owned())
}

#[cfg(unix)]
fn set_private_directory(path: &Path) -> Result<(), String> {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(path, fs::Permissions::from_mode(0o700))
        .map_err(|_| "cannot restrict .duckfleet permissions".to_owned())
}

#[cfg(not(unix))]
fn set_private_directory(_path: &Path) -> Result<(), String> {
    Ok(())
}

#[cfg(unix)]
fn write_private(path: &Path, bytes: &[u8]) -> Result<(), String> {
    use std::os::unix::fs::OpenOptionsExt;
    let mut options = fs::OpenOptions::new();
    options.write(true).create(true).truncate(true).mode(0o600);
    let mut file = options
        .open(path)
        .map_err(|_| "cannot write protected local configuration".to_owned())?;
    std::io::Write::write_all(&mut file, bytes)
        .map_err(|_| "cannot write protected local configuration".to_owned())?;
    use std::os::unix::fs::PermissionsExt;
    file.set_permissions(fs::Permissions::from_mode(0o600))
        .map_err(|_| "cannot restrict local credential permissions".to_owned())
}

#[cfg(not(unix))]
fn write_private(path: &Path, bytes: &[u8]) -> Result<(), String> {
    fs::write(path, bytes).map_err(|_| "cannot write protected local configuration".to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temporary(name: &str) -> PathBuf {
        let path = std::env::temp_dir().join(format!(
            "duckfleet-cli-config-{name}-{}",
            uuid::Uuid::new_v4()
        ));
        fs::create_dir(&path).unwrap();
        path
    }

    #[test]
    fn init_separates_project_defaults_from_credentials() {
        let directory = temporary("init");
        init_in(
            &directory,
            Some("http://flag.example"),
            Some("flag-secret"),
            Some("http://environment.example"),
            Some("environment-secret"),
            false,
        )
        .unwrap();
        let project = fs::read_to_string(directory.join(PROJECT_FILE)).unwrap();
        assert!(project.contains("http://flag.example"));
        assert!(!project.contains("secret"));
        let local = fs::read_to_string(directory.join(STATE_DIR).join(LOCAL_FILE)).unwrap();
        assert!(local.contains("flag-secret"));
        assert_eq!(
            fs::read_to_string(directory.join(".gitignore")).unwrap(),
            ".duckfleet/\n"
        );
        init_in(&directory, None, None, None, None, false).unwrap_err();
        init_in(&directory, None, None, None, None, true).unwrap();
        assert_eq!(
            fs::read_to_string(directory.join(".gitignore"))
                .unwrap()
                .matches(".duckfleet/")
                .count(),
            1
        );
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                fs::metadata(directory.join(STATE_DIR).join(LOCAL_FILE))
                    .unwrap()
                    .permissions()
                    .mode()
                    & 0o777,
                0o600
            );
            assert_eq!(
                fs::metadata(directory.join(STATE_DIR))
                    .unwrap()
                    .permissions()
                    .mode()
                    & 0o777,
                0o700
            );
        }
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn precedence_is_flag_environment_then_local_or_project() {
        let directory = temporary("precedence");
        init_in(
            &directory,
            Some("http://project.example"),
            Some("local-secret"),
            None,
            None,
            false,
        )
        .unwrap();
        let resolved = resolve_from(
            &directory,
            None,
            None,
            Some("http://environment.example"),
            Some("environment-secret"),
        )
        .unwrap();
        assert_eq!(resolved.url.as_deref(), Some("http://environment.example"));
        assert_eq!(resolved.token.as_deref(), Some("environment-secret"));
        assert_eq!(resolved.url_source, "environment");
        assert_eq!(resolved.token_source, "environment");
        let resolved = resolve_from(
            &directory,
            Some("http://flag.example"),
            Some("flag-secret"),
            Some("http://environment.example"),
            Some("environment-secret"),
        )
        .unwrap();
        assert_eq!(resolved.url.as_deref(), Some("http://flag.example"));
        assert_eq!(resolved.token.as_deref(), Some("flag-secret"));
        let resolved = resolve_from(&directory, None, None, None, None).unwrap();
        assert_eq!(resolved.url.as_deref(), Some("http://project.example"));
        assert_eq!(resolved.token.as_deref(), Some("local-secret"));
        let rendered = resolved.redacted().to_string();
        assert!(!rendered.contains("local-secret"));
        assert!(rendered.contains("<redacted>"));
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn child_directories_find_project_and_malformed_json_fails() {
        let directory = temporary("discovery");
        let child = directory.join("child");
        fs::create_dir(&child).unwrap();
        init_in(
            &directory,
            Some("http://project.example"),
            None,
            None,
            None,
            false,
        )
        .unwrap();
        let resolved = resolve_from(&child, None, None, None, None).unwrap();
        assert_eq!(resolved.url.as_deref(), Some("http://project.example"));
        fs::write(directory.join(PROJECT_FILE), b"{broken").unwrap();
        assert!(resolve_from(&child, None, None, None, None).is_err());
        fs::remove_dir_all(directory).unwrap();
    }
}
