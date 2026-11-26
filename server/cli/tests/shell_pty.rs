#![cfg(unix)]

use rexpect::session::{PtySession, spawn_command};
use std::process::Command;

fn spawn_shell(extra: &str) -> PtySession {
    let binary = env!("CARGO_BIN_EXE_duckfleet");
    let url = std::env::var("DUCKFLEET_URL").unwrap_or_else(|_| "http://127.0.0.1:18080".into());
    let token = std::env::var("DUCKFLEET_TOKEN")
        .unwrap_or_else(|_| "duckfleet-local-development-token".into());
    let mut command = Command::new(binary);
    command
        .args(["shell", "--url", &url, "--token", &token])
        .args(extra.split_whitespace())
        .env("TERM", "xterm-256color");
    spawn_command(command, Some(20_000)).unwrap()
}

fn statement(session: &mut PtySession, sql: &str, prompt: &str) {
    session.send_line(&format!("{sql};")).unwrap();
    session.exp_string("request_id:").unwrap();
    session.exp_string(prompt).unwrap();
}

#[test]
#[ignore = "requires the real Compose stack"]
fn persistent_multiline_shell_and_active_cancellation() {
    let mut shell = spawn_shell("--no-history");
    shell.exp_string("duckfleet> ").unwrap();
    shell.send("select ").unwrap();
    shell.flush().unwrap();
    std::thread::sleep(std::time::Duration::from_millis(200));
    shell.send_control('c').unwrap();
    shell.exp_string("duckfleet> ").unwrap();
    statement(
        &mut shell,
        "create temporary table shell_pty (id integer, note varchar)",
        "duckfleet> ",
    );
    statement(&mut shell, "begin", "duckfleet(tx)> ");
    statement(
        &mut shell,
        "insert into shell_pty values (1, 'rolled-back')",
        "duckfleet(tx)> ",
    );
    statement(&mut shell, "rollback", "duckfleet> ");
    statement(
        &mut shell,
        "insert into shell_pty values (2, 'semi;colon')",
        "duckfleet> ",
    );
    shell.send_line("select").unwrap();
    shell.exp_string("...> ").unwrap();
    shell.send_line("* from shell_pty;").unwrap();
    shell.exp_string("semi;colon").unwrap();
    shell.exp_string("duckfleet> ").unwrap();

    shell.send_line("\\format vertical").unwrap();
    shell.exp_string("format vertical").unwrap();
    statement(&mut shell, "select * from shell_pty", "duckfleet> ");
    shell.send_line("\\timing").unwrap();
    shell.exp_string("timing on").unwrap();
    shell.send_line("select * from shell_pty;").unwrap();
    shell.exp_string("client_total_ms:").unwrap();
    shell.exp_string("server_elapsed_ms:").unwrap();
    shell.exp_string("duckfleet> ").unwrap();
    shell.send_line("\\dt").unwrap();
    shell.exp_string("table_schema").unwrap();
    shell.exp_string("duckfleet> ").unwrap();
    shell.send_line("\\d shell_pty").unwrap();
    shell.exp_string("column_name").unwrap();
    shell.exp_string("duckfleet> ").unwrap();

    shell
        .send_line("select sum(sin(i::double)) from range(100000000000) t(i);")
        .unwrap();
    shell.exp_string("request_id:").unwrap();
    shell.send_control('c').unwrap();
    shell.exp_string("canceling request_id:").unwrap();
    shell.exp_string("cancellation requested for").unwrap();
    shell.exp_string("duckfleet> ").unwrap();
    shell.send_line("select 42 as after_cancel;").unwrap();
    shell.exp_string("after_cancel").unwrap();
    shell.exp_string("42").unwrap();
    shell.exp_string("duckfleet> ").unwrap();
    shell.send_line("\\quit").unwrap();
    shell.exp_eof().unwrap();
}

#[test]
#[ignore = "requires the real Compose stack"]
fn history_is_private_persistent_and_filters_credentials() {
    let directory =
        std::env::temp_dir().join(format!("duckfleet-shell-history-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir(&directory).unwrap();
    std::fs::write(
        directory.join("duckfleet.json"),
        b"{\"version\":1,\"url\":\"http://127.0.0.1:18080\"}\n",
    )
    .unwrap();
    let binary = env!("CARGO_BIN_EXE_duckfleet");
    let token = std::env::var("DUCKFLEET_TOKEN")
        .unwrap_or_else(|_| "duckfleet-local-development-token".into());
    let spawn_history = || {
        let mut command = Command::new(binary);
        command
            .current_dir(&directory)
            .args(["shell", "--token", &token])
            .env("TERM", "xterm-256color");
        spawn_command(command, Some(20_000)).unwrap()
    };
    let mut shell = spawn_history();
    shell.exp_string("duckfleet> ").unwrap();
    statement(&mut shell, "select 1 as retained_history", "duckfleet> ");
    statement(
        &mut shell,
        "select 'password' as filtered_history",
        "duckfleet> ",
    );
    shell.send_line("\\quit").unwrap();
    shell.exp_eof().unwrap();
    let history_path = directory.join(".duckfleet/shell_history");
    let history = std::fs::read_to_string(&history_path).unwrap();
    assert!(history.contains("retained_history"));
    assert!(!history.contains("password"));
    use std::os::unix::fs::PermissionsExt;
    assert_eq!(
        std::fs::metadata(&history_path)
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o600
    );
    let mut restarted = spawn_history();
    restarted.exp_string("duckfleet> ").unwrap();
    restarted.send_line("\\quit").unwrap();
    restarted.exp_eof().unwrap();
    std::fs::remove_dir_all(directory).unwrap();
}
