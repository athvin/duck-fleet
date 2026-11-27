#[path = "../../src/advise_cli.rs"]
mod advise_cli;
#[path = "../../src/advise_dbt.rs"]
mod advise_dbt;
#[path = "../../src/catalog.rs"]
mod catalog;
#[path = "../../src/cli.rs"]
mod cli;
#[path = "../../src/cli_config.rs"]
mod cli_config;
#[allow(dead_code)]
#[path = "../../src/client.rs"]
mod client;
#[path = "../../src/dev.rs"]
mod dev;
#[path = "../../src/export_cli.rs"]
mod export_cli;
#[path = "../../src/history_cli.rs"]
mod history_cli;
#[path = "../../src/import_cli.rs"]
mod import_cli;
mod protocol;
#[path = "../../src/shell.rs"]
mod shell;
#[path = "../../src/snapshot_cli.rs"]
mod snapshot_cli;

mod credentials {
    pub(crate) use crate::protocol::valid_principal;
}
mod metadata {
    pub(crate) use crate::protocol::find_relation;
}
mod query {
    pub(crate) const DEFAULT_QUEUE_TIMEOUT: u64 = duckfleet::protocol::DEFAULT_QUEUE_TIMEOUT;
}

#[tokio::main]
async fn main() {
    if rustls::crypto::ring::default_provider()
        .install_default()
        .is_err()
    {
        eprintln!("a rustls crypto provider was already installed");
        std::process::exit(1);
    }
    let args: Vec<String> = std::env::args().skip(1).collect();
    std::process::exit(cli::run(args).await);
}
