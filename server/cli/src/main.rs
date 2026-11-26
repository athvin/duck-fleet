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
#[path = "../../src/history_cli.rs"]
mod history_cli;
#[path = "../../src/import_cli.rs"]
mod import_cli;
mod protocol;
#[path = "../../src/shell.rs"]
mod shell;

mod artifacts {
    pub(crate) use crate::protocol::{
        MAX_CHUNK_BYTES, MAX_MANIFEST_BYTES, Manifest, ManifestChunk,
    };
}
mod compute {
    pub(crate) use crate::protocol::ComputeRequest;
}
mod credentials {
    pub(crate) use crate::protocol::valid_principal;
}
mod job_policy {
    pub(crate) use crate::protocol::{JobLimits, JobRequest, JobResultRequest, ResultFormat};
}
mod metadata {
    pub(crate) use crate::protocol::find_relation;
}
mod query {
    pub(crate) const DEFAULT_QUEUE_TIMEOUT: u64 = crate::protocol::DEFAULT_QUEUE_TIMEOUT;
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
