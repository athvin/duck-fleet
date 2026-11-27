//! `duckfleet ui` in the server binary. The admin console ships only in the
//! native `duckfleet` CLI, which embeds the web bundle and proxies it to a
//! gateway with the CLI's credential; the server image carries no web bundle.

pub(crate) const DEFAULT_PORT: u16 = 7878;

pub(crate) struct Options {
    pub url: String,
    pub token: String,
    pub port: u16,
}

pub(crate) async fn run(options: Options) -> i32 {
    let Options { url, token, port } = options;
    let _ = (url, token, port);
    eprintln!("duckfleet ui is available in the native duckfleet CLI, not the server binary");
    2
}
