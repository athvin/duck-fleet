# Native DuckFleet CLI

The `duckfleet` executable speaks the existing HTTP/Arrow API. It does not embed DuckDB and does not require Cargo, Docker, Python, or dbt to install or run.

## Install

With Homebrew on macOS arm64, Linux x86_64, or Linux aarch64:

```sh
brew install athvin/duckfleet/duckfleet
duckfleet version
```

To install a release archive directly, download the asset for your platform from the [DuckFleet releases page](https://github.com/athvin/duck-fleet/releases), then verify it against `SHA256SUMS` before extracting it:

```sh
curl -fLO https://github.com/athvin/duck-fleet/releases/download/duckfleet-v0.1.0/duckfleet-0.1.0-aarch64-apple-darwin.tar.gz
curl -fLO https://github.com/athvin/duck-fleet/releases/download/duckfleet-v0.1.0/SHA256SUMS
shasum -a 256 -c SHA256SUMS --ignore-missing
tar -xzf duckfleet-0.1.0-aarch64-apple-darwin.tar.gz
install -m 0755 duckfleet-0.1.0-aarch64-apple-darwin/duckfleet /usr/local/bin/duckfleet
duckfleet version
```

Use `sha256sum -c SHA256SUMS --ignore-missing` on Linux. Replace the archive and target directory with the names for your platform and installation prefix.

## Initialize a project

Start the supported local stack with `make up`, then initialize the directory in which you will run DuckFleet commands:

```sh
export DUCKFLEET_TOKEN=duckfleet-local-development-token
duckfleet init
duckfleet sql 'select 42'
duckfleet history
duckfleet workers
duckfleet pools
```

`init` creates two kinds of configuration:

- `duckfleet.json` contains versioned, non-secret project defaults and is safe to commit. The default URL is `http://127.0.0.1:18080`.
- `.duckfleet/config.json` contains local protected state. If `DUCKFLEET_TOKEN` or `--token` is present during `init`, the token is stored here rather than in `duckfleet.json`. The directory and file have user-only permissions on macOS and Linux.

The command adds `.duckfleet/` to `.gitignore` exactly once. Existing configuration is not overwritten unless `init --force` is explicit. Without `--force`, an existing local credential file is preserved. You can keep the token in `DUCKFLEET_TOKEN` instead of storing it locally; pass it to `init` only when persistent project-local credentials are wanted.

Run `duckfleet config show` to inspect the resolved URL, configuration location, and value sources. The token is always shown as `<redacted>` and is never included in command output by `init` or `config show`.

Flags override environment variables, which override project defaults:

1. `--url` and `--token`
2. `DUCKFLEET_URL` and `DUCKFLEET_TOKEN`
3. `duckfleet.json` and `.duckfleet/config.json`

Commands discover `duckfleet.json` in the current directory or its nearest ancestor. Malformed JSON fails before any network request. Existing scripts that set `DUCKFLEET_URL` and `DUCKFLEET_TOKEN` continue to work without project files.

## Source-based development

The native crate reuses the command implementation in `server/src/cli.rs` and HTTP/Arrow transport in `server/src/client.rs`; it is not a second client implementation.

```sh
cd server
cargo run -p duckfleet-cli -- version
cargo test -p duckfleet-cli
```

The Docker Compose `api` service and `DUCKFLEET_ROLE=cli duckfleet-server` role remain available for existing development and harness workflows.

## Releases

A tag named `duckfleet-v<version>` runs the native CLI release workflow. Each platform job builds and packages the binary, extracts the archive, and runs `version`, `init`, and redaction checks against that extracted executable. The release job verifies and publishes `SHA256SUMS` and generates a Homebrew formula containing the same checksums.
