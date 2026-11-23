# Supported single-host installation

DuckFleet supports Linux `x86_64` and `aarch64` hosts running Docker Engine with
Compose v2 and cgroup v2. The worker requires CPU and memory cgroup limits; a
runtime that hides or ignores those limits is refused. Neither dbt nor Kubernetes
is installed on the host or required for this profile. The initial build downloads
Rust, Debian/Ubuntu, PostgreSQL, RustFS, DuckDB 2.0.1, its pinned extensions and
the native Rust dbt test binary. Subsequent starts reuse those images.

One small worker needs 1 CPU, 1 GiB memory and 2 GiB spill disk. Allow another
1 GiB for the gateway, PostgreSQL, object store and build overhead. A single host
is a single availability zone and a single PostgreSQL/object-store failure domain;
this profile is not HA. Remote access must terminate TLS at a reverse proxy before
forwarding to the loopback API; the profile deliberately exposes no public port.

## Install and authenticate

From a source checkout on the host:

```sh
bash scripts/install-single-host.sh
```

The script requires Docker and OpenSSL, creates `.duckfleet/single-host.env` with
independent random credentials, sets directory/file modes to 700/600, and starts
the API on `127.0.0.1:18080`. It preserves that file on subsequent runs. Do not
copy the development passwords into it or commit it. Read `DUCKFLEET_TOKEN` from
that root-only file when authenticating; use a separate issued principal for
ordinary users. `DUCKFLEET_CONFIG_HOME` relocates the protected configuration.

The API workflow is exercised without dbt by `make test`; its dbt-free image opens
a session, creates and queries a table, inspects history and repeats the workflow
after restart. For a smoke query, use the Rust CLI from `server/`:

```sh
set -a; source .duckfleet/single-host.env; set +a
export DUCKFLEET_URL=http://127.0.0.1:18080
cargo run --locked -- sql 'create table if not exists analytics.smoke(id integer)'
cargo run --locked -- sql 'insert into analytics.smoke values (1)'
cargo run --locked -- sql 'select * from analytics.smoke'
curl -fsS -H "Authorization: Bearer $DUCKFLEET_TOKEN" \
  "$DUCKFLEET_URL/v1/queries"
```

Rust is only needed to invoke the CLI from source, not to run the installation.

## Capacity and storage

`compose.yaml` exposes the persistent worker's CPU, memory, spill disk, queue,
retention and profiling settings. Use a Compose override file rather than editing
secrets into the tracked file. Additional named pools are available with the
`pools` profile and `DUCKFLEET_POOLS_DIRECTORY`; `examples/pools/compose.json` is
the validated example. Resource changes replace workers, so drain sessions first.

For external PostgreSQL or S3-compatible object storage, replace the corresponding
services and supply their DSN/endpoint and credentials through a root-only Compose
environment file. Preserve separate catalog and platform credentials. The bundled
PostgreSQL and RustFS each have one container and no independent HA or off-host
backup. `DUCKFLEET_HISTORY_RETENTION_DAYS` controls history retention; usage
billing evidence is not deleted with query history.

## Upgrade, backup, restore and rollback

Back up both PostgreSQL databases and the object-store data before upgrading:

```sh
set -a; source .duckfleet/single-host.env; set +a
bash scripts/backup-catalog.sh compose .duckfleet/backups/$(date -u +%Y-%m-%d)
bash scripts/stop-compute.sh compose
```

Pull the intended revision and run `bash scripts/migrate-platform.sh compose`,
then start with the installation command. Test the upgrade against a disposable
Compose project and copied volumes first. Restore with
`bash scripts/restore-catalog.sh compose <backup-directory>` while compute is
stopped; it preserves lake data, credentials, history and usage evidence. Roll
back the source/images only after restoring a backup compatible with that older
schema. Never start an older binary over a newer platform schema.

Measure capacity on your own hardware. The reproducible method is `make test`,
which writes `artifacts/cold-start.json` for the same Compose worker sizes and
records persistent, isolated-cold and warm-node open timings where supported.
Isolated startup is unavailable in this single-host profile. Idle container usage
can be sampled with `docker stats --no-stream` after the stack is ready and before
any session opens; record host CPU, memory, Docker version and sample interval.
These are local measurements, not a universal cost claim.

On 2026-11-23, an Apple `arm64` host running Docker 28.3.3 reported 196.9 MiB
combined RSS for the four long-lived containers after the restart workflow
(7.7 MiB gateway, 30.9 MiB worker, 65.4 MiB PostgreSQL, 92.9 MiB RustFS). This
was one `docker stats --no-stream` sample on a shared Docker VM, not a controlled
CPU benchmark. The same installation completed the dbt-free create/query/history
workflow before and after restart with independently generated credentials. A
subsequent warm seven-statement create/insert/query/history workflow, including
migration and service waits, took 13.02 seconds end to end.
