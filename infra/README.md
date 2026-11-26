# DuckFleet AWS development harness

Status: **tested on AWS.** The AWS development harness — publish, deploy, live
dbt run, suspend — passed on main for `5808e99` after the platform bootstrap.
Docker and mock-provider tests remain the credential-free pull-request gates;
only main touches AWS.

The deploy target is configuration in `scripts/aws/target.env`, read by operator
scripts and CI. Set `DUCKFLEET_CLUSTER` and `DUCKFLEET_KUBE_CONTEXT` explicitly for
local cluster operations; `KUBE_CONTEXT` is also accepted. Select an AWS profile
with `AWS_PROFILE` or `DUCKFLEET_AWS_PROFILE`, or use the normal AWS credential chain.
There is no default cluster, context, or operator profile. In GitHub Actions, set
the repository variable `DUCKFLEET_CLUSTER`; CI creates its own explicit context.
Missing cluster configuration fails before cluster operations. These settings
select an existing cluster; they do not provision or migrate one.

The development lane uses the namespace `duckfleet-dev`, account `797781631727`,
and region `us-east-2`. DuckFleet owns its S3 bucket, image repositories, Terraform
state bucket, and dedicated node pool. On a shared EKS Auto Mode cluster, the
control plane, networking, NodeClass, and `gp3` StorageClass remain operator-owned.
The ownership boundary supplies pod-create RBAC and capacity for 2-CPU workers
(#39). Shared-cluster upgrades and outages still affect this lane; the dedicated
cluster module below is an optional alternative.

| Boundary | DuckFleet resource |
| --- | --- |
| Terraform state | the bucket and key named in `scripts/aws/target.env`, with S3 locking |
| Kubernetes | `duckfleet-dev`, restricted Pod Security, resource quota |
| Object storage | `duckfleet-dev-797781631727-us-east-2`, private, TLS, encryption, versioning |
| Worker IAM | `duckfleet-dev-worker`, one Pod Identity association for `duckfleet-dev/duckfleet-worker` |
| Registry | `duckfleet-dev/worker`, `duckfleet-dev/dbt`, immutable tags/digests |
| Metadata + history | dedicated PostgreSQL StatefulSet, separate `lake` and `platform` databases, 10 GiB encrypted gp3 PVC; the chart's `postgres.bundled: false` swaps it for an operator-run server |
| API | private ClusterIP; no ingress, public IP, DNS or load balancer — the chart's opt-in `exposure` values stay off here, and the render checks prove the default produces no Ingress |

The smallest test uses two **prestarted pods**, 1 CPU / 1 GiB and 2 CPU / 2 GiB,
with DuckDB limits of 1 thread / 512 MiB and 2 threads / 1 GiB. The adapter routes
whole model lifecycles to these workers. They may share an EC2 node. This does
**not** test dynamic EC2 instance creation, dedicated nodes, or 32 CPU workers.
PostgreSQL is dedicated to this disposable experiment.
This is not a production database/backup/HA design.

The account, bucket, and region above are *this* lane's, not the chart's. The Helm
chart (`infra/helm/duckfleet`) takes `workerImage`, `dbtImage`, `bucket`, `region`,
and `secretName` as values whose schema pins only their shape — a digest-pinned
image, a legal S3 bucket name, a real region — so another organization installs it
against its own account by supplying its own. The development lane's specific
values are pinned where that pin belongs: once, in `scripts/aws/target.env`, built
into the images and the deploy, and asserted by `the Rust `aws-ci-test` suite`. The chart
itself names no account. `values-aws.yaml` supplies this lane's bucket, region,
secret, `gp3` class and EKS placement explicitly; `deploy.sh` always applies that
overlay before the generated immutable image values. Generic renders use the
provider-neutral defaults and retain only the Linux OS selector.

## Back up the catalog

The catalog is the lake: the bucket's Parquet is unreachable without the rows that
name it. The same script the harness drills runs against this namespace:

```sh
DUCKFLEET_KUBE_CONTEXT=$DUCKFLEET_KUBE_CONTEXT DUCKFLEET_NAMESPACE=duckfleet-dev \
  bash scripts/backup-catalog.sh kube backups/$(date -u +%Y-%m-%d)
```

Dumps both databases as custom-format archives. Restore with
`scripts/restore-catalog.sh kube` into a freshly initialised PostgreSQL, with
compute stopped around it; the README's "Backup and restore" section states the
recovery-point semantics, which the Compose harness proves on every run. How often
this runs, and how many directories are kept, is the operator's policy — nothing
schedules it automatically.

## Named persistent pools

The default remains one `small` pool at 1 CPU, 1Gi memory and 2Gi spill disk.
Resource-only requests still select isolated compute. To keep a larger allocation
warm, configure named pools and select one through HTTP or the CLI:

```yaml
defaultPool: small
persistentPools:
  small: {cpu: 1, memory: 1Gi, disk: 2Gi, workers: 1}
  interactive:
    cpu: 2
    memory: 3Gi
    disk: 4Gi
    workers: 2
    maxSessionsPerWorker: 32
```

The chart builds each worker and the gateway's `duckfleet-pools` ConfigMap from the
same definitions. CPU, memory and disk are independent. Each worker's spill volume
and ephemeral-storage request match `disk`; its pod limit includes another 1Gi
for container overhead. DuckDB's memory and spill headroom follow the existing
worker rules. A pool with `workers: 0` remains configured and discoverable while
new sessions receive an availability refusal.

`persistentPools: {}` uses the legacy `workerReplicas` input, whose empty map means
one small worker. Do not combine named pools with a nonempty `workerReplicas` map.
When adopting pools with `helm upgrade --reuse-values`, clear the inherited map
explicitly with `--set workerReplicas=null`; an empty override map alone may merge
with the previous values. The `small` pool retains worker-small, worker-small-1,
and their existing recovery identities and immutable selectors. Other pools use
worker-pool-<name>-<index> and stable pool-<name>-<index> recovery IDs, so a pool
named small-1 cannot collide with the small pool's second worker. Pool names use
lowercase DNS labels, start with a letter, and are at most 40 characters.

Named configurations mount `/etc/duckfleet-pools/pools.json` using
`DUCKFLEET_POOLS_FILE`. The ConfigMap has no gateway pod-template checksum:
updating it does not restart the gateway or move open sessions. After its directory
projection updates, the bootstrap operator applies it with
`POST /v1/pools/reload` and checks `GET /v1/pools`. Invalid reloads preserve the
preceding configuration. Existing sessions retain their selected worker and
allocation; new opens use the updated policy. Switching between legacy and named
configuration changes the pod template and restarts the gateway. Changing a
worker's allocation or session cap replaces that worker too, so drain sessions before either
operation. Remove a persistent identity only after its reservations are settled;
restarting its original stable identity is the recovery path after an abrupt loss.

A non-Helm gateway can use `DUCKFLEET_POOLS_JSON` for immutable inline configuration
instead of `DUCKFLEET_POOLS_FILE`; the two sources are mutually exclusive. With
neither source it uses the historical `DUCKFLEET_WORKER_URLS` small pool. Pool
membership is routing configuration, not data isolation or a separate budget.
Pool `authority` is `writer` by default; `reader` uses the `duckfleet-reader`
ServiceAccount and only `CATALOG_READER_DSN`. Provision the distinct reader role
and Pod Identity before adding reader pools. The cluster Terraform's reader IAM
allows `GetObject` and prefix-bounded `ListBucket`, never object writes or result
writes. Existing writer pools and isolated sessions are unchanged. A reader
request for isolated compute rewrites the pod to the reader ServiceAccount and
catalog secret, and removes result-write environment values.

The current pinned dbt adapter supports its existing resource fields but silently
drops pool/mode fields; use HTTP or the CLI for explicit pool selection until the
coordinated adapter release described in the root README is available.

`computePolicy` renders `DUCKFLEET_COMPUTE_POLICY_JSON` on the gateway and every
worker. Its optional deployment/principal CPU, memory and disk ceilings are
checked before isolated provisioning; `idle_timeout_seconds` and
`max_lifetime_seconds` bound idle and total worker lifetime. Empty retains the
fixed-capacity default. `/v1/compute` reports lifecycle usage and rate-card
estimates separately from query billing.

`workerMaxSessions` defaults to 32; a named pool's `maxSessionsPerWorker` overrides
it (1–1024). `workerQueueCapacity` defaults to 64 queued worker operations and
`gatewayMaxSessions` to 64 backend sessions (both 1–4096). More open sessions do
not make one worker execute SQL in parallel. Isolated workers retain their
one-session lifetime regardless of persistent-pool limits.

Durable waiting SQL has separate bounds: `workerMaxQueuedQueries` defaults to 64
(1–4096) and `workerMaxQueuedBytes` to 16777216 bytes (1–256MiB). These render
`DUCKFLEET_WORKER_MAX_QUEUED_QUERIES` and `DUCKFLEET_WORKER_MAX_QUEUED_BYTES`;
`workerQueueCapacity` still bounds ordinary worker operations. One worker executes
one statement at a time. Queue count/bytes, session limits, and shared admission
limits all apply; the first exhausted limit refuses further admission.

Configure shared pool/principal admission through the gateway:

```yaml
admissionLimits:
  default:
    maxQueuedQueries: 256
    maxRunningQueries: 16
    maxQueuedPerPrincipal: 16
  isolated:
    maxRunningQueries: 4
persistentPools:
  small: {cpu: 1, memory: 1Gi, disk: 2Gi, workers: 1}
  interactive:
    cpu: 2
    memory: 3Gi
    disk: 4Gi
    workers: 2
    admission: {maxRunningQueries: 2, maxRunningPerPrincipal: 1}
```

Queued limits accept 1–65535, running limits 1–1024; principal limits cannot exceed
their pool limits. An omitted `maxRunningPerPrincipal` inherits the pool's effective
running limit. Explicit global defaults remain in effect unless a pool overrides
them. The chart translates these fields into snake_case JSON under the gateway's
`DUCKFLEET_ADMISSION_LIMITS_JSON` (`default`/`isolated`) and each named pool's
optional `admission` object. Environment defaults load at process startup; registry
file reload changes pool overrides. PostgreSQL enforces caps across workers and
credentials for the same principal. Scope keys are `persistent:<pool>` and
`isolated`; changing registry revision never creates another allowance.

The gateway publishes validated policy before serving, without waiting for worker
readiness. Named workers report unavailable until policy exists; they cannot invent
defaults. Compose therefore starts gateways after workers start, without waiting
for their health, while retaining migration completion dependencies. Lowering caps
affects future admission/selection and preserves accepted work. Removed pools keep
their policy while existing sessions may need it. Worker-local bounds require a
worker rollout; shared policy changes use the existing explicit registry reload.

Each request's `queue_timeout_seconds` defaults to 10 (1–120), independently of its
1–60 second execution timeout. The synchronous transport still times out at 75 seconds;
a timeout/disconnect does not cancel durable work. Inspect the same request ID.
During drain, queued/preparing requests are refused as `queue_drained` without SQL
and with `retry_safe=false`; only already executing work finishes within the
existing grace. Cancellation and cleanup retain independent control capacity.

Compose provides an optional two-pool example without changing its normal server:

```sh
docker compose --profile pools up -d --wait server-pools
# Named gateway: http://localhost:18081; normal gateway remains on 18080.
```

This starts the default small worker and an interactive worker at 2 CPU, 3Gi and
4Gi disk. `examples/pools/compose.json` supplies the registry. Override
`DUCKFLEET_POOLS_DIRECTORY` to mount another directory containing `compose.json`;
mounting the directory allows atomic file replacement to be seen on reload.
The local acceptance script uses a copy in `artifacts/pools-config` and leaves the
tracked example unchanged. Stop the optional allocation with
`docker compose --profile pools stop server-pools worker-pool-interactive-0`.

## Durable jobs and private results

Compose enables job results using its local RustFS credentials. The Helm chart
keeps `results.enabled: false` until the operator supplies result-prefix access
for both gateways and workers. Enable it with the reviewed overlay in
[`examples/jobs-values.yaml`](../examples/jobs-values.yaml). Standalone processes
can use [`examples/jobs.env`](../examples/jobs.env) as a configuration reference;
the placeholder credentials must be replaced and must not be committed.

Result objects live under `s3://<bucket>/<results.prefix>/`, separate from
`lakePrefix`. Neither prefix may contain the other: DuckLake cleanup must never
own result objects. Gateways receive the lake path only for this overlap check,
through `DUCKFLEET_RESULT_LAKE_DATA_PATH`; they receive no lake credentials. The
gateway needs `s3:GetObject`, `s3:PutObject`, `s3:DeleteObject`, and
`s3:AbortMultipartUpload` on the result prefix for result retrieval, import
uploads, and cleanup. Workers need `s3:GetObject` and `s3:PutObject` there in
addition to their existing lake access. Neither role needs result-prefix listing.
Clients retrieve authenticated result chunks through the gateway; responses
contain no result bucket keys or presigned URLs. Import responses contain an
unsigned server-selected staging URL, never storage credentials.

The cluster Terraform module adds a result-only IAM role and Pod Identity
associations for `duckfleet-results` and `duckfleet-gateway`. A gateway without
isolated compute uses the former service account without a Kubernetes API token;
an isolated-enabled gateway keeps the latter. Workers keep `duckfleet-worker`.
Apply the reviewed IAM changes before enabling results. Set Terraform
`lake_prefix`/`results_prefix` and chart `lakePrefix`/`results.prefix` consistently.
Team namespace provisioning selects `<namespace>/lake` and
`<namespace>/results`; its operator must install matching per-namespace IAM and
Pod Identity associations. The primary namespace's associations do not cover it.

AWS uses `DUCKFLEET_RESULT_AUTH_MODE=pod_identity` and the explicit EKS container
credential endpoint/token injected by Pod Identity. An absent or malformed
identity fails configuration; result storage does not fall back to a node role.
Local and other S3-compatible stores use `static` with explicit result credentials.
Custom HTTP endpoints require `DUCKFLEET_RESULT_ALLOW_HTTP=true`; deployed AWS
configuration uses HTTPS. The result path and credentials must agree across
gateways and workers before submitting jobs.

`jobPolicy` maps directly to `DUCKFLEET_JOB_POLICY_JSON`. Its optional `ceilings`
and `isolated` fields, and each named pool's optional `jobLimits`, are complete
envelopes with `runtime_seconds`, `result_bytes`, `result_rows`, and
`retention_seconds`. The hard maximum is 3600 seconds, 1 GiB, 10 million rows,
and 24 hours. Defaults are 300 seconds and one-hour retention; omitted options
clip to the current matching principal, pool, and operator limits. A principal
compute grant without `job_limits` keeps a 60-second runtime and one-hour
retention. Raising only the operator or pool envelope never raises that principal
grant. Explicit over-limit requests are refused. Pools preserve their own lower
envelopes; changing global defaults does not rewrite accepted jobs or replay hashes.

Job queue time defaults to 120 seconds and accepts explicit values from 1 to
3600 seconds. Its original deadline is durable; reconnecting or retrying the
same ID does not extend it. Interactive queries retain their separate 30-second
execution default, 600-second maximum, and 10-second queue default.

An isolated pod's `maxSessionSeconds` remains an operator-owned lifetime limit.
Provisioning refuses a job whose runtime plus the completion allowance cannot
fit that lifetime. Startup and queuing still consume pod lifetime. Persistent
workers drain for 615 seconds and Kubernetes grants 630 seconds; longer jobs can
be interrupted during rollout or shutdown. Neither a maximum execution allowance
nor durable acceptance promises survival beyond the pod's deadline or grace.
No SQL is replayed to recover from a lost worker.

PostgreSQL controls result expiry immediately. Cleanup deletes only journaled
objects under the private prefix; temporary object-store failures are retried
without rerunning SQL. The development bucket adds a two-day result-only lifecycle
backstop, including noncurrent versions, and a one-day abandoned multipart rule.
Custom/team prefixes need corresponding lifecycle rules. Do not apply result
expiration to `lake/`: DuckLake owns its data-file retention. Backups of the
platform database retain result metadata but do not replace object-store backup
or extend an expired result's authorization window.

These are platform and object-prefix permissions for trusted SQL executors.
They do not isolate a hostile SQL process from the worker's environment or
credentials; that separate execution boundary is tracked in #106.

## Platform schema and maintenance

The release runs a PostgreSQL-only migration Job before compute can become ready.
Workers validate the schema and recover their own exclusive identities; global
retention and budget reconciliation run through the existing gateway. See the
root README's "Platform migrations and maintenance" section for their bounds,
manual commands and operational tables. This is separate from DuckLake data
compaction and snapshot expiry below.

Schema 5 adds job identity/options, binding and artifact journal state plus finite
job admission policy. Migration preserves old request fingerprints and retained
accounting; it does not refill budgets or restore revoked grants. Full SQL,
parameters and application context are not copied into retained job option rows.
Repeated current-version migration remains read-only.

`platformMigration.runId` changes the migration Job identity independently of the
image digest. Deploy and namespace-provisioning helpers supply a fresh run ID.
After restoring or retargeting `PLATFORM_DSN`, explicitly rerun migration with
`scripts/migrate-platform.sh kube` or a new run ID before restarting compute.
The migration uses the `duckfleet-no-aws` service account, has no mounted API token,
and receives no object-store credentials. Failed Jobs prevent rollout success;
inspect their logs to diagnose the schema or database problem.

Schema 5 requires a quiesced upgrade from any older schema: all old gateways, persistent
workers, isolated workers, and any separately managed direct workers must stop
before migration. A per-Deployment Recreate rollout is not a fleet-wide barrier.
The deploy/provision/manual migration helpers perform a read-only preflight and
refuse a schema-changing apply while old compute remains. Fresh installations and
same-schema rollouts continue normally. Runtime schema checks remain authoritative;
the chart's schema annotations only identify deployment intent.

Use the explicit reversible operator command:

```sh
DUCKFLEET_KUBE_CONTEXT=<context> DUCKFLEET_NAMESPACE=<namespace> \
  scripts/stop-compute.sh kube
# Or, for the current Compose project:
scripts/stop-compute.sh compose
```

The command stops gateways first, then all named/sibling workers and isolated Pods,
respecting execution grace. It preserves PostgreSQL, Secrets, volumes, object
storage, Deployments and the current migration Job template. A timeout exits with
an error and never migrates or restarts anything; complete shutdown before proceeding.
Do not use
`suspended:true` for this flow: suspension also stops bundled PostgreSQL and removes
the migration Job.

After stopping compute, rerun administrator database provisioning. Schema 4 requires
the gateway role to have a finite `temp_file_limit` of at most 16MB; the bundled
provisioner sets 16MB, and an external database administrator must apply the same
setting. The gateway cannot raise this setting. Then apply the schema 6 chart/image with a fresh
`platformMigration.runId` and wait for migration/rollout; new compute cannot become
usable before schema 6, its runtime grants, and its admission policy exist. For Compose, build/pull the
desired image, run `scripts/migrate-platform.sh compose`, then explicitly start
the desired services. Bare Helm/template-apply users must follow the same stop/apply
sequence. Do not automatically roll back the binary after migration commits.

Restore also uses this stop flow, restores both databases under their dedicated
owners with backup ACLs discarded, runs a fresh migration and explicit
`platform-grants` repair, and leaves compute stopped for explicit restart. A schema 1–4 backup is upgraded to 5
by the current image. `migrate-platform.sh kube` requires the current schema 6 Job
template; cloning an old schema 1–4 image cannot perform this upgrade. Apply the
desired chart only after old compute has stopped. Same-image DB reset/retarget
still requires a fresh Job/runId; prior completion does not prove the new database.

Successful deployment helpers prune superseded successful migration Jobs and their
completed Pods only after the current migration and workloads are ready. They
preserve current references, unfinished and failed Jobs, and unrelated resources.
Plain ``duckfleet-ops aws-wait-rollout`` is read-only; cleanup requires `--prune-migrations`.
Helm retains its ordinary lifecycle and may remove superseded failed Jobs on the
next upgrade or uninstall, so export failure logs before retrying a Helm release.
Template/apply installations require explicit cleanup of failed Jobs. The current
release Job has no TTL because manual recovery reuses its template; ad-hoc Jobs
created by `migrate-platform.sh` expire one day after completion or failure.

## Database roles and platform RBAC

Schema 3 uses separate PostgreSQL logins. Runtime startup checks actual privileges
and refuses an owner, superuser, inherited elevated role, schema CREATE permission,
or mutable audit/usage ledger. The application checks principal permissions and
compute grants separately; service database roles are not end-user PostgreSQL RLS.

| Component | Secret key mapped to `PLATFORM_DSN` | Other credentials |
| --- | --- | --- |
| Migration/grant-repair Job | `PLATFORM_MIGRATION_DSN` | None |
| Gateway | `PLATFORM_GATEWAY_DSN` | `PLATFORM_MAINTENANCE_DSN` mapped to `DUCKFLEET_MAINTENANCE_DSN`; public `DUCKFLEET_TOKEN`; distinct `DUCKFLEET_INTERNAL_TOKEN` |
| Persistent/isolated SQL worker | `PLATFORM_WORKER_DSN` | Dedicated nonsuperuser `CATALOG_DSN`; internal token only |
| PostgreSQL-only maintenance | `PLATFORM_MAINTENANCE_DSN` | None |

The migrator owns platform objects and does not have CREATEROLE. Workers cannot
read credential hashes, manage credentials/grants, change budget limits, or
update/delete/truncate audit and usage rows. Gateway and maintainer roles also
cannot modify retained audit/usage rows or perform DDL. The catalog login owns
only the dedicated lake database/schema, has no platform membership or CONNECT
grant, and is distinct from every platform role. Public schema CREATE and default
database CONNECT are revoked in the two dedicated databases.

Compose automatically runs `platform-provision` before `platform-migrate`; only the
provisioning task receives its development administrator credential. Its fixed
local passwords are development fixtures. Fresh bundled Helm PostgreSQL runs the
same reviewed SQL during initial creation. Helm runtime Pods mount individual
Secret keys, never the complete Secret. A worker's service account has no Secret
read permission. The PostgreSQL administrator and role password seeds appear only
in the database Pod, not a migration or compute Pod.

`databaseRoles` may explicitly name `migrator`, `gateway`, `worker`, `maintainer`,
and `catalog`. Defaults derive distinct names from the release namespace plus a
hash, permitting multiple deployments on one PostgreSQL cluster. The supplied
Secret must match those names. ``duckfleet-ops database-roles` <namespace>` generates a
fresh development Secret for piping directly to `kubectl create -f -`; do not save
its output in build logs or artifacts. Production credentials belong in the
operator's secret manager.

For external PostgreSQL, provision first using the environment template in
`examples/database-roles.env`, replacing every placeholder with a distinct secret:

```sh
# Export the reviewed variables through your secret manager or operator shell.
scripts/provision-database.sh
```

This explicit administrator action creates constrained LOGIN roles and dedicated
databases, grants connection access, and transfers only objects in each database's
public schema. It never uses cluster-wide `REASSIGN OWNED`. Existing elevated
component roles are refused for review. Existing databases with a different owner
require `DUCKFLEET_ADOPT_DATABASE_OWNER=yes` after all compute is stopped. The
administrator must be allowed to create roles/databases and transfer these objects;
the application migrator is not silently elevated to obtain those rights.

Create the chart Secret with the four platform DSNs from the table, `CATALOG_DSN`, separate
public/internal tokens, and set `postgres.bundled=false`, reviewed external CIDRs,
and `databaseRoles` to your chosen names. External database deployments do not need
the administrator or `DATABASE_*_PASSWORD` seed keys in their chart Secret. Apply
the chart after provisioning; the migration installs explicit table/column grants
in the same transaction as schema 6. Ordinary current-version migration remains
read-only. For an intentional grant repair after restoring or changing role names:

```sh
scripts/migrate-platform.sh compose platform-grants
# Or, with DUCKFLEET_KUBE_CONTEXT and DUCKFLEET_NAMESPACE set:
scripts/migrate-platform.sh kube platform-grants
```

The repair uses only the migration credential and never reseeds revoked principal
grants. Runtime policies and credential mutations remain audited application
operations. A grant repair changes database privileges and should run while compute
is stopped when coupled to role/credential replacement.

Before a schema 2 upgrade, review `authorization.defaultPersistentGrant` (default
`{pool: small, cpu: 1, memory: 1Gi, disk: 2Gi}`). The chart requires this explicit
envelope to cover its configured default pool; named custom defaults need a matching
reviewed value. Migration adopts existing named principals as analysts with own
visibility and this bounded persistent grant only. Other pools, isolation, operator
visibility, and administration need explicit grants. Repeated migration never
restores revoked grants. `DUCKFLEET_NAMESPACE` is the release namespace (Compose:
`local`); runtime rejects a restored database with a different persisted label.

For an existing Compose volume, stop compute, export the original PostgreSQL admin
password (older development volumes used `local-catalog-password`), and explicitly
adopt the old database ownership before migration:

```sh
scripts/stop-compute.sh compose
export DUCKFLEET_POSTGRES_ADMIN_PASSWORD='<existing administrator password>'
DUCKFLEET_ADOPT_DATABASE_OWNER=yes docker compose run --rm platform-provision
scripts/migrate-platform.sh compose
# Resume the desired services explicitly after successful migration.
docker compose up -d --wait server
```

For an existing bundled Kubernetes PVC, stop compute first. Upgrade its Secret
without rotating existing admin/bootstrap credentials, using the same namespace and
Secret name as the deployment:

```sh
kubectl --context "$DUCKFLEET_KUBE_CONTEXT" -n "$DUCKFLEET_NAMESPACE" get secret duckfleet-dev -o json \
  | bash scripts/ops.sh database-roles "$DUCKFLEET_NAMESPACE" --upgrade \
  | kubectl --context "$DUCKFLEET_KUBE_CONTEXT" -n "$DUCKFLEET_NAMESPACE" apply -f -
```

Render/apply only `templates/postgres.yaml` from the desired chart with your existing
values, and wait for its StatefulSet rollout. This mounts the reviewed provisioning
SQL and role password seeds without starting compute or migration. An administrator
then runs `env DUCKFLEET_ADOPT_DATABASE_OWNER=yes bash /docker-entrypoint-initdb.d/init.sh`
inside the PostgreSQL Pod. Apply the complete desired chart with a fresh migration
run ID only after provisioning succeeds. External PostgreSQL uses the explicit
administrator helper instead. The normal deploy helper never invents permission
to transfer an existing database or stop compute.

The AWS development lane has an equivalent one-time path that does not require
`pods/exec`: manually dispatch `AWS development harness` on `main` with
`upgrade_legacy_database=true`. This stops compute, preserves the existing
administrator/bootstrap credentials while adding separated role credentials,
applies only the bundled PostgreSQL resources, and runs the reviewed provisioning
SQL as a bounded `postgres:17.6-bookworm` Job with explicit ownership adoption.
Ordinary pushes leave this option false and never render that Job. The Job uses
only named Secret keys, has no AWS identity or API token, and is deleted after
success; the complete deployment and migration resume only after it completes.

This is platform-interface RBAC for trusted SQL executors. Until executor isolation
in #106, SQL runs in a process holding its worker/catalog credentials and internal
service token; file/environment access may expose that authority. Role separation
protects append-only records from runtime edits but cannot prove that a stolen
runtime credential did not forge an INSERT. It is not table RBAC, row masking, or
hostile-SQL isolation.

## Bounding the development lake

The scale acceptance run deposits roughly a hundred Parquet tables per deploy, and
DuckLake snapshots and small files accumulate on top across runs. The AWS lane
bounds this with the platform's own maintenance — `scripts/aws/maintain.sh` runs
compaction, then expiry of snapshots older than a week, then cleanup of the files
that leaves unreferenced — as a **continue-on-error** step after the smoke test's
verdict and before compute is suspended. Deliberately not in `test.sh`: a
destructive, file-deleting operation must never be able to fail a deploy or
corrupt the lake mid-verification, so it is best-effort and the next run continues
where an interrupted sweep left off (duck-fleet#80). Expiry forfeits time travel
past a week on the dev lake, which is disposable; a production target would keep a
longer window or move maintenance to a dedicated schedule.

## Rotate the catalog credential

The development password was written to pod logs in plaintext until #20 scrubbed it, so
the credential in use before that fix should be treated as disclosed. #20 stopped the
leak; this replaces what leaked.

```sh
bash scripts/aws/rotate-catalog-credential.sh
```

One command because the database user and the Secret have to change **together**.
`deploy.sh` writes that Secret only when it is absent and the PostgreSQL PVC outlives a
redeploy, so changing either alone leaves the workers holding a password the server no
longer accepts — which presents as an unreachable catalog rather than as a credential
problem. The script changes the database first, since the reverse would leave the
database reachable only with a credential that no longer exists anywhere, then applies
the Secret, restarts the deployments (a Secret change does not reach a running pod's
environment), and finally requires the fleet to answer before reporting success.

Only `DATABASE_CATALOG_PASSWORD` and the password in `CATALOG_DSN` change.
Platform runtime, migration, maintainer, and PostgreSQL administrator credentials
remain unchanged. Host, port, database and user survive exactly; `DUCKFLEET_TOKEN` is left alone because it is the
bootstrap credential, not a database credential, and rotating it here would log the
operator out as a side effect of database maintenance. Humans and services should
hold credentials issued through `POST /v1/credentials` (see the README's
Credentials section), which are revocable one at a time instead.

Statements already running finish within the worker's drain window (#16); sessions open
at that moment end.

## Establish the ownership boundary

Run this once per cluster, before the first deploy. It is what makes a namespace
DuckFleet's rather than merely one DuckFleet installs into.

```sh
AWS_PROFILE=<an administrative profile> bash scripts/aws/apply-boundary.sh
```

It applies `infra/kubernetes/boundary`: the namespace and the label the deploy checks
for, the Role and RoleBinding that let the gateway create session pods, the
ResourceQuota that caps them, and the Karpenter NodePool DuckFleet's capacity comes
from. Idempotent and additive — nothing in it removes or narrows an existing object —
and it verifies the grant by reading it back rather than trusting that `apply` reported
success.

The NodePool is tainted `duckfleet.io/dedicated`, so the capacity is DuckFleet's in
both directions: a neighbour cannot consume what DuckFleet pays for, and DuckFleet
cannot consume theirs. It permits `xlarge` as well as `large`, which is what makes a
2-CPU Guaranteed worker provisionable at all — the shared pool is capped to `large`,
2 vCPU with ~1780m allocatable, against a 2-CPU pod claiming 2050m with daemonset
overhead. `limits.cpu` is the spend ceiling and `WhenEmpty` consolidation is what makes
`suspended: true` actually stop costing money.

It references an existing **NodeClass** by name — `DUCKFLEET_NODE_CLASS` in
`scripts/aws/target.env`, defaulting to the `default` that EKS Auto Mode creates. A
NodeClass carries the node IAM role and the subnet and security group selectors, which
are facts about a cluster DuckFleet does not own; restating them here would mean keeping
a copy in step with infrastructure we do not control. The script checks the named
NodeClass exists **before** applying, because a pool pointing at a missing one applies
cleanly and then silently provisions nothing.

**It needs cluster admin, and the deploy identity deliberately is not.** That split is
the point. `scripts/aws/check-access.sh` asserts the deploy can create none of a Role,
a RoleBinding or a ResourceQuota, and fails the deploy if it ever can, so a release
that carried its own RBAC could not install itself — which is exactly why isolated
compute shipped disabled everywhere but the kind harness until this existed (#39). The
script checks for admin up front and refuses, rather than leaving a namespace and a
quota behind with no grant.

Granting a capability and deploying a release are different acts by different
identities. Keeping them apart is what lets the automated lane stay unable to widen its
own permissions, on a cluster DuckFleet owns as much as on one it shares.

Once applied, nothing more is needed by hand: the next deploy computes the flip
itself. ``duckfleet-ops aws-deploy-values`` reads the API server's live endpoints, enables
isolated compute with the egress CIDRs those endpoints require, pins the workers to the
`duckfleet` pool with its toleration, and raises the medium worker to its full 2-CPU
request — all as a deploy-time overlay on `values.json`, because the API-server address
is a fact about the cluster, not a chart default. The gateway then asks the API server
whether it holds the grant and reports the answer as `isolated_compute` on
`GET /v1/workers`, so the boundary and the deploy can land in either order and a missing
grant is a `501 compute_unavailable` refusal rather than a session that fails once a pod
could not be created. Removing the RoleBinding is therefore a supported way to turn the
feature off. `make test-k8s` applies this same boundary to its kind cluster, so a
boundary that stops matching the chart fails on a pull request.

## The exit: a cluster DuckFleet owns

**Not applied, and not the plan.** DuckFleet deploys into a namespace it owns on a
shared cluster, and the ownership boundary above is what makes that work. This module
exists so the decision stays reversible, and is kept `validate`-clean and mock-tested
on every pull request so it does not rot.

What is still shared, and deliberately accepted: the control plane, the VPC, and the
cluster's lifecycle. Their upgrades and outages are DuckFleet's too, and an
administrator there can change the boundary objects. The taint and the quota bound
DuckFleet's impact on its neighbours, not the reverse. The trade is the standing cost
below against accepting that coupling — worth making when the coupling starts costing
more than the cluster does, and not before.

Note that owning a cluster would not have removed the need for the boundary. The two
capabilities that looked blocked by shared ownership — 2-CPU workers and pod-create
RBAC — were really the release trying to grant its own permissions and assuming someone
else's node pool was sized for it. On a cluster DuckFleet owned, the first of those
would have silently succeeded, which is worse than failing loudly.

`infra/terraform/cluster` defines an EKS Auto Mode cluster, its VPC, the IAM roles
Auto Mode requires, the worker identity and Pod Identity association, and a CI access
entry scoped to the `duckfleet-dev` namespace.

**It is applied by an operator, never by CI.** The cluster needs `iam:CreateRole`,
`eks:CreateCluster` and `ec2:CreateVpc`; CI applies on every main push and its plan
guard allows only the lake bucket and the two image repositories. Putting the cluster
there would trade a permanent widening of an automated apply for a one-time creation.
It has its own state key for the same reason. A test asserts both, so this cannot drift.

Standing cost is the control plane at $0.10/hour, about **$73/month**, plus an
instance-dependent EKS Auto Mode fee on top of EC2 while nodes exist — and the lane
suspends compute after each run, so that is minutes per run rather than continuous.
There is deliberately **no NAT Gateway**: at roughly $32/month per availability zone it
would cost more than the cluster it serves. The subnets are public with an Internet
Gateway, which is free, and an S3 gateway endpoint keeps DuckLake traffic off the
internet at no charge. AWS recommends private subnets; this is a disposable
development lane with no inbound exposure, so the trade is deliberate. A production
target should take private subnets and pay for the NAT.

```sh
source scripts/aws/target.env
terraform -chdir=infra/terraform/cluster init \
  -backend-config="bucket=$DUCKFLEET_STATE_BUCKET" \
  -backend-config="key=$DUCKFLEET_CLUSTER_STATE_KEY" \
  -backend-config="region=$DUCKFLEET_REGION"
terraform -chdir=infra/terraform/cluster plan     # read this before applying
terraform -chdir=infra/terraform/cluster apply    # about 15 minutes
terraform -chdir=infra/terraform/cluster output -raw target_env
```

The last command prints the values to put in `scripts/aws/target.env`. Changing them
there is what moves the lane onto the new cluster, and it is a separate reviewed pull
request.

Then apply the ownership boundary to the new cluster, exactly as above. Owning the
cluster changes who may grant a capability, not whether granting one is the deploy's
job — `check-access.sh` still asserts the deploy identity can create no Role, and the
module's CI access entry is scoped to `AmazonEKSEditPolicy` so that stays true. Confirm
`DUCKFLEET_NODE_CLASS` names a NodeClass that exists on the new cluster;
`apply-boundary.sh` lists the candidates if it does not.

Expect the first apply to need iteration. The module is `validate`-clean and passes
mocked-provider tests, which catches schema and scope errors but cannot establish that
the cluster converges. Keep the plan output if it fails.

## Moving Terraform state to a bucket DuckFleet owns

Done (#12): state lived under a key inside a shared bucket and now lives
in `duckfleet-terraform-state-797781631727`, which DuckFleet owns outright. The
location is configuration (`DUCKFLEET_STATE_BUCKET`, `DUCKFLEET_STATE_KEY`) taken
through `-backend-config`, so the bucket is named in the same one place as the rest
of the target. The steps below are the record of how it moved, and how to move it
again for another target.

Terraform cannot create the bucket that holds its own state, so this is the one piece
of infrastructure outside Terraform. It holds state and nothing else.

```sh
# 1. Create it. Idempotent, and refuses any bucket not named duckfleet-*.
DUCKFLEET_STATE_BUCKET=duckfleet-terraform-state-797781631727 \
  bash scripts/aws/bootstrap-state.sh

# 2. Copy the state across. Leaves the old copy untouched and verifies the resource
#    count matches before reporting success. Prompts to confirm; set
#    DUCKFLEET_ASSUME_YES=true where there is no terminal (a runner, an agent shell).
DUCKFLEET_ASSUME_YES=true DUCKFLEET_STATE_BUCKET=duckfleet-terraform-state-797781631727 \
  bash scripts/aws/migrate-state.sh

# 3. Change the default in scripts/aws/target.env and open a pull request.
```

The order matters and is why nothing ends up half-moved: CI reads the same
`target.env`, so until step 3 merges, CI keeps initialising against the old bucket
while the new one sits ready. If step 2 is skipped, step 3's first apply would
initialise an empty state and plan to create resources that already exist — which the
plan guard rejects, but the clearer protection is simply not to reorder these.

## Checks without AWS

Requires Terraform 1.11.4+, Helm 3, and Rust 1.99. All chart and infrastructure assertions run through `duckfleet-ops`; no Python or PyYAML installation is needed.

```sh
make check-infra
make check
make test
```

`check-infra` validates Terraform, runs **mocked** AWS provider tests,
and inspects the rendered Helm resources. It never contacts the account.
`make check` also exercises the pinned DuckDB AWS extension against a local
container-credentials HTTP fixture, including its authorization token file.
These checks cannot establish EKS network-policy enforcement or IAM correctness.

## Bootstrap and CI deployment

Cluster operators provision `duckfleet-ci-dev`, worker IAM and Pod Identity,
and EKS access. The operator-applied boundary in `infra/kubernetes/boundary`
establishes the `duckfleet-dev` namespace, security labels, quota, RBAC, and
node pool. The deployment identity cannot grant these permissions to itself.
Keep operator-owned resources out of the deployment's Terraform state.

The CI role trusts only the immutable `athvin/duck-fleet` repository identity on
`main`. It can manage the dedicated bucket and two repositories, read/write only
`duckfleet/dev.tfstate`, and delete only its lock file. It cannot write IAM/EKS,
read unrelated state or secrets, change namespace security/quota/RBAC, or deploy into unrelated
namespaces. Kubernetes application credentials remain privileged within the
trusted DuckFleet development namespace; this is not customer SQL isolation.

After the bootstrap has applied, merging this repo's CI change enables
`AWS development harness`. Each main push affecting deployment inputs (or a manual dispatch on main) does:

1. Wait for both Docker and infrastructure checks on this exact SHA; refuse a
   superseded main revision.
2. Obtain short-lived OIDC credentials and verify effective namespace permissions.
3. Plan and apply only DuckFleet storage/image resources. Reject incomplete plans,
   deletes, replacements, imports, moves and resources outside the explicit list.
4. Build on native x86 and ARM runners in parallel, publish immutable architecture
   tags, and assemble a commit index only after both builds succeed. Validate both
   platforms before deploying by digest. A rerun reuses immutable tags; a registry
   permission error fails instead of masquerading as a cache miss. Secrets are generated once via
   stdin; redeployment preserves the database password.
5. Run the live dbt sizing harness and upload only verified fixture evidence.
6. Run a separate cleanup job even after a deployment/test failure, scale the
   gateway, workers and PostgreSQL to zero, and wait for their pods to disappear.

```sh
# Retry a failed main run after fixing its external prerequisite:
gh workflow run aws-dev.yml --repo athvin/duck-fleet --ref main
```

Workflow runs serialize, without canceling an in-flight deployment. Documentation-only
pushes do not start AWS runs; the credential-free required checks still run. PR validation
receives no AWS credentials. Terraform plan/state and raw Kubernetes/secret output
are never uploaded. The runner uses an explicit `duckfleet-ci-dev` context; local operator scripts
default to the context named in `scripts/aws/target.env` and never use the
workstation default, which may point at a production cluster. No static AWS
credentials are created.

Do not use scripts to bypass a local mutation guard; Terraform applies belong to
CI. Operators may inspect resources with their authorized profile. Before the
first run verify account capacity, Auto Mode Pod Identity/network policy, `gp3`,
and the VPC S3 gateway endpoint. Do not delete the generated Secret while keeping
the PostgreSQL PVC.

The harness runs actual dbt build/test/incremental cycles, changes the same model
from medium to small and back, checks hooks/session affinity and rejected sizes,
and reads Parquet directly from the real S3 path through a worker. It records
model placement, process/cgroup evidence, pod UID, node and resource requests/
limits in `artifacts/aws/evidence.json`; detailed logs go in the same directory.
The image build forces an expired fixture credential to receive HTTP 403 and
requires a fresh container-credential fetch and signed retry. The live harness
additionally sends prefix-neighbor requests through a worker to an in-cluster S3
fixture and requires them to be unsigned; real Pod Identity is proven by the S3
lake read. Only a successful `test.sh` with that evidence warrants saying the AWS
test passed. The live lane does not yet automate the local suite's
worker-restart/unavailable worker orchestration, long-duration live credential
renewal, broader IAM adversarial probes, or network-policy enforcement tests.
These remain gates before external tenants.

To inspect the private API, use an explicit-context port forward from an authorized
operator session (the API still requires the generated bearer token):

```sh
# The context and namespace come from scripts/aws/target.env.
source scripts/aws/target.env
kubectl --context "$DUCKFLEET_KUBE_CONTEXT" -n "$DUCKFLEET_NAMESPACE" \
  port-forward service/server 18081:8080
```

## Stop compute and retain the experiment

Run after evidence collection to avoid leaving baseline compute running:

```sh
# Defaults come from scripts/aws/target.env; override either to target elsewhere.
bash scripts/aws/suspend.sh
```

This stops the test Job and scales gateway, workers, and PostgreSQL to zero. S3 versions, images, and the PVC continue to incur storage
charges. Namespace quotas cap requested resources, **not dollars**; shared node
capacity and idle time make the actual bill workload-dependent. No AWS spend
commitments, new cluster, NAT gateway, or public endpoint are introduced.

S3 destruction is protected in DuckFleet Terraform; namespace/quota ownership stays
in the platform bootstrap. A canceled workflow or runner outage can interrupt
cleanup, so check the suspend job and inspect live replicas after a failed run. Full cleanup requires
an explicit data-retention decision: removing an EBS PVC under the shared `gp3`
class deletes its volume. S3 versioning is not a backup of the PostgreSQL catalog.

## Authentication and isolation

Workers use the [DuckDB 2.0 AWS credential chain with automatic refresh](https://duckdb.org/docs/2.0/core_extensions/aws)
and the [EKS Pod Identity container endpoint](https://docs.aws.amazon.com/eks/latest/userguide/pod-id-how-it-works.html).
The identity requires the exact cluster/namespace/service-account tuple and grants
S3 access only to `lake/` in DuckFleet's bucket. Gateway, database and dbt test pods
have no AWS role. The gateway reads query history through its platform database
connection, independently of worker availability. No static AWS access keys are generated or deployed.

Network policies permit same-namespace SQL/database traffic, DNS, and worker
HTTPS plus the Pod Identity endpoint; private-network egress and EC2 IMDS are
excluded from the worker internet rule. Policy enforcement still needs live
verification. Workers execute trusted test SQL, and HTTPS egress is not restricted
to S3 destinations. This is not an arbitrary customer SQL sandbox. PostgreSQL and
internal API traffic are plaintext within the isolated development namespace.
