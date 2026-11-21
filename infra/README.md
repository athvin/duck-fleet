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
into the images and the deploy, and asserted by `tests/test_aws_ci.py`. The chart
itself names no account.

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

## Platform schema and maintenance

The release runs a PostgreSQL-only migration Job before compute can become ready.
Workers validate the schema and recover their own exclusive identities; global
retention and budget reconciliation run through the existing gateway. See the
root README's "Platform migrations and maintenance" section for their bounds,
manual commands and operational tables. This is separate from DuckLake data
compaction and snapshot expiry below.

`platformMigration.runId` changes the migration Job identity independently of the
image digest. Deploy and namespace-provisioning helpers supply a fresh run ID.
After restoring or retargeting `PLATFORM_DSN`, explicitly rerun migration with
`scripts/migrate-platform.sh kube` or a new run ID before restarting compute.
The migration uses the `duckfleet-no-aws` service account, has no mounted API token,
and receives no object-store credentials. Failed Jobs prevent rollout success;
inspect their logs to diagnose the schema or database problem.

For this recovery flow, stop compute by scaling its Deployments to zero while
retaining the release's migration Job. Setting `suspended: true` removes that Job
from the Helm release, so the manual recovery helper cannot reuse its template.

Successful deployment helpers prune superseded successful migration Jobs and their
completed Pods only after the current migration and workloads are ready. They
preserve current references, unfinished and failed Jobs, and unrelated resources.
Plain `wait-rollout.py` is read-only; cleanup requires `--prune-migrations`.
Helm retains its ordinary lifecycle and may remove superseded failed Jobs on the
next upgrade or uninstall, so export failure logs before retrying a Helm release.
Template/apply installations require explicit cleanup of failed Jobs. The current
release Job has no TTL because manual recovery reuses its template; ad-hoc Jobs
created by `migrate-platform.sh` expire one day after completion or failure.

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

Only the password changes. The DSNs are edited rather than rebuilt, so host, port,
database and user survive exactly; `DUCKFLEET_TOKEN` is left alone because it is the
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
itself. `scripts/aws/deploy_values.py` reads the API server's live endpoints, enables
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

Requires Terraform 1.11.4+, Helm 3, Python 3.10+ and PyYAML (`pip install PyYAML==6.0.3`).

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
Only a successful `test.sh` with that evidence warrants saying the AWS test passed.
The live lane does not yet automate the local suite's worker-restart/unavailable
worker orchestration, long-duration credential refresh, IAM adversarial probes,
or network-policy enforcement tests. These remain gates before external tenants.

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

Workers use the [DuckDB 1.5 AWS credential chain with automatic refresh](https://duckdb.org/docs/1.5/core_extensions/aws)
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
