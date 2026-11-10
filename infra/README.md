# DuckFleet AWS development harness

Status: CI deployment code prepared; **not yet tested on AWS**. The Docker gates
passed on main after the query-history release. Live deployment requires the
[shared-platform bootstrap](https://github.com/athvin/entity-resolution-engine/issues/1082)
to merge and apply first. Docker and mock-provider tests are not AWS evidence.

The proposed live target is `duckfleet-dev` on the existing **er-dev** EKS Auto
Mode cluster in account `797781631727`, region `us-east-2`. This adds an AWS test
lane alongside Docker Compose. DuckFleet owns its Terraform state, S3 bucket and image repositories;
the existing cluster, networking, node pools, and `gp3` StorageClass remain owned
by the ER platform. This shares the AWS account, cluster capacity, and bill.

| Boundary | DuckFleet resource |
| --- | --- |
| Terraform state | `s3://er-terraform-state-797781631727/duckfleet/dev.tfstate` with S3 locking |
| Kubernetes | `duckfleet-dev`, restricted Pod Security, resource quota |
| Object storage | `duckfleet-dev-797781631727-us-east-2`, private, TLS, encryption, versioning |
| Worker IAM | `duckfleet-dev-worker`, one Pod Identity association for `duckfleet-dev/duckfleet-worker` |
| Registry | `duckfleet-dev/worker`, `duckfleet-dev/dbt`, immutable tags/digests |
| Metadata + history | dedicated PostgreSQL StatefulSet, separate `lake` and `platform` databases, 10 GiB encrypted gp3 PVC |
| API | private ClusterIP; no ingress, public IP, DNS or load balancer |

The smallest test uses two **prestarted pods**, 1 CPU / 1 GiB and 2 CPU / 2 GiB,
with DuckDB limits of 1 thread / 512 MiB and 2 threads / 1 GiB. The adapter routes
whole model lifecycles to these workers. They may share an EC2 node. This does
**not** test dynamic EC2 instance creation, dedicated nodes, or 32 CPU workers.
PostgreSQL is separate from ER's Aurora databases for this disposable experiment.
This is not a production database/backup/HA design.

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

The shared-platform repository owns `duckfleet-ci-dev`, worker IAM and Pod
Identity, EKS access, and the `duckfleet-dev` namespace, security labels, quota and
RBAC. Its Terraform CI and cluster-objects workflow apply that bootstrap. These
resources have never been in DuckFleet's remote state; moving their definitions
before the first apply needs no state migration. Do not import them here later.

The CI role trusts only the immutable `athvin/duck-fleet` repository identity on
`main`. It can manage the dedicated bucket and two repositories, read/write only
`duckfleet/dev.tfstate`, and delete only its lock file. It cannot write IAM/EKS,
read ER state or secrets, change namespace security/quota/RBAC, or deploy into ER
namespaces. Kubernetes application credentials remain privileged within the
trusted DuckFleet development namespace; this is not customer SQL isolation.

After the bootstrap has applied, merging this repo's CI change enables
`AWS development harness`. Each main push (or a manual dispatch on main) does:

1. Wait for both Docker and infrastructure checks on this exact SHA; refuse a
   superseded main revision.
2. Obtain short-lived OIDC credentials and verify effective namespace permissions.
3. Plan and apply only DuckFleet storage/image resources. Reject incomplete plans,
   deletes, replacements, imports, moves and resources outside the explicit list.
4. Publish both CPU architectures under immutable commit tags and deploy by digest.
   A rerun reuses an existing immutable commit tag. Secrets are generated once via
   stdin; redeployment preserves the database password.
5. Run the live dbt sizing harness and upload only verified fixture evidence.
6. Run a separate cleanup job even after a deployment/test failure, scale the
   gateway, workers and PostgreSQL to zero, and wait for their pods to disappear.

```sh
# Retry a failed main run after fixing its external prerequisite:
gh workflow run aws-dev.yml --repo athvin/duck-fleet --ref main
```

Workflow runs serialize, without canceling an in-flight deployment. PR validation
receives no AWS credentials. Terraform plan/state and raw Kubernetes/secret output
are never uploaded. The runner uses an explicit `duckfleet-ci-dev` context; local
operator scripts default to `er-dev-admin` and never use the workstation default
(which may be **er-prod**). No static AWS credentials are created.

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
kubectl --context er-dev-admin -n duckfleet-dev port-forward service/server 18081:8080
```

## Stop compute and retain the experiment

Run after evidence collection to avoid leaving baseline compute running:

```sh
AWS_PROFILE=er-platform KUBE_CONTEXT=er-dev-admin bash scripts/aws/suspend.sh
```

This scales gateway, workers, and PostgreSQL to zero; the completed test Job
expires after 24 hours. S3 versions, images, and the PVC continue to incur storage
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
