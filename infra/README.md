# DuckFleet AWS development harness

Status: prepared for deployment; **not yet tested on AWS**. The authoring session's
command guard refused cloud operations and the configured Kubernetes SSO token
was expired. Docker test results are not evidence of an AWS deployment.

The proposed live target is `duckfleet-dev` on the existing **er-dev** EKS Auto
Mode cluster in account `797781631727`, region `us-east-2`. This adds an AWS test
lane alongside Docker Compose. DuckFleet owns its Terraform state and resources;
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

`check-infra` validates Terraform, runs **mocked** AWS/Kubernetes provider tests,
and inspects the rendered Helm resources. It never contacts the account.
`make check` also exercises the pinned DuckDB AWS extension against a local
container-credentials HTTP fixture, including its authorization token file.
These checks cannot establish EKS network-policy enforcement or IAM correctness.

## Live deployment prerequisites

A human-operated session or approved deployment runner must perform mutations.
Do not use these scripts to evade an agent's command guard. Renew SSO in that
session, use `er-platform`, and explicitly target the `er-dev` context. Scripts
verify account and cluster endpoint before deployment/testing. The default
operator context is `er-dev-admin`; `KUBE_CONTEXT` can select another authenticated
er-dev context. The workstation's default may be **er-prod** and is never used.

The ER GitHub OIDC roles trust `athvin/entity-resolution-engine`, not this repo.
The new `Infrastructure checks` workflow intentionally has no cloud credentials
or automatic apply. Before enabling CI applies, bootstrap a **DuckFleet-specific**
OIDC role and scoped EKS access through the existing platform's reviewed CI path.
Do not widen ER's role trust or borrow its secrets. Its current state policy also
needs a separately scoped grant for `duckfleet/dev.tfstate` and `.tflock` if a
new CI role uses the shared backend. The following commands are an operator
runbook, not a claim that this bootstrap or an apply has happened.

Review current EC2 On-Demand vCPU quota and er-dev capacity before applying;
dev and prod share the account quota. Confirm Auto Mode Pod Identity and network
policy support, `gp3` StorageClass, and the VPC S3 gateway endpoint are healthy.

## Apply, publish, deploy, test

All commands below are from the repository root, after committing reviewed code.
Use Terraform 1.11.4 or newer (the workstation's Homebrew binary may be 1.5.7).

```sh
export AWS_PROFILE=er-platform AWS_REGION=us-east-2 KUBE_CONTEXT=er-dev-admin
aws sso login --profile er-platform
terraform -chdir=infra/terraform/dev init
terraform -chdir=infra/terraform/dev plan -out=duckfleet.tfplan
# Review: only DuckFleet resources should be created; no ER updates/deletes.
terraform -chdir=infra/terraform/dev apply duckfleet.tfplan
bash scripts/aws/publish.sh
bash scripts/aws/deploy.sh
bash scripts/aws/test.sh
```

The apply creates the namespace, quota, bucket, repositories, IAM role and Pod
Identity binding. It never manages the shared cluster. Publishing pushes both
architectures to the dedicated ECR repositories and records manifest digests in
ignored `artifacts/aws/values.json`. ECR tags are immutable: repeat publishing of
the same commit may fail; use the recorded digest values for redeployment.
Deployment generates database and gateway secrets once, via stdin, without
printing or committing them; redeployment preserves the existing password.
Do not delete that Secret while retaining the PostgreSQL volume.

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
helm upgrade duckfleet infra/helm/duckfleet --kube-context er-dev-admin \
  --namespace duckfleet-dev -f artifacts/aws/values.json --set suspended=true --wait
```

This scales gateway, workers, and PostgreSQL to zero; the completed test Job
expires after 24 hours. S3 versions, images, and the PVC continue to incur storage
charges. Namespace quotas cap requested resources, **not dollars**; shared node
capacity and idle time make the actual bill workload-dependent. No AWS spend
commitments, new cluster, NAT gateway, or public endpoint are introduced.

S3 and namespace destruction are protected in Terraform. Full cleanup requires
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
