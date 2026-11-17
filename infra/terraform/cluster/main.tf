/*
 * The EKS cluster DuckFleet owns. Operator-applied, not CI-applied.
 *
 * Separate from infra/terraform/dev on purpose. That module is applied by CI on every
 * main push, and its plan guard allows only the lake bucket and the two image
 * repositories. Creating a cluster from there would mean granting the CI role
 * iam:CreateRole, eks:CreateCluster and ec2:CreateVpc — widening the blast radius of
 * an automated apply from "storage and images" to "the account's networking and
 * identity", permanently, in order to create something once. The infra runbook states
 * that the CI role cannot write IAM or EKS, and that stays true.
 *
 * So this has its own state key and is applied by hand, once, by an operator with the
 * permissions for it. Merging this file creates nothing.
 *
 * Cost, which is why the network below is shaped as it is: the control plane is
 * $0.10/hour (~$73/month) and EKS Auto Mode adds a per-instance fee on top of EC2.
 * The subnets are public and there is no NAT Gateway, because a NAT Gateway is about
 * $32/month per availability zone — more than the cluster it would serve. EKS permits
 * public subnets; nodes reach ECR and the control plane over the Internet Gateway,
 * which is free, and the S3 gateway endpoint keeps DuckLake traffic off the internet
 * at no charge.
 *
 * AWS recommends private subnets. This is a disposable development lane with no
 * inbound exposure — the chart forbids Ingress, LoadBalancer Services and public IPs
 * — so the trade is deliberate rather than overlooked. A production target should
 * take private subnets and pay for the NAT.
 */
terraform {
  required_version = ">= 1.11, < 2.0"
  required_providers {
    aws = { source = "hashicorp/aws", version = "~> 6.0" }
  }
  # Partial configuration, like the dev module: the location comes from
  # scripts/aws/target.env so the cluster's state is named in the same one place.
  backend "s3" {
    encrypt      = true
    use_lockfile = true
  }
}

provider "aws" {
  region              = var.region
  allowed_account_ids = [var.account]
  default_tags {
    tags = { Project = "duckfleet", Environment = "dev", ManagedBy = "terraform" }
  }
}

variable "region" {
  type    = string
  default = "us-east-2"
}
variable "account" {
  type    = string
  default = "797781631727"
}
variable "cluster" {
  type        = string
  default     = "duckfleet-dev"
  description = "Cluster name; must match DUCKFLEET_CLUSTER in scripts/aws/target.env."
}
variable "namespace" {
  type        = string
  default     = "duckfleet-dev"
  description = "The only namespace the CI identity is granted access to."
}
variable "kubernetes_version" {
  type    = string
  default = "1.34"
}
variable "ci_role" {
  type        = string
  default     = "duckfleet-ci-dev"
  description = "The existing CI role, granted namespace-scoped access below."
}
variable "lake_bucket" {
  type    = string
  default = "duckfleet-dev-797781631727-us-east-2"
}

data "aws_availability_zones" "available" {
  state = "available"
  filter {
    name   = "opt-in-status"
    values = ["opt-in-not-required"]
  }
}

locals {
  # Two, which is the EKS minimum. More would spread nodes across more zones and add
  # cross-zone data transfer for no benefit on a single-node development lane.
  zones = slice(data.aws_availability_zones.available.names, 0, 2)

  # Trust policies are written with jsonencode rather than through
  # aws_iam_policy_document, because a mocked data source yields a placeholder string
  # and a policy built from one cannot be asserted on by a plan-only test. The dev
  # module writes its bucket policy the same way.
  #
  # sts:TagSession accompanies AssumeRole for the EKS service and Pod Identity
  # principals, which pass session tags; EC2 does not.
  assume = {
    for name, principal in {
      cluster = { service = "eks.amazonaws.com", tag_session = true }
      node    = { service = "ec2.amazonaws.com", tag_session = false }
      worker  = { service = "pods.eks.amazonaws.com", tag_session = true }
      } : name => jsonencode({
        Version = "2012-10-17"
        Statement = [{
          Effect    = "Allow"
          Principal = { Service = principal.service }
          Action    = principal.tag_session ? ["sts:AssumeRole", "sts:TagSession"] : ["sts:AssumeRole"]
        }]
    })
  }
}

resource "aws_vpc" "main" {
  cidr_block = "10.42.0.0/16"
  # Both are required: without them nodes cannot register with the cluster.
  enable_dns_support   = true
  enable_dns_hostnames = true
  tags                 = { Name = var.cluster }
}

resource "aws_internet_gateway" "main" {
  vpc_id = aws_vpc.main.id
  tags   = { Name = var.cluster }
}

resource "aws_subnet" "public" {
  for_each                = { for index, zone in local.zones : zone => index }
  vpc_id                  = aws_vpc.main.id
  availability_zone       = each.key
  cidr_block              = cidrsubnet(aws_vpc.main.cidr_block, 4, each.value)
  map_public_ip_on_launch = true
  tags                    = { Name = "${var.cluster}-${each.key}" }
}

resource "aws_route_table" "public" {
  vpc_id = aws_vpc.main.id
  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.main.id
  }
  tags = { Name = var.cluster }
}

resource "aws_route_table_association" "public" {
  for_each       = aws_subnet.public
  subnet_id      = each.value.id
  route_table_id = aws_route_table.public.id
}

# A gateway endpoint, not an interface endpoint: gateway endpoints for S3 are free,
# and this is the path every DuckLake read and write takes.
resource "aws_vpc_endpoint" "s3" {
  vpc_id            = aws_vpc.main.id
  service_name      = "com.amazonaws.${var.region}.s3"
  vpc_endpoint_type = "Gateway"
  route_table_ids   = [aws_route_table.public.id]
  tags              = { Name = "${var.cluster}-s3" }
}

resource "aws_iam_role" "cluster" {
  name               = "${var.cluster}-cluster"
  assume_role_policy = local.assume.cluster
}

# Auto Mode acts in the account on the cluster's behalf — provisioning instances,
# volumes and load balancers — so this is AWS's documented set rather than a guess.
resource "aws_iam_role_policy_attachment" "cluster" {
  for_each = toset([
    "AmazonEKSClusterPolicy",
    "AmazonEKSComputePolicy",
    "AmazonEKSBlockStoragePolicyV2",
    "AmazonEKSLoadBalancingPolicy",
    "AmazonEKSNetworkingPolicy",
  ])
  role       = aws_iam_role.cluster.name
  policy_arn = "arn:aws:iam::aws:policy/${each.key}"
}

# Carried by every node Auto Mode launches. Workload permissions do not belong here —
# they go through Pod Identity below, so a pod gets the worker's S3 access rather than
# the node's ability to talk to EKS.
resource "aws_iam_role" "node" {
  name               = "${var.cluster}-node"
  assume_role_policy = local.assume.node
}

resource "aws_iam_role_policy_attachment" "node" {
  for_each = toset([
    "AmazonEKSWorkerNodeMinimalPolicy",
    "AmazonEC2ContainerRegistryPullOnly",
  ])
  role       = aws_iam_role.node.name
  policy_arn = "arn:aws:iam::aws:policy/${each.key}"
}

resource "aws_eks_cluster" "main" {
  name     = var.cluster
  version  = var.kubernetes_version
  role_arn = aws_iam_role.cluster.arn

  vpc_config {
    subnet_ids = [for subnet in aws_subnet.public : subnet.id]
    # Public access so an operator and CI can reach the API without a bastion or a
    # VPN. Private as well, so in-cluster traffic to the API stays internal — that is
    # the path the gateway uses to create isolated session pods.
    endpoint_public_access  = true
    endpoint_private_access = true
  }

  compute_config {
    enabled       = true
    node_role_arn = aws_iam_role.node.arn
    # `system` carries the managed add-ons, `general-purpose` carries workloads.
    # Without `system` there is nowhere for cluster components to run.
    node_pools = ["general-purpose", "system"]
  }
  storage_config {
    block_storage { enabled = true }
  }
  kubernetes_network_config {
    # Enabled because it is the configuration AWS documents and so the one most likely
    # to apply cleanly first time. Nothing uses it — the chart forbids Ingress and
    # LoadBalancer Services. Worth turning off once the cluster is proven.
    elastic_load_balancing { enabled = true }
  }
  # Access entries rather than the aws-auth ConfigMap, which is what allows the grant
  # below to be namespace-scoped and managed as infrastructure.
  access_config {
    authentication_mode = "API"
    # Whoever applies this holds admin, so there is a way in before any access entry
    # exists. Without it, a mistake in the entry below would lock everyone out.
    bootstrap_cluster_creator_admin_permissions = true
  }

  depends_on = [aws_iam_role_policy_attachment.cluster, aws_iam_role_policy_attachment.node]
}

# CI gets edit inside one namespace and nothing cluster-wide: application objects, and
# not the RBAC or quotas that bound them. Those belong to the ownership boundary in
# infra/kubernetes/boundary, which an operator applies — owning the cluster changes who
# can grant a capability, not whether granting one is the deploy's job.
#
# Edit rather than admin on purpose. AmazonEKSAdminPolicy grants
# `rbac.authorization.k8s.io` roles and rolebindings `create`, and check-access.sh fails
# the deploy when the identity can — so admin here would abort the first deploy into a
# cluster this module had just built correctly. AmazonEKSEditPolicy carries no RBAC
# rules at all, and reads resourcequotas without being able to set them, which is
# exactly the shape that gate asserts.
#
# Known gap, recorded rather than guessed at: AmazonEKSEditPolicy carries no
# `storage.k8s.io` rules, so `deploy.sh`'s storage class preflight would be forbidden
# here. It needs a cluster-scoped read grant, which is a boundary object and so belongs
# in infra/kubernetes/boundary — but writing it blind is how the two defects above got
# here, so it waits until there is a cluster to read it back from. This module is the
# documented exit, not the deployed target; see infra/README.md.
resource "aws_eks_access_entry" "ci" {
  cluster_name  = aws_eks_cluster.main.name
  principal_arn = "arn:aws:iam::${var.account}:role/${var.ci_role}"
  type          = "STANDARD"
}

resource "aws_eks_access_policy_association" "ci" {
  cluster_name  = aws_eks_cluster.main.name
  principal_arn = aws_eks_access_entry.ci.principal_arn
  # An EKS access policy, not an IAM policy: these live under `eks::aws:
  # cluster-access-policy/` and carry Kubernetes permissions rather than IAM ones. The
  # IAM-shaped ARN this used to carry does not resolve, so the association would have
  # failed on the first apply.
  policy_arn = "arn:aws:eks::aws:cluster-access-policy/AmazonEKSEditPolicy"
  access_scope {
    type       = "namespace"
    namespaces = [var.namespace]
  }
}

# The identity a worker pod assumes: the lake prefix and nothing else. A worker runs
# user SQL, so its credentials are the ones most worth bounding.
resource "aws_iam_role" "worker" {
  name               = "${var.cluster}-worker"
  assume_role_policy = local.assume.worker
}

resource "aws_iam_role_policy" "worker" {
  name = "lake-access"
  role = aws_iam_role.worker.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:AbortMultipartUpload"]
        Resource = "arn:aws:s3:::${var.lake_bucket}/lake/*"
      },
      {
        Effect   = "Allow"
        Action   = "s3:ListBucket"
        Resource = "arn:aws:s3:::${var.lake_bucket}"
        # Prefix-conditioned, so a worker cannot enumerate the whole bucket.
        Condition = { StringLike = { "s3:prefix" = ["lake/*", "lake"] } }
      },
    ]
  })
}

# Binds that role to the one service account the workers run as. The gateway's service
# account is not associated, because the gateway never touches S3.
resource "aws_eks_pod_identity_association" "worker" {
  cluster_name    = aws_eks_cluster.main.name
  namespace       = var.namespace
  service_account = "duckfleet-worker"
  role_arn        = aws_iam_role.worker.arn
}

output "cluster_name" { value = aws_eks_cluster.main.name }
output "cluster_endpoint" { value = aws_eks_cluster.main.endpoint }
output "worker_role_arn" { value = aws_iam_role.worker.arn }
# What to put in scripts/aws/target.env once this has applied.
output "target_env" {
  value = <<-EOT
    DUCKFLEET_CLUSTER=${aws_eks_cluster.main.name}
    DUCKFLEET_KUBE_CONTEXT=${aws_eks_cluster.main.arn}
    DUCKFLEET_REQUIRE_BOOTSTRAP=false
  EOT
}
