terraform {
  required_version = ">= 1.11, < 2.0"
  required_providers {
    kubernetes = { source = "hashicorp/kubernetes", version = "~> 2.38" }
    aws        = { source = "hashicorp/aws", version = "~> 6.0" }
  }
  backend "s3" {
    bucket       = "er-terraform-state-797781631727"
    key          = "duckfleet/dev.tfstate"
    region       = "us-east-2"
    encrypt      = true
    use_lockfile = true
  }
}

provider "aws" {
  region              = "us-east-2"
  allowed_account_ids = ["797781631727"]
  default_tags {
    tags = { Project = "duckfleet", Environment = "dev", ManagedBy = "terraform" }
  }
}

# Existing ER cluster is a read-only dependency, never imported or managed here.
data "aws_eks_cluster" "dev" { name = "er-dev" }

locals {
  namespace = "duckfleet-dev"
  bucket    = "duckfleet-dev-797781631727-us-east-2"
}

resource "aws_s3_bucket" "lake" {
  bucket        = local.bucket
  force_destroy = false
  lifecycle { prevent_destroy = true }
}
resource "aws_s3_bucket_public_access_block" "lake" {
  bucket                  = aws_s3_bucket.lake.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}
resource "aws_s3_bucket_ownership_controls" "lake" {
  bucket = aws_s3_bucket.lake.id
  rule { object_ownership = "BucketOwnerEnforced" }
}
resource "aws_s3_bucket_server_side_encryption_configuration" "lake" {
  bucket = aws_s3_bucket.lake.id
  rule {
    apply_server_side_encryption_by_default { sse_algorithm = "AES256" }
  }
}
resource "aws_s3_bucket_versioning" "lake" {
  bucket = aws_s3_bucket.lake.id
  versioning_configuration { status = "Enabled" }
}
resource "aws_s3_bucket_policy" "lake" {
  bucket = aws_s3_bucket.lake.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "RequireTLS", Effect = "Deny", Principal = "*", Action = "s3:*"
      Resource  = [aws_s3_bucket.lake.arn, "${aws_s3_bucket.lake.arn}/*"]
      Condition = { Bool = { "aws:SecureTransport" = "false" } }
    }]
  })
}
resource "aws_s3_bucket_lifecycle_configuration" "lake" {
  bucket = aws_s3_bucket.lake.id
  rule {
    id     = "abandoned-multipart"
    status = "Enabled"
    filter { prefix = "" }
    abort_incomplete_multipart_upload { days_after_initiation = 1 }
    # No object expiration: DuckLake owns file/snapshot cleanup.
  }
}
resource "aws_ecr_repository" "images" {
  for_each             = toset(["worker", "dbt"])
  name                 = "duckfleet-dev/${each.key}"
  image_tag_mutability = "IMMUTABLE"
  force_delete         = false
  image_scanning_configuration { scan_on_push = true }
  encryption_configuration { encryption_type = "AES256" }
}
resource "aws_iam_role" "worker" {
  name = "duckfleet-dev-worker"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow", Principal = { Service = "pods.eks.amazonaws.com" }
      Action = ["sts:AssumeRole", "sts:TagSession"]
      Condition = { StringEquals = {
        "aws:RequestTag/eks-cluster-arn"            = data.aws_eks_cluster.dev.arn
        "aws:RequestTag/kubernetes-namespace"       = local.namespace
        "aws:RequestTag/kubernetes-service-account" = "duckfleet-worker"
      } }
    }]
  })
}
resource "aws_iam_role_policy" "lake" {
  name = "duckfleet-lake-only"
  role = aws_iam_role.worker.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:AbortMultipartUpload", "s3:ListMultipartUploadParts"]
        Resource = ["${aws_s3_bucket.lake.arn}/lake/*"]
      },
      {
        Effect    = "Allow", Action = ["s3:ListBucket"], Resource = [aws_s3_bucket.lake.arn]
        Condition = { StringLike = { "s3:prefix" = ["lake/*"] } }
      },
      { Effect = "Allow", Action = ["s3:GetBucketLocation"], Resource = [aws_s3_bucket.lake.arn] }
    ]
  })
}
resource "aws_eks_pod_identity_association" "worker" {
  cluster_name         = data.aws_eks_cluster.dev.name
  namespace            = local.namespace
  service_account      = "duckfleet-worker"
  role_arn             = aws_iam_role.worker.arn
  disable_session_tags = false
}

output "namespace" { value = local.namespace }
output "bucket" { value = aws_s3_bucket.lake.id }
output "image_repositories" { value = { for name, repo in aws_ecr_repository.images : name => repo.repository_url } }
output "worker_role_arn" { value = aws_iam_role.worker.arn }
