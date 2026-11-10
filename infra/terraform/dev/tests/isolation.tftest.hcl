mock_provider "kubernetes" {}
mock_provider "aws" {
  mock_data "aws_eks_cluster" {
    defaults = {
      endpoint              = "https://example.invalid"
      certificate_authority = [{ data = "dGVzdA==" }]
      name                  = "er-dev"
      arn                   = "arn:aws:eks:us-east-2:797781631727:cluster/er-dev"
    }
  }
  mock_resource "aws_s3_bucket" {
    defaults = { arn = "arn:aws:s3:::duckfleet-dev-797781631727-us-east-2" }
  }
}
run "isolated_dev_resources" {
  command = plan
  assert {
    condition     = aws_s3_bucket.lake.bucket == "duckfleet-dev-797781631727-us-east-2" && !aws_s3_bucket.lake.force_destroy
    error_message = "DuckFleet must own its bucket and retain its data."
  }
  assert {
    condition     = aws_eks_pod_identity_association.worker.namespace == "duckfleet-dev" && aws_eks_pod_identity_association.worker.cluster_name == "er-dev"
    error_message = "Pod Identity must be scoped to DuckFleet on dev."
  }
  assert {
    condition     = jsondecode(aws_iam_role.worker.assume_role_policy).Statement[0].Condition.StringEquals["aws:RequestTag/kubernetes-service-account"] == "duckfleet-worker"
    error_message = "The trust policy must bind the service account."
  }
  assert {
    condition     = jsondecode(aws_iam_role_policy.lake.policy).Statement[0].Resource == ["arn:aws:s3:::duckfleet-dev-797781631727-us-east-2/lake/*"]
    error_message = "Workers must have no object grants outside the DuckFleet lake."
  }
}

override_resource {
  target          = aws_s3_bucket.lake
  override_during = plan
  values          = { arn = "arn:aws:s3:::duckfleet-dev-797781631727-us-east-2" }
}
