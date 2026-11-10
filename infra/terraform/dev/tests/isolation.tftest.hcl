mock_provider "aws" {}
run "isolated_dev_resources" {
  command = plan
  assert {
    condition     = aws_s3_bucket.lake.bucket == "duckfleet-dev-797781631727-us-east-2" && !aws_s3_bucket.lake.force_destroy
    error_message = "DuckFleet must own its bucket and retain its data."
  }
  assert {
    condition     = toset([for repo in aws_ecr_repository.images : repo.name]) == toset(["duckfleet-dev/worker", "duckfleet-dev/dbt"])
    error_message = "Only the two DuckFleet repositories are owned here."
  }
  assert {
    condition     = alltrue([for repo in aws_ecr_repository.images : repo.image_tag_mutability == "IMMUTABLE" && !repo.force_delete])
    error_message = "Published revisions must be immutable and retained."
  }
}
