// What a mocked plan can actually establish: the shape of the request, the scope of
// the grants, and the cost-bearing choices. It cannot establish that the cluster comes
// up — only an apply does that, and the apply is an operator's.
mock_provider "aws" {
  // A mocked data source returns nothing by default, and the module slices two zones
  // out of this list, so the mock has to supply them.
  mock_data "aws_availability_zones" {
    defaults = {
      names = ["us-east-2a", "us-east-2b", "us-east-2c"]
    }
  }
}

run "cluster_is_auto_mode_in_its_own_network" {
  command = plan

  assert {
    condition     = aws_eks_cluster.main.access_config[0].authentication_mode == "API"
    error_message = "Access entries are what allow the namespace-scoped CI grant."
  }
  assert {
    condition     = aws_eks_cluster.main.compute_config[0].enabled && aws_eks_cluster.main.storage_config[0].block_storage[0].enabled
    error_message = "Auto Mode must manage compute and storage, or nodes never appear."
  }
  assert {
    condition     = toset(aws_eks_cluster.main.compute_config[0].node_pools) == toset(["general-purpose", "system"])
    error_message = "Without the system pool there is nowhere for managed add-ons to run."
  }
  assert {
    condition     = aws_vpc.main.enable_dns_support && aws_vpc.main.enable_dns_hostnames
    error_message = "Nodes cannot register with the cluster unless the VPC resolves DNS."
  }
  assert {
    condition     = length(aws_subnet.public) == 2
    error_message = "EKS requires subnets in at least two availability zones."
  }
}

run "no_nat_gateway_and_s3_stays_off_the_internet" {
  command = plan

  // A NAT Gateway is about $32/month per zone, more than the control plane it would
  // serve. Public subnets plus the free Internet Gateway are the deliberate trade.
  assert {
    condition     = alltrue([for subnet in aws_subnet.public : subnet.map_public_ip_on_launch])
    error_message = "Nodes in public subnets need public addresses to reach ECR without a NAT."
  }
  assert {
    condition     = aws_vpc_endpoint.s3.vpc_endpoint_type == "Gateway"
    error_message = "An interface endpoint for S3 would cost money; the gateway kind is free."
  }
}

run "ci_access_is_confined_to_one_namespace" {
  command = plan

  assert {
    condition     = aws_eks_access_policy_association.ci.access_scope[0].type == "namespace"
    error_message = "Cluster-wide access would make the deploy identity an admin of everything."
  }
  assert {
    condition     = aws_eks_access_policy_association.ci.access_scope[0].namespaces == toset(["duckfleet-dev"])
    error_message = "CI must reach the DuckFleet namespace and no other."
  }
  # AmazonEKSAdminPolicy maps to the `admin` ClusterRole, which may create Roles and
  # RoleBindings. scripts/aws/check-access.sh fails the deploy when the identity can,
  # so admin here would abort the first deploy into a cluster this module had just
  # built correctly — and it would let the deploy widen its own grant, which is the
  # reason the ownership boundary is applied separately in the first place.
  # The whole ARN, not its suffix. EKS access policies live under a different ARN
  # namespace from IAM policies, and this previously named an `iam::aws:policy/` ARN
  # that does not resolve — so the association would have failed on the first apply
  # while reading as correct.
  assert {
    condition     = aws_eks_access_policy_association.ci.policy_arn == "arn:aws:eks::aws:cluster-access-policy/AmazonEKSEditPolicy"
    error_message = "CI must hold edit, not admin: anything that can create Roles fails check-access.sh."
  }
}

run "worker_identity_reaches_lake_and_result_prefixes_only" {
  command = plan

  // The worker runs user SQL, so its credentials are the ones worth bounding.
  assert {
    condition = alltrue([
      for statement in jsondecode(aws_iam_role_policy.worker.policy).Statement :
      toset(statement.Action) == toset(["s3:PutObject"])
      if statement.Resource == "arn:aws:s3:::duckfleet-dev-797781631727-us-east-2/results/*"
    ])
    error_message = "Worker artifact access must not grant result reads or deletes."
  }
  assert {
    condition     = aws_eks_pod_identity_association.worker.service_account == "duckfleet-worker"
    error_message = "Only the worker service account may assume the lake role."
  }
  assert {
    condition = alltrue([
      for statement in jsondecode(aws_iam_role_policy.worker.policy).Statement :
      can(regex("duckfleet-dev-797781631727-us-east-2", jsonencode(statement.Resource)))
    ])
    error_message = "The worker role must not reach outside DuckFleet's own bucket."
  }
  assert {
    condition = alltrue([
      for statement in jsondecode(aws_iam_role_policy.worker.policy).Statement :
      can(statement.Condition.StringLike["s3:prefix"])
      if can(regex("ListBucket", jsonencode(statement.Action)))
    ])
    error_message = "Listing must be prefix-conditioned, or a worker can enumerate the bucket."
  }
  assert {
    condition = alltrue([
      for name, policy in local.assume :
      can(regex("sts:TagSession", policy)) == (name != "node")
    ])
    error_message = "EKS and Pod Identity principals pass session tags; EC2 does not."
  }
}

run "reader_identity_cannot_write_lake_or_results" {
  command = plan

  assert {
    condition     = !can(regex("PutObject|DeleteObject|AbortMultipart", jsonencode(aws_iam_role_policy.reader.policy)))
    error_message = "Reader lake access must not include PutObject, DeleteObject, or multipart writes."
  }
  assert {
    condition = alltrue([
      for statement in jsondecode(aws_iam_role_policy.reader.policy).Statement :
      !can(regex("/results/", statement.Resource))
    ])
    error_message = "Readers receive no result-write or result-read credentials."
  }
  assert {
    condition = alltrue([
      for statement in jsondecode(aws_iam_role_policy.reader.policy).Statement :
      can(statement.Condition.StringLike["s3:prefix"])
      if can(regex("ListBucket", jsonencode(statement.Action)))
    ])
    error_message = "Reader listing must remain bounded to the lake prefix."
  }
  assert {
    condition     = aws_eks_pod_identity_association.reader.service_account == "duckfleet-reader"
    error_message = "Only the separate reader service account may assume the read-only role."
  }
}

run "result_identity_cannot_read_lake_or_write_results" {
  command = plan
  assert {
    condition = alltrue([
      for statement in jsondecode(aws_iam_role_policy.gateway_results.policy).Statement :
      statement.Resource == "arn:aws:s3:::duckfleet-dev-797781631727-us-east-2/results/*" &&
      toset(statement.Action) == toset(["s3:GetObject", "s3:DeleteObject"])
    ])
    error_message = "Gateway result identity must have only result-prefix read/delete."
  }
  assert {
    condition     = toset(keys(aws_eks_pod_identity_association.gateway_results)) == toset(["duckfleet-gateway", "duckfleet-results"])
    error_message = "Only gateway service accounts may assume the result-reader role."
  }
}
