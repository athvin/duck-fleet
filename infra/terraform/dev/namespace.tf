# Authentication follows AWS_PROFILE; no static token or kubeconfig is stored.
provider "kubernetes" {
  host                   = data.aws_eks_cluster.dev.endpoint
  cluster_ca_certificate = base64decode(data.aws_eks_cluster.dev.certificate_authority[0].data)
  exec {
    api_version = "client.authentication.k8s.io/v1beta1"
    command     = "aws"
    args        = ["eks", "get-token", "--cluster-name", "er-dev", "--region", "us-east-2"]
  }
}
resource "kubernetes_namespace_v1" "dev" {
  metadata {
    name = local.namespace
    labels = {
      "app.kubernetes.io/part-of"          = "duckfleet"
      "pod-security.kubernetes.io/enforce" = "restricted"
      "pod-security.kubernetes.io/audit"   = "restricted"
      "pod-security.kubernetes.io/warn"    = "restricted"
    }
  }
  lifecycle { prevent_destroy = true }
}
resource "kubernetes_resource_quota_v1" "dev" {
  metadata {
    name      = "duckfleet-budget"
    namespace = kubernetes_namespace_v1.dev.metadata[0].name
  }
  spec {
    hard = {
      "requests.cpu"     = "6", "limits.cpu" = "8"
      "requests.memory"  = "8Gi", "limits.memory" = "10Gi"
      "requests.storage" = "20Gi", "persistentvolumeclaims" = "1"
      "pods"             = "10", "services.loadbalancers" = "0", "services.nodeports" = "0"
    }
  }
}
