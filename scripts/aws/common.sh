#!/usr/bin/env bash
# Sourced by operator-run AWS scripts. Never use the caller's default context.
set -euo pipefail
ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
# GitHub OIDC supplies short-lived environment credentials, not an SSO profile.
if [[ "${GITHUB_ACTIONS:-false}" != true ]]; then
  export AWS_PROFILE=${AWS_PROFILE:-er-platform}
fi
export AWS_REGION=us-east-2
export AWS_DEFAULT_REGION=us-east-2
NAMESPACE=duckfleet-dev
KUBE_CONTEXT=${KUBE_CONTEXT:-er-dev-admin}
KUBE=(kubectl --context "$KUBE_CONTEXT" --namespace "$NAMESPACE")
assert_account() {
  local account
  account=$(aws sts get-caller-identity --query Account --output text)
  [[ "$account" == 797781631727 ]] || { echo 'Wrong AWS account' >&2; exit 1; }
}
assert_cluster() {
  assert_account
  local expected actual
  expected=$(aws eks describe-cluster --name er-dev --query cluster.endpoint --output text)
  actual=$(kubectl config view --minify --context "$KUBE_CONTEXT" -o jsonpath='{.clusters[0].cluster.server}')
  [[ "$expected" == "$actual" ]] || { echo 'Context is not the er-dev endpoint' >&2; exit 1; }
  [[ $("${KUBE[@]}" get namespace "$NAMESPACE" -o jsonpath='{.metadata.labels.app\.kubernetes\.io/part-of}') == duckfleet ]] || {
    echo 'Apply the shared-platform DuckFleet bootstrap first' >&2; exit 1;
  }
}
