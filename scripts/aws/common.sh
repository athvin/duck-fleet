#!/usr/bin/env bash
# Sourced by operator-run AWS scripts. Never use the caller's default context.
set -euo pipefail
ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
# The deploy target is configuration, named in exactly one file so that moving it is
# a change there rather than a hunt through scripts and a workflow.
# shellcheck source=scripts/aws/target.env
source "$ROOT/scripts/aws/target.env"
# GitHub OIDC supplies short-lived environment credentials, not an SSO profile.
if [[ "${GITHUB_ACTIONS:-false}" != true && -n "${DUCKFLEET_AWS_PROFILE}" ]]; then
  export AWS_PROFILE=${AWS_PROFILE:-$DUCKFLEET_AWS_PROFILE}
fi
export AWS_REGION=$DUCKFLEET_REGION
export AWS_DEFAULT_REGION=$DUCKFLEET_REGION
NAMESPACE=$DUCKFLEET_NAMESPACE
# Still honours KUBE_CONTEXT, which the operator documentation and existing runbooks
# already use.
KUBE_CONTEXT=${KUBE_CONTEXT:-$DUCKFLEET_KUBE_CONTEXT}
KUBE=(kubectl --context "$KUBE_CONTEXT" --namespace "$NAMESPACE")
assert_account() {
  local account
  account=$(aws sts get-caller-identity --query Account --output text)
  [[ "$account" == "$DUCKFLEET_ACCOUNT" ]] || { echo 'Wrong AWS account' >&2; exit 1; }
}
assert_cluster() {
  : "${DUCKFLEET_CLUSTER:?Set DUCKFLEET_CLUSTER to the deployment cluster}"
  : "${KUBE_CONTEXT:?Set DUCKFLEET_KUBE_CONTEXT or KUBE_CONTEXT explicitly}"
  assert_account
  local expected actual
  expected=$(aws eks describe-cluster --name "$DUCKFLEET_CLUSTER" --query cluster.endpoint --output text)
  actual=$(kubectl config view --minify --context "$KUBE_CONTEXT" -o jsonpath='{.clusters[0].cluster.server}')
  [[ "$expected" == "$actual" ]] || {
    echo "Context $KUBE_CONTEXT is not the $DUCKFLEET_CLUSTER endpoint" >&2; exit 1;
  }
  # On a cluster DuckFleet does not own, the namespace is created for it and the label
  # is how we know that happened. A cluster we own has nothing to wait for, so the
  # requirement is configuration rather than an assumption.
  if [[ "$DUCKFLEET_REQUIRE_BOOTSTRAP" == true ]]; then
    [[ $("${KUBE[@]}" get namespace "$NAMESPACE" -o jsonpath='{.metadata.labels.app\.kubernetes\.io/part-of}') == duckfleet ]] || {
      echo 'Apply the shared-platform DuckFleet bootstrap first' >&2; exit 1;
    }
  fi
}
