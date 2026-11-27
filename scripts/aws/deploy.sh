#!/usr/bin/env bash
# Operator entry point, not an agent-side workaround for the AWS mutation guard.
set -euo pipefail
source "$(dirname "$0")/common.sh"
cd "$ROOT"
assert_cluster
[[ -f artifacts/aws/values.json ]] || { echo 'Publish images first' >&2; exit 1; }
# The values only the live cluster can supply — isolated compute on with the API
# server's real egress address, workers pinned to the DuckFleet pool — merged into
# values.json before anything reads it. Written here rather than baked into chart
# defaults because the API server address is a fact about the cluster (#39).
bash scripts/ops.sh aws-deploy-values "$KUBE_CONTEXT" "$NAMESPACE"
if [[ "${DUCKFLEET_UPGRADE_LEGACY_DATABASE:-false}" == true ]]; then
  bash scripts/aws/upgrade-database-roles.sh
fi
bash scripts/ops.sh compute-lifecycle check kube --context "$KUBE_CONTEXT" --namespace "$NAMESPACE"
# A policy that cannot reach the cluster's resolver presents as an unreachable
# catalog, which cost six deploys to diagnose. Fail here instead.
bash scripts/ops.sh aws-check-dns-egress "$DUCKFLEET_CLUSTER" "$AWS_REGION"
# And that the gateway may reach the API server, when isolated compute is enabled. A
# rule written against the Service address matches nothing once kube-proxy has
# rewritten the destination; see #32.
bash scripts/ops.sh aws-check-api-egress
# Require the existing EKS Auto Mode storage class, never change shared cluster objects.
[[ $("${KUBE[@]}" get storageclass gp3 -o jsonpath='{.provisioner}') == ebs.csi.eks.amazonaws.com ]] || {
  echo 'Expected Auto Mode gp3 storage class is unavailable' >&2; exit 1;
}
if [[ -z $("${KUBE[@]}" get secret duckfleet-dev --ignore-not-found -o name) ]]; then
  bash scripts/ops.sh database-roles "$NAMESPACE" | "${KUBE[@]}" create -f -
fi
"${KUBE[@]}" get secret duckfleet-dev -o json | bash scripts/ops.sh database-roles "$NAMESPACE" --check
# Helm applies the release; convergence is waited for separately. `helm --wait` only
# ever reports "context deadline exceeded", which says nothing about which pod failed
# or why, and spends the whole timeout before saying even that.
migration_run=$(bash scripts/ops.sh random-id)
helm upgrade --install duckfleet infra/helm/duckfleet --kube-context "$KUBE_CONTEXT" \
  --namespace "$NAMESPACE" -f infra/helm/duckfleet/values-aws.yaml \
  -f artifacts/aws/values.json --set suspended=false \
  --set-string "platformMigration.runId=$migration_run" --timeout 5m
bash scripts/ops.sh aws-wait-rollout --namespace "$NAMESPACE" --context "$KUBE_CONTEXT" --prune-migrations

# Whether the gateway holds the pod grant is asked and answered by the harness, not
# here: it runs in-cluster and reaches the gateway directly, where it reads
# isolated_compute on GET /v1/workers and exercises an isolated session. The deploy
# identity cannot — it holds pods create/get/list/watch/delete from the boundary but
# not pods/exec or pods/portforward, so it has no in-cluster path to the gateway, and
# putting the question here failed the deploy the moment isolated compute was on.
# The Rust AWS harness asserts the feature is live (#39).
