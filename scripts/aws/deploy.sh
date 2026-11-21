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
python3 scripts/aws/deploy_values.py "$KUBE_CONTEXT"
# A policy that cannot reach the cluster's resolver presents as an unreachable
# catalog, which cost six deploys to diagnose. Fail here instead.
python3 scripts/aws/check-dns-egress.py "$DUCKFLEET_CLUSTER" "$AWS_REGION"
# And that the gateway may reach the API server, when isolated compute is enabled. A
# rule written against the Service address matches nothing once kube-proxy has
# rewritten the destination; see #32.
python3 scripts/aws/check-api-egress.py "$KUBE_CONTEXT"
# Require the existing EKS Auto Mode storage class, never change ER cluster objects.
[[ $("${KUBE[@]}" get storageclass gp3 -o jsonpath='{.provisioner}') == ebs.csi.eks.amazonaws.com ]] || {
  echo 'Expected Auto Mode gp3 storage class is unavailable' >&2; exit 1;
}
if [[ -z $("${KUBE[@]}" get secret duckfleet-dev --ignore-not-found -o name) ]]; then
  python3 - <<'PY' | "${KUBE[@]}" create -f -
import json,secrets
password=secrets.token_hex(32)
dsn=f'host=postgres port=5432 user=duckfleet password={password}'
print(json.dumps({'apiVersion':'v1','kind':'Secret','metadata':{'name':'duckfleet-dev','namespace':'duckfleet-dev'},'type':'Opaque','stringData':{'POSTGRES_PASSWORD':password,'DUCKFLEET_TOKEN':secrets.token_hex(32),'CATALOG_DSN':dsn+' dbname=lake','PLATFORM_DSN':dsn+' dbname=platform'}}))
PY
fi
# Helm applies the release; convergence is waited for separately. `helm --wait` only
# ever reports "context deadline exceeded", which says nothing about which pod failed
# or why, and spends the whole timeout before saying even that.
migration_run=$(python3 -c 'import uuid; print(uuid.uuid4().hex)')
helm upgrade --install duckfleet infra/helm/duckfleet --kube-context "$KUBE_CONTEXT" \
  --namespace "$NAMESPACE" -f artifacts/aws/values.json --set suspended=false \
  --set-string "platformMigration.runId=$migration_run" --timeout 5m
python3 scripts/aws/wait-rollout.py --namespace "$NAMESPACE" --context "$KUBE_CONTEXT" --prune-migrations

# Whether the gateway holds the pod grant is asked and answered by the harness, not
# here: it runs in-cluster and reaches the gateway directly, where it reads
# isolated_compute on GET /v1/workers and exercises an isolated session. The deploy
# identity cannot — it holds pods create/get/list/watch/delete from the boundary but
# not pods/exec or pods/portforward, so it has no in-cluster path to the gateway, and
# putting the question here failed the deploy the moment isolated compute was on.
# aws_harness.py asserts the feature is live (#39).
