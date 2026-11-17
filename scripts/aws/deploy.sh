#!/usr/bin/env bash
# Operator entry point, not an agent-side workaround for the AWS mutation guard.
set -euo pipefail
source "$(dirname "$0")/common.sh"
cd "$ROOT"
assert_cluster
[[ -f artifacts/aws/values.json ]] || { echo 'Publish images first' >&2; exit 1; }
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
# Isolated compute needs a namespaced Role, and this identity cannot create one —
# check-access.sh asserts exactly that and fails the deploy if it ever can. Catch an
# inconsistent release here, naming the reason, rather than letting helm fail partway
# through applying it on a forbidden object.
if helm template duckfleet infra/helm/duckfleet --kube-context "$KUBE_CONTEXT" \
     --namespace "$NAMESPACE" -f artifacts/aws/values.json | grep -q '^kind: Role$'; then
  "${KUBE[@]}" auth can-i create roles.rbac.authorization.k8s.io --quiet || {
    echo 'This release enables isolated compute, which needs a namespaced Role the' >&2
    echo 'deploy identity is not permitted to create. See duck-fleet#39; until that' >&2
    echo 'grant exists the feature runs in the local Kubernetes harness only.' >&2
    exit 1; }
fi
# Helm applies the release; convergence is waited for separately. `helm --wait` only
# ever reports "context deadline exceeded", which says nothing about which pod failed
# or why, and spends the whole timeout before saying even that.
helm upgrade --install duckfleet infra/helm/duckfleet --kube-context "$KUBE_CONTEXT" \
  --namespace "$NAMESPACE" -f artifacts/aws/values.json --set suspended=false --timeout 5m
python3 scripts/aws/wait-rollout.py --namespace "$NAMESPACE" --context "$KUBE_CONTEXT"
