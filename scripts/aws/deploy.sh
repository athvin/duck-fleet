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
# Helm applies the release; convergence is waited for separately. `helm --wait` only
# ever reports "context deadline exceeded", which says nothing about which pod failed
# or why, and spends the whole timeout before saying even that.
helm upgrade --install duckfleet infra/helm/duckfleet --kube-context "$KUBE_CONTEXT" \
  --namespace "$NAMESPACE" -f artifacts/aws/values.json --set suspended=false --timeout 5m
python3 scripts/aws/wait-rollout.py --namespace "$NAMESPACE" --context "$KUBE_CONTEXT"

# Isolated compute is configured by the release but granted outside it, by the
# ownership boundary an operator applies — and this identity cannot read RBAC to check,
# which is the same scoping that makes the separation worth having. So the question is
# put to the gateway, which asks the API server at startup and reports the answer.
#
# Read from inside the pod so the bearer token stays in the environment the kubelet
# gave it and never reaches a CI log.
if python3 -c "
import sys, yaml
values = yaml.safe_load(open('infra/helm/duckfleet/values.yaml'))
sys.exit(0 if (values.get('isolatedCompute') or {}).get('enabled') else 1)
"; then
  reported=$("${KUBE[@]}" exec deployment/server -- sh -c 'curl -sS \
    -H "Authorization: Bearer $DUCKFLEET_TOKEN" http://localhost:8080/v1/workers' \
    | python3 -c 'import json,sys; print(json.load(sys.stdin)["isolated_compute"])')
  if [[ "$reported" != True ]]; then
    # A warning rather than a failure: the platform is serving, and `isolated` is
    # refused with a reason instead of failing a caller's statement when a pod could
    # not be created. But a release that ships the feature switched off by a missing
    # grant should say so rather than look deployed.
    echo "WARNING: this release enables isolated compute but the gateway is not" >&2
    echo "permitted to create pods. Apply the ownership boundary with an" >&2
    echo "administrative profile: bash scripts/aws/apply-boundary.sh (#39)" >&2
  else
    printf '%s\n' 'isolated compute is enabled and the gateway holds the pod grant'
  fi
fi
