#!/usr/bin/env bash
# Operator entry point, not an agent-side workaround for the AWS mutation guard.
set -euo pipefail
source "$(dirname "$0")/common.sh"
cd "$ROOT"
assert_cluster
[[ -f artifacts/aws/values.json ]] || { echo 'Publish images first' >&2; exit 1; }
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
helm upgrade --install duckfleet infra/helm/duckfleet --kube-context "$KUBE_CONTEXT" \
  --namespace "$NAMESPACE" -f artifacts/aws/values.json --set suspended=false --wait --timeout 15m
