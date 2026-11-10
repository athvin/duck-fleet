#!/usr/bin/env bash
# Prove the effective EKS identity before creating workloads. No impersonation.
set -euo pipefail
source "$(dirname "$0")/common.sh"
assert_cluster
for target in deployments.apps statefulsets.apps jobs.batch secrets; do
  "${KUBE[@]}" auth can-i create "$target" --quiet || { echo "Missing deploy permission: $target" >&2; exit 1; }
done
for target in roles.rbac.authorization.k8s.io rolebindings.rbac.authorization.k8s.io resourcequotas; do
  answer=$("${KUBE[@]}" auth can-i create "$target") || [[ "$?" == 1 && "$answer" == no ]]
  if [[ "$answer" != no ]]; then
    echo "Unexpected boundary mutation permission: $target" >&2; exit 1
  fi
done
for namespace in ci kube-system; do
  answer=$(kubectl --context "$KUBE_CONTEXT" -n "$namespace" auth can-i get secrets) || [[ "$?" == 1 && "$answer" == no ]]
  if [[ "$answer" != no ]]; then
    echo "Unexpected secret access outside DuckFleet: $namespace" >&2; exit 1
  fi
done
printf '%s\n' 'Effective EKS identity is scoped to DuckFleet application resources'
