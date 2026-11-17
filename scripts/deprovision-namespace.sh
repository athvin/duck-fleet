#!/usr/bin/env bash
# Removes a provisioned namespace whole: compute, catalog, credentials, and the
# bundled PostgreSQL's volume go with it. The lake prefix in the bucket does
# not — objects belong to the operator's storage, and the command that deletes
# them is printed rather than run, because deleting data is said out loud here
# the same way the restore guard says it.
#
#   DUCKFLEET_CONFIRM_DEPROVISION=yes DUCKFLEET_KUBE_CONTEXT=<ctx> \
#     scripts/deprovision-namespace.sh <namespace>
set -euo pipefail
ns="${1:?usage: deprovision-namespace.sh <namespace>}"
context="${DUCKFLEET_KUBE_CONTEXT:?set DUCKFLEET_KUBE_CONTEXT}"
if [ "${DUCKFLEET_CONFIRM_DEPROVISION:-}" != yes ]; then
  echo "set DUCKFLEET_CONFIRM_DEPROVISION=yes: this removes $ns — its compute," \
    "catalog, credentials, and PostgreSQL volume" >&2
  exit 64
fi
# The primary namespace dies only with a second, different confession.
if [ "$ns" = "${DUCKFLEET_NAMESPACE:-duckfleet-dev}" ] \
  && [ "${DUCKFLEET_DEPROVISION_PRIMARY:-}" != yes ]; then
  echo "$ns is the primary namespace; set DUCKFLEET_DEPROVISION_PRIMARY=yes as well" >&2
  exit 64
fi
kubectl --context "$context" delete namespace "$ns" --wait=true --timeout=300s
echo "removed namespace $ns; its lake prefix survives in the bucket until you run:"
echo "  aws s3 rm --recursive s3://<bucket>/$ns/"
