#!/usr/bin/env bash
# Establishes the DuckFleet ownership boundary. Operator-run, once per cluster.
#
# This is the one thing DuckFleet cannot do for itself. Everything the deploy installs
# is an application object the deploy identity may create; the boundary is the
# namespace it installs into, the identity the gateway runs as, the grant that lets it
# create session pods, and the quota that caps them. Granting a capability is a
# different act from deploying a release, and keeping them apart is precisely what
# lets scripts/aws/check-access.sh keep asserting that the deploy can never widen its
# own permissions.
#
# Needs cluster admin, which the deploy identity deliberately is not, so run it with an
# administrative profile rather than the one in target.env:
#
#   AWS_PROFILE=<admin> bash scripts/aws/apply-boundary.sh
#
# infra/README.md names the profile for each target.
set -euo pipefail
ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
# This script is what satisfies the bootstrap requirement, so it cannot assert it:
# the namespace label it checks for is created below.
DUCKFLEET_REQUIRE_BOOTSTRAP=false
source "$ROOT/scripts/aws/common.sh"
cd "$ROOT"
assert_cluster

# Checked before anything is applied, and reported as what it is. Running this with
# the deploy profile fails on the first Role otherwise, after the namespace and quota
# have already been created — a half-applied boundary that reads like a bug.
if [[ "$("${KUBE[@]}" auth can-i create roles.rbac.authorization.k8s.io)" != yes ]]; then
  cat >&2 <<EOF
This needs cluster admin, and $KUBE_CONTEXT cannot create Roles.

The DuckFleet deploy identity is scoped so that it cannot — check-access.sh asserts
that and fails the deploy if it ever changes. Re-run with an administrative profile.
EOF
  exit 1
fi

# Checked before rendering, because a node pool pointing at a NodeClass that does not
# exist applies cleanly and then provisions nothing — a pool that looks established and
# silently cannot place a pod.
if ! "${KUBE[@]}" get nodeclass.eks.amazonaws.com "$DUCKFLEET_NODE_CLASS" > /dev/null 2>&1; then
  echo "No NodeClass named $DUCKFLEET_NODE_CLASS on $DUCKFLEET_CLUSTER. Available:" >&2
  kubectl --context "$KUBE_CONTEXT" get nodeclass.eks.amazonaws.com \
    -o custom-columns=NAME:.metadata.name --no-headers >&2 || true
  echo 'Set DUCKFLEET_NODE_CLASS in scripts/aws/target.env to one of these.' >&2
  exit 1
fi

boundary=$(python3 scripts/aws/render_boundary.py "$NAMESPACE" "$DUCKFLEET_NODE_CLASS")
cat <<EOF
Applying the DuckFleet ownership boundary
  cluster    $DUCKFLEET_CLUSTER
  namespace  $NAMESPACE
  node class $DUCKFLEET_NODE_CLASS
$(printf '%s\n' "$boundary" | python3 -c "
import sys, yaml
for document in yaml.safe_load_all(sys.stdin):
    print(f\"  {document['kind'].lower():<13}{document['metadata']['name']}\")
")

Idempotent, and additive: nothing here removes or narrows an existing object.
EOF
[[ "${DUCKFLEET_ASSUME_YES:-false}" == true ]] || {
  read -r -p 'Continue? [y/N] ' reply
  [[ "$reply" == [yY] ]] || { echo 'Aborted'; exit 1; }
}

printf '%s\n' "$boundary" | kubectl --context "$KUBE_CONTEXT" apply -f - > /dev/null

# Verified by reading it back, rather than trusting that apply reported success. The
# grant is the whole point of this script, so an apply that silently matched an
# existing narrower Role would be the one failure worth catching.
[[ $("${KUBE[@]}" get namespace "$NAMESPACE" \
  -o jsonpath='{.metadata.labels.app\.kubernetes\.io/part-of}') == duckfleet ]]
"${KUBE[@]}" get resourcequota duckfleet > /dev/null
# Read as JSON and checked in Python rather than with a jsonpath filter: the grant is
# the point of this script, so the check that it arrived intact should not rest on
# kubectl's reduced jsonpath dialect.
"${KUBE[@]}" get role duckfleet-isolated-pods -o json | python3 -c "
import json, sys
rules = json.load(sys.stdin)['rules']
granted = {verb for rule in rules if 'pods' in rule['resources'] for verb in rule['verbs']}
missing = {'create', 'get', 'list', 'watch', 'delete'} - granted
if missing:
    raise SystemExit(f'the pod grant is missing {sorted(missing)}; it has {sorted(granted)}')
"
# The node pool is cluster-scoped, so it is read without the namespace. Reported rather
# than merely checked: whether it can provision the largest worker the release asks for
# is the question this pool exists to answer, and `kubectl get` says nothing about it.
kubectl --context "$KUBE_CONTEXT" get nodepool.karpenter.sh duckfleet -o json | python3 -c "
import json, sys
spec = json.load(sys.stdin)['spec']
sizes = next(r['values'] for r in spec['template']['spec']['requirements']
             if r['key'].endswith('instance-size'))
taints = [f\"{t['key']}={t['value']}:{t['effect']}\" for t in spec['template']['spec']['taints']]
print(f\"node pool: sizes {sorted(sizes)}, cpu ceiling {spec['limits']['cpu']}, taints {taints}\")
"

# The gateway asks the API server about its own access once, at startup, so a grant
# applied under a running gateway is not noticed until something replaces it. Restarted
# here rather than left for the next deploy, which would make this script appear to
# have done nothing. Skipped when there is no release yet, which is the normal order.
if "${KUBE[@]}" get deployment server > /dev/null 2>&1; then
  "${KUBE[@]}" rollout restart deployment/server > /dev/null
  "${KUBE[@]}" rollout status deployment/server --timeout=300s
fi

# Where the gateway's isolated-compute egress rule must let it reach the API server,
# recorded where the deploy can read it — the deploy identity is scoped to its own
# namespace and cannot read `default`. Both the Service ClusterIP and the endpoints it
# resolves to go in, because which one the egress policy matches is CNI-dependent and
# the two clusters this runs on disagree: kind's Calico matches the rewritten endpoint
# (the node IP, #32), while EKS's VPC CNI matches the pre-rewrite ClusterIP — a rule
# naming only the endpoints there blocks the gateway's call to the ClusterIP and the
# pod create times out, exactly as DNS to 172.20.0.10 only works because its rule
# names the Service CIDR. Recording both on the service port covers either CNI. The
# deploy consumes this ConfigMap through deploy_values.py; refreshed on every apply.
service=$(kubectl --context "$KUBE_CONTEXT" -n default get service kubernetes -o json)
api=$(kubectl --context "$KUBE_CONTEXT" -n default get endpoints kubernetes -o json)
python3 - "$NAMESPACE" "$service" "$api" <<'PY' | "${KUBE[@]}" apply -f -
import json, sys
namespace, svc, described = sys.argv[1], json.loads(sys.argv[2]), json.loads(sys.argv[3])
subsets = described.get("subsets", [])
cidrs = {f"{a['ip']}/32" for s in subsets for a in s.get("addresses", [])}
ports = {p["port"] for s in subsets for p in s.get("ports", [])}
# The ClusterIP on the Service's own port: what the gateway actually connects to, and
# what EKS's VPC CNI evaluates the egress rule against.
cidrs.add(f"{svc['spec']['clusterIP']}/32")
ports.update(p["port"] for p in svc["spec"].get("ports", []))
cidrs, ports = sorted(cidrs), sorted(ports)
if not cidrs or not ports:
    raise SystemExit("the kubernetes Service resolves to no endpoints")
print(json.dumps({
    "apiVersion": "v1", "kind": "ConfigMap",
    "metadata": {"name": "duckfleet-api-egress", "namespace": namespace},
    "data": {"cidrs": json.dumps(cidrs), "ports": json.dumps(ports)},
}))
PY
printf '%s\n' 'PASS: the boundary is established; the deploy may enable isolated compute'
cat <<'EOF'

The gateway asks the API server whether it holds this grant and reports the answer on
GET /v1/workers as `isolated_compute`, so removing the RoleBinding is a supported way
to turn the feature off — it degrades to a refusal rather than to a failure.
EOF
