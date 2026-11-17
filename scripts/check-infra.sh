#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
TF=${TERRAFORM:-terraform}
# Never reuse a locally initialized live backend in this credential-free gate.
validation_dir=$(mktemp -d)
trap 'rm -rf "$validation_dir"' EXIT
export TF_DATA_DIR="$validation_dir/terraform"
values="$validation_dir/values.json"
rendered="$validation_dir/rendered.yaml"
"$TF" fmt -check -recursive infra/terraform
"$TF" -chdir=infra/terraform/dev init -backend=false -input=false
"$TF" -chdir=infra/terraform/dev validate
"$TF" -chdir=infra/terraform/dev test
# The operator-applied cluster module. Validated and mock-tested here even though CI
# never applies it, so a broken cluster definition fails on a pull request rather than
# in front of whoever is running the apply.
"$TF" -chdir=infra/terraform/cluster init -backend=false -input=false
"$TF" -chdir=infra/terraform/cluster validate
"$TF" -chdir=infra/terraform/cluster test
python3 - <<'PY' > "$values"
import json
print(json.dumps({kind+'Image':'797781631727.dkr.ecr.us-east-2.amazonaws.com/duckfleet-dev/'+kind+'@sha256:'+'a'*64 for kind in ['worker','dbt']}))
PY
helm lint infra/helm/duckfleet --namespace duckfleet-dev -f "$values"
helm template duckfleet infra/helm/duckfleet --namespace duckfleet-dev -f "$values" > "$rendered"
python3 scripts/check-chart.py "$rendered"
# A CPU request may sit below its limit so a class fits constrained capacity, but a
# request above the limit would schedule a worker onto a node that cannot honour the
# limit its cgroup assertion depends on. Prove that is rejected, not merely rendered.
overcommitted="$validation_dir/overcommitted.yaml"
if helm template duckfleet infra/helm/duckfleet --namespace duckfleet-dev -f "$values" \
     --set-json 'workerCpuRequests={"medium":4}' > "$overcommitted" 2>/dev/null \
   && python3 scripts/check-chart.py "$overcommitted" > /dev/null 2>&1; then
  echo 'A worker CPU request above its limit must fail the chart check' >&2
  exit 1
fi
echo 'PASS: a CPU request above its limit is rejected'
# The DuckFleet node pool is tainted, so the selector and the toleration only work
# together: the pin without the toleration is a fleet that never schedules, and the
# toleration without the pin lets a pod onto a neighbour's node. Both halves are
# constructed here rather than taken from the defaults — which currently pin neither,
# so relying on them would test whichever state happened to be configured and silently
# stop testing anything when it changed. That is exactly what it did.
PINNED='nodeSelector={"kubernetes.io/os":"linux","eks.amazonaws.com/compute-type":"auto","karpenter.sh/nodepool":"duckfleet"}'
TOLERATED='tolerations=[{"key":"duckfleet.io/dedicated","operator":"Equal","value":"true","effect":"NoSchedule"}]'
UNPINNED='nodeSelector={"kubernetes.io/os":"linux","eks.amazonaws.com/compute-type":"auto"}'
mismatched="$validation_dir/mismatched.yaml"
for half in "pinned without a toleration:$PINNED;tolerations=[]" \
            "tolerated without the pin:$UNPINNED;$TOLERATED"; do
  description=${half%%:*}
  settings=${half#*:}
  if helm template duckfleet infra/helm/duckfleet --namespace duckfleet-dev -f "$values" \
       --set-json "${settings%%;*}" --set-json "${settings#*;}" > "$mismatched" 2>/dev/null \
     && python3 scripts/check-chart.py "$mismatched" > /dev/null 2>&1; then
    echo "The DuckFleet node pool $description must fail the chart check" >&2
    exit 1
  fi
done
echo 'PASS: the node pool pin and its toleration are rejected unless they agree'
# And the paired state renders, so the gate is not simply refusing everything.
paired="$validation_dir/paired.yaml"
helm template duckfleet infra/helm/duckfleet --namespace duckfleet-dev -f "$values" \
  --set-json "$PINNED" --set-json "$TOLERATED" --set-json 'workerCpuRequests=null' > "$paired"
python3 scripts/check-chart.py "$paired" > /dev/null
echo 'PASS: pinning the node pool and tolerating its taint together is accepted'
# Isolated compute is off by default, so the assertions covering it only run against
# a render that turns it on. Both states are checked: the default must keep producing
# a gateway with no Kubernetes token at all.
isolated="$validation_dir/isolated.yaml"
helm template duckfleet infra/helm/duckfleet --namespace duckfleet-dev -f "$values" \
  --set isolatedCompute.enabled=true \
  --set 'isolatedCompute.apiServerCidrs[0]=10.96.0.1/32' > "$isolated"
python3 scripts/check-chart.py "$isolated" > /dev/null
echo 'PASS: isolated compute renders its pod template and API egress, and no RBAC'
# The grant and the quota live outside the release because the deploy identity cannot
# create either. Render the boundary for the same namespace the chart is restricted to
# and prove the two do not overlap: anything in both would be applied twice, by two
# identities, with the operator's copy silently losing to the release's.
boundary="$validation_dir/boundary.yaml"
python3 scripts/aws/render_boundary.py duckfleet-dev > "$boundary"
python3 - "$isolated" "$boundary" <<'PY'
import sys, yaml
def identify(path):
    with open(path) as handle:
        return {(d['kind'], d['metadata']['name'])
                for d in yaml.safe_load_all(handle) if d}
release, boundary = identify(sys.argv[1]), identify(sys.argv[2])
both = release & boundary
assert not both, f'claimed by both the release and the boundary: {sorted(both)}'
# The gateway's identity is the hinge: the chart must create it so its pod can start,
# and the boundary must bind it so it can do anything. One without the other is a
# gateway that either cannot run or cannot work.
assert ('ServiceAccount', 'duckfleet-gateway') in release
assert ('RoleBinding', 'duckfleet-isolated-pods') in boundary
PY
echo 'PASS: the release and the ownership boundary are disjoint and meet at the gateway identity'
# Enabling it without saying where the API server is must fail the render. Defaulting
# that would mean opening egress to reach an address nobody declared.
if helm template duckfleet infra/helm/duckfleet --namespace duckfleet-dev -f "$values" \
     --set isolatedCompute.enabled=true > /dev/null 2>&1; then
  echo 'Isolated compute without apiServerCidrs must fail the render' >&2
  exit 1
fi
echo 'PASS: isolated compute without a declared API server address is rejected'
# Exposure follows the same shape: off by default — the default render above already
# proved no Ingress — and when enabled, every fact belongs to the operator. The full
# configuration must render and satisfy the TLS and NetworkPolicy invariants.
exposed="$validation_dir/exposed.yaml"
helm template duckfleet infra/helm/duckfleet --namespace duckfleet-dev -f "$values" \
  --set exposure.enabled=true \
  --set exposure.host=duckfleet.example.com \
  --set exposure.tlsSecretName=duckfleet-tls \
  --set exposure.controllerNamespace=ingress-nginx > "$exposed"
python3 scripts/check-chart.py "$exposed" > /dev/null
echo 'PASS: exposure renders an Ingress that terminates TLS and opens only the controller door'
# Missing any of the three operator facts must fail the render, not default.
for missing in tlsSecretName controllerNamespace host; do
  args=(--set exposure.enabled=true)
  [ "$missing" != host ] && args+=(--set exposure.host=duckfleet.example.com)
  [ "$missing" != tlsSecretName ] && args+=(--set exposure.tlsSecretName=duckfleet-tls)
  [ "$missing" != controllerNamespace ] && args+=(--set exposure.controllerNamespace=ingress-nginx)
  if helm template duckfleet infra/helm/duckfleet --namespace duckfleet-dev -f "$values" \
       "${args[@]}" > /dev/null 2>&1; then
    echo "Exposure without $missing must fail the render" >&2
    exit 1
  fi
done
echo 'PASS: exposure without its host, certificate secret, or controller namespace is rejected'
for script in scripts/*.sh scripts/aws/*.sh scripts/ci/*.sh; do bash -n "$script"; done
# The local Kubernetes harness renders the real chart, so a chart change that moves
# something out from under the transform is caught here rather than in a cluster.
for expect in \
  'image,cpu-request,s3-auth,s3-path,node-selector,storage-class,object-store-port:--no-hooks' \
  'image,cpu-request,s3-path,node-selector:--show-only templates/harness.yaml' \
  'image,cpu-request,s3-auth,s3-path,node-selector,storage-class,object-store-port,isolated-pod:--no-hooks --set isolatedCompute.enabled=true --set isolatedCompute.apiServerCidrs[0]=10.96.0.1/32'; do
  # shellcheck disable=SC2086
  helm template duckfleet infra/helm/duckfleet --namespace duckfleet-dev -f "$values" \
    ${expect#*:} | python3 scripts/kind/localize.py --expect "${expect%%:*}" > /dev/null
done
echo 'PASS: the local Kubernetes transform still matches the rendered chart'

python3 -m unittest discover -s tests -p test_aws_ci.py
