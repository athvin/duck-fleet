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
# The chart is a self-hosted warehouse for any single organization, so it must install
# for an account that is not this one. A foreign registry, bucket, and region must pass
# schema validation (the values schema pins shape — a digest-pinned image, a legal
# bucket, a real region — not this account's identity). The dev lane's own values stay
# pinned at the deploy, single-sourced in scripts/aws/target.env.
foreign="$validation_dir/foreign.yaml"
python3 - <<'PY' > "$validation_dir/foreign-values.json"
import json
reg = "123456789012.dkr.ecr.eu-west-1.amazonaws.com/acme-analytics"
print(json.dumps({
    "workerImage": f"{reg}/worker@sha256:" + "b" * 64,
    "dbtImage": f"{reg}/dbt@sha256:" + "b" * 64,
    "bucket": "acme-ducklake-eu-west-1",
    "region": "eu-west-1",
    "secretName": "acme-duckfleet",
}))
PY
helm template duckfleet infra/helm/duckfleet --namespace duckfleet-team-a \
  -f "$validation_dir/foreign-values.json" > "$foreign"
python3 scripts/check-chart.py "$foreign" > /dev/null
# A mutable tag instead of a digest must still be refused: that is the one image
# property the schema keeps, because a warehouse must pin what it runs.
if helm template duckfleet infra/helm/duckfleet --namespace duckfleet-dev \
     --set workerImage=acme/worker:latest --set dbtImage=acme/dbt:latest > /dev/null 2>&1; then
  echo 'A mutable image tag must fail schema validation' >&2
  exit 1
fi
echo 'PASS: the chart installs for a foreign account and still refuses an unpinned image'
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
# The external-catalog render: no bundled PostgreSQL object survives the toggle,
# and an address the operator named reaches the isolation policy's egress — a
# catalog the policy blocks presents as workers that never become ready, a long
# way from the value that caused it.
external="$validation_dir/external-postgres.yaml"
helm template duckfleet infra/helm/duckfleet --namespace duckfleet-dev -f "$values" \
  --set postgres.bundled=false \
  --set 'postgres.externalCidrs[0]=10.20.30.40/32' > "$external"
python3 scripts/check-chart.py "$external" > /dev/null
if grep -q 'duckfleet-postgres' "$external"; then
  echo 'postgres.bundled=false must render no bundled PostgreSQL' >&2
  exit 1
fi
grep -q '10.20.30.40/32' "$external"
echo 'PASS: an external catalog renders no bundled PostgreSQL and opens egress to the named address'
# Team namespaces widened the namespace guard from exactly duckfleet-dev to the
# duckfleet- prefix. What the guard was for must survive the widening: a render
# can never land in a neighbour's namespace on a shared cluster.
if helm template duckfleet infra/helm/duckfleet --namespace someone-elses-namespace -f "$values" > /dev/null 2>&1; then
  echo 'The chart must refuse to render into a namespace DuckFleet does not own' >&2
  exit 1
fi
helm template duckfleet infra/helm/duckfleet --namespace duckfleet-team-b -f "$values" > /dev/null
echo 'PASS: the chart renders for duckfleet-* namespaces and refuses everything else'
# The wire front, enabled: the Service carries 5432 and the invariants hold.
wire="$validation_dir/pgwire.yaml"
helm template duckfleet infra/helm/duckfleet --namespace duckfleet-dev -f "$values" \
  --set pgwire.enabled=true > "$wire"
python3 scripts/check-chart.py "$wire" > /dev/null
echo 'PASS: the wire front renders only when asked for'
# Scale-out within a class: a second small worker renders as its own Deployment and
# Service with its own recurring identity, the gateway's endpoint list names every
# replica, and the legacy objects keep the exact names and selectors an existing
# deployment already has — a Deployment selector is immutable, so index 0 changing
# it would fail every in-place upgrade.
scaled="$validation_dir/scaled.yaml"
helm template duckfleet infra/helm/duckfleet --namespace duckfleet-dev -f "$values" \
  --set workerReplicas.small=2 > "$scaled"
python3 scripts/check-chart.py "$scaled" > /dev/null
python3 - "$scaled" <<'PY'
import sys, yaml
docs = [d for d in yaml.safe_load_all(open(sys.argv[1])) if d]
workers = {d['metadata']['name']: d for d in docs
           if d['kind'] == 'Deployment' and d['metadata']['name'].startswith('worker-')}
assert set(workers) == {'worker-small', 'worker-small-1', 'worker-medium'}, sorted(workers)
identities = []
for deployment in workers.values():
    env = {e['name']: e.get('value') for e in
           deployment['spec']['template']['spec']['containers'][0]['env']}
    identities.append(env['DUCKFLEET_WORKER_ID'])
assert len(set(identities)) == len(identities), f'worker identities collide: {identities}'
assert workers['worker-small']['spec']['selector']['matchLabels'] == \
    {'app': 'duckfleet-worker', 'size': 'small'}, 'index 0 selector changed: upgrades break'
assert workers['worker-small-1']['spec']['selector']['matchLabels'].get('worker-index') == '1'
gateway = next(d for d in docs if d['kind'] == 'Deployment' and d['metadata']['name'] == 'server')
env = {e['name']: e.get('value') for e in
       gateway['spec']['template']['spec']['containers'][0]['env']}
urls = env['DUCKFLEET_WORKER_URLS'].split(',')
assert sorted(urls) == ['http://worker-medium:8080', 'http://worker-small-1:8080',
                        'http://worker-small:8080'], urls
services = {d['metadata']['name']: d['spec']['selector'] for d in docs
            if d['kind'] == 'Service' and d['metadata']['name'].startswith('worker-')}
assert all('worker-index' in selector for selector in services.values()), \
    'a worker Service without worker-index balances across siblings mid-session'
assert services['worker-small-1']['worker-index'] == '1', services['worker-small-1']
PY
echo 'PASS: a second same-size worker renders its own identity, Service, and endpoint entry'
# The #39 flip, as the AWS deploy computes it: isolated compute on, workers pinned to
# the DuckFleet pool with the toleration its taint needs, the medium worker at its
# full 2-CPU request. Chart defaults stay unpinned — the flip is a deploy-time overlay
# (scripts/aws/deploy_values.py), since the API-server CIDR is a fact about the
# cluster — so this renders that overlay shape to keep it from regressing unnoticed.
# Rendered through the real overlay deploy_values.py writes, not an approximation —
# a -f values file, so Helm's null-delete of the compute-type selector takes effect
# exactly as it does on a deploy, which --set-json would merge away.
flip="$validation_dir/isolated-pinned.yaml"
flip_overlay="$validation_dir/isolated-overlay.json"
python3 - "$flip_overlay" <<'PY'
import importlib.util, json, sys
spec = importlib.util.spec_from_file_location('dv', 'scripts/aws/deploy_values.py')
dv = importlib.util.module_from_spec(spec); spec.loader.exec_module(dv)
json.dump(dv.overlay(['10.42.3.17/32', '10.42.9.4/32'], [443]), open(sys.argv[1], 'w'))
PY
helm template duckfleet infra/helm/duckfleet --namespace duckfleet-dev -f "$values" \
  -f "$flip_overlay" > "$flip"
python3 scripts/check-chart.py "$flip" > /dev/null
python3 - "$flip" <<'PY'
import sys, yaml
docs = [d for d in yaml.safe_load_all(open(sys.argv[1])) if d]
med = next(d for d in docs if d['kind'] == 'Deployment'
           and d['metadata']['name'] == 'worker-medium')['spec']['template']['spec']
assert str(med['containers'][0]['resources']['requests']['cpu']) == '2', 'medium not at 2 CPU'
assert med['nodeSelector'].get('karpenter.sh/nodepool') == 'duckfleet', 'medium not pinned'
# The Auto Mode compute-type label must NOT ride along with the pool pin: it is added
# at node bootstrap, so Karpenter would not provision a new custom-pool node for a pod
# requiring it (the isolated session pod never scheduled until this was dropped).
assert 'eks.amazonaws.com/compute-type' not in med['nodeSelector'], 'compute-type blocks Karpenter'
assert any(t['key'] == 'duckfleet.io/dedicated' for t in med['tolerations']), 'taint not tolerated'
# The isolated session pod is the object #90 was actually about: the gateway creates it
# from this template, and it carried `compute-type: null` — an impossible label match
# Karpenter will not provision for — until the placement helper learned to drop
# null-valued keys. Assert it here on the exact manifest, not only on the worker it
# shares the helper with, and reject any null the merge might leave behind.
import json
iso = json.loads(next(d for d in docs if d['kind'] == 'ConfigMap'
                      and d['metadata']['name'] == 'duckfleet-isolated-pod')
                 ['data']['pod.json'])['spec']
assert 'eks.amazonaws.com/compute-type' not in iso['nodeSelector'], 'isolated pod carries compute-type (#90)'
assert all(v is not None for v in iso['nodeSelector'].values()), \
    f"isolated pod has a null selector value, an impossible match (#90): {iso['nodeSelector']}"
assert iso['nodeSelector'].get('karpenter.sh/nodepool') == 'duckfleet', 'isolated pod not pinned'
PY
echo 'PASS: the AWS flip render — isolated on, pinned, medium at its full 2 CPU — is valid'
# And deploy_values builds exactly that overlay from the CIDRs and ports the
# boundary recorded, which is what makes the egress rule name the endpoints rather
# than the ClusterIP a rewritten rule would miss.
python3 - <<'PY'
import importlib.util
spec = importlib.util.spec_from_file_location('dv', 'scripts/aws/deploy_values.py')
dv = importlib.util.module_from_spec(spec); spec.loader.exec_module(dv)
o = dv.overlay(['10.42.9.4/32', '10.42.3.17/32'], [443])
assert o['isolatedCompute']['enabled'] is True
assert o['isolatedCompute']['apiServerCidrs'] == ['10.42.3.17/32', '10.42.9.4/32'], o['isolatedCompute']
assert o['isolatedCompute']['apiServerPorts'] == [443]
assert o['isolatedCompute']['readySeconds'] >= 300, 'cold Karpenter provisioning needs a generous ready wait'
# The gateway must wait out a cold node launch before reading Unschedulable as a
# verdict, or it deletes the pod before Karpenter provisions for it (#90). The chart
# default is 0 (fixed-node kind, where Unschedulable is terminal at once); the deploy
# must raise it, and never past the ready budget that bounds the whole wait.
assert o['isolatedCompute']['provisioningSeconds'] >= 120, 'on-demand nodes need a provisioning grace'
assert o['isolatedCompute']['provisioningSeconds'] <= o['isolatedCompute']['readySeconds'], \
    'the provisioning grace cannot outlast the ready budget'
assert o['nodeSelector']['karpenter.sh/nodepool'] == 'duckfleet'
# Carried as null — the overlay's instruction to Helm to delete the chart default,
# which the flip render above confirms is gone from the rendered pod.
assert o['nodeSelector']['eks.amazonaws.com/compute-type'] is None, 'compute-type must be null-deleted'
assert o['tolerations'][0]['key'] == 'duckfleet.io/dedicated'
assert o['workerCpuRequests'] == {'medium': 2}
print('deploy_values overlay: enabled, pinned, tolerated, medium at 2')
PY
echo 'PASS: deploy_values builds the flip overlay from the recorded egress CIDRs'
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
