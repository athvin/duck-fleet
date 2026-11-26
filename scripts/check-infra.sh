#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
TF=${TERRAFORM:-terraform}
ops() { bash scripts/ops.sh "$@"; }
validation_dir=$(mktemp -d)
trap 'rm -rf "$validation_dir"' EXIT
export TF_DATA_DIR="$validation_dir/terraform"
values="$validation_dir/values.json"
rendered="$validation_dir/rendered.yaml"
"$TF" fmt -check -recursive infra/terraform
"$TF" -chdir=infra/terraform/dev init -backend=false -input=false
"$TF" -chdir=infra/terraform/dev validate
"$TF" -chdir=infra/terraform/dev test
"$TF" -chdir=infra/terraform/cluster init -backend=false -input=false
"$TF" -chdir=infra/terraform/cluster validate
"$TF" -chdir=infra/terraform/cluster test
ops helm-values default > "$values"
helm lint infra/helm/duckfleet --namespace duckfleet-dev -f "$values"
helm template duckfleet infra/helm/duckfleet --namespace duckfleet-dev -f "$values" > "$rendered"
ops check-chart "$rendered"
foreign="$validation_dir/foreign.yaml"
ops helm-values foreign > "$validation_dir/foreign-values.json"
helm template duckfleet infra/helm/duckfleet --namespace duckfleet-team-a \
  -f "$validation_dir/foreign-values.json" > "$foreign"
ops check-chart "$foreign" > /dev/null
if helm template duckfleet infra/helm/duckfleet --namespace duckfleet-dev \
     --set workerImage=acme/worker:latest --set dbtImage=acme/dbt:latest > /dev/null 2>&1; then
  echo 'A mutable image tag must fail schema validation' >&2
  exit 1
fi
echo 'PASS: the chart installs for a foreign account and still refuses an unpinned image'
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
     && ops check-chart "$mismatched" > /dev/null 2>&1; then
    echo "The DuckFleet node pool $description must fail the chart check" >&2
    exit 1
  fi
done
echo 'PASS: the node pool pin and its toleration are rejected unless they agree'
paired="$validation_dir/paired.yaml"
helm template duckfleet infra/helm/duckfleet --namespace duckfleet-dev -f "$values" \
  --set-json "$PINNED" --set-json "$TOLERATED" > "$paired"
ops check-chart "$paired" > /dev/null
echo 'PASS: pinning the node pool and tolerating its taint together is accepted'
isolated="$validation_dir/isolated.yaml"
helm template duckfleet infra/helm/duckfleet --namespace duckfleet-dev -f "$values" \
  --set isolatedCompute.enabled=true \
  --set 'isolatedCompute.apiServerCidrs[0]=10.96.0.1/32' > "$isolated"
ops check-chart "$isolated" > /dev/null
echo 'PASS: isolated compute renders its pod template and API egress, and no RBAC'
results="$validation_dir/results.yaml"
helm template duckfleet infra/helm/duckfleet --namespace duckfleet-dev -f "$values" \
  --set results.enabled=true > "$results"
ops check-chart "$results" > /dev/null
for prefix in lake lake/nested; do
  if helm template duckfleet infra/helm/duckfleet --namespace duckfleet-dev -f "$values" \
       --set results.enabled=true --set "results.prefix=$prefix" > /dev/null 2>&1; then
    echo 'Result artifacts must never share the DuckLake managed prefix' >&2
    exit 1
  fi
done
helm template duckfleet infra/helm/duckfleet --namespace duckfleet-dev -f "$values" \
  --set results.enabled=true --set isolatedCompute.enabled=true \
  --set 'isolatedCompute.apiServerCidrs[0]=10.96.0.1/32' > "$results"
ops check-chart "$results" > /dev/null
echo 'PASS: result-only gateway identity, isolated coexistence, and prefix separation'
boundary="$validation_dir/boundary.yaml"
ops aws-render-boundary duckfleet-dev > "$boundary"
ops boundary-disjoint "$isolated" "$boundary"
echo 'PASS: the release and the ownership boundary are disjoint and meet at the gateway identity'
if helm template duckfleet infra/helm/duckfleet --namespace duckfleet-dev -f "$values" \
     --set isolatedCompute.enabled=true > /dev/null 2>&1; then
  echo 'Isolated compute without apiServerCidrs must fail the render' >&2
  exit 1
fi
echo 'PASS: isolated compute without a declared API server address is rejected'
exposed="$validation_dir/exposed.yaml"
helm template duckfleet infra/helm/duckfleet --namespace duckfleet-dev -f "$values" \
  --set exposure.enabled=true \
  --set exposure.host=duckfleet.example.com \
  --set exposure.tlsSecretName=duckfleet-tls \
  --set exposure.controllerNamespace=ingress-nginx > "$exposed"
ops check-chart "$exposed" > /dev/null
echo 'PASS: exposure renders an Ingress that terminates TLS and opens only the controller door'
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
external="$validation_dir/external-postgres.yaml"
helm template duckfleet infra/helm/duckfleet --namespace duckfleet-dev -f "$values" \
  --set postgres.bundled=false \
  --set 'postgres.externalCidrs[0]=10.20.30.40/32' > "$external"
ops check-chart "$external" > /dev/null
if grep -q 'duckfleet-postgres' "$external"; then
  echo 'postgres.bundled=false must render no bundled PostgreSQL' >&2
  exit 1
fi
grep -q '10.20.30.40/32' "$external"
echo 'PASS: an external catalog renders no bundled PostgreSQL and opens egress to the named address'
if helm template duckfleet infra/helm/duckfleet --namespace someone-elses-namespace -f "$values" > /dev/null 2>&1; then
  echo 'The chart must refuse to render into a namespace DuckFleet does not own' >&2
  exit 1
fi
helm template duckfleet infra/helm/duckfleet --namespace duckfleet-team-b -f "$values" > /dev/null
echo 'PASS: the chart renders for duckfleet-* namespaces and refuses everything else'
wire="$validation_dir/pgwire.yaml"
helm template duckfleet infra/helm/duckfleet --namespace duckfleet-dev -f "$values" \
  --set pgwire.enabled=true > "$wire"
ops check-chart "$wire" > /dev/null
echo 'PASS: the wire front renders only when asked for'
scaled="$validation_dir/scaled.yaml"
helm template duckfleet infra/helm/duckfleet --namespace duckfleet-dev -f "$values" \
  --set workerReplicas.small=2 > "$scaled"
ops chart-scaled "$scaled"
echo 'PASS: a second same-size worker renders its own identity, Service, and endpoint entry'
autoscaling="$validation_dir/autoscaling.yaml"
helm template duckfleet infra/helm/duckfleet --namespace duckfleet-dev -f "$values" \
  --set defaultPool=interactive \
  --set authorization.defaultPersistentGrant.pool=interactive \
  --set-json 'persistentPools={"interactive":{"cpu":1,"memory":"1Gi","disk":"2Gi","workers":2}}' \
  --set-json 'autoscaling={"interactive":{"min_workers":1,"max_workers":2}}' > "$autoscaling"
ops chart-autoscaling "$autoscaling"
echo 'PASS: autoscaling starts only minimum capacity and retains bounded standby endpoints'
flip="$validation_dir/isolated-pinned.yaml"
flip_overlay="$validation_dir/isolated-overlay.json"
ops deploy-overlay "$flip_overlay"
helm template duckfleet infra/helm/duckfleet --namespace duckfleet-dev -f "$values" \
  -f "$flip_overlay" > "$flip"
ops chart-flip "$flip"
echo 'PASS: isolated compute is enabled, pinned, tolerated, and free of impossible selectors'
for script in scripts/*.sh scripts/aws/*.sh scripts/ci/*.sh; do bash -n "$script"; done
for expect in \
  'image,cpu-request,s3-auth,s3-path,storage-class,object-store-port:--no-hooks' \
  'image,cpu-request,s3-path:--show-only templates/harness.yaml' \
  'image,cpu-request,s3-auth,s3-path,storage-class,object-store-port,isolated-pod:--no-hooks --set isolatedCompute.enabled=true --set isolatedCompute.apiServerCidrs[0]=10.96.0.1/32'; do
  # shellcheck disable=SC2086
  helm template duckfleet infra/helm/duckfleet --namespace duckfleet-dev -f "$values" \
    --set results.enabled=true ${expect#*:} | ops kind-localize --expect "${expect%%:*}" > /dev/null
done
echo 'PASS: the local Kubernetes transform still matches the rendered chart'
ops aws-ci-test
