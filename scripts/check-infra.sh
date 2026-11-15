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
for script in scripts/*.sh scripts/aws/*.sh scripts/ci/*.sh; do bash -n "$script"; done
# The local Kubernetes harness renders the real chart, so a chart change that moves
# something out from under the transform is caught here rather than in a cluster.
for expect in \
  'image,cpu-request,s3-auth,s3-path,node-selector,storage-class,object-store-port:--no-hooks' \
  'image,cpu-request,s3-path,node-selector:--show-only templates/harness.yaml'; do
  # shellcheck disable=SC2086
  helm template duckfleet infra/helm/duckfleet --namespace duckfleet-dev -f "$values" \
    ${expect#*:} | python3 scripts/kind/localize.py --expect "${expect%%:*}" > /dev/null
done
echo 'PASS: the local Kubernetes transform still matches the rendered chart'

python3 -m unittest discover -s tests -p test_aws_ci.py
