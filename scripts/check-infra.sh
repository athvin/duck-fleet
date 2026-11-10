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
for script in scripts/aws/*.sh; do bash -n "$script"; done

python3 -m unittest discover -s tests -p test_aws_ci.py
