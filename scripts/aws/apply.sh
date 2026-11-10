#!/usr/bin/env bash
# CI-owned apply. The plan stays local to the runner and is never an artifact.
set -euo pipefail
source "$(dirname "$0")/common.sh"
cd "$ROOT"
[[ "${GITHUB_ACTIONS:-false}" == true && "${GITHUB_REF:-}" == refs/heads/main ]] || {
  echo 'Apply runs only in the reviewed main-branch GitHub workflow' >&2; exit 1;
}
assert_account
export TF_WORKSPACE=default
plan=$(mktemp)
trap 'rm -f "$plan"' EXIT
terraform -chdir=infra/terraform/dev init -input=false
terraform -chdir=infra/terraform/dev plan -input=false -lock-timeout=5m -out="$plan"
terraform -chdir=infra/terraform/dev show -json "$plan" | python3 scripts/aws/plan_guard.py
terraform -chdir=infra/terraform/dev apply -input=false -lock-timeout=5m "$plan"
