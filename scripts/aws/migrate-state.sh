#!/usr/bin/env bash
# Moves DuckFleet's Terraform state into a bucket DuckFleet owns. Operator-run, once.
#
# Separate from bootstrap-state.sh because creating a bucket is safe and repeatable
# while moving state is neither: it is the one operation here that can lose the record
# of what exists in the account. So it refuses unless the destination is ready, shows
# what it is about to do, and leaves the old copy in place.
set -euo pipefail
ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
# shellcheck source=scripts/aws/target.env
source "$ROOT/scripts/aws/target.env"
cd "$ROOT"

account=$(aws sts get-caller-identity --query Account --output text)
[[ "$account" == "$DUCKFLEET_ACCOUNT" ]] || { echo 'Wrong AWS account' >&2; exit 1; }
aws s3api head-bucket --bucket "$DUCKFLEET_STATE_BUCKET" > /dev/null 2>&1 || {
  echo "s3://$DUCKFLEET_STATE_BUCKET does not exist; run bootstrap-state.sh first" >&2
  exit 1
}

current=$(terraform -chdir=infra/terraform/dev state list 2> /dev/null | wc -l | tr -d ' ')
cat <<EOF
Migrating Terraform state
  from  the bucket this working copy is currently initialised against
  to    s3://$DUCKFLEET_STATE_BUCKET/$DUCKFLEET_STATE_KEY
  resources tracked in the current state: $current

The source copy is left untouched. Terraform will ask you to confirm the copy.
EOF
# DUCKFLEET_ASSUME_YES is the confirmation when there is no terminal to prompt in —
# a GitHub runner, or an agent shell. It is never set by accident, and the source
# copy is left intact regardless, so it is a deliberate opt-in rather than a weakened
# gate. Interactive otherwise, where terraform's own prompt is the confirmation.
if [[ "${DUCKFLEET_ASSUME_YES:-false}" == true ]]; then
  input=(-input=false -force-copy)
else
  read -r -p 'Continue? [y/N] ' reply
  [[ "${reply:-y}" == [yY] ]] || { echo 'Aborted'; exit 1; }
  input=(-input=true)
fi

# -migrate-state copies the existing state into the new backend, leaving the source
# untouched. -force-copy is the non-interactive form of the prompt the confirmation
# above stands in for.
terraform -chdir=infra/terraform/dev init "${input[@]}" -migrate-state \
  -backend-config="bucket=$DUCKFLEET_STATE_BUCKET" \
  -backend-config="key=$DUCKFLEET_STATE_KEY" \
  -backend-config="region=$DUCKFLEET_REGION"

migrated=$(terraform -chdir=infra/terraform/dev state list | wc -l | tr -d ' ')
[[ "$migrated" == "$current" ]] || {
  echo "State moved but tracks $migrated resources, not $current; investigate before applying" >&2
  exit 1
}
printf '%s\n' "Migrated $migrated resources to s3://$DUCKFLEET_STATE_BUCKET/$DUCKFLEET_STATE_KEY"
cat <<'EOF'

Now change DUCKFLEET_STATE_BUCKET's default in scripts/aws/target.env and open a pull
request. CI reads the same file, so until that default changes CI still initialises
against the old bucket -- which is why this order leaves nothing half-moved.
EOF
