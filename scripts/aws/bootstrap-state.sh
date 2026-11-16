#!/usr/bin/env bash
# Creates the S3 bucket that holds DuckFleet's Terraform state. Operator-run, once.
#
# Terraform cannot create the bucket it stores its own state in, so this is the one
# piece of infrastructure that is not under Terraform. It is deliberately small and
# trivially re-creatable: it holds no business data, only state, and the state can be
# re-imported if it were ever lost.
#
# Idempotent: re-running against an existing bucket re-applies the settings and
# changes nothing else.
set -euo pipefail
ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
# shellcheck source=scripts/aws/target.env
source "$ROOT/scripts/aws/target.env"

bucket=$DUCKFLEET_STATE_BUCKET
region=$DUCKFLEET_REGION

account=$(aws sts get-caller-identity --query Account --output text)
[[ "$account" == "$DUCKFLEET_ACCOUNT" ]] || {
  echo "Wrong AWS account: $account, expected $DUCKFLEET_ACCOUNT" >&2; exit 1;
}
# Refuse to touch a bucket this repository does not own. Without this, a mistyped
# override would apply a policy to somebody else's bucket.
[[ "$bucket" == duckfleet-* ]] || {
  echo "Refusing to bootstrap $bucket: DuckFleet's state bucket must be named duckfleet-*" >&2
  echo "The entity-resolution bucket is not ours to configure; see #12." >&2
  exit 1
}

if aws s3api head-bucket --bucket "$bucket" 2> /dev/null; then
  echo "Bucket $bucket already exists; re-applying settings"
else
  # us-east-1 is the one region that rejects a location constraint.
  if [[ "$region" == us-east-1 ]]; then
    aws s3api create-bucket --bucket "$bucket" --region "$region"
  else
    aws s3api create-bucket --bucket "$bucket" --region "$region" \
      --create-bucket-configuration "LocationConstraint=$region"
  fi
fi

# Versioning first: state is the one object where losing a write matters, and S3
# locking stores its lock alongside it.
aws s3api put-bucket-versioning --bucket "$bucket" \
  --versioning-configuration Status=Enabled
aws s3api put-bucket-encryption --bucket "$bucket" \
  --server-side-encryption-configuration \
  '{"Rules":[{"ApplyServerSideEncryptionByDefault":{"SSEAlgorithm":"AES256"},"BucketKeyEnabled":true}]}'
aws s3api put-public-access-block --bucket "$bucket" \
  --public-access-block-configuration \
  'BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true'
aws s3api put-bucket-ownership-controls --bucket "$bucket" \
  --ownership-controls 'Rules=[{ObjectOwnership=BucketOwnerEnforced}]'
# Terraform state in transit is as sensitive as the resources it describes.
aws s3api put-bucket-policy --bucket "$bucket" --policy "$(
  cat <<JSON
{"Version":"2012-10-17","Statement":[{
  "Sid":"RequireTLS","Effect":"Deny","Principal":"*","Action":"s3:*",
  "Resource":["arn:aws:s3:::$bucket","arn:aws:s3:::$bucket/*"],
  "Condition":{"Bool":{"aws:SecureTransport":"false"}}}]}
JSON
)"
# Old state versions accumulate forever otherwise. Ninety days is well past any
# window in which rolling back to one would be a sane thing to do.
aws s3api put-bucket-lifecycle-configuration --bucket "$bucket" \
  --lifecycle-configuration "$(
  cat <<'JSON'
{"Rules":[{
  "ID":"expire-old-state-versions","Status":"Enabled","Filter":{"Prefix":""},
  "NoncurrentVersionExpiration":{"NoncurrentDays":90},
  "AbortIncompleteMultipartUpload":{"DaysAfterInitiation":1}}]}
JSON
)"

printf '%s\n' "Bootstrapped s3://$bucket for Terraform state in $region"
cat <<EOF

Next, migrate the existing state into it:

  DUCKFLEET_STATE_BUCKET=$bucket bash scripts/aws/migrate-state.sh

Until that runs and the default in scripts/aws/target.env is changed, the lane keeps
using the bucket named there, so nothing is in a half-moved condition.
EOF
