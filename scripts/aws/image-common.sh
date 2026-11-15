#!/usr/bin/env bash
# Shared by publishing and manifest assembly. No AWS call on source.
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"
registry=797781631727.dkr.ecr.us-east-2.amazonaws.com
image_revision() {
  [[ -z $(git status --porcelain) ]] || { echo 'Commit the reviewed source before publishing' >&2; return 1; }
  git rev-parse HEAD
}
# Exit 1 means only ImageNotFound; all other lookup failures are fatal (exit 2).
image_digest() {
  local component="$1" tag="$2" error_file digest
  error_file=$(mktemp)
  if digest=$(aws ecr describe-images --repository-name "duckfleet-dev/$component" \
      --image-ids "imageTag=$tag" --query 'imageDetails[0].imageDigest' --output text 2> "$error_file"); then
    rm -f "$error_file"
    [[ "$digest" =~ ^sha256:[0-9a-f]{64}$ ]] || { echo 'Invalid registry digest' >&2; return 2; }
    printf '%s\n' "$digest"
  else
    if grep -q ImageNotFoundException "$error_file"; then
      rm -f "$error_file"
      return 1
    fi
    cat "$error_file" >&2
    rm -f "$error_file"
    return 2
  fi
}
registry_login() {
  aws ecr get-login-password | docker login --username AWS --password-stdin "$registry"
}
