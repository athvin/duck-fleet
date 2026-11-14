#!/usr/bin/env bash
# Assemble immutable native images only after BOTH architecture jobs succeed.
set -euo pipefail
source "$(dirname "$0")/image-common.sh"
cd "$ROOT"
[[ "$#" == 0 || ( "$#" == 1 && "$1" == --existing ) ]] || exit 1
assert_account
revision=$(image_revision)
mkdir -p artifacts/aws
registry_login
for component in worker dbt; do
  repository="$registry/duckfleet-dev/$component"
  if digest=$(image_digest "$component" "$revision"); then
    printf 'Reusing immutable index %s@%s\n' "$component" "$digest"
  else
    status=$?
    [[ "$status" == 1 && "$#" == 0 ]] || exit "$status"
    amd64=$(image_digest "$component" "$revision-amd64")
    arm64=$(image_digest "$component" "$revision-arm64")
    docker buildx imagetools create --tag "$repository:$revision" "$repository@$amd64" "$repository@$arm64"
    digest=$(image_digest "$component" "$revision")
  fi
  printf '%s\n' "$digest" > "artifacts/aws/$component-digest.txt"
  docker buildx imagetools inspect --raw "$repository@$digest" > "artifacts/aws/$component-index.json"
done
python3 scripts/aws/image_values.py
