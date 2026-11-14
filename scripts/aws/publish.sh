#!/usr/bin/env bash
# No argument retains the operator's multi-arch build. CI passes its native arch.
set -euo pipefail
source "$(dirname "$0")/image-common.sh"
cd "$ROOT"
platforms=linux/amd64,linux/arm64
suffix=
if [[ "$#" -gt 0 ]]; then
  [[ "$#" == 1 && ( "$1" == amd64 || "$1" == arm64 ) ]] || { echo 'Expected amd64 or arm64' >&2; exit 1; }
  case "$(uname -m)" in
    x86_64) native_arch=amd64 ;;
    aarch64|arm64) native_arch=arm64 ;;
    *) echo 'Unsupported native architecture' >&2; exit 1 ;;
  esac
  [[ "$native_arch" == "$1" ]] || { echo 'Requested architecture must match the runner' >&2; exit 1; }
  platforms="linux/$1"
  suffix="-$1"
fi
assert_account
revision=$(image_revision)
mkdir -p artifacts/aws
registry_login
for component in worker dbt; do
  dockerfile=docker/server.Dockerfile
  [[ "$component" == worker ]] || dockerfile=docker/dbt.Dockerfile
  tag="$revision$suffix"
  image="$registry/duckfleet-dev/$component:$tag"
  if digest=$(image_digest "$component" "$tag"); then
    printf 'Reusing immutable %s@%s\n' "$component" "$digest"
  else
    status=$?
    [[ "$status" == 1 ]] || exit "$status"
    docker buildx build --platform "$platforms" -f "$dockerfile" --tag "$image" --push .
    digest=$(image_digest "$component" "$tag")
  fi
  printf '%s\n' "$digest" > "artifacts/aws/$component-digest.txt"
done
# A native job publishes only its architecture; the downstream job combines both.
if [[ -z "$suffix" ]]; then
  bash scripts/aws/assemble-images.sh --existing
fi
