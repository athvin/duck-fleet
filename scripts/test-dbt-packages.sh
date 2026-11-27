#!/usr/bin/env bash
# Popular dbt packages against the real Compose stack (#216): dbt deps from the
# dbt Hub, then compat/dbt-packages/run.sh inside the harness's dbt image. The
# local stack has no isolated compute, so the isolated model runs persistent here;
# the EKS run covers that tier (compat/dbt-packages/README.md).
set -euo pipefail
trap 'echo "dbt package test failed at line $LINENO" >&2' ERR
root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
work="$root/artifacts/dbt-packages-test"
cleanup() {
  # dbt runs as root in its container, so its target/ and dbt_packages/ are not ours.
  docker run --rm -v "$root/artifacts:/artifacts" alpine:3.20 rm -rf /artifacts/dbt-packages-test >/dev/null 2>&1 || true
}
trap cleanup EXIT
cleanup
mkdir -p "$work"
cp -R "$root/compat/dbt-packages/." "$work/"
rm -rf "$work/target" "$work/dbt_packages" "$work/logs"
cd "$root"
docker compose --profile test run --rm -T -v "$work:/project" -w /project \
  -e DUCKFLEET_COMPAT_ISOLATED=false dbt bash ./run.sh
