#!/usr/bin/env bash
# Proves what the Compose harness cannot: scheduling, probes, real cgroup limits
# derived from requests that differ from limits, and pod replacement.
#
# The manifests come from the chart that actually deploys, rewritten for a local
# cluster by scripts/kind/localize.py, so a chart change that breaks this is caught
# here rather than after a merge. No AWS credentials are used or needed.
set -euo pipefail
cd "$(dirname "$0")/.."
CLUSTER=${DUCKFLEET_KIND_CLUSTER:-duckfleet-local}
NS=duckfleet-dev
CONTEXT="kind-$CLUSTER"
KUBE=(kubectl --context "$CONTEXT" -n "$NS")
REGISTRY=797781631727.dkr.ecr.us-east-2.amazonaws.com/duckfleet-dev
work=$(mktemp -d)
mkdir -p artifacts/kind

for tool in kind kubectl helm docker python3; do
  command -v "$tool" > /dev/null || { echo "missing required tool: $tool" >&2; exit 1; }
done

cleanup() {
  local status=$?
  if [[ "$status" -ne 0 ]]; then
    "${KUBE[@]}" get pods -o wide > artifacts/kind/pods.txt 2>&1 || true
    "${KUBE[@]}" get events --sort-by=.lastTimestamp > artifacts/kind/events.txt 2>&1 || true
    "${KUBE[@]}" logs -l app=duckfleet-worker --tail=200 --prefix > artifacts/kind/workers.log 2>&1 || true
    "${KUBE[@]}" logs -l app=duckfleet-gateway --tail=200 --prefix > artifacts/kind/gateway.log 2>&1 || true
  fi
  rm -rf "$work"
  [[ "${DUCKFLEET_KEEP_CLUSTER:-false}" == true ]] || kind delete cluster --name "$CLUSTER" > /dev/null 2>&1 || true
  exit "$status"
}
trap cleanup EXIT

kind delete cluster --name "$CLUSTER" > /dev/null 2>&1 || true
kind create cluster --name "$CLUSTER" --wait 120s

# Built locally and loaded into the node; localize.py rewrites the chart's ECR
# digests to these tags with imagePullPolicy Never, so nothing is ever pulled.
docker build --target runtime --tag duckfleet-worker:local -f docker/server.Dockerfile .
docker build --tag duckfleet-dbt:local -f docker/dbt.Dockerfile .
docker build --tag duckfleet-s3:local -f docker/s3.Dockerfile .
kind load docker-image --name "$CLUSTER" duckfleet-worker:local duckfleet-dbt:local duckfleet-s3:local

kubectl --context "$CONTEXT" create namespace "$NS"
# Same shape the AWS deploy generates, created here rather than committed.
python3 - <<'PY' | "${KUBE[@]}" create -f -
import json, secrets
password = secrets.token_hex(32)
dsn = f'host=postgres port=5432 user=duckfleet password={password}'
print(json.dumps({'apiVersion': 'v1', 'kind': 'Secret',
                  'metadata': {'name': 'duckfleet-dev'}, 'type': 'Opaque',
                  'stringData': {'POSTGRES_PASSWORD': password,
                                 'DUCKFLEET_TOKEN': secrets.token_hex(32),
                                 'CATALOG_DSN': dsn + ' dbname=lake',
                                 'PLATFORM_DSN': dsn + ' dbname=platform'}}))
PY

"${KUBE[@]}" apply -f infra/kind/object-store.yaml
"${KUBE[@]}" rollout status deployment/object-store --timeout=240s
"${KUBE[@]}" wait --for=condition=complete job/bucket-init --timeout=240s

digest=$(printf 'a%.0s' {1..64})
render() {
  helm template duckfleet infra/helm/duckfleet --namespace "$NS" \
    --set "workerImage=$REGISTRY/worker@sha256:$digest" \
    --set "dbtImage=$REGISTRY/dbt@sha256:$digest" "$@"
}
# Hooks are rendered separately so the harness Job runs after the release is ready.
# Each render declares the rewrites it must contain, so a chart change that moves one
# of them fails here instead of leaving the local run testing a different shape.
render --no-hooks | python3 scripts/kind/localize.py \
  --expect image,cpu-request,s3-auth,s3-path,node-selector,storage-class,object-store-port \
  > "$work/release.yaml"
"${KUBE[@]}" apply -f "$work/release.yaml"

"${KUBE[@]}" rollout status statefulset/duckfleet-postgres --timeout=300s
# Workers come up alongside PostgreSQL rather than after it, so reaching ready here
# also exercises the catalog wait: without it they would exit and crash-loop.
for deployment in worker-small worker-medium server; do
  "${KUBE[@]}" rollout status "deployment/$deployment" --timeout=420s
done

python3 scripts/kind/assert_limits.py --context "$CONTEXT" --namespace "$NS" \
  | tee artifacts/kind/worker-limits.jsonl

# Runs the chart's own Job spec, re-pointed at the shared harness. The AWS harness it
# normally runs additionally asserts the real development bucket, which cannot be
# true here; the Parquet check below covers the local equivalent.
"${KUBE[@]}" apply -f infra/kind/harness-artifacts.yaml
harness() { # harness <job-name> <command> [timeout]
  local name="$1" command="$2" timeout="${3:-1200}"
  render --show-only templates/harness.yaml \
    | python3 scripts/kind/localize.py --expect image,cpu-request,s3-path,node-selector \
    | python3 scripts/kind/harness_job.py --name "$name" --command "$command" \
      --artifacts-claim duckfleet-harness-artifacts \
    > "$work/$name.yaml"
  "${KUBE[@]}" apply -f "$work/$name.yaml"
  python3 scripts/kind/await_job.py --context "$CONTEXT" --namespace "$NS" \
    --job "$name" --timeout "$timeout"
  "${KUBE[@]}" logs "job/$name" -c dbt > "artifacts/kind/$name.log"
}

harness duckfleet-harness 'python /app/tests/harness.py'
grep -q 'PASS: protocol checks and dbt lifecycle' artifacts/kind/duckfleet-harness.log
grep -q 'PASS: model config selected small and medium workers' artifacts/kind/duckfleet-harness.log

"${KUBE[@]}" apply -f infra/kind/bucket-verify.yaml
python3 scripts/kind/await_job.py --context "$CONTEXT" --namespace "$NS" \
  --job bucket-verify --timeout 300
"${KUBE[@]}" logs job/bucket-verify > artifacts/kind/bucket-verify.log
grep -q 'PASS:' artifacts/kind/bucket-verify.log

# Pod replacement, which Compose cannot exercise: these are new pods on a new
# sandbox, not restarted containers, so committed data and history must come from
# the catalog rather than from anything the old pod held.
"${KUBE[@]}" delete pod -l app=duckfleet-worker
"${KUBE[@]}" delete pod -l app=duckfleet-gateway
for deployment in worker-small worker-medium server; do
  "${KUBE[@]}" rollout status "deployment/$deployment" --timeout=420s
done
harness duckfleet-harness-replaced 'python /app/tests/harness.py --after-restart' 600
grep -q 'PASS: data and query history survived' artifacts/kind/duckfleet-harness-replaced.log

"${KUBE[@]}" get pods -o wide > artifacts/kind/pods.txt
echo 'PASS: the release converged on Kubernetes, the dbt harness passed in-cluster,'
echo '      and data plus history survived worker and gateway pod replacement'
