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
# Isolated compute is enabled here and nowhere else. The gateway needs RBAC to create
# pods, the AWS deploy identity cannot grant it any, and check-access.sh fails the
# deploy if that ever becomes possible — so this cluster, which we own outright, is
# where the feature is actually exercised. The API server is reachable at the default
# service address in kind.
render --no-hooks \
  --set isolatedCompute.enabled=true \
  --set 'isolatedCompute.apiServerCidrs[0]=10.96.0.1/32' \
  --set isolatedCompute.readySeconds=180 \
  | python3 scripts/kind/localize.py \
  --expect image,cpu-request,s3-auth,s3-path,node-selector,storage-class,object-store-port,isolated-pod \
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

# On-demand compute. The harness asserts what the API reports; these assert the
# cluster facts it cannot see, which is the half that matters for a leak.
isolated_pods() { "${KUBE[@]}" get pods -l duckfleet-compute=isolated -o name; }
# Waited for rather than asserted: the run above opens and closes an isolated session
# of its own, so its pod may still be terminating. Starting from empty is what makes
# the assertions below about *these* pods.
await_no_isolated_pods() {
  for _ in $(seq 1 30); do
    [[ -z "$(isolated_pods)" ]] && return 0
    sleep 2
  done
  echo "session pods outlived their sessions: $(isolated_pods)" >&2
  return 1
}
await_no_isolated_pods
harness duckfleet-isolated 'python /app/tests/harness.py --isolated' 600
grep -q 'PASS: an isolated session ran on its own pod' artifacts/kind/duckfleet-isolated.log
grep -q 'PASS: an unplaceable isolated request was refused' artifacts/kind/duckfleet-isolated.log
# Closed sessions leave no pods. Checked after the run rather than during it, because
# "the pod existed" is proven by the session having answered at all.
for _ in $(seq 1 30); do
  [[ -z "$(isolated_pods)" ]] && break
  sleep 2
done
test -z "$(isolated_pods)" || {
  echo "session pods outlived their sessions: $(isolated_pods)" >&2; exit 1; }
echo 'PASS: closing an isolated session removed its pod'

# The layer that does not depend on the gateway behaving well: abandon a session, then
# destroy the gateway. The owner reference must have the pod collected regardless, and
# the replacement gateway must settle what the pod was holding.
harness duckfleet-leak 'python /app/tests/harness.py --leak-isolated' 600
leaked=$("${KUBE[@]}" get pods -l duckfleet-compute=isolated -o jsonpath='{.items[0].metadata.name}')
test -n "$leaked"
echo "abandoned session pod: $leaked"
held() {
  "${KUBE[@]}" exec statefulset/duckfleet-postgres -- psql -U duckfleet -d platform -tAc \
    "SELECT count(*) FROM reservations WHERE state='held' AND worker_id LIKE 'duckfleet-session-%'" \
    | tr -d '[:space:]'
}
# Without this the settlement check below would pass on a ledger that never held
# anything: a reservation is taken per statement, so an idle abandoned session owes
# nothing and would prove nothing.
test "$(held)" -ge 1 || { echo 'the abandoned session held no funds to settle' >&2; exit 1; }
echo "funds held by the abandoned pod: $(held)"
"${KUBE[@]}" delete pod -l app=duckfleet-gateway --wait=true
for _ in $(seq 1 60); do
  "${KUBE[@]}" get "pod/$leaked" > /dev/null 2>&1 || break
  sleep 2
done
"${KUBE[@]}" get "pod/$leaked" > /dev/null 2>&1 && {
  echo "an abandoned session pod survived the gateway that owned it" >&2; exit 1; }
echo 'PASS: an abandoned session pod was collected with the gateway that owned it'
"${KUBE[@]}" rollout status deployment/server --timeout=420s
# Nothing may still be held against a pod that no longer exists: an isolated identity
# never returns to recover its own funds, so the replacement gateway has to.
for _ in $(seq 1 30); do
  [[ "$(held)" == 0 ]] && break
  sleep 2
done
held > artifacts/kind/held-isolated.txt
grep -qx 0 artifacts/kind/held-isolated.txt
echo 'PASS: a new gateway settled the reservations the abandoned pod was holding'

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
