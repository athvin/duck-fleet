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
# Pinned like every other dependency here; a floating CNI would change what
# enforcement means between runs.
CALICO_VERSION=${CALICO_VERSION:-v3.30.4}
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
    # A restarted container's death is only in its previous instance's log and its
    # recorded last state; without these a crash reads as a mystery restart.
    "${KUBE[@]}" logs -l app=duckfleet-worker --tail=200 --prefix --previous > artifacts/kind/workers-previous.log 2>&1 || true
    "${KUBE[@]}" logs -l app=duckfleet-gateway --tail=200 --prefix --previous > artifacts/kind/gateway-previous.log 2>&1 || true
    "${KUBE[@]}" get pods -o yaml > artifacts/kind/pods.yaml 2>&1 || true
  fi
  rm -rf "$work"
  [[ "${DUCKFLEET_KEEP_CLUSTER:-false}" == true ]] || kind delete cluster --name "$CLUSTER" > /dev/null 2>&1 || true
  exit "$status"
}
trap cleanup EXIT

kind delete cluster --name "$CLUSTER" > /dev/null 2>&1 || true
# No --wait: with the default CNI disabled the nodes stay NotReady until one is
# installed, so readiness is waited for after Calico below.
kind create cluster --name "$CLUSTER" --config infra/kind/cluster.yaml
# An enforcing CNI, pinned. kind's default ignores NetworkPolicy entirely, which made
# the local suite reassuring rather than silent about isolation and let #30 reach six
# consecutive AWS deploys. The added setup time is reported because this runs on every
# pull request.
cni_started=$SECONDS
kubectl --context "$CONTEXT" apply -f "https://raw.githubusercontent.com/projectcalico/calico/${CALICO_VERSION}/manifests/calico.yaml"
kubectl --context "$CONTEXT" -n kube-system rollout status daemonset/calico-node --timeout=300s
kubectl --context "$CONTEXT" wait --for=condition=Ready nodes --all --timeout=300s
printf 'enforcing CNI (calico %s) ready in %ss\n' "$CALICO_VERSION" "$((SECONDS - cni_started))"

# Built locally and loaded into the node; localize.py rewrites the chart's ECR
# digests to these tags with imagePullPolicy Never, so nothing is ever pulled.
docker build --target runtime --tag duckfleet-worker:local -f docker/server.Dockerfile .
docker build --tag duckfleet-dbt:local -f docker/dbt.Dockerfile .
docker build --tag duckfleet-s3:local -f docker/s3.Dockerfile .
kind load docker-image --name "$CLUSTER" duckfleet-worker:local duckfleet-dbt:local duckfleet-s3:local

# The same boundary an operator applies to the real cluster, rather than a convenient
# local equivalent: it creates the namespace, the pod grant isolated compute needs and
# the quota that caps it. Applying the real thing is what makes this harness able to
# catch a boundary that no longer matches the chart — a quota that rejected a pod, or a
# grant missing a verb, would show up here instead of on a deploy.
python3 scripts/aws/render_boundary.py "$NS" | kubectl --context "$CONTEXT" apply -f -
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
# The API server's endpoint, read from the cluster rather than assumed. Egress policy
# is matched after kube-proxy rewrites the destination, so permitting the Service's
# ClusterIP on 443 matches nothing: on kind the traffic arrives at the node's address
# on 6443. Assuming otherwise is what denied the gateway on the first run with an
# enforcing CNI, and would have denied it on EKS too.
api_address=$(kubectl --context "$CONTEXT" -n default get endpoints kubernetes \
  -o jsonpath='{.subsets[0].addresses[0].ip}')
api_port=$(kubectl --context "$CONTEXT" -n default get endpoints kubernetes \
  -o jsonpath='{.subsets[0].ports[0].port}')
printf 'API server endpoint: %s:%s\n' "$api_address" "$api_port"
render --no-hooks \
  --set isolatedCompute.enabled=true \
  --set "isolatedCompute.apiServerCidrs[0]=$api_address/32" \
  --set "isolatedCompute.apiServerPorts[0]=$api_port" \
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

# Isolation, actually enforced. Runs before the dbt harness because a cluster that
# cannot resolve DNS or reach PostgreSQL would fail there in a way that looks like an
# application problem — and because every pod above reaching ready is itself the proof
# that the permitted paths work.
python3 scripts/kind/check_networkpolicy.py --context "$CONTEXT" --namespace "$NS" \
  --image duckfleet-worker:local --release "$work/release.yaml" \
  --evidence artifacts/kind/networkpolicy.json

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
# Both ceilings, named separately, because they refuse at different moments: the
# namespace quota before a pod exists and the scheduler after one does. Asserting only
# one would pass while the other path went untested.
for ceiling in 'the namespace quota' 'the scheduler'; do
  grep -q "PASS: an isolated request $ceiling cannot serve was refused" \
    artifacts/kind/duckfleet-isolated.log
done
# Closed sessions leave no pods. Checked after the run rather than during it, because
# "the pod existed" is proven by the session having answered at all.
for _ in $(seq 1 30); do
  [[ -z "$(isolated_pods)" ]] && break
  sleep 2
done
test -z "$(isolated_pods)" || {
  echo "session pods outlived their sessions: $(isolated_pods)" >&2; exit 1; }
echo 'PASS: closing an isolated session removed its pod'

# What the pod costs in latency against a persistent worker (#18). Times a
# persistent open, a cold isolated open (pod created, scheduled, ready), and a
# second isolated open with the node and image warm. The absolute numbers vary
# with the sandbox; the ordering — a pod that must be created cannot open faster
# than a worker that already exists — is the invariant the stage asserts.
harness duckfleet-cold-start 'python /app/tests/harness.py --cold-start' 600
grep -q 'PASS: cold start measured' artifacts/kind/duckfleet-cold-start.log
for _ in $(seq 1 30); do
  [[ -z "$(isolated_pods)" ]] && break
  sleep 2
done

# The dbt layer over the same capability: a model whose meta.platform requests a
# pod of its own, run exactly as a user runs models. The --isolated stage above
# proved the API; this proves the adapter carries the mode through resolved node
# config with the session, hook, and durability guarantees intact — the other
# half of #19, whose refusal half runs where isolation is absent (Compose).
harness duckfleet-dbt-isolated 'python /app/tests/harness.py --dbt-isolated' 900
grep -q 'PASS: a dbt model ran isolated on its own pod' artifacts/kind/duckfleet-dbt-isolated.log
for _ in $(seq 1 30); do
  [[ -z "$(isolated_pods)" ]] && break
  sleep 2
done
test -z "$(isolated_pods)" || {
  echo "the dbt model's session pod outlived the model: $(isolated_pods)" >&2; exit 1; }
echo 'PASS: the dbt-requested session pod was removed once the model finished'

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
# The money and the diagnostics have to be reconciled together: an isolated identity
# never restarts to mark its own rows, so a `running` record naming a dead session pod
# would stand forever — unprunable, and claiming work is active on a worker that is gone.
stuck_running() {
  "${KUBE[@]}" exec statefulset/duckfleet-postgres -- psql -U duckfleet -d platform -tAc \
    "SELECT count(*) FROM query_history WHERE status='running' AND worker_id LIKE 'duckfleet-session-%'" \
    | tr -d '[:space:]'
}
test "$(stuck_running)" == 0 || {
  echo "a dead session pod left query_history rows running: $(stuck_running)" >&2; exit 1; }
interrupted=$("${KUBE[@]}" exec statefulset/duckfleet-postgres -- psql -U duckfleet -d platform -tAc \
  "SELECT count(*) FROM query_history WHERE worker_id='$leaked' AND status='interrupted' AND failure_kind='interrupted'" \
  | tr -d '[:space:]')
test "$interrupted" -ge 1 || {
  echo "the abandoned pod's running record was not marked interrupted" >&2; exit 1; }
echo 'PASS: the abandoned pod query records were reconciled to interrupted with the settle'

# Eviction mid-statement, which nothing else here exercises: the tests below delete
# pods *between* runs. The signal is the ledger rather than a file, because the harness
# Job writes to a volume claim this host cannot read — and because a `running` history
# row naming its worker is a more direct statement of "the engine is busy with this"
# than any marker would be.
running_worker() {
  "${KUBE[@]}" exec statefulset/duckfleet-postgres -- psql -U duckfleet -d platform -tAc \
    "SELECT worker_id FROM query_history WHERE status='running'
       AND sql LIKE 'insert into api_demo.evicted%' LIMIT 1" 2> /dev/null | tr -d '[:space:]'
}
harness duckfleet-evicted 'python /app/tests/harness.py --evicted-midquery' 900 &
evicted=$!
victim=""
for _ in $(seq 1 120); do
  victim=$(running_worker)
  [[ -n "$victim" ]] && break
  sleep 2
done
if [[ -z "$victim" ]]; then
  echo 'no statement ever reached running; nothing to evict' >&2
  kill "$evicted" 2> /dev/null || true
  exit 1
fi
echo "deleting worker-$victim while it is executing"
"${KUBE[@]}" delete pod -l "app=duckfleet-worker,size=$victim" --wait=false
wait "$evicted"
grep -q 'PASS: a statement whose worker was deleted mid-run' artifacts/kind/duckfleet-evicted.log
for deployment in worker-small worker-medium; do
  "${KUBE[@]}" rollout status "deployment/$deployment" --timeout=420s
done

# And again with no grace period at all, which is the case the ticket actually asks
# about: eviction *during* an open transaction, with no chance to drain. The run above
# answered honestly that the statement finished first — that is the drain working, not
# an interrupted transaction, so it does not establish how an interrupted one is
# reported.
harness duckfleet-evicted-hard \
  'python /app/tests/harness.py --evicted-midquery --expect-classified' 900 &
evicted_hard=$!
victim=""
for _ in $(seq 1 120); do
  victim=$("${KUBE[@]}" exec statefulset/duckfleet-postgres -- psql -U duckfleet -d platform -tAc \
    "SELECT worker_id FROM query_history WHERE status='running'
       AND sql LIKE 'insert into api_demo.evicted_hard%' LIMIT 1" 2> /dev/null | tr -d '[:space:]')
  [[ -n "$victim" ]] && break
  sleep 2
done
if [[ -z "$victim" ]]; then
  echo 'no statement ever reached running for the abrupt eviction' >&2
  kill "$evicted_hard" 2> /dev/null || true
  exit 1
fi
echo "killing worker-$victim outright while it is executing"
"${KUBE[@]}" delete pod -l "app=duckfleet-worker,size=$victim" --grace-period=0 --force --wait=false
wait "$evicted_hard"
grep -q 'PASS: a statement whose worker was deleted mid-run' artifacts/kind/duckfleet-evicted-hard.log
for deployment in worker-small worker-medium; do
  "${KUBE[@]}" rollout status "deployment/$deployment" --timeout=420s
done

# The gateway's own version of the abrupt eviction: kill it with no grace while a
# statement is mid-flight *through* it. The drain covers the graceful path; this is
# the one with no chance to drain. The caller must get classified outcomes — never a
# raw socket error — a plain reopen must ride through the replacement window on the
# client's bounded open retry, and the statement's true outcome must be knowable
# from history even though the gateway that proxied it is gone.
harness duckfleet-gateway-replaced 'python /app/tests/harness.py --gateway-replaced' 900 &
replaced=$!
busy=""
for _ in $(seq 1 120); do
  busy=$("${KUBE[@]}" exec statefulset/duckfleet-postgres -- psql -U duckfleet -d platform -tAc \
    "SELECT 1 FROM query_history WHERE status='running'
       AND sql LIKE 'insert into api_demo.gateway_replaced%' LIMIT 1" 2> /dev/null | tr -d '[:space:]')
  [[ -n "$busy" ]] && break
  sleep 2
done
if [[ -z "$busy" ]]; then
  echo 'no statement ever reached running through the gateway' >&2
  kill "$replaced" 2> /dev/null || true
  exit 1
fi
echo 'killing the gateway outright while a statement is in flight through it'
"${KUBE[@]}" delete pod -l app=duckfleet-gateway --grace-period=0 --force --wait=false
wait "$replaced"
grep -q 'PASS: the in-flight request failed classified' artifacts/kind/duckfleet-gateway-replaced.log
grep -q 'PASS: a reopen rode through the gateway replacement' artifacts/kind/duckfleet-gateway-replaced.log
grep -q 'PASS: a gateway replaced without grace left classified outcomes' artifacts/kind/duckfleet-gateway-replaced.log
"${KUBE[@]}" rollout status deployment/server --timeout=420s

"${KUBE[@]}" apply -f infra/kind/bucket-verify.yaml
python3 scripts/kind/await_job.py --context "$CONTEXT" --namespace "$NS" \
  --job bucket-verify --timeout 300
"${KUBE[@]}" logs job/bucket-verify > artifacts/kind/bucket-verify.log
grep -q 'PASS:' artifacts/kind/bucket-verify.log

# The backup path an operator would actually run, against a real cluster. The full
# destroy-and-restore drill lives in the Compose harness; here the kube mode of the
# same script must produce archives pg_restore recognises for both databases, or
# the drill is proving a path nobody runs in production.
DUCKFLEET_KUBE_CONTEXT="$CONTEXT" DUCKFLEET_NAMESPACE="$NS" \
  bash scripts/backup-catalog.sh kube "$work/catalog-backup"
for database in lake platform; do
  test -s "$work/catalog-backup/$database.dump"
  "${KUBE[@]}" exec -i statefulset/duckfleet-postgres -- pg_restore --list \
    < "$work/catalog-backup/$database.dump" > /dev/null
done
echo 'PASS: the kube backup produced restorable archives for both databases'

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

# The external-catalog path, last because it swaps the catalog out from under the
# release: the same chart pointed at a PostgreSQL it does not own, which is what
# running against RDS looks like scaled to kind. The in-namespace stand-in needs
# no externalCidrs — the isolation policy already permits 5432 between this
# namespace's pods; the out-of-namespace egress render is proven in check-infra.
"${KUBE[@]}" apply -f infra/kind/external-postgres.yaml
"${KUBE[@]}" rollout status deployment/external-postgres --timeout=240s
password=$("${KUBE[@]}" get secret duckfleet-dev -o jsonpath='{.data.POSTGRES_PASSWORD}' | base64 -d)
python3 - "$password" <<'PY' | "${KUBE[@]}" patch secret duckfleet-dev --type merge --patch-file /dev/stdin
import json, sys
dsn = f'host=external-postgres port=5432 user=duckfleet password={sys.argv[1]}'
print(json.dumps({'stringData': {'CATALOG_DSN': dsn + ' dbname=lake',
                                 'PLATFORM_DSN': dsn + ' dbname=platform'}}))
PY
render --no-hooks \
  --set postgres.bundled=false \
  --set isolatedCompute.enabled=true \
  --set "isolatedCompute.apiServerCidrs[0]=$api_address/32" \
  --set "isolatedCompute.apiServerPorts[0]=$api_port" \
  --set isolatedCompute.readySeconds=180 \
  | python3 scripts/kind/localize.py \
  --expect image,cpu-request,s3-auth,s3-path,node-selector,object-store-port,isolated-pod \
  > "$work/release-external.yaml"
"${KUBE[@]}" apply -f "$work/release-external.yaml"
# apply does not prune, so the bundled instance is removed by name — exactly what
# an operator migrating to an external catalog does after restoring into it.
"${KUBE[@]}" delete statefulset/duckfleet-postgres service/postgres configmap/duckfleet-postgres-init
for deployment in worker-small worker-medium server; do
  "${KUBE[@]}" rollout restart "deployment/$deployment"
done
for deployment in worker-small worker-medium server; do
  "${KUBE[@]}" rollout status "deployment/$deployment" --timeout=420s
done
# Two probes against the fresh external catalog: overlapping DuckLake writes from
# both workers — commit races resolved through the catalog under test — and the
# credential matrix, which exercises the platform database's whole surface.
harness duckfleet-external 'python /app/tests/harness.py --concurrency' 600
grep -q 'PASS: .* concurrent-write cases match the documented DuckLake semantics' \
  artifacts/kind/duckfleet-external.log
harness duckfleet-external-credentials 'python /app/tests/harness.py --credentials' 600
grep -q 'PASS: credentials are issued' artifacts/kind/duckfleet-external-credentials.log
if "${KUBE[@]}" get statefulset duckfleet-postgres > /dev/null 2>&1; then
  echo 'the bundled PostgreSQL survived postgres.bundled=false' >&2
  exit 1
fi
echo 'PASS: the release ran a full workflow against an external PostgreSQL, bundled instance gone'

# A second team namespace from the operator scripts: its own boundary, catalog,
# credentials, and lake prefix, live beside the first — then the isolation claim
# earns its negative test, and teardown removes the namespace whole while the
# first feels nothing.
TEAM=duckfleet-team-b
DUCKFLEET_KUBE_CONTEXT="$CONTEXT" \
DUCKFLEET_PROVISION_TRANSFORM="python3 scripts/kind/localize.py --expect image,cpu-request,s3-auth,s3-path,node-selector,storage-class,object-store-port --data-path s3://duckfleet/$TEAM/lake/" \
  bash scripts/provision-namespace.sh "$TEAM" \
  --set "workerImage=$REGISTRY/worker@sha256:$digest" \
  --set "dbtImage=$REGISTRY/dbt@sha256:$digest"
team=(kubectl --context "$CONTEXT" -n "$TEAM")
# On AWS, S3 is outside every namespace; in kind the stand-in store is a
# namespace-local pod, so the new namespace brings its own — which is also the
# aligned model: a team namespace shares storage infrastructure with nobody.
"${team[@]}" apply -f infra/kind/object-store.yaml
"${team[@]}" rollout status deployment/object-store --timeout=240s
"${team[@]}" wait --for=condition=complete job/bucket-init --timeout=240s
team_token=$("${team[@]}" get secret duckfleet-dev -o jsonpath='{.data.DUCKFLEET_TOKEN}' | base64 -d)
dev_password=$("${KUBE[@]}" get secret duckfleet-dev -o jsonpath='{.data.POSTGRES_PASSWORD}' | base64 -d)
"${team[@]}" port-forward svc/server 18081:8080 > /dev/null 2>&1 &
team_forward=$!
sleep 3
python3 scripts/kind/team_namespace_probe.py --port 18081 \
  --token "$team_token" --dev-password "$dev_password"
kill "$team_forward" 2> /dev/null || true
DUCKFLEET_CONFIRM_DEPROVISION=yes DUCKFLEET_KUBE_CONTEXT="$CONTEXT" \
  bash scripts/deprovision-namespace.sh "$TEAM"
if kubectl --context "$CONTEXT" get namespace "$TEAM" > /dev/null 2>&1; then
  echo "namespace $TEAM survived deprovisioning" >&2
  exit 1
fi
"${KUBE[@]}" get deployment server > /dev/null
echo 'PASS: a second namespace was provisioned, isolated at the network even with the'
echo '      right password, and removed whole while the first namespace felt nothing'

"${KUBE[@]}" get pods -o wide > artifacts/kind/pods.txt
echo 'PASS: the release converged on Kubernetes, the dbt harness passed in-cluster,'
echo '      and data plus history survived worker and gateway pod replacement'
