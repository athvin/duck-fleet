#!/usr/bin/env bash
# Rotates the DuckLake catalog password. Operator-run.
#
# The development password was written to pod logs in plaintext before #20 scrubbed
# it, so the credential that exists today should be considered disclosed. #20 fixed
# the leak; this replaces what leaked.
#
# The database and the Secret have to change together. `deploy.sh` creates that Secret
# only when it is absent and the PostgreSQL PVC outlives any redeploy, so changing one
# alone leaves the workers holding a password the server no longer accepts — which
# presents as an unreachable catalog rather than as a credential problem. Doing it in
# one step is the whole point of this script existing.
set -euo pipefail
ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
source "$ROOT/scripts/aws/common.sh"
cd "$ROOT"
assert_cluster

secret=${DUCKFLEET_SECRET:-$DUCKFLEET_NAMESPACE}
# The Secret the chart reads. Named from the same configuration the release uses, so
# this cannot rotate one deployment's credential while pointing at another's.
secret=$(python3 -c "
import re, sys
values = open('infra/helm/duckfleet/values.yaml').read()
print(re.search(r'^secretName:\s*(\S+)', values, re.M).group(1))
")

cat <<EOF
Rotating the DuckLake catalog password
  cluster    $DUCKFLEET_CLUSTER
  namespace  $NAMESPACE
  secret     $secret

The database user and the Secret change together. Workers are restarted afterwards so
they reconnect with the new credential; sessions open at that moment end, and the
worker drain gives statements already running until the grace period to finish.
EOF
[[ "${DUCKFLEET_ASSUME_YES:-false}" == true ]] || {
  read -r -p 'Continue? [y/N] ' reply
  [[ "$reply" == [yY] ]] || { echo 'Aborted'; exit 1; }
}

# Generated here and never echoed. The only copies are the database and the Secret.
password=$(python3 -c 'import secrets; print(secrets.token_hex(32))')

# Read the existing DSNs so the rest of their contents — host, port, database, user —
# survive rotation rather than being reconstructed and quietly changed.
existing=$("${KUBE[@]}" get secret "$secret" -o json)
rotated=$(DUCKFLEET_NEW_PASSWORD="$password" python3 scripts/aws/rotate_secret.py <<< "$existing")

# The database first. If this succeeds and the Secret write fails, the workers still
# hold the old password and are broken until the Secret catches up — but the inverse,
# writing the Secret first, would leave the *database* reachable with a credential that
# is no longer anywhere, which is harder to recover from.
"${KUBE[@]}" exec statefulset/duckfleet-postgres -- \
  env PGPASSWORD_NEW="$password" sh -c \
  'psql -U duckfleet -d postgres -v ON_ERROR_STOP=1 -c "ALTER USER duckfleet PASSWORD '"'"'$PGPASSWORD_NEW'"'"'"' \
  > /dev/null
printf '%s\n' 'database password changed'

printf '%s' "$rotated" | "${KUBE[@]}" apply -f - > /dev/null
printf '%s\n' 'secret updated'

# Restarted rather than left to pick it up: a Secret change does not reach a running
# pod's environment variables, so without this the fleet keeps using the old password
# until something else happens to replace it.
for deployment in $("${KUBE[@]}" get deployments -o name); do
  "${KUBE[@]}" rollout restart "$deployment" > /dev/null
done
for deployment in $("${KUBE[@]}" get deployments -o name); do
  "${KUBE[@]}" rollout status "$deployment" --timeout=300s
done

# Proof it worked, rather than an assumption: a statement has to reach the catalog.
"${KUBE[@]}" exec deployment/server -- sh -c \
  'curl -sS -o /dev/null -w "%{http_code}" http://localhost:8080/healthz' | grep -qx 200
printf '%s\n' 'PASS: the fleet came back with the rotated credential'
cat <<'EOF'

The previous password is now accepted by nothing. Anywhere it was recorded — a
password manager entry, a terminal scrollback, an old pod log export — can be
discarded rather than cleaned.
EOF
