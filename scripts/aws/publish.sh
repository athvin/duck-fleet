#!/usr/bin/env bash
# Run after the reviewed Terraform apply creates the dedicated ECR repositories.
set -euo pipefail
source "$(dirname "$0")/common.sh"
cd "$ROOT"
assert_account
[[ -z $(git status --porcelain) ]] || { echo 'Commit the reviewed source before publishing' >&2; exit 1; }
revision=$(git rev-parse HEAD)
registry=797781631727.dkr.ecr.us-east-2.amazonaws.com
mkdir -p artifacts/aws
aws ecr get-login-password | docker login --username AWS --password-stdin "$registry"
for component in worker dbt; do
  dockerfile=docker/server.Dockerfile
  [[ "$component" == worker ]] || dockerfile=docker/dbt.Dockerfile
  image="$registry/duckfleet-dev/$component:$revision"
  # Immutable tags make reruns safe: reuse the exact committed revision if published.
  if aws ecr describe-images --repository-name "duckfleet-dev/$component" \
      --image-ids "imageTag=$revision" --query 'imageDetails[0].imageDigest' --output text \
      > "artifacts/aws/$component-digest.txt" 2> "artifacts/aws/$component-lookup.err"; then
    python3 - "$component" <<'PYMETA'
import json,sys
from pathlib import Path
root=Path('artifacts/aws'); kind=sys.argv[1]
digest=(root/f'{kind}-digest.txt').read_text().strip()
assert digest.startswith('sha256:') and len(digest)==71
(root/f'{kind}-image.json').write_text(json.dumps({'containerimage.digest':digest}))
PYMETA
    continue
  fi
  grep -q ImageNotFoundException "artifacts/aws/$component-lookup.err" || {
    cat "artifacts/aws/$component-lookup.err" >&2; exit 1;
  }
  docker buildx build --platform linux/amd64,linux/arm64 -f "$dockerfile" \
    --tag "$image" --push --metadata-file "artifacts/aws/$component-image.json" .
done
python3 - <<'PY'
import json
from pathlib import Path
root=Path('artifacts/aws')
registry='797781631727.dkr.ecr.us-east-2.amazonaws.com/duckfleet-dev'
values={f'{kind}Image':f'{registry}/{kind}@'+json.loads((root/f'{kind}-image.json').read_text())['containerimage.digest'] for kind in ['worker','dbt']}
(root/'values.json').write_text(json.dumps(values,indent=2)+'\n')
PY
printf 'Immutable deployment images recorded in artifacts/aws/values.json\n'
