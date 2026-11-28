#!/usr/bin/env bash
# Prints code=true unless every file changed since BASE is documentation, for
# $GITHUB_OUTPUT. A docs-only change skips the harness jobs; skipped jobs still let
# the run conclude successfully, which is what aws-wait-checks reads.
#
# Documentation means a Markdown file outside server/: server/sdk/README.md is the
# crate's rustdoc (include_str!) and runs as doc tests, so it is code here.
# Anything uncertain (no base, an unreachable base, a new branch) counts as code.
set -euo pipefail
base=${1:-}
if [[ -z "$base" || "$base" =~ ^0+$ ]] || ! git cat-file -e "$base^{commit}" 2> /dev/null; then
  echo "no comparable base (${base:-none}); running everything" >&2
  echo code=true
  exit 0
fi
changed=$(git diff --name-only "$base" HEAD)
code=false
while IFS= read -r path; do
  [[ -z "$path" ]] && continue
  if [[ "$path" != *.md || "$path" == server/* ]]; then
    echo "code change: $path" >&2
    code=true
    break
  fi
done <<< "$changed"
[[ -n "$changed" ]] || code=true
[[ "$code" == true ]] || echo "documentation-only change; harness jobs will be skipped" >&2
echo "code=$code"
