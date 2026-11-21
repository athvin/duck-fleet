#!/usr/bin/env bash
# Explicit reversible operator action. Leaves databases, volumes, and Job templates.
set -euo pipefail
ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
exec python3 "$ROOT/scripts/compute_lifecycle.py" stop "$@"
