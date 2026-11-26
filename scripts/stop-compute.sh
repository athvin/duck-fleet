#!/usr/bin/env bash
# Explicit reversible operator action. Leaves databases, volumes, and Job templates.
set -euo pipefail
ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
exec bash "$ROOT/scripts/ops.sh" compute-lifecycle stop "$@"
