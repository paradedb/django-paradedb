#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

source "${SCRIPT_DIR}/run_paradedb.sh"

PORT="${PARADEDB_PORT:-5432}"
USER="${PARADEDB_USER:-postgres}"
PASSWORD="${PARADEDB_PASSWORD:-postgres}"
DB="${PARADEDB_DB:-postgres}"

export PARADEDB_INTEGRATION=1
export PARADEDB_TEST_DSN="postgres://${USER}:${PASSWORD}@localhost:${PORT}/${DB}"
export PGPASSWORD="${PASSWORD}"

if ! command -v uv >/dev/null 2>&1; then
  echo "uv is required to run integration tests." >&2
  echo "Install uv, then rerun this script." >&2
  exit 1
fi

PYTEST_CMD=(uv run --extra dev pytest)

if [[ $# -gt 0 ]]; then
  "${PYTEST_CMD[@]}" "$@"
else
  "${PYTEST_CMD[@]}" -m integration
fi
