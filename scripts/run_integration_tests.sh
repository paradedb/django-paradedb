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

if [[ $# -gt 0 ]]; then
  ./.venv/bin/python -m pytest "$@"
else
  ./.venv/bin/python -m pytest -m integration
fi
