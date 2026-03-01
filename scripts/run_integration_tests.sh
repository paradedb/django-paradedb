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

PYTHON_BIN="${PYTHON_BIN:-}"
if [[ -z "${PYTHON_BIN}" ]]; then
  if [[ -x "./.venv/bin/python" ]]; then
    PYTHON_BIN="./.venv/bin/python"
  elif command -v python3 >/dev/null 2>&1; then
    PYTHON_BIN="$(command -v python3)"
  elif command -v python >/dev/null 2>&1; then
    PYTHON_BIN="$(command -v python)"
  else
    echo "Unable to find a Python interpreter. Set PYTHON_BIN or install Python." >&2
    exit 1
  fi
fi

if ! "${PYTHON_BIN}" -m pytest --version >/dev/null 2>&1; then
  echo "pytest is not available for ${PYTHON_BIN}. Install dev dependencies first." >&2
  echo "Hint: pip install -e '.[dev]'" >&2
  exit 1
fi

if [[ $# -gt 0 ]]; then
  "${PYTHON_BIN}" -m pytest "$@"
else
  "${PYTHON_BIN}" -m pytest -m integration
fi
