#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${REPO_ROOT}"

if [[ -z "${PARADEDB_TEST_DSN:-${DATABASE_URL:-}}" ]]; then
  # shellcheck source=scripts/run_paradedb.sh
  source "${SCRIPT_DIR}/run_paradedb.sh"
fi

export PARADEDB_TEST_DSN="${PARADEDB_TEST_DSN:-${DATABASE_URL}}"
export DATABASE_URL="${DATABASE_URL:-${PARADEDB_TEST_DSN}}"
export PGPASSWORD="${PGPASSWORD:-${PARADEDB_PASSWORD:-postgres}}"

if ! command -v uv >/dev/null 2>&1; then
  echo "uv is required to run integration tests." >&2
  echo "Install uv, then rerun this script." >&2
  exit 1
fi

PYTEST_CMD=(uv run --extra dev pytest)

"${PYTEST_CMD[@]}" "$@"
