#!/usr/bin/env bash

set -euo pipefail

if ! command -v uv >/dev/null 2>&1; then
  echo "uv is required to run unit tests." >&2
  echo "Install uv, then rerun this script." >&2
  exit 1
fi

if [[ "${1:-}" == "--django" ]]; then
  DJANGO_SPEC="Django~=${2:?'--django requires a version argument (e.g. 4.2, 5.2, 6.0)'}.0"
  shift 2
fi

PYTEST_CMD=(uv run --extra dev)
if [[ -n "${DJANGO_SPEC:-}" ]]; then
  PYTEST_CMD+=(--with "${DJANGO_SPEC}")
fi
PYTEST_CMD+=(pytest)

if [[ $# -gt 0 ]]; then
  "${PYTEST_CMD[@]}" "$@"
else
  "${PYTEST_CMD[@]}" -m "not integration"
fi
