#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

WORK_DIR="$(mktemp -d)"
trap 'rm -rf "${WORK_DIR}"' EXIT

PYTHON_BIN="${PYTHON_BIN:-}"
if [[ -z "${PYTHON_BIN}" ]]; then
  if command -v python >/dev/null 2>&1; then
    PYTHON_BIN="$(command -v python)"
  elif command -v python3 >/dev/null 2>&1; then
    PYTHON_BIN="$(command -v python3)"
  else
    echo "Unable to find a Python interpreter. Set PYTHON_BIN or install Python." >&2
    exit 1
  fi
fi

DIST_DIR="${WORK_DIR}/dist"
"${PYTHON_BIN}" -m pip wheel . --no-deps -w "${DIST_DIR}"

"${PYTHON_BIN}" -m venv "${WORK_DIR}/venv"
PYTHON_BIN="${WORK_DIR}/venv/bin/python"
PIP_BIN="${WORK_DIR}/venv/bin/pip"

"${PIP_BIN}" install --upgrade pip
"${PIP_BIN}" install "${DIST_DIR}"/django_paradedb-*.whl

DJANGO_SETTINGS_MODULE= "${PYTHON_BIN}" - <<'PY'
from django.conf import settings
import django

if not settings.configured:
    settings.configure(
        SECRET_KEY="smoke",
        INSTALLED_APPS=["django.contrib.contenttypes"],
        DATABASES={"default": {"ENGINE": "django.db.backends.sqlite3", "NAME": ":memory:"}},
    )

django.setup()

from django.db import models
from paradedb.search import Match, ParadeDB


class SmokeModel(models.Model):
    description = models.TextField()

    class Meta:
        app_label = "smoke"


sql = str(
    SmokeModel.objects.filter(description=ParadeDB(Match("shoes", operator="AND"))).query
)
if "&&&" not in sql:
    raise SystemExit("Wheel smoke test failed: expected ParadeDB SQL operator.")
PY
