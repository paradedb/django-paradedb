#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUTPUT_DIR="${1:-$ROOT_DIR/site}"
cd "$ROOT_DIR"

uv run pdoc \
  --output-directory "$OUTPUT_DIR" \
  --edit-url "paradedb=https://github.com/paradedb/django-paradedb/blob/main/paradedb/" \
  --footer-text "django-paradedb API documentation" \
  paradedb
