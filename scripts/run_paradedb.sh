#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
IMAGE="${PARADEDB_IMAGE:-paradedb/paradedb:0.21.4-pg18}"
CONTAINER_NAME="${PARADEDB_CONTAINER_NAME:-paradedb-integration}"
INIT_MOCK_ITEMS="${PARADEDB_INIT_MOCK_ITEMS:-0}"

source "${SCRIPT_DIR}/paradedb_env.sh"

if ! command -v docker >/dev/null 2>&1; then
  echo "docker is required to run ParadeDB" >&2
  exit 1
fi

if ! docker ps -a --format '{{.Names}}' | grep -Eq "^${CONTAINER_NAME}$"; then
  echo "Starting ParadeDB container ${CONTAINER_NAME} from ${IMAGE}..."
  docker run -d \
    --name "${CONTAINER_NAME}" \
    -e "POSTGRES_USER=${PARADEDB_USER}" \
    -e "POSTGRES_PASSWORD=${PARADEDB_PASSWORD}" \
    -e "POSTGRES_DB=${PARADEDB_DB}" \
    -p "${PARADEDB_PORT}:5432" \
    "${IMAGE}" >/dev/null
else
  echo "Container ${CONTAINER_NAME} already exists; starting it..."
  docker start "${CONTAINER_NAME}" >/dev/null
fi

echo "Waiting for ParadeDB to become ready..."
for _ in {1..30}; do
  if docker exec "${CONTAINER_NAME}" pg_isready -U "${PARADEDB_USER}" -d "${PARADEDB_DB}" >/dev/null 2>&1; then
    break
  fi
  sleep 2
done

if ! docker exec "${CONTAINER_NAME}" pg_isready -U "${PARADEDB_USER}" -d "${PARADEDB_DB}" >/dev/null 2>&1; then
  echo "ParadeDB did not become ready in time" >&2
  exit 1
fi

echo "ParadeDB is running in container ${CONTAINER_NAME}."
echo "To set env vars for tests/examples, run:"
echo "  source scripts/paradedb_env.sh"

if [[ "${INIT_MOCK_ITEMS}" == "1" ]]; then
  echo "Initializing mock_items table..."
  docker exec "${CONTAINER_NAME}" psql -U "${PARADEDB_USER}" -d "${PARADEDB_DB}" -v ON_ERROR_STOP=1 \
    -c "CREATE EXTENSION IF NOT EXISTS pg_search; CALL paradedb.create_bm25_test_table(schema_name => 'public', table_name => 'mock_items');" \
    >/dev/null
  echo "mock_items initialized."
fi
