#!/usr/bin/env bash

# Check if script is being run directly or sourced
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
  RUNNING=1
  set -euo pipefail
else
  RUNNING=0
fi

IMAGE="${PARADEDB_IMAGE:-paradedb/paradedb:0.21.4-pg18}"
CONTAINER_NAME="${PARADEDB_CONTAINER_NAME:-paradedb-integration}"
DATABASE_URL="${DATABASE_URL:-postgresql://postgres:postgres@localhost:5432/postgres}"
export DATABASE_URL

if ! command -v docker >/dev/null 2>&1; then
  echo "docker is required to run ParadeDB" >&2
  if [[ "$RUNNING" == "1" ]]; then exit 1; else return 1; fi
fi

if ! docker ps -a --format '{{.Names}}' | grep -Eq "^${CONTAINER_NAME}$"; then
  echo "Starting ParadeDB container ${CONTAINER_NAME} from ${IMAGE}..."
  docker run -d \
    --name "${CONTAINER_NAME}" \
    -e "POSTGRES_USER=postgres" \
    -e "POSTGRES_PASSWORD=postgres" \
    -e "POSTGRES_DB=postgres" \
    -p "5432:5432" \
    "${IMAGE}" >/dev/null
else
  echo "Container ${CONTAINER_NAME} already exists; starting it..."
  docker start "${CONTAINER_NAME}" >/dev/null
fi

echo "Waiting for ParadeDB to become ready..."
for _ in {1..30}; do
  if docker exec "${CONTAINER_NAME}" pg_isready -U postgres -d postgres >/dev/null 2>&1; then
    break
  fi
  sleep 2
done

if ! docker exec "${CONTAINER_NAME}" pg_isready -U postgres -d postgres >/dev/null 2>&1; then
  echo "ParadeDB did not become ready in time" >&2
  if [[ "$RUNNING" == "1" ]]; then exit 1; else return 1; fi
fi

echo "ParadeDB is running in container ${CONTAINER_NAME}."
echo "DATABASE_URL is set to: ${DATABASE_URL}"

if [[ "$RUNNING" == "0" ]]; then
  echo "You can now use the examples in your current shell."
fi
