"""Shared fixtures for ParadeDB integration tests."""

from __future__ import annotations

from collections.abc import Iterable

import pytest
from django.db import connection

try:
    import psycopg2  # noqa: F401
except ImportError as exc:
    raise RuntimeError("psycopg2 is required to run integration tests") from exc


def _require_postgres() -> None:
    engine = connection.settings_dict.get("ENGINE", "")
    if "postgresql" not in engine:
        pytest.fail("Integration tests require a Postgres/ParadeDB backend")


def _assert_columns_exist(required: Iterable[str]) -> None:
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name = 'mock_items'"
        )
        columns = {row[0] for row in cursor.fetchall()}
    missing = set(required) - columns
    if missing:
        pytest.fail(
            f"mock_items missing required columns: {', '.join(sorted(missing))}"
        )


@pytest.fixture(scope="session")
def paradedb_ready(django_db_setup: object, django_db_blocker: object) -> None:
    """Ensure ParadeDB is available and mock data is seeded."""
    _ = django_db_setup
    _require_postgres()

    with django_db_blocker.unblock(), connection.cursor() as cursor:
        try:
            cursor.execute("CREATE EXTENSION IF NOT EXISTS pg_search;")
        except Exception as exc:  # pragma: no cover - defensive skip
            pytest.fail(
                f"ParadeDB pg_search extension unavailable in target database: {exc}"
            )
        cursor.execute(
            "CALL paradedb.create_bm25_test_table(schema_name => 'public', table_name => 'mock_items');"
        )
        cursor.execute("DROP INDEX IF EXISTS mock_items_bm25_idx;")
        cursor.execute(
            "CREATE INDEX mock_items_bm25_idx ON mock_items USING bm25 (id, description) WITH (key_field='id');"
        )
        cursor.execute(
            "SELECT 1 FROM pg_indexes WHERE schemaname = 'public' AND indexname = 'mock_items_bm25_idx';"
        )
        index_present = cursor.fetchone() is not None
        cursor.execute("SELECT COUNT(*) FROM mock_items;")
        (row_count,) = cursor.fetchone()

        _assert_columns_exist(["id", "description"])
        assert row_count > 0, "mock_items should be seeded with rows"
        assert index_present, "mock_items_bm25_idx should exist"

        connection.commit()


@pytest.fixture(scope="function")
def mock_items(paradedb_ready: None) -> None:
    """Function-scoped dependency that guarantees mock_items is available."""
    _ = paradedb_ready
    return None
