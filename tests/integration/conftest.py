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
            "CREATE INDEX mock_items_bm25_idx ON mock_items USING bm25 ("
            "id, "
            "description, "
            "category, "
            "rating, "
            "in_stock, "
            "((metadata->>'color')::pdb.literal('alias=metadata_color')), "
            "((metadata->>'location')::pdb.literal('alias=metadata_location'))"
            ") WITH (key_field='id');"
        )
        cursor.execute(
            "SELECT 1 FROM pg_indexes WHERE schemaname = 'public' AND indexname = 'mock_items_bm25_idx';"
        )
        index_present = cursor.fetchone() is not None
        cursor.execute("SELECT COUNT(*) FROM mock_items;")
        (row_count,) = cursor.fetchone()

        _assert_columns_exist(
            [
                "id",
                "description",
                "category",
                "rating",
                "in_stock",
                "created_at",
                "metadata",
            ]
        )
        assert row_count > 0, "mock_items should be seeded with rows"
        assert index_present, "mock_items_bm25_idx should exist"

        connection.commit()


@pytest.fixture(scope="function")
def mock_items(paradedb_ready: None) -> None:
    """Function-scoped dependency that guarantees mock_items is available."""
    _ = paradedb_ready
    return None


def _seed_json_table(*, table: str, index: str, json_fields: str) -> None:
    with connection.cursor() as cursor:
        cursor.execute(f"DROP TABLE IF EXISTS {table};")
        cursor.execute(
            f"CREATE TABLE {table} (id integer primary key, metadata jsonb);"
        )
        cursor.execute(
            f"""
            INSERT INTO {table} (id, metadata) VALUES
            (1, '{{"color":"red","location":"US","size":3,"flags":{{"featured":true}},"tags":["a","b"],"metrics":{{"1":"one","2":"two"}},"deep":{{"nested":{{"value":"x"}}}},"dot.key":"dk","weird key":"space","dash-key":"d","arrnums":[1,2,2],"bools":[true,false],"mixed":[1,"1",true,null],"nullval":null,"emptystr":"","numfloat":2.5,"objarray":[{{"k":"v1"}},{{"k":"v2"}}],"nested_array":[["x","y"],["y"]],"obj":{{"inner":1}},"objarray_deep":[{{"meta":{{"code":"c1"}}}},{{"meta":{{"code":null}}}},{{}}]}}'),
            (2, '{{"color":"blue","location":"CA","size":5,"flags":{{"featured":false}},"tags":["b","c"],"metrics":{{"1":"uno"}},"deep":{{"nested":{{"value":"y"}}}},"dot.key":"dk2","arrnums":[3],"bools":[true],"mixed":["1"],"emptystr":"","numfloat":2.5,"objarray":[{{"k":"v1"}}],"nested_array":[["y"]],"obj":{{"inner":2}},"objarray_deep":[{{"meta":{{"code":"c2"}}}}]}}'),
            (3, '{{"color":"red","location":"US","size":3,"tags":[],"metrics":{{}},"deep":{{}},"arrnums":[],"bools":[],"mixed":[],"nullval":null,"objarray":[],"nested_array":[],"objarray_deep":[]}}');
            """
        )
        cursor.execute(f"DROP INDEX IF EXISTS {index};")
        cursor.execute(
            f"CREATE INDEX {index} ON {table} USING bm25 (id, metadata) WITH (key_field='id', json_fields='{json_fields}');"
        )
        connection.commit()


@pytest.fixture(scope="function")
def json_items(paradedb_ready: None) -> None:
    """Ensure json_items table is available with default JSON field settings."""
    _ = paradedb_ready
    _seed_json_table(
        table="json_items",
        index="json_items_bm25_idx",
        json_fields='{"metadata":{"fast":true}}',
    )
    return None


@pytest.fixture(scope="function")
def json_items_no_expand(paradedb_ready: None) -> None:
    """Ensure json_items_no_expand table uses expand_dots=false."""
    _ = paradedb_ready
    _seed_json_table(
        table="json_items_no_expand",
        index="json_items_no_expand_bm25_idx",
        json_fields='{"metadata":{"fast":true,"expand_dots":false}}',
    )
    return None
