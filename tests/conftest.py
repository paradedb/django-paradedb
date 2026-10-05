"""Pytest configuration"""

from __future__ import annotations

from collections.abc import Iterable

import django
import pytest
from django.conf import settings
from django.db import connection

from paradedb import ParadeDBIndex, Tokenizer
from tests._db_config import database_settings
from tests.models import MockItem


def pytest_configure(config: object) -> None:
    """Ensure Django is initialized and register custom markers."""
    config.addinivalue_line(
        "markers",
        "integration: marks tests that require a ParadeDB Postgres instance",
    )

    if not settings.configured:
        settings.configure(
            INSTALLED_APPS=["django.contrib.contenttypes", "tests"],
            DATABASES={"default": database_settings()},
            DEFAULT_AUTO_FIELD="django.db.models.BigAutoField",
            SECRET_KEY="tests-secret-key",
            MIGRATION_MODULES={"tests": None},
        )
    elif "postgresql" not in settings.DATABASES.get("default", {}).get("ENGINE", ""):
        settings.DATABASES["default"] = database_settings()

    django.setup()


try:
    import psycopg  # noqa: F401
except ImportError as exc:
    raise RuntimeError("psycopg is required to run integration tests") from exc


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
            cursor.execute("CREATE EXTENSION IF NOT EXISTS vector;")
            cursor.execute("CREATE EXTENSION IF NOT EXISTS pg_search CASCADE;")
        except Exception as exc:  # pragma: no cover - defensive skip
            pytest.fail(
                f"ParadeDB pg_search extension unavailable in target database: {exc}"
            )
        cursor.execute(
            "CALL paradedb.create_paradedb_test_table(schema_name => 'public', table_name => 'mock_items');"
        )
        cursor.execute("DROP INDEX IF EXISTS mock_items_search_idx;")
        cursor.execute(
            "CREATE INDEX mock_items_search_idx ON mock_items USING paradedb ("
            "id, "
            "description, "
            "category, "
            "rating, "
            "in_stock, "
            "metadata, "
            "embedding vector_cosine_ops, "
            "(((description || ' ' || category)::pdb.simple('alias=combined')))"
            ') WITH (json_fields=\'{"metadata":{"fast":true}}\');'
        )
        cursor.execute(
            "SELECT 1 FROM pg_indexes WHERE schemaname = 'public' AND indexname = 'mock_items_search_idx';"
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
                "embedding",
            ]
        )
        assert row_count > 0, "mock_items should be seeded with rows"
        assert index_present, "mock_items_search_idx should exist"

        connection.commit()


@pytest.fixture(scope="function")
def mock_items(paradedb_ready: None) -> None:
    """Function-scoped dependency that guarantees mock_items is available."""
    _ = paradedb_ready
    return None


@pytest.fixture
def partitioned_vector_index(transactional_db, paradedb_ready):
    _ = transactional_db, paradedb_ready
    index = ParadeDBIndex(
        fields={
            "id": {},
            "rating": {},
            "description": {
                "tokenizers": [
                    {"tokenizer": Tokenizer.simple(options={"pnorms": True})},
                    {
                        "tokenizer": Tokenizer.jieba(
                            options={"alias": "description_jieba", "search_mode": False}
                        )
                    },
                    {
                        "tokenizer": Tokenizer.chinese_compatible(
                            options={
                                "alias": "description_chinese",
                                "chinese_convert": "t2s",
                            }
                        )
                    },
                ]
            },
            "embedding": {"metric": "l2"},
        },
        name="pg26_idx",
        partition_by="rating,id",
        target_segment_count=8,
        vector_fields={"embedding": {"quantization": False}},
    )
    ddl = str(index.create_sql(MockItem, connection.schema_editor())).replace(
        '"mock_items"', '"pg26_items"'
    )
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                "CREATE TABLE pg26_items (id int, rating int, description text, embedding vector(64))"
            )
            cursor.execute(
                "INSERT INTO pg26_items SELECT i, i % 3, 'partitioned shoes', ARRAY(SELECT sin(i*j)::real FROM generate_series(1,64) j)::vector FROM generate_series(1, 2048) i"
            )
            cursor.execute(ddl)
        yield index
    finally:
        with connection.cursor() as cursor:
            cursor.execute("DROP TABLE IF EXISTS pg26_items")
