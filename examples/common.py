"""Common utilities for django-paradedb examples.

This module provides shared Django setup, database configuration,
and model definitions used across all examples.
"""

import os
from urllib.parse import urlparse

import django
from django.conf import settings

from paradedb.indexes import ParadeDBIndex
from paradedb.queryset import ParadeDBManager
from paradedb.search import Tokenizer
from paradedb.vector import VectorField


def configure_django() -> None:
    """Configure Django settings for standalone scripts.

    Uses standard PostgreSQL environment variables:
    - PGHOST (default: localhost)
    - PGPORT (default: 5432)
    - PGUSER (default: postgres)
    - PGPASSWORD (default: empty)
    - PGDATABASE (default: postgres)
    """
    if settings.configured:
        return

    database_url = os.environ.get("DATABASE_URL")
    if database_url:
        parsed = urlparse(database_url)
        name = (parsed.path or "/postgres").lstrip("/") or "postgres"
        database_settings = {
            "ENGINE": "django.db.backends.postgresql",
            "NAME": name,
            "USER": parsed.username or "postgres",
            "PASSWORD": parsed.password or "",
            "HOST": parsed.hostname or "localhost",
            "PORT": int(parsed.port or 5432),
        }
    else:
        database_settings = {
            "ENGINE": "django.db.backends.postgresql",
            "NAME": os.environ.get("PGDATABASE", "postgres"),
            "USER": os.environ.get("PGUSER", "postgres"),
            "PASSWORD": os.environ.get("PGPASSWORD", ""),
            "HOST": os.environ.get("PGHOST", "localhost"),
            "PORT": int(os.environ.get("PGPORT", "5432")),
        }

    settings.configure(
        DEBUG=os.environ.get("DJANGO_DEBUG", "0") == "1",
        DATABASES={"default": database_settings},
        INSTALLED_APPS=["django.contrib.contenttypes"],
        DEFAULT_AUTO_FIELD="django.db.models.BigAutoField",
    )
    django.setup()


def setup_mock_items() -> int:
    """Create mock_items table with a ParadeDB index. Returns row count."""
    from django.db import connection

    with connection.cursor() as cursor:
        cursor.execute("CREATE EXTENSION IF NOT EXISTS vector")
        cursor.execute("CREATE EXTENSION IF NOT EXISTS pg_search CASCADE")
        cursor.execute(
            "CALL paradedb.create_bm25_test_table("
            "schema_name => 'public', table_name => 'mock_items')"
        )
        cursor.execute("DROP INDEX IF EXISTS search_idx;")

    # Render index SQL from the unmanaged model metadata.
    with connection.schema_editor(atomic=False) as schema_editor:
        for index in MockItem._meta.indexes:
            statement = index.create_sql(model=MockItem, schema_editor=schema_editor)
            schema_editor.execute(statement)

    return MockItem.objects.count()


# Pre-computed 8-dim query embeddings in the same embedding space as the
# mock_items.embedding column seeded by paradedb.create_bm25_test_table().
QUERY_EMBEDDINGS: dict[str, list[float]] = {
    "running shoes": [-0.02, 0.47, -0.76, 0.13, 0.34, 0.04, 0.19, -0.19],
    "footwear for exercise": [-0.06, 0.40, -0.71, 0.02, 0.39, 0.15, 0.30, -0.14],
    "wireless earbuds": [-0.08, 0.19, -0.88, 0.16, 0.30, 0.03, -0.08, -0.23],
}

# Examples are standalone scripts, so this module intentionally configures Django
# at import time to keep per-example boilerplate minimal.
configure_django()

# Import models after Django is configured
from django.db import models  # noqa: E402


def _mock_items_indexes() -> list[ParadeDBIndex]:
    return [
        ParadeDBIndex(
            fields={
                "id": {},
                "description": {},
                "rating": {},
                "category": {"tokenizer": Tokenizer.literal({"alias": "category"})},
                "metadata": {"json_fields": {"fast": True}},
                "embedding": {"metric": "cosine"},
            },
            key_field="id",
            name="search_idx",
        ),
    ]


class MockItem(models.Model):
    """ParadeDB's built-in mock_items table.

    This unmanaged model maps to the mock_items table created by
    paradedb.create_bm25_test_table(). It contains sample product
    data with a pre-configured ParadeDB index on description, rating,
    category, native metadata subfields like ``metadata.color``, and
    a pre-populated 8-dim ``embedding`` vector column.
    """

    id = models.IntegerField(primary_key=True)
    description = models.TextField()
    category = models.CharField(max_length=100)
    rating = models.IntegerField()
    in_stock = models.BooleanField()
    created_at = models.DateTimeField()
    metadata = models.JSONField(null=True)
    embedding = VectorField(dimensions=8, null=True)

    objects = ParadeDBManager()

    class Meta:
        app_label = "examples"
        managed = False
        db_table = "mock_items"
        indexes = _mock_items_indexes()

    def __str__(self) -> str:
        return self.description
