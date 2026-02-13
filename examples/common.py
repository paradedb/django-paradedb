"""Common utilities for django-paradedb examples.

This module provides shared Django setup, database configuration,
and model definitions used across all examples.
"""

import os
from urllib.parse import urlparse

import django
from django.conf import settings

from paradedb.indexes import BM25Index
from paradedb.queryset import ParadeDBManager


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
        DEBUG=True,
        DATABASES={"default": database_settings},
        INSTALLED_APPS=["django.contrib.contenttypes"],
        DEFAULT_AUTO_FIELD="django.db.models.BigAutoField",
    )
    django.setup()


def setup_mock_items() -> int:
    """Create mock_items table with BM25 index. Returns row count."""
    from django.db import connection

    with connection.cursor() as cursor:
        cursor.execute("CREATE EXTENSION IF NOT EXISTS pg_search")
        cursor.execute(
            "CALL paradedb.create_bm25_test_table("
            "schema_name => 'public', table_name => 'mock_items')"
        )
        cursor.execute("DROP INDEX IF EXISTS mock_items_bm25_idx;")

    # Render index SQL from the unmanaged model metadata.
    with connection.schema_editor(atomic=False) as schema_editor:
        for index in MockItem._meta.indexes:
            statement = index.create_sql(model=MockItem, schema_editor=schema_editor)
            schema_editor.execute(statement)

    return MockItem.objects.count()


# Initialize Django when this module is imported
configure_django()

# Import models after Django is configured
from django.db import models  # noqa: E402


def _mock_items_indexes() -> list[BM25Index]:
    return [
        BM25Index(
            fields={
                "id": {},
                "description": {},
                "rating": {},
                "category": {"tokenizer": "literal", "alias": "category"},
                "metadata": {
                    "json_keys": {
                        "color": {"tokenizer": "literal"},
                        "location": {"tokenizer": "literal"},
                    }
                },
            },
            key_field="id",
            name="mock_items_bm25_idx",
        ),
    ]


class MockItem(models.Model):
    """ParadeDB's built-in mock_items table.

    This unmanaged model maps to the mock_items table created by
    paradedb.create_bm25_test_table(). It contains sample product
    data with a pre-configured BM25 index on the description field.
    """

    id = models.IntegerField(primary_key=True)
    description = models.TextField()
    category = models.CharField(max_length=100)
    rating = models.IntegerField()
    in_stock = models.BooleanField()
    created_at = models.DateTimeField()
    metadata = models.JSONField(null=True)

    objects = ParadeDBManager()

    class Meta:
        app_label = "examples"
        managed = False
        db_table = "mock_items"
        indexes = _mock_items_indexes()

    def __str__(self) -> str:
        return self.description


# Optional: MockItem with vector embedding field for hybrid search examples
try:
    from pgvector.django import VectorField

    class MockItemWithEmbedding(models.Model):
        """MockItem with vector embedding for hybrid search examples."""

        id = models.IntegerField(primary_key=True)
        description = models.TextField()
        category = models.CharField(max_length=100)
        rating = models.IntegerField()
        in_stock = models.BooleanField()
        created_at = models.DateTimeField()
        metadata = models.JSONField(null=True)
        embedding = VectorField(dimensions=384, null=True)

        objects = ParadeDBManager()

        class Meta:
            app_label = "examples"
            managed = False
            db_table = "mock_items"
            indexes = _mock_items_indexes()

        def __str__(self) -> str:
            return self.description

except ImportError:
    MockItemWithEmbedding = None  # type: ignore[misc, assignment]
