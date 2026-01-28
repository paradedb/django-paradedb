"""Common utilities for django-paradedb examples.

This module provides shared Django setup, database configuration,
and model definitions used across all examples.
"""

import os

import django
from django.conf import settings


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

    settings.configure(
        DEBUG=True,
        DATABASES={
            "default": {
                "ENGINE": "django.db.backends.postgresql",
                "NAME": os.environ.get("PGDATABASE", "postgres"),
                "USER": os.environ.get("PGUSER", "postgres"),
                "PASSWORD": os.environ.get("PGPASSWORD", ""),
                "HOST": os.environ.get("PGHOST", "localhost"),
                "PORT": int(os.environ.get("PGPORT", "5432")),
            }
        },
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

    from django.db import models

    class MockItem(models.Model):
        id = models.IntegerField(primary_key=True)

        class Meta:
            app_label = "examples"
            managed = False
            db_table = "mock_items"

        def __str__(self) -> str:
            return f"MockItem(id={self.id})"

    return MockItem.objects.count()


# Initialize Django when this module is imported
configure_django()

# Import models after Django is configured
from django.db import models  # noqa: E402


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

    class Meta:
        app_label = "examples"
        managed = False
        db_table = "mock_items"

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

        class Meta:
            app_label = "examples"
            managed = False
            db_table = "mock_items"

        def __str__(self) -> str:
            return self.description

except ImportError:
    MockItemWithEmbedding = None  # type: ignore[misc, assignment]
