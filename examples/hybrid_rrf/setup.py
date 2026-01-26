#!/usr/bin/env python
"""Setup script: Generate and store embeddings for hybrid search."""

import os
from urllib.parse import urlparse

import django
import httpx
from django.conf import settings
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.environ.get(
    "DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/postgres"
)
OPENROUTER_API_KEY = os.environ.get("OPENROUTER_API_KEY")

if not OPENROUTER_API_KEY:
    raise ValueError("OPENROUTER_API_KEY not found in environment. Add it to .env file")

parsed = urlparse(DATABASE_URL)
if not settings.configured:
    settings.configure(
        DEBUG=True,
        DATABASES={
            "default": {
                "ENGINE": "django.db.backends.postgresql",
                "NAME": parsed.path.lstrip("/"),
                "USER": parsed.username or "postgres",
                "PASSWORD": parsed.password or "",
                "HOST": parsed.hostname or "localhost",
                "PORT": parsed.port or 5432,
            }
        },
        INSTALLED_APPS=["django.contrib.contenttypes"],
        DEFAULT_AUTO_FIELD="django.db.models.BigAutoField",
    )

django.setup()

from django.db import connection, models  # noqa: E402
from pgvector.django import VectorField  # noqa: E402


class MockItem(models.Model):
    """ParadeDB's mock_items table with vector embeddings."""

    id = models.IntegerField(primary_key=True)
    description = models.TextField()
    category = models.CharField(max_length=100)
    rating = models.IntegerField()
    in_stock = models.BooleanField()
    created_at = models.DateTimeField()
    metadata = models.JSONField(null=True)
    embedding = VectorField(dimensions=384, null=True)

    class Meta:
        app_label = "hybrid_rrf"
        managed = False
        db_table = "mock_items"


def get_embedding(text: str) -> list[float]:
    """Get embedding from OpenRouter."""
    response = httpx.post(
        "https://openrouter.ai/api/v1/embeddings",
        headers={
            "Authorization": f"Bearer {OPENROUTER_API_KEY}",
            "Content-Type": "application/json",
        },
        json={
            "model": "sentence-transformers/paraphrase-minilm-l6-v2",
            "input": text,
        },
        timeout=30.0,
    )
    response.raise_for_status()
    return response.json()["data"][0]["embedding"]


def setup() -> None:
    """Generate embeddings and add vector column."""
    with connection.cursor() as cursor:
        # Ensure extensions exist
        cursor.execute("CREATE EXTENSION IF NOT EXISTS vector")
        cursor.execute("CREATE EXTENSION IF NOT EXISTS pg_search")

        # Create mock_items if not exists
        cursor.execute(
            "CALL paradedb.create_bm25_test_table("
            "schema_name => 'public', table_name => 'mock_items')"
        )

        # Add embedding column if not exists
        cursor.execute(
            """
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_name = 'mock_items' AND column_name = 'embedding'
                ) THEN
                    ALTER TABLE mock_items ADD COLUMN embedding vector(384);
                END IF;
            END $$;
            """
        )

    # Generate embeddings for all items
    items = MockItem.objects.filter(embedding__isnull=True)
    total = items.count()

    if total == 0:
        print("✓ All items already have embeddings")
        return

    print(f"Generating embeddings for {total} items...")

    for i, item in enumerate(items, 1):
        print(f"  [{i}/{total}] {item.description[:50]}...")
        embedding = get_embedding(item.description)
        item.embedding = embedding
        item.save()

    print(f"✓ Generated {total} embeddings")


if __name__ == "__main__":
    print("=" * 60)
    print("Hybrid Search Setup - Generating Embeddings")
    print("=" * 60)
    setup()
    print("\nSetup complete! Run: python examples/hybrid_rrf/example.py")
