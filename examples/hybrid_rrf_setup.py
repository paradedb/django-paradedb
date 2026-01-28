#!/usr/bin/env python
"""Setup script: Generate and store embeddings for hybrid search."""

import os

import httpx
from _common import MockItemWithEmbedding as MockItem
from _common import setup_mock_items
from dotenv import load_dotenv

load_dotenv()

OPENROUTER_API_KEY = os.environ.get("OPENROUTER_API_KEY")

if not OPENROUTER_API_KEY:
    raise ValueError("OPENROUTER_API_KEY not found in environment. Add it to .env file")

if MockItem is None:
    raise ImportError("pgvector is required for this example. pip install pgvector")


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
    from django.db import connection

    setup_mock_items()

    with connection.cursor() as cursor:
        # Ensure vector extension exists
        cursor.execute("CREATE EXTENSION IF NOT EXISTS vector")

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
