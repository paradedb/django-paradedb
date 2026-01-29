#!/usr/bin/env python
"""Setup script: Load embeddings for hybrid search from CSV."""

import ast
import csv
from pathlib import Path

from _common import MockItemWithEmbedding as MockItem
from _common import setup_mock_items

if MockItem is None:
    raise ImportError("pgvector is required for this example. pip install pgvector")


def load_embeddings_from_csv(csv_path: Path) -> dict[int, list[float]]:
    """Load embeddings from CSV file."""
    embeddings = {}
    with csv_path.open() as f:
        reader = csv.DictReader(f)
        for row in reader:
            id = int(row["id"])
            # Parse the vector string back to list
            embedding_str = row["embedding"]
            embedding = ast.literal_eval(embedding_str)
            embeddings[id] = embedding
    return embeddings


def setup() -> None:
    """Load embeddings from CSV into database."""
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

    # Check if embeddings already loaded
    items_with_embeddings = MockItem.objects.filter(embedding__isnull=False).count()
    if items_with_embeddings > 0:
        print(f"✓ {items_with_embeddings} items already have embeddings")
        return

    # Load embeddings from CSV
    csv_path = Path(__file__).parent / "mock_items_embeddings.csv"
    if not csv_path.exists():
        print(f"✗ CSV file not found: {csv_path}")
        return

    print(f"Loading embeddings from {csv_path}...")
    embeddings = load_embeddings_from_csv(csv_path)
    total = len(embeddings)
    print(f"Found {total} embeddings in CSV")

    # Update items with embeddings
    for i, (item_id, embedding) in enumerate(embeddings.items(), 1):
        try:
            item = MockItem.objects.get(id=item_id)
            item.embedding = embedding
            item.save()
            print(f"  [{i}/{total}] {item.description[:50]}...")
        except MockItem.DoesNotExist:
            print(f"  [{i}/{total}] Skipping ID {item_id} - not found")

    print(f"✓ Loaded {total} embeddings")


if __name__ == "__main__":
    print("=" * 60)
    print("Hybrid Search Setup - Loading Embeddings from CSV")
    print("=" * 60)
    setup()
    print("\nSetup complete! Run: python examples/hybrid_rrf.py")
