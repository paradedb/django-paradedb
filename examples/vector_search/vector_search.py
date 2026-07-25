#!/usr/bin/env python
"""Vector search example using ParadeDB's bm25 index.

ParadeDB indexes pgvector ``vector`` columns inside its bm25 index and serves
Top-K nearest-neighbor queries. Three things are required for index-accelerated
vector search:

1. A ``@@@`` predicate to activate the index scan — use ``ParadeDB(All())``
   for a pure vector query.
2. A distance operator that matches the metric of the index opclass
   (``l2`` ↔ ``L2Distance``, ``cosine`` ↔ ``CosineDistance``,
   ``ip`` ↔ ``InnerProduct``).
3. A ``LIMIT`` (slice the queryset) to get Top-K pushdown.

Requires a pg_search build with vector support in bm25 indexes.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common import MockItemWithEmbedding

from paradedb.indexes import ParadeDBIndex
from paradedb.search import All, MatchAll, ParadeDB
from paradedb.vector import CosineDistance


def paradedb_supports_vectors() -> bool:
    from django.db import connection

    with connection.cursor() as cursor:
        cursor.execute("CREATE EXTENSION IF NOT EXISTS pg_search")
        cursor.execute(
            "SELECT 1 FROM pg_opclass o JOIN pg_am a ON o.opcmethod = a.oid "
            "WHERE a.amname = 'paradedb' AND o.opcname = 'vector_cosine_ops';"
        )
        return cursor.fetchone() is not None


def create_vector_index() -> None:
    """Recreate the mock_items index with the embedding column included."""
    from django.db import connection

    index = ParadeDBIndex(
        fields={
            "id": {},
            "description": {},
            "embedding": {"metric": "cosine"},
        },
        key_field="id",
        name="mock_items_bm25_idx",
    )
    with connection.cursor() as cursor:
        cursor.execute("DROP INDEX IF EXISTS mock_items_bm25_idx;")
    with connection.schema_editor(atomic=False) as schema_editor:
        schema_editor.execute(
            index.create_sql(model=MockItemWithEmbedding, schema_editor=schema_editor)
        )
    print("✓ Created bm25 index with embedding vector_cosine_ops")


def demo_semantic_search(query: str, query_embedding: list[float]) -> None:
    """Pure vector Top-K query with the mandatory match-all predicate."""
    print(f"\n--- Semantic Search: '{query}' ---")
    results = (
        MockItemWithEmbedding.objects.filter(id=ParadeDB(All()))
        .annotate(distance=CosineDistance("embedding", query_embedding))
        .order_by("distance")[:5]
    )
    for item in results:
        print(f"  • {item.description[:55]:<55} (distance: {item.distance:.4f})")


def demo_filtered_semantic_search(query: str, query_embedding: list[float]) -> None:
    """Vector ordering with a full-text predicate instead of match-all."""
    print(f"\n--- Keyword-Filtered Semantic Search: 'shoes' + '{query}' ---")
    results = MockItemWithEmbedding.objects.filter(
        description=ParadeDB(MatchAll("shoes"))
    ).order_by(CosineDistance("embedding", query_embedding))[:5]
    for item in results:
        print(f"  • {item.description[:55]}")


if __name__ == "__main__":
    print("=" * 70)
    print("django-paradedb Vector Search Example")
    print("=" * 70)

    if not paradedb_supports_vectors():
        print(
            "\nSkipping: this ParadeDB build does not support vector columns in "
            "bm25 indexes."
        )
        raise SystemExit(0)

    # Reuse the hybrid_rrf setup to load embeddings into mock_items
    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "hybrid_rrf"))
    from setup import QUERY_EMBEDDINGS, setup

    setup()
    create_vector_index()

    for query in ["running shoes", "footwear for exercise", "wireless earbuds"]:
        demo_semantic_search(query, QUERY_EMBEDDINGS[query])

    demo_filtered_semantic_search(
        "footwear for exercise", QUERY_EMBEDDINGS["footwear for exercise"]
    )

    print("\n" + "=" * 70)
    print("Done!")
