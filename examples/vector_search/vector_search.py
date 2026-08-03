#!/usr/bin/env python
"""Vector search example using a ParadeDB index.

ParadeDB indexes pgvector ``vector`` columns inside its search index and serves
Top-K nearest-neighbor queries. Three things are required for index-accelerated
vector search:

1. A ``@@@`` predicate to activate the index scan — use ``ParadeDB(All())``
   for a pure vector query.
2. A distance operator that matches the metric of the index opclass
   (``l2`` ↔ ``L2Distance``, ``cosine`` ↔ ``CosineDistance``,
   ``ip`` ↔ ``InnerProduct``).
3. A ``LIMIT`` (slice the queryset) to get Top-K pushdown.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common import QUERY_EMBEDDINGS, MockItem, setup_mock_items

from paradedb.search import All, MatchAll, ParadeDB
from paradedb.vector import CosineDistance


def demo_semantic_search(query: str, query_embedding: list[float]) -> None:
    """Pure vector Top-K query with the mandatory match-all predicate."""
    print(f"\n--- Semantic Search: '{query}' ---")
    results = (
        MockItem.objects.filter(id=ParadeDB(All()))
        .annotate(distance=CosineDistance("embedding", query_embedding))
        .order_by("distance")[:5]
    )
    for item in results:
        print(f"  • {item.description[:55]:<55} (distance: {item.distance:.4f})")


def demo_filtered_semantic_search(query: str, query_embedding: list[float]) -> None:
    """Vector ordering with a full-text predicate instead of match-all."""
    print(f"\n--- Keyword-Filtered Semantic Search: 'shoes' + '{query}' ---")
    results = MockItem.objects.filter(description=ParadeDB(MatchAll("shoes"))).order_by(
        CosineDistance("embedding", query_embedding)
    )[:5]
    for item in results:
        print(f"  • {item.description[:55]}")


if __name__ == "__main__":
    print("=" * 70)
    print("django-paradedb Vector Search Example")
    print("=" * 70)

    count = setup_mock_items()
    print(f"Loaded {count} products")

    for query in ["running shoes", "footwear for exercise", "wireless earbuds"]:
        demo_semantic_search(query, QUERY_EMBEDDINGS[query])

    demo_filtered_semantic_search(
        "footwear for exercise", QUERY_EMBEDDINGS["footwear for exercise"]
    )

    print("\n" + "=" * 70)
    print("Done!")
