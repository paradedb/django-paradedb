#!/usr/bin/env python
"""Faceted search example using ParadeDB's facets() helper.

This example demonstrates how to fetch faceted counts alongside a search
query using the ParadeDBQuerySet.facets() helper (Top-N rows + buckets).

Note:
- Facets in this demo use `category` / `metadata_color` aliases indexed with
  the `literal` tokenizer and numeric `rating`.
- For this pattern, no extra `fast` field config is required in the index.
"""

import sys
from pathlib import Path

# Add parent directory to path to import common module
sys.path.insert(0, str(Path(__file__).parent.parent))
from common import MockItem, setup_mock_items

from paradedb.search import ParadeDB

try:
    from paradedb.queryset import ParadeDBQuerySet
except Exception:  # pragma: no cover - runtime fallback for type checking
    ParadeDBQuerySet = None  # type: ignore[assignment]


def demo_facets_with_rows(query: str) -> None:
    """Fetch Top-N rows with facet buckets using a window aggregation."""
    print("\n--- Facets + Rows (Top-N) ---")
    queryset = MockItem.objects.filter(description=ParadeDB(query)).order_by("-rating")[
        :5
    ]
    rows, facets = queryset.facets(  # type: ignore[attr-defined]
        "category", "rating", "metadata_color", include_rows=True
    )

    print("Top results:")
    for item in rows:
        color = item.metadata.get("color") if item.metadata else "N/A"
        stock = "In Stock" if item.in_stock else "Out of Stock"
        print(
            f"  • {item.description[:50]}... "
            f"[{item.category}] (rating: {item.rating}, {stock}, color: {color})"
        )

    print("\nFacet buckets:")
    for key, data in facets.items():
        buckets = data.get("buckets", []) if isinstance(data, dict) else []
        print(f"{key} ({len(buckets)} buckets)")
        for bucket in buckets:
            print(f"  • {bucket.get('key')}: {bucket.get('doc_count')}")


if __name__ == "__main__":
    print("=" * 60)
    print("django-paradedb Faceted Search Example")
    print("=" * 60)

    count = setup_mock_items()
    print(f"Loaded {count} mock items")

    search_query = "shoes"
    print(f"\nQuery: '{search_query}'")

    demo_facets_with_rows(search_query)

    print("\n" + "=" * 60)
    print("Done!")
