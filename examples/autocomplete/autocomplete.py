#!/usr/bin/env python
"""Autocomplete (as-you-type) search example."""

import sys
from pathlib import Path

from paradedb.functions import Score
from paradedb.search import ParadeDB, Parse

# Add parent directory to path to import common module
sys.path.insert(0, str(Path(__file__).parent.parent))
from models import AutocompleteItem


def demo_autocomplete() -> None:
    """As-you-type autocomplete."""
    print("\n" + "=" * 60)
    print("Autocomplete")
    print("=" * 60)

    queries = [
        "run",
        "runn",
        "running",
        "wire",
        "wirel",
        "wireles",
        "wireless",
        "blue",
        "blueto",
        "bluetooth",
    ]

    for query in queries:
        print(f"\nUser types: '{query}' →")

        # Autocomplete query
        results = (
            AutocompleteItem.objects.filter(
                description=ParadeDB(Parse(f"description_ngram:{query}"))
            )
            .annotate(score=Score())
            .order_by("-score")[:5]
        )

        if results:
            for item in results:
                print(f"  • {item.description[:50]}... (score: {item.score:.2f})")
        else:
            print("  (no results)")


if __name__ == "__main__":
    print("=" * 60)
    print("django-paradedb Autocomplete Example")
    print("Fast as-you-type search")
    print("=" * 60)

    # Import setup here to avoid circular import issues
    sys.path.insert(0, str(Path(__file__).parent))
    from setup import setup_autocomplete_table

    # Ensure table and index exist before running the demo.
    count = setup_autocomplete_table()
    print(f"Loaded {count} products from autocomplete_items table\n")

    demo_autocomplete()
    print("\nDone.")
