#!/usr/bin/env python
"""Quickstart example for django-paradedb."""

import sys
from pathlib import Path

# Add parent directory to path to import common module
sys.path.insert(0, str(Path(__file__).parent.parent))
from common import MockItem, setup_mock_items

from paradedb.functions import Score, Snippet
from paradedb.search import ParadeDB, Phrase


def demo_basic_search() -> None:
    """Basic keyword search."""
    print("\n--- Basic Search: 'shoes' ---")
    for item in MockItem.objects.filter(description=ParadeDB("shoes"))[:5]:
        print(f"  • {item.description[:60]}...")


def demo_scored_search() -> None:
    """Search with BM25 scores."""
    print("\n--- Scored Search: 'running' ---")
    results = (
        MockItem.objects.filter(description=ParadeDB("running"))
        .annotate(score=Score())
        .order_by("-score")[:5]
    )
    for item in results:
        print(f"  • {item.description[:50]}... (score: {item.score:.2f})")


def demo_phrase_search() -> None:
    """Phrase search."""
    print("\n--- Phrase Search: 'running shoes' ---")
    results = (
        MockItem.objects.filter(description=ParadeDB(Phrase("running shoes")))
        .annotate(score=Score())
        .order_by("-score")[:5]
    )
    for item in results:
        print(f"  • {item.description[:50]}... (score: {item.score:.2f})")


def demo_snippet_highlighting() -> None:
    """Snippet highlighting."""
    print("\n--- Snippet Highlighting: 'shoes' ---")
    results = (
        MockItem.objects.filter(description=ParadeDB("shoes"))
        .annotate(
            score=Score(),
            snippet=Snippet("description", start_sel="<b>", stop_sel="</b>"),
        )
        .order_by("-score")[:3]
    )
    for item in results:
        print(f"  • {item.snippet}")


def demo_filtered_search() -> None:
    """Search with Django ORM filters."""
    print("\n--- Filtered Search: 'shoes' + in_stock + rating >= 4 ---")
    results = (
        MockItem.objects.filter(
            description=ParadeDB("shoes"),
            in_stock=True,
            rating__gte=4,
        )
        .annotate(score=Score())
        .order_by("-score")[:5]
    )
    for item in results:
        print(f"  • {item.description[:40]}... (rating: {item.rating})")


if __name__ == "__main__":
    print("=" * 60)
    print("django-paradedb Quickstart Example")
    print("=" * 60)

    count = setup_mock_items()
    print(f"Loaded {count} mock items")

    demo_basic_search()
    demo_scored_search()
    demo_phrase_search()
    demo_snippet_highlighting()
    demo_filtered_search()

    print("\n" + "=" * 60)
    print("Done!")
