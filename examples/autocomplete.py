#!/usr/bin/env python
"""Autocomplete (as-you-type) search example.

Simple, typo-tolerant autocomplete using fuzzy matching.

Run `python examples/autocomplete_setup.py` first to create the table.
"""

from paradedb.functions import Score
from paradedb.search import Fuzzy, ParadeDB


# Define model inline for the autocomplete_items table
def get_autocomplete_model():
    """Get AutocompleteItem model (defined inline)."""
    import os
    import sys

    examples_dir = os.path.dirname(__file__)
    if examples_dir not in sys.path:
        sys.path.insert(0, examples_dir)
    from _common import configure_django

    configure_django()

    from django.db import models

    from paradedb.queryset import ParadeDBManager

    class AutocompleteItem(models.Model):
        id = models.IntegerField(primary_key=True)
        description = models.TextField()
        category = models.CharField(max_length=100)
        rating = models.IntegerField()
        in_stock = models.BooleanField()
        created_at = models.DateTimeField()

        objects = ParadeDBManager()

        class Meta:
            app_label = "examples"
            managed = False
            db_table = "autocomplete_items"

        def __str__(self) -> str:
            return self.description

    return AutocompleteItem


AutocompleteItem = get_autocomplete_model()


def demo_fuzzy_autocomplete() -> None:
    """Typo-tolerant search with fuzzy matching."""
    print("\n" + "=" * 60)
    print("Fuzzy Search (Typo Tolerance)")
    print("=" * 60)

    typo_queries = [
        ("sheos", "shoes"),  # Common typo
        ("wireles", "wireless"),  # Missing 's'
        ("runing", "running"),  # Missing 'n'
    ]

    for typo, correct in typo_queries:
        print(f"\nUser types: '{typo}' (meant: '{correct}') →")

        # Use fuzzy matching with edit distance
        results = (
            AutocompleteItem.objects.filter(
                description=ParadeDB(Fuzzy(typo, distance=1))
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
    print("Fast as-you-type search with fuzzy matching")
    print("=" * 60)

    count = AutocompleteItem.objects.count()
    print(f"Loaded {count} products from autocomplete_items table\n")

    demo_fuzzy_autocomplete()
    print("\nDone.")
