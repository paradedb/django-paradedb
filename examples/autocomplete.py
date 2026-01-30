#!/usr/bin/env python
"""Autocomplete (as-you-type) search example."""

from autocomplete_setup import setup_autocomplete_table

from paradedb.functions import Score
from paradedb.search import ParadeDB, Parse


# Define model inline for the autocomplete_items table
def get_autocomplete_model():
    """Get AutocompleteItem model (defined inline)."""
    import sys
    from pathlib import Path

    examples_dir = Path(__file__).parent
    if str(examples_dir) not in sys.path:
        sys.path.insert(0, str(examples_dir))
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

    # Ensure table and index exist before running the demo.
    count = setup_autocomplete_table()
    print(f"Loaded {count} products from autocomplete_items table\n")

    demo_autocomplete()
    print("\nDone.")
