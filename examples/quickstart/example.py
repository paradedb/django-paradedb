#!/usr/bin/env python
"""Quickstart example for django-paradedb full-text search."""

import os
from urllib.parse import urlparse

import django
from django.conf import settings

DATABASE_URL = os.environ.get(
    "DATABASE_URL", "postgres://postgres:postgres@localhost:5432/postgres"
)

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

from paradedb.functions import Score, Snippet  # noqa: E402
from paradedb.search import ParadeDB, Phrase  # noqa: E402


class MockItem(models.Model):
    """ParadeDB's built-in mock_items table."""

    id = models.IntegerField(primary_key=True)
    description = models.TextField()
    category = models.CharField(max_length=100)
    rating = models.IntegerField()
    in_stock = models.BooleanField()
    created_at = models.DateTimeField()
    metadata = models.JSONField(null=True)

    class Meta:
        app_label = "quickstart"
        managed = False
        db_table = "mock_items"

    def __str__(self):
        return self.description


def setup_mock_data() -> None:
    """Ensure mock_items table exists with BM25 index."""
    with connection.cursor() as cursor:
        cursor.execute("CREATE EXTENSION IF NOT EXISTS pg_search")
        cursor.execute(
            "CALL paradedb.create_bm25_test_table("
            "schema_name => 'public', table_name => 'mock_items')"
        )
    print(f"Loaded {MockItem.objects.count()} mock items")


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

    setup_mock_data()
    demo_basic_search()
    demo_scored_search()
    demo_phrase_search()
    demo_snippet_highlighting()
    demo_filtered_search()

    print("\n" + "=" * 60)
    print("Done!")
