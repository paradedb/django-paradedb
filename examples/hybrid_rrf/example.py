#!/usr/bin/env python
"""Hybrid search example using BM25 + vector search with Reciprocal Rank Fusion."""

import os
from urllib.parse import urlparse

import django
import httpx
from django.conf import settings
from dotenv import load_dotenv
from pgvector.django import CosineDistance

load_dotenv()

DATABASE_URL = os.environ.get(
    "DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/postgres"
)
OPENROUTER_API_KEY = os.environ.get("OPENROUTER_API_KEY")

if not OPENROUTER_API_KEY:
    raise ValueError("OPENROUTER_API_KEY not found in environment. Add it to .env file")

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

from django.db import models  # noqa: E402

from paradedb.functions import Score  # noqa: E402
from paradedb.search import ParadeDB  # noqa: E402
from pgvector.django import VectorField  # noqa: E402


class MockItem(models.Model):
    """ParadeDB's mock_items table with vector embeddings."""

    id = models.IntegerField(primary_key=True)
    description = models.TextField()
    category = models.CharField(max_length=100)
    rating = models.IntegerField()
    in_stock = models.BooleanField()
    created_at = models.DateTimeField()
    metadata = models.JSONField(null=True)
    embedding = VectorField(dimensions=384, null=True)

    class Meta:
        app_label = "hybrid_rrf"
        managed = False
        db_table = "mock_items"

    def __str__(self):
        return self.description


def get_query_embedding(text: str) -> list[float]:
    """Get embedding for query text from OpenRouter."""
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


def bm25_search(query: str, top_k: int = 20) -> list[tuple[int, float]]:
    """BM25 full-text search - returns (id, score) pairs."""
    results = (
        MockItem.objects.filter(description=ParadeDB(query))
        .annotate(score=Score())
        .order_by("-score")[:top_k]
        .values_list("id", "score")
    )
    return list(results)


def vector_search(query: str, top_k: int = 20) -> list[tuple[int, float]]:
    """Vector similarity search - returns (id, distance) pairs."""
    query_embedding = get_query_embedding(query)

    results = (
        MockItem.objects.annotate(
            distance=CosineDistance("embedding", query_embedding)
        )
        .order_by("distance")[:top_k]
        .values_list("id", "distance")
    )
    return list(results)


def reciprocal_rank_fusion(
    bm25_results: list[tuple[int, float]],
    vector_results: list[tuple[int, float]],
    k: int = 60,
) -> list[tuple[int, float]]:
    """
    Combine BM25 and vector search results using Reciprocal Rank Fusion.

    RRF formula: score = sum(1 / (k + rank_i)) for all rankings
    where k is a constant (typically 60) and rank_i is the rank in each list.
    """
    scores: dict[int, float] = {}

    # Add BM25 rankings
    for rank, (item_id, _) in enumerate(bm25_results, start=1):
        scores[item_id] = scores.get(item_id, 0.0) + 1 / (k + rank)

    # Add vector rankings
    for rank, (item_id, _) in enumerate(vector_results, start=1):
        scores[item_id] = scores.get(item_id, 0.0) + 1 / (k + rank)

    # Sort by RRF score (descending)
    return sorted(scores.items(), key=lambda x: x[1], reverse=True)


def display_results(
    query: str, bm25_results: list, vector_results: list, rrf_results: list
) -> None:
    """Display search results side by side."""
    print(f"\n{'='*80}")
    print(f"Query: '{query}'")
    print("=" * 80)

    # Fetch items by ID for display
    all_ids = (
        {id for id, _ in bm25_results[:5]}
        | {id for id, _ in vector_results[:5]}
        | {id for id, _ in rrf_results[:5]}
    )
    items = {item.id: item for item in MockItem.objects.filter(id__in=all_ids)}

    print("\nBM25 Results (keyword):")
    for i, (item_id, score) in enumerate(bm25_results[:5], 1):
        item = items.get(item_id)
        if item:
            print(f"  {i}. {item.description[:60]:<60} (score: {score:.2f})")

    print("\nVector Results (semantic):")
    for i, (item_id, distance) in enumerate(vector_results[:5], 1):
        item = items.get(item_id)
        if item:
            print(f"  {i}. {item.description[:60]:<60} (dist: {distance:.3f})")

    print("\nHybrid RRF Results (combined):")
    for i, (item_id, rrf_score) in enumerate(rrf_results[:5], 1):
        item = items.get(item_id)
        if item:
            print(f"  {i}. {item.description[:60]:<60} (RRF: {rrf_score:.4f})")


def demo(query: str) -> None:
    """Run hybrid search demo for a query."""
    # Get results from both methods
    bm25_results = bm25_search(query, top_k=20)
    vector_results = vector_search(query, top_k=20)

    # Combine with RRF
    rrf_results = reciprocal_rank_fusion(bm25_results, vector_results)

    # Display
    display_results(query, bm25_results, vector_results, rrf_results)


if __name__ == "__main__":
    print("=" * 80)
    print("Hybrid Search with Reciprocal Rank Fusion (RRF)")
    print("=" * 80)
    print("\nCombining BM25 (keyword) + Vector (semantic) search")
    print("RRF formula: score = sum(1 / (k + rank_i)) across all rankings")

    # Demo queries showing different strengths
    demo("running shoes")  # BM25 excels (exact keywords)
    demo("footwear for exercise")  # Vector excels (semantic similarity)
    demo("wireless earbuds")  # Both contribute

    print("\n" + "=" * 80)
    print("Notice how RRF combines the best of both approaches!")
    print("=" * 80)
