#!/usr/bin/env python
"""Hybrid search example using BM25 + vector search with Reciprocal Rank Fusion.

Uses a single SQL query with CTEs to combine BM25 full-text search and vector
similarity search, fused via Reciprocal Rank Fusion (RRF).

No raw SQL - built entirely with Django ORM + django-cte + pgvector + django-paradedb.

Reference: https://www.paradedb.com/blog/hybrid-search-in-postgresql-the-missing-manual
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from common import MockItemWithEmbedding as MockItem
from django.db.models import F, FloatField, Sum, Window
from django.db.models.expressions import ExpressionWrapper, Value
from django.db.models.functions import Cast, RowNumber
from django_cte import CTE, with_cte
from pgvector.django import CosineDistance

from paradedb.functions import Score
from paradedb.search import ParadeDB

if MockItem is None:
    raise ImportError("pgvector is required for this example. pip install pgvector")


def get_query_embedding(text: str) -> str:
    """Get pre-computed embedding for query text, returned as a string."""
    sys.path.insert(0, str(Path(__file__).parent))
    from setup import QUERY_EMBEDDINGS

    return QUERY_EMBEDDINGS[text]


def _rrf_score(k: float) -> ExpressionWrapper:
    """Build the RRF score expression: 1.0 / (k + rank)."""
    return ExpressionWrapper(
        Value(1.0, output_field=FloatField())
        / (Value(k, output_field=FloatField()) + Cast(F("rank"), FloatField())),
        output_field=FloatField(),
    )


def hybrid_search(
    query: str,
    query_embedding: list[float],
    *,
    top_k: int = 20,
    rrf_k: int = 60,
    limit: int = 5,
) -> list:
    """Single-query hybrid search combining BM25 + vector via RRF.

    Builds one SQL statement with four CTEs:
      1. fulltext    - BM25 ranked results via ParadeDB
      2. semantic    - vector similarity ranked results via pgvector
      3. rrf         - UNION ALL of RRF score contributions
      4. rrf_scores  - aggregated RRF scores per item

    Args:
        query: Text query for BM25 full-text search.
        query_embedding: Vector embedding for similarity search.
        top_k: Number of candidates from each ranker.
        rrf_k: RRF constant (typically 60).
        limit: Number of final results to return.
    """
    # CTE 1: BM25 full-text search with ROW_NUMBER rank
    fulltext_qs = (
        MockItem.objects.filter(description=ParadeDB(query))
        .annotate(bm25=Score())
        .annotate(rank=Window(expression=RowNumber(), order_by=F("bm25").desc()))
        .order_by("-bm25")
        .values("id", "rank")[:top_k]
    )
    fulltext_cte = CTE(fulltext_qs, name="fulltext")

    # CTE 2: Vector similarity search with ROW_NUMBER rank
    semantic_qs = (
        MockItem.objects.filter(embedding__isnull=False)
        .annotate(distance=CosineDistance("embedding", query_embedding))
        .annotate(rank=Window(expression=RowNumber(), order_by=F("distance").asc()))
        .order_by("distance")
        .values("id", "rank")[:top_k]
    )
    semantic_cte = CTE(semantic_qs, name="semantic")

    # CTE 3: UNION ALL of RRF contributions from each ranker
    rrf_expr = _rrf_score(float(rrf_k))
    fulltext_rrf = (
        fulltext_cte.queryset().annotate(score=rrf_expr).values("id", "score")
    )
    semantic_rrf = (
        semantic_cte.queryset().annotate(score=rrf_expr).values("id", "score")
    )
    rrf_cte = CTE(fulltext_rrf.union(semantic_rrf, all=True), name="rrf")

    # CTE 4: Aggregate RRF scores per item
    rrf_scores_qs = (
        rrf_cte.queryset()
        .values("id")
        .annotate(rrf_score=Sum("score"))
        .order_by("-rrf_score")
    )[:limit]
    rrf_scores_cte = CTE(rrf_scores_qs, name="rrf_scores")

    # Final SELECT: join aggregated scores back to model for full fields
    final_qs = with_cte(
        fulltext_cte,
        semantic_cte,
        rrf_cte,
        rrf_scores_cte,
        select=rrf_scores_cte.join(MockItem, id=rrf_scores_cte.col.id)
        .annotate(rrf_score=rrf_scores_cte.col.rrf_score)
        .order_by("-rrf_score"),
    )
    return list(final_qs)


def display_results(query: str, results: list) -> None:
    """Display hybrid search results."""
    print(f"\n{'=' * 70}")
    print(f"Query: '{query}'")
    print("=" * 70)

    for i, item in enumerate(results, 1):
        print(f"  {i}. {item.description[:60]:<60} (RRF: {item.rrf_score:.4f})")


def demo(query: str) -> None:
    """Run hybrid search demo for a query."""
    query_embedding = get_query_embedding(query)
    results = hybrid_search(query, query_embedding)
    display_results(query, results)


if __name__ == "__main__":
    print("=" * 70)
    print("Hybrid Search with Reciprocal Rank Fusion (RRF)")
    print("=" * 70)
    print("\nSingle-query CTE: BM25 (keyword) + Vector (semantic)")
    print("RRF formula: score = sum(1 / (k + rank)) across all rankings")

    sys.path.insert(0, str(Path(__file__).parent))
    from setup import setup

    setup()

    demo("running shoes")
    demo("footwear for exercise")
    demo("wireless earbuds")

    print("\n" + "=" * 70)
    print("All results produced by a single SQL query per search.")
    print("=" * 70)
