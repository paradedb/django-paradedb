"""Integration tests for ParadeDB with Django window functions and complex annotations.

These tests validate that ParadeDB search works correctly with various
window functions, cumulative calculations, and complex annotation chains.
"""

from __future__ import annotations

import pytest
from django.db.models import Avg, Count, F, Max, Min, Sum, Window
from django.db.models.functions import (
    Coalesce,
    DenseRank,
    FirstValue,
    Lag,
    LastValue,
    Lead,
    NthValue,
    Ntile,
    PercentRank,
    Rank,
    RowNumber,
)
from tests.models import MockItem

from paradedb.functions import Score, Snippet
from paradedb.search import ParadeDB, Phrase

pytestmark = [
    pytest.mark.integration,
    pytest.mark.django_db(transaction=True),
    pytest.mark.usefixtures("mock_items"),
]


class TestRowNumberWindow:
    """Test ROW_NUMBER() window function with ParadeDB."""

    def test_row_number_no_partition(self) -> None:
        """ROW_NUMBER() OVER (ORDER BY score DESC)."""
        queryset = (
            MockItem.objects.filter(description=ParadeDB("shoes"))
            .annotate(search_score=Score())
            .annotate(
                row_num=Window(
                    expression=RowNumber(),
                    order_by=F("search_score").desc(),
                )
            )
            .order_by("row_num")
        )
        results = list(queryset)
        assert len(results) > 0
        for i, item in enumerate(results, start=1):
            assert item.row_num == i

    def test_row_number_partition_by_in_stock(self) -> None:
        """ROW_NUMBER() OVER (PARTITION BY in_stock ORDER BY rating)."""
        queryset = MockItem.objects.filter(description=ParadeDB("shoes")).annotate(
            row_in_stock=Window(
                expression=RowNumber(),
                partition_by=[F("in_stock")],
                order_by=F("rating").desc(),
            )
        )
        results = list(queryset)
        assert len(results) > 0
        for item in results:
            assert item.row_in_stock >= 1


class TestRankingWindows:
    """Test RANK(), DENSE_RANK(), PERCENT_RANK() with ParadeDB."""

    def test_rank_by_score(self) -> None:
        """RANK() OVER (ORDER BY score DESC)."""
        queryset = (
            MockItem.objects.filter(description=ParadeDB("shoes"))
            .annotate(search_score=Score())
            .annotate(
                score_rank=Window(
                    expression=Rank(),
                    order_by=F("search_score").desc(),
                )
            )
            .order_by("score_rank")
        )
        results = list(queryset)
        assert len(results) > 0
        assert results[0].score_rank == 1

    def test_dense_rank_by_rating(self) -> None:
        """DENSE_RANK() OVER (ORDER BY rating DESC)."""
        queryset = MockItem.objects.filter(description=ParadeDB("shoes")).annotate(
            rating_dense_rank=Window(
                expression=DenseRank(),
                order_by=F("rating").desc(),
            )
        )
        results = list(queryset)
        assert len(results) > 0
        ranks = [r.rating_dense_rank for r in results]
        assert 1 in ranks

    def test_percent_rank(self) -> None:
        """PERCENT_RANK() OVER (ORDER BY score)."""
        queryset = (
            MockItem.objects.filter(description=ParadeDB("shoes"))
            .annotate(search_score=Score())
            .annotate(
                pct_rank=Window(
                    expression=PercentRank(),
                    order_by=F("search_score").desc(),
                )
            )
        )
        results = list(queryset)
        assert len(results) > 0
        for item in results:
            assert 0 <= item.pct_rank <= 1

    def test_ntile_quartiles(self) -> None:
        """NTILE(4) for quartiles."""
        queryset = (
            MockItem.objects.filter(description=ParadeDB("shoes"))
            .annotate(search_score=Score())
            .annotate(
                quartile=Window(
                    expression=Ntile(4),
                    order_by=F("search_score").desc(),
                )
            )
        )
        results = list(queryset)
        assert len(results) > 0
        for item in results:
            assert 1 <= item.quartile <= 4


class TestValueAccessWindows:
    """Test FIRST_VALUE, LAST_VALUE, NTH_VALUE, LAG, LEAD with ParadeDB."""

    def test_first_value(self) -> None:
        """FIRST_VALUE(rating) OVER (ORDER BY score DESC)."""
        queryset = (
            MockItem.objects.filter(description=ParadeDB("shoes"))
            .annotate(search_score=Score())
            .annotate(
                top_rating=Window(
                    expression=FirstValue("rating"),
                    order_by=F("search_score").desc(),
                )
            )
        )
        results = list(queryset)
        assert len(results) > 0
        first_rating = results[0].top_rating
        for item in results:
            assert item.top_rating == first_rating

    def test_last_value(self) -> None:
        """LAST_VALUE(rating) OVER (PARTITION BY category)."""
        queryset = MockItem.objects.filter(description=ParadeDB("shoes")).annotate(
            last_in_category=Window(
                expression=LastValue("rating"),
                partition_by=[F("category")],
                order_by=F("rating").asc(),
            )
        )
        results = list(queryset)
        assert len(results) > 0

    def test_lag_previous_score(self) -> None:
        """LAG(score, 1) - get previous row's score."""
        queryset = (
            MockItem.objects.filter(description=ParadeDB("shoes"))
            .annotate(search_score=Score())
            .annotate(
                prev_score=Window(
                    expression=Lag("search_score", offset=1),
                    order_by=F("search_score").desc(),
                )
            )
            .order_by("-search_score")
        )
        results = list(queryset)
        assert len(results) > 0
        assert results[0].prev_score is None

    def test_lead_next_rating(self) -> None:
        """LEAD(rating, 1) - get next row's rating."""
        queryset = (
            MockItem.objects.filter(description=ParadeDB("shoes"))
            .annotate(
                next_rating=Window(
                    expression=Lead("rating", offset=1),
                    order_by=F("rating").desc(),
                )
            )
            .order_by("-rating")
        )
        results = list(queryset)
        assert len(results) > 0
        assert results[-1].next_rating is None

    def test_nth_value(self) -> None:
        """NTH_VALUE(id, 2) - get 2nd row's id."""
        queryset = (
            MockItem.objects.filter(description=ParadeDB("shoes"))
            .annotate(search_score=Score())
            .annotate(
                second_id=Window(
                    expression=NthValue("id", nth=2),
                    order_by=F("search_score").desc(),
                )
            )
            .order_by("-search_score")
        )
        results = list(queryset)
        assert len(results) >= 2
        assert results[0].second_id is None
        assert results[1].second_id is not None


class TestAggregateWindows:
    """Test aggregate functions as window functions with ParadeDB."""

    def test_running_count(self) -> None:
        """COUNT(*) OVER (ORDER BY score) - cumulative count."""
        queryset = (
            MockItem.objects.filter(description=ParadeDB("shoes"))
            .annotate(search_score=Score())
            .annotate(
                running_count=Window(
                    expression=Count("*"),
                    order_by=F("search_score").desc(),
                )
            )
            .order_by("-search_score")
        )
        results = list(queryset)
        assert len(results) > 0
        prev_count = 0
        for item in results:
            assert item.running_count >= prev_count
            prev_count = item.running_count
        assert results[-1].running_count == len(results)

    def test_running_avg(self) -> None:
        """AVG(rating) OVER (ORDER BY score) - running average."""
        queryset = (
            MockItem.objects.filter(description=ParadeDB("shoes"))
            .annotate(search_score=Score())
            .annotate(
                running_avg=Window(
                    expression=Avg("rating"),
                    order_by=F("search_score").desc(),
                )
            )
        )
        results = list(queryset)
        assert len(results) > 0
        for item in results:
            assert 1 <= item.running_avg <= 5

    def test_partition_sum(self) -> None:
        """SUM(rating) OVER (PARTITION BY category)."""
        queryset = MockItem.objects.filter(description=ParadeDB("shoes")).annotate(
            category_total=Window(
                expression=Sum("rating"),
                partition_by=[F("category")],
            )
        )
        results = list(queryset)
        assert len(results) > 0
        for item in results:
            assert item.category_total > 0

    def test_partition_min_max(self) -> None:
        """MIN/MAX OVER (PARTITION BY in_stock)."""
        queryset = MockItem.objects.filter(description=ParadeDB("shoes")).annotate(
            min_in_group=Window(
                expression=Min("rating"),
                partition_by=[F("in_stock")],
            ),
            max_in_group=Window(
                expression=Max("rating"),
                partition_by=[F("in_stock")],
            ),
        )
        results = list(queryset)
        assert len(results) > 0
        for item in results:
            assert item.min_in_group <= item.rating <= item.max_in_group


class TestComplexAnnotationChains:
    """Test complex annotation chains combining multiple features."""

    def test_score_rank_combined(self) -> None:
        """Score + Rank in single query (Snippet excluded - unsupported with windows)."""
        queryset = (
            MockItem.objects.filter(description=ParadeDB("shoes"))
            .annotate(search_score=Score())
            .annotate(
                score_rank=Window(
                    expression=Rank(),
                    order_by=F("search_score").desc(),
                )
            )
            .order_by("score_rank")
        )
        results = list(queryset)
        assert len(results) > 0
        for item in results:
            assert item.search_score > 0
            assert item.score_rank >= 1

    def test_snippet_without_window_works(self) -> None:
        """Snippet works without window functions."""
        queryset = (
            MockItem.objects.filter(description=ParadeDB("shoes"))
            .annotate(
                search_score=Score(),
                snippet=Snippet("description"),
            )
            .order_by("-search_score")
        )
        results = list(queryset)
        assert len(results) > 0
        for item in results:
            assert item.search_score > 0
            assert item.snippet is not None

    def test_multiple_window_functions(self) -> None:
        """Multiple window functions in same query."""
        queryset = (
            MockItem.objects.filter(description=ParadeDB("shoes"))
            .annotate(search_score=Score())
            .annotate(
                global_rank=Window(
                    expression=RowNumber(),
                    order_by=F("search_score").desc(),
                ),
                category_rank=Window(
                    expression=RowNumber(),
                    partition_by=[F("category")],
                    order_by=F("search_score").desc(),
                ),
                pct_rank=Window(
                    expression=PercentRank(),
                    order_by=F("search_score").desc(),
                ),
            )
        )
        results = list(queryset)
        assert len(results) > 0
        for item in results:
            assert item.global_rank >= 1
            assert item.category_rank >= 1
            assert 0 <= item.pct_rank <= 1

    def test_window_with_filters_and_ordering(self) -> None:
        """Window function with additional filters and ordering."""
        queryset = (
            MockItem.objects.filter(description=ParadeDB("shoes"))
            .annotate(search_score=Score())
            .annotate(
                rank=Window(
                    expression=RowNumber(),
                    order_by=F("search_score").desc(),
                )
            )
            .filter(rating__gte=3)
            .order_by("rank")[:5]
        )
        results = list(queryset)
        for item in results:
            assert item.rating >= 3

    def test_window_in_subquery_style(self) -> None:
        """Window function result used in further filtering."""
        base_queryset = (
            MockItem.objects.filter(description=ParadeDB("shoes"))
            .annotate(search_score=Score())
            .annotate(
                quartile=Window(
                    expression=Ntile(4),
                    order_by=F("search_score").desc(),
                )
            )
        )
        top_quartile = [item for item in base_queryset if item.quartile == 1]
        assert len(top_quartile) > 0
        for item in top_quartile:
            assert item.quartile == 1

    def test_annotation_with_coalesce_and_window(self) -> None:
        """Coalesce + Window function combination."""
        queryset = (
            MockItem.objects.filter(description=ParadeDB("shoes"))
            .annotate(search_score=Score())
            .annotate(
                prev_score=Window(
                    expression=Lag("search_score", offset=1, default=0.0),  # type: ignore[arg-type]
                    order_by=F("search_score").desc(),
                ),
            )
            .annotate(
                safe_prev=Coalesce(F("prev_score"), 0.0),
            )
            .order_by("-search_score")
        )
        results = list(queryset)
        assert len(results) > 0
        for item in results:
            assert item.safe_prev >= 0


class TestWindowWithPhraseAndComplexSearch:
    """Test window functions with complex ParadeDB search types."""

    def test_phrase_search_with_ranking(self) -> None:
        """Phrase search with window ranking."""
        queryset = (
            MockItem.objects.filter(
                description=ParadeDB(Phrase("running shoes", slop=2))
            )
            .annotate(search_score=Score())
            .annotate(
                rank=Window(
                    expression=Rank(),
                    order_by=F("search_score").desc(),
                )
            )
        )
        results = list(queryset)
        if results:
            assert results[0].rank == 1

    def test_combined_search_with_partitioned_window(self) -> None:
        """Combined search with partitioned window function."""
        queryset = (
            MockItem.objects.filter(description=ParadeDB("shoes"))
            .annotate(search_score=Score())
            .annotate(
                rank_in_category=Window(
                    expression=DenseRank(),
                    partition_by=[F("category")],
                    order_by=F("search_score").desc(),
                ),
                category_avg=Window(
                    expression=Avg("search_score"),
                    partition_by=[F("category")],
                ),
            )
        )
        results = list(queryset)
        assert len(results) > 0
        for item in results:
            assert item.rank_in_category >= 1
            assert item.category_avg > 0
