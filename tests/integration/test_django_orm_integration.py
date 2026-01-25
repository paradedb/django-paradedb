"""Integration tests for ParadeDB + Django ORM feature combinations.

These tests validate that ParadeDB search works correctly when combined
with native Django ORM features: Q objects, negation, standard filters,
annotations, window functions, and ordering.
"""

from __future__ import annotations

import pytest
from django.db.models import F, Q, Window
from django.db.models.functions import RowNumber
from tests.models import MockItem

from paradedb.functions import Score
from paradedb.search import PQ, ParadeDB, Phrase

pytestmark = [
    pytest.mark.integration,
    pytest.mark.django_db,
    pytest.mark.usefixtures("mock_items"),
]


def _ids(queryset) -> set[int]:  # type: ignore[no-untyped-def]
    return set(queryset.values_list("id", flat=True))


class TestQObjectIntegration:
    """Test ParadeDB combined with Django Q objects."""

    def test_q_or_paradedb_conditions(self) -> None:
        """Q OR between ParadeDB searches returns union of results."""
        ids = _ids(
            MockItem.objects.filter(
                Q(description=ParadeDB("shoes")) | Q(description=ParadeDB("wireless"))
            )
        )
        assert ids == {3, 4, 5, 12}

    def test_q_and_paradedb_with_standard_filter(self) -> None:
        """Q AND combining ParadeDB with standard ORM filter."""
        ids = _ids(
            MockItem.objects.filter(Q(description=ParadeDB("shoes")), Q(rating__gte=4))
        )
        for item in MockItem.objects.filter(id__in=ids):
            assert item.rating >= 4
            assert "shoes" in item.description.lower()

    def test_complex_q_or_and_combination(self) -> None:
        """Complex Q: (search AND filter) OR (search AND filter)."""
        queryset = MockItem.objects.filter(
            Q(description=ParadeDB("shoes"), rating__gte=4)
            | Q(description=ParadeDB("wireless"), in_stock=True)
        )
        assert queryset.exists()
        for item in queryset:
            is_shoes_high_rated = (
                "shoes" in item.description.lower() and item.rating >= 4
            )
            is_wireless_in_stock = (
                "wireless" in item.description.lower() and item.in_stock
            )
            assert is_shoes_high_rated or is_wireless_in_stock


class TestNegationIntegration:
    """Test ParadeDB combined with Django Q negation."""

    def test_negation_excludes_term(self) -> None:
        """~Q excludes ParadeDB matches from results."""
        all_shoes = _ids(MockItem.objects.filter(description=ParadeDB("shoes")))
        without_running = _ids(
            MockItem.objects.filter(
                Q(description=ParadeDB("shoes")), ~Q(description=ParadeDB("running"))
            )
        )
        assert without_running < all_shoes  # strict subset
        for item in MockItem.objects.filter(id__in=without_running):
            assert "running" not in item.description.lower()

    def test_double_negation(self) -> None:
        """Double negation ~~Q should equal original."""
        direct = _ids(MockItem.objects.filter(description=ParadeDB("shoes")))
        double_neg = _ids(MockItem.objects.filter(~~Q(description=ParadeDB("shoes"))))
        assert direct == double_neg


class TestStandardFiltersIntegration:
    """Test ParadeDB combined with standard Django ORM filters."""

    def test_with_exact_filter(self) -> None:
        """ParadeDB + exact match filter."""
        queryset = MockItem.objects.filter(description=ParadeDB("shoes"), in_stock=True)
        for item in queryset:
            assert item.in_stock is True

    def test_with_comparison_filters(self) -> None:
        """ParadeDB + comparison operators (gte, lte)."""
        queryset = MockItem.objects.filter(
            description=ParadeDB("shoes"), rating__gte=3, rating__lte=5
        )
        for item in queryset:
            assert 3 <= item.rating <= 5

    def test_with_multiple_standard_filters(self) -> None:
        """ParadeDB + multiple standard filters chained."""
        queryset = (
            MockItem.objects.filter(description=ParadeDB("shoes"))
            .filter(rating__gte=3)
            .filter(in_stock=True)
        )
        assert queryset.exists()
        for item in queryset:
            assert item.rating >= 3
            assert item.in_stock is True


class TestScoreAnnotationIntegration:
    """Test ParadeDB Score annotation with real execution."""

    def test_score_annotation_returns_float(self) -> None:
        """Score annotation produces numeric values."""
        queryset = MockItem.objects.filter(description=ParadeDB("shoes")).annotate(
            search_score=Score()
        )
        first = queryset.first()
        assert first is not None
        assert isinstance(first.search_score, float)
        assert first.search_score > 0

    def test_score_ordering_desc(self) -> None:
        """ORDER BY score DESC returns highest scores first."""
        queryset = (
            MockItem.objects.filter(description=ParadeDB("shoes"))
            .annotate(search_score=Score())
            .order_by("-search_score")
        )
        scores = list(queryset.values_list("search_score", flat=True))
        assert scores == sorted(scores, reverse=True)

    def test_score_ordering_asc(self) -> None:
        """ORDER BY score ASC returns lowest scores first."""
        queryset = (
            MockItem.objects.filter(description=ParadeDB("shoes"))
            .annotate(search_score=Score())
            .order_by("search_score")
        )
        scores = list(queryset.values_list("search_score", flat=True))
        assert scores == sorted(scores)

    def test_score_filter_gt(self) -> None:
        """Filter by score > threshold."""
        queryset = (
            MockItem.objects.filter(description=ParadeDB("shoes"))
            .annotate(search_score=Score())
            .filter(search_score__gt=0)
        )
        for item in queryset:
            assert item.search_score > 0

    def test_score_with_standard_ordering(self) -> None:
        """Score annotation combined with secondary ordering."""
        queryset = (
            MockItem.objects.filter(description=ParadeDB("shoes"))
            .annotate(search_score=Score())
            .order_by("-search_score", "id")
        )
        results = list(queryset)
        assert len(results) > 0


class TestWindowFunctionIntegration:
    """Test ParadeDB combined with Django window functions."""

    def test_row_number_partition_by_category(self) -> None:
        """Window function: ROW_NUMBER() OVER (PARTITION BY category)."""
        queryset = MockItem.objects.filter(description=ParadeDB("shoes")).annotate(
            rank_in_category=Window(
                expression=RowNumber(),
                partition_by=[F("category")],
                order_by=F("rating").desc(),
            )
        )
        results = list(queryset)
        assert len(results) > 0
        for item in results:
            assert hasattr(item, "rank_in_category")
            assert isinstance(item.rank_in_category, int)
            assert item.rank_in_category >= 1

    def test_row_number_with_score_ordering(self) -> None:
        """Window function with ParadeDB score in order_by."""
        queryset = (
            MockItem.objects.filter(description=ParadeDB("shoes"))
            .annotate(search_score=Score())
            .annotate(
                rank_by_score=Window(
                    expression=RowNumber(),
                    order_by=F("search_score").desc(),
                )
            )
        )
        results = list(queryset.order_by("rank_by_score"))
        assert len(results) > 0
        for i, item in enumerate(results, start=1):
            assert item.rank_by_score == i


class TestChainingIntegration:
    """Test chained QuerySet operations with ParadeDB."""

    def test_filter_annotate_filter_order(self) -> None:
        """Chain: filter -> annotate -> filter -> order_by."""
        queryset = (
            MockItem.objects.filter(description=ParadeDB("shoes"))
            .annotate(search_score=Score())
            .filter(search_score__gt=0, rating__gte=3)
            .order_by("-search_score")
        )
        results = list(queryset)
        for item in results:
            assert item.search_score > 0
            assert item.rating >= 3

    def test_values_with_paradedb(self) -> None:
        """QuerySet.values() works with ParadeDB filtered results."""
        values = list(
            MockItem.objects.filter(description=ParadeDB("shoes")).values(
                "id", "description", "rating"
            )
        )
        assert len(values) > 0
        for v in values:
            assert "id" in v
            assert "description" in v
            assert "rating" in v

    def test_values_list_with_annotation(self) -> None:
        """QuerySet.values_list() with Score annotation."""
        results = list(
            MockItem.objects.filter(description=ParadeDB("shoes"))
            .annotate(search_score=Score())
            .values_list("id", "search_score")
        )
        assert len(results) > 0
        for id_, score in results:
            assert isinstance(id_, int)
            assert isinstance(score, float)

    def test_distinct_with_paradedb(self) -> None:
        """QuerySet.distinct() with ParadeDB search."""
        queryset = (
            MockItem.objects.filter(description=ParadeDB("shoes"))
            .values("category")
            .distinct()
        )
        categories = [r["category"] for r in queryset]
        assert len(categories) == len(set(categories))

    def test_count_with_paradedb(self) -> None:
        """QuerySet.count() with ParadeDB search."""
        count = MockItem.objects.filter(description=ParadeDB("shoes")).count()
        assert count > 0
        assert isinstance(count, int)

    def test_exists_with_paradedb(self) -> None:
        """QuerySet.exists() with ParadeDB search."""
        assert MockItem.objects.filter(description=ParadeDB("shoes")).exists()
        assert not MockItem.objects.filter(
            description=ParadeDB("nonexistentterm12345xyz")
        ).exists()


class TestPhraseSearchIntegration:
    """Test Phrase search with Django ORM features."""

    def test_phrase_with_q_combination(self) -> None:
        """Phrase search combined with Q objects."""
        ids = _ids(
            MockItem.objects.filter(
                Q(description=ParadeDB(Phrase("running shoes"))) | Q(rating=5)
            )
        )
        phrase_ids = _ids(
            MockItem.objects.filter(description=ParadeDB(Phrase("running shoes")))
        )
        rating_ids = _ids(MockItem.objects.filter(rating=5))
        assert ids == phrase_ids | rating_ids

    def test_phrase_with_slop_and_filter(self) -> None:
        """Phrase with slop combined with standard filter."""
        queryset = MockItem.objects.filter(
            description=ParadeDB(Phrase("running shoes", slop=2)),
            in_stock=True,
        )
        for item in queryset:
            assert item.in_stock is True


class TestPQCombinationsIntegration:
    """Test PQ object combinations with Django ORM features."""

    def test_pq_or_with_standard_filter(self) -> None:
        """PQ OR combined with standard filter."""
        queryset = MockItem.objects.filter(
            description=ParadeDB(PQ("running") | PQ("wireless")),
            in_stock=True,
        )
        for item in queryset:
            assert item.in_stock is True

    def test_pq_and_with_annotation(self) -> None:
        """PQ AND combined with Score annotation."""
        queryset = (
            MockItem.objects.filter(description=ParadeDB(PQ("running") & PQ("shoes")))
            .annotate(search_score=Score())
            .order_by("-search_score")
        )
        results = list(queryset)
        assert len(results) > 0
        for item in results:
            assert item.search_score > 0
