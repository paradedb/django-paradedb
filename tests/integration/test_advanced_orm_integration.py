"""Advanced integration tests for ParadeDB + Django ORM combinations.

These tests exercise complex Q composition, nested boolean logic,
annotation chains, and ordering edge cases against a real ParadeDB instance.
"""

from __future__ import annotations

import pytest
from django.db.models import Avg, Count, Max, Min, Q, Sum
from django.db.models.functions import Coalesce, Lower
from tests.models import MockItem

from paradedb.functions import Score, Snippet
from paradedb.search import Fuzzy, ParadeDB, Parse, Phrase, Regex, Term

pytestmark = [
    pytest.mark.integration,
    pytest.mark.django_db(transaction=True),
    pytest.mark.usefixtures("mock_items"),
]


def _ids(queryset) -> set[int]:  # type: ignore[no-untyped-def]
    return set(queryset.values_list("id", flat=True))


class TestComplexQComposition:
    """Test complex Q object boolean compositions with ParadeDB."""

    def test_triple_or_paradedb(self) -> None:
        """Q OR with three ParadeDB conditions."""
        # shoes: 3 results (id=3,4,5), keyboard: 2 results (id=1,2), earbuds: 1 result (id=12)
        queryset = MockItem.objects.filter(
            Q(description=ParadeDB("shoes"))
            | Q(description=ParadeDB("keyboard"))
            | Q(description=ParadeDB("earbuds"))
        )
        ids = _ids(queryset)
        assert ids == {1, 2, 3, 4, 5, 12}

    def test_nested_and_or(self) -> None:
        """Nested (A AND B) OR (C AND D) with ParadeDB."""
        queryset = MockItem.objects.filter(
            (Q(description=ParadeDB("shoes")) & Q(rating__gte=4))
            | (Q(description=ParadeDB("keyboard")) & Q(in_stock=True))
        )
        for item in queryset:
            shoes_match = "shoes" in item.description.lower() and item.rating >= 4
            keyboard_match = "keyboard" in item.description.lower() and item.in_stock
            assert shoes_match or keyboard_match

    def test_deeply_nested_q(self) -> None:
        """Deeply nested Q: ((A OR B) AND C) OR D."""
        queryset = MockItem.objects.filter(
            (
                (Q(description=ParadeDB("shoes")) | Q(description=ParadeDB("boots")))
                & Q(rating__gte=3)
            )
            | Q(category="Electronics")
        )
        for item in queryset:
            footwear_rated = (
                "shoes" in item.description.lower()
                or "boots" in item.description.lower()
            ) and item.rating >= 3
            electronics = item.category == "Electronics"
            assert footwear_rated or electronics

    def test_q_not_with_or(self) -> None:
        """NOT combined with OR: (A OR B) AND NOT C."""
        all_footwear = _ids(
            MockItem.objects.filter(
                Q(description=ParadeDB("shoes")) | Q(description=ParadeDB("boots"))
            )
        )
        without_running = _ids(
            MockItem.objects.filter(
                (Q(description=ParadeDB("shoes")) | Q(description=ParadeDB("boots")))
                & ~Q(description=ParadeDB("running"))
            )
        )
        assert without_running <= all_footwear
        for item in MockItem.objects.filter(id__in=without_running):
            assert "running" not in item.description.lower()

    def test_multiple_not_conditions(self) -> None:
        """Multiple NOT conditions: A AND NOT B AND NOT C."""
        queryset = MockItem.objects.filter(
            Q(description=ParadeDB("shoes"))
            & ~Q(description=ParadeDB("running"))
            & ~Q(description=ParadeDB("hiking"))
        )
        for item in queryset:
            assert "shoes" in item.description.lower()
            assert "running" not in item.description.lower()
            assert "hiking" not in item.description.lower()

    def test_not_or_composition(self) -> None:
        """NOT (A OR B) - exclude multiple terms."""
        all_items = _ids(MockItem.objects.all())
        excluded = _ids(
            MockItem.objects.filter(
                ~(
                    Q(description=ParadeDB("shoes"))
                    | Q(description=ParadeDB("keyboard"))
                )
            )
        )
        shoes_keyboard = _ids(
            MockItem.objects.filter(
                Q(description=ParadeDB("shoes")) | Q(description=ParadeDB("keyboard"))
            )
        )
        assert excluded == all_items - shoes_keyboard


class TestParadeDBWithStandardFilters:
    """Test ParadeDB combined with various Django filter types."""

    def test_with_in_lookup(self) -> None:
        """ParadeDB + __in lookup."""
        queryset = MockItem.objects.filter(
            description=ParadeDB("shoes"),
            rating__in=[3, 4, 5],
        )
        for item in queryset:
            assert item.rating in [3, 4, 5]

    def test_with_range_lookup(self) -> None:
        """ParadeDB + __range lookup."""
        queryset = MockItem.objects.filter(
            description=ParadeDB("shoes"),
            rating__range=(3, 5),
        )
        for item in queryset:
            assert 3 <= item.rating <= 5

    def test_with_isnull_lookup(self) -> None:
        """ParadeDB + __isnull lookup."""
        queryset = MockItem.objects.filter(
            description=ParadeDB("shoes"),
            metadata__isnull=False,
        )
        for item in queryset:
            assert item.metadata is not None

    def test_with_exclude(self) -> None:
        """ParadeDB filter with exclude()."""
        all_shoes = _ids(MockItem.objects.filter(description=ParadeDB("shoes")))
        excluded = _ids(
            MockItem.objects.filter(description=ParadeDB("shoes")).exclude(rating__lt=4)
        )
        assert excluded <= all_shoes
        for item in MockItem.objects.filter(id__in=excluded):
            assert item.rating >= 4


class TestScoreAnnotationAdvanced:
    """Advanced Score annotation tests."""

    def test_score_with_multiple_annotations(self) -> None:
        """Score combined with other annotations."""
        queryset = (
            MockItem.objects.filter(description=ParadeDB("shoes"))
            .annotate(
                search_score=Score(),
                desc_lower=Lower("description"),
            )
            .order_by("-search_score")
        )
        for item in queryset:
            assert item.search_score > 0
            assert item.desc_lower == item.description.lower()

    def test_score_filter_range(self) -> None:
        """Filter score within a range."""
        queryset = (
            MockItem.objects.filter(description=ParadeDB("shoes"))
            .annotate(search_score=Score())
            .filter(search_score__gte=0.1, search_score__lte=100)
        )
        for item in queryset:
            assert 0.1 <= item.search_score <= 100

    def test_score_with_coalesce(self) -> None:
        """Score with Coalesce for null handling."""
        queryset = MockItem.objects.filter(description=ParadeDB("shoes")).annotate(
            search_score=Score(),
            safe_score=Coalesce(Score(), 0.0),
        )
        for item in queryset:
            assert item.safe_score >= 0

    def test_score_ordering_with_tiebreaker(self) -> None:
        """Score ordering with multiple tiebreakers."""
        queryset = (
            MockItem.objects.filter(description=ParadeDB("shoes"))
            .annotate(search_score=Score())
            .order_by("-search_score", "-rating", "id")
        )
        results = list(queryset)
        assert len(results) > 0
        prev_score = float("inf")
        for item in results:
            assert item.search_score <= prev_score
            prev_score = item.search_score

    def test_score_in_values(self) -> None:
        """Score annotation in values() output."""
        values = list(
            MockItem.objects.filter(description=ParadeDB("shoes"))
            .annotate(search_score=Score())
            .values("id", "description", "search_score")
            .order_by("-search_score")[:5]
        )
        assert len(values) > 0
        for v in values:
            assert "search_score" in v
            assert v["search_score"] > 0


class TestSnippetAnnotationAdvanced:
    """Advanced Snippet annotation tests."""

    def test_snippet_with_score(self) -> None:
        """Snippet and Score annotations together."""
        queryset = (
            MockItem.objects.filter(description=ParadeDB("shoes"))
            .annotate(
                search_score=Score(),
                snippet=Snippet("description"),
            )
            .order_by("-search_score")
        )
        for item in queryset:
            assert item.search_score > 0
            assert item.snippet is not None
            assert "<b>" in item.snippet

    def test_snippet_ordering_by_score(self) -> None:
        """Snippet results ordered by score."""
        results = list(
            MockItem.objects.filter(description=ParadeDB("shoes"))
            .annotate(
                search_score=Score(),
                snippet=Snippet("description"),
            )
            .order_by("-search_score")
            .values_list("snippet", "search_score")
        )
        scores = [r[1] for r in results]
        assert scores == sorted(scores, reverse=True)


class TestAggregationsWithParadeDB:
    """Test Django aggregations with ParadeDB filters."""

    def test_count_aggregation(self) -> None:
        """COUNT with ParadeDB filter."""
        count = MockItem.objects.filter(description=ParadeDB("shoes")).count()
        assert count > 0

    def test_avg_aggregation(self) -> None:
        """AVG rating with ParadeDB filter."""
        result = MockItem.objects.filter(description=ParadeDB("shoes")).aggregate(
            avg_rating=Avg("rating")
        )
        assert result["avg_rating"] is not None
        assert 1 <= result["avg_rating"] <= 5

    def test_min_max_aggregation(self) -> None:
        """MIN/MAX with ParadeDB filter."""
        result = MockItem.objects.filter(description=ParadeDB("shoes")).aggregate(
            min_rating=Min("rating"),
            max_rating=Max("rating"),
        )
        assert result["min_rating"] <= result["max_rating"]

    def test_sum_aggregation(self) -> None:
        """SUM with ParadeDB filter (count items)."""
        result = MockItem.objects.filter(description=ParadeDB("shoes")).aggregate(
            total_rating=Sum("rating")
        )
        assert result["total_rating"] is not None
        assert result["total_rating"] > 0

    def test_group_by_with_paradedb(self) -> None:
        """GROUP BY with ParadeDB filter."""
        results = (
            MockItem.objects.filter(description=ParadeDB("shoes"))
            .values("category")
            .annotate(count=Count("id"))
            .order_by("-count")
        )
        assert len(list(results)) > 0


class TestSlicingAndPagination:
    """Test slicing and pagination with ParadeDB."""

    def test_limit_with_score_ordering(self) -> None:
        """LIMIT with score ordering."""
        results = list(
            MockItem.objects.filter(description=ParadeDB("shoes"))
            .annotate(search_score=Score())
            .order_by("-search_score")[:3]
        )
        assert len(results) <= 3
        scores = [r.search_score for r in results]
        assert scores == sorted(scores, reverse=True)

    def test_offset_limit(self) -> None:
        """OFFSET and LIMIT (pagination)."""
        all_results = list(
            MockItem.objects.filter(description=ParadeDB("shoes"))
            .annotate(search_score=Score())
            .order_by("-search_score", "id")
        )
        if len(all_results) > 2:
            page2 = list(
                MockItem.objects.filter(description=ParadeDB("shoes"))
                .annotate(search_score=Score())
                .order_by("-search_score", "id")[1:3]
            )
            assert page2[0].id == all_results[1].id

    def test_first_with_paradedb(self) -> None:
        """first() with ParadeDB and ordering."""
        first = (
            MockItem.objects.filter(description=ParadeDB("shoes"))
            .annotate(search_score=Score())
            .order_by("-search_score")
            .first()
        )
        assert first is not None
        assert first.search_score > 0

    def test_last_with_paradedb(self) -> None:
        """last() with ParadeDB and ordering."""
        last = (
            MockItem.objects.filter(description=ParadeDB("shoes"))
            .annotate(search_score=Score())
            .order_by("-search_score")
            .last()
        )
        assert last is not None


class TestMultipleSearchTypes:
    """Test combining different ParadeDB search types."""

    def test_phrase_with_term_in_q(self) -> None:
        """Phrase OR Term in Q composition."""
        queryset = MockItem.objects.filter(
            Q(description=ParadeDB(Phrase("running shoes")))
            | Q(description=ParadeDB(Term("keyboard")))
        )
        assert queryset.exists()

    def test_fuzzy_with_score(self) -> None:
        """Fuzzy search with Score annotation."""
        queryset = (
            MockItem.objects.filter(description=ParadeDB(Fuzzy("shoez", distance=1)))
            .annotate(search_score=Score())
            .order_by("-search_score")
        )
        results = list(queryset)
        assert len(results) > 0

    def test_regex_with_filters(self) -> None:
        """Regex search with standard filters."""
        queryset = MockItem.objects.filter(
            description=ParadeDB(Regex(".*shoes.*")),
            rating__gte=3,
        )
        for item in queryset:
            assert item.rating >= 3

    def test_parse_with_q_and_score(self) -> None:
        """Parse query with Q and Score."""
        queryset = (
            MockItem.objects.filter(
                Q(description=ParadeDB(Parse("shoes", lenient=True))) & Q(in_stock=True)
            )
            .annotate(search_score=Score())
            .order_by("-search_score")
        )
        for item in queryset:
            assert item.in_stock is True
            assert item.search_score > 0


class TestEdgeCasesRealDB:
    """Edge cases that need real DB execution to validate."""

    def test_empty_result_with_annotations(self) -> None:
        """Empty result set with annotations doesn't error."""
        queryset = (
            MockItem.objects.filter(description=ParadeDB("xyznonexistent123"))
            .annotate(search_score=Score())
            .order_by("-search_score")
        )
        results = list(queryset)
        assert results == []

    def test_score_on_no_matches(self) -> None:
        """Score annotation on empty result."""
        count = (
            MockItem.objects.filter(description=ParadeDB("xyznonexistent123"))
            .annotate(search_score=Score())
            .count()
        )
        assert count == 0

    def test_chained_filters_all_paradedb(self) -> None:
        """Multiple chained ParadeDB filters (AND semantics)."""
        queryset = MockItem.objects.filter(description=ParadeDB("shoes")).filter(
            description=ParadeDB("running")
        )
        for item in queryset:
            assert "shoes" in item.description.lower()
            assert "running" in item.description.lower()

    def test_or_same_field_different_terms(self) -> None:
        """OR on same field with different search terms."""
        queryset = MockItem.objects.filter(
            Q(description=ParadeDB("shoes")) | Q(description=ParadeDB("keyboard"))
        )
        ids = _ids(queryset)
        shoes_ids = _ids(MockItem.objects.filter(description=ParadeDB("shoes")))
        keyboard_ids = _ids(MockItem.objects.filter(description=ParadeDB("keyboard")))
        assert ids == shoes_ids | keyboard_ids

    def test_filter_after_annotation(self) -> None:
        """Filter applied after annotation works correctly."""
        queryset = (
            MockItem.objects.filter(description=ParadeDB("shoes"))
            .annotate(search_score=Score())
            .filter(rating__gte=4)
            .filter(search_score__gt=0)
        )
        for item in queryset:
            assert item.rating >= 4
            assert item.search_score > 0
