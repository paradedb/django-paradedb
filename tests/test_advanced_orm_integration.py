from __future__ import annotations

import pytest
from django.db.models import Q
from django.db.models.functions import Coalesce

from paradedb.functions import Score
from paradedb.search import (
    MatchAll,
    ParadeDB,
    Phrase,
    Term,
)
from tests.models import MockItem

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
            Q(description=ParadeDB(MatchAll("shoes")))
            | Q(description=ParadeDB(MatchAll("keyboard")))
            | Q(description=ParadeDB(MatchAll("earbuds")))
        )
        ids = _ids(queryset)
        assert ids == {1, 2, 3, 4, 5, 12}

    def test_deeply_nested_q(self) -> None:
        """Deeply nested Q: ((A OR B) AND C) OR D."""
        queryset = MockItem.objects.filter(
            (
                (
                    Q(description=ParadeDB(MatchAll("shoes")))
                    | Q(description=ParadeDB(MatchAll("boots")))
                )
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
                Q(description=ParadeDB(MatchAll("shoes")))
                | Q(description=ParadeDB(MatchAll("boots")))
            )
        )
        without_running = _ids(
            MockItem.objects.filter(
                (
                    Q(description=ParadeDB(MatchAll("shoes")))
                    | Q(description=ParadeDB(MatchAll("boots")))
                )
                & ~Q(description=ParadeDB(MatchAll("running")))
            )
        )
        assert without_running <= all_footwear
        for item in MockItem.objects.filter(id__in=without_running):
            assert "running" not in item.description.lower()


class TestScoreAnnotationAdvanced:
    """Advanced Score annotation tests."""

    def test_score_filter_range(self) -> None:
        """Filter score within a range."""
        queryset = (
            MockItem.objects.filter(description=ParadeDB(MatchAll("shoes")))
            .annotate(search_score=Score())
            .filter(search_score__gte=0.1, search_score__lte=100)
        )
        for item in queryset:
            assert 0.1 <= item.search_score <= 100

    def test_score_with_coalesce(self) -> None:
        """Score with Coalesce for null handling."""
        queryset = MockItem.objects.filter(
            description=ParadeDB(MatchAll("shoes"))
        ).annotate(
            search_score=Score(),
            safe_score=Coalesce(Score(), 0.0),
        )
        for item in queryset:
            assert item.safe_score >= 0


class TestMultipleSearchTypes:
    """Test combining different ParadeDB search types."""

    def test_phrase_with_term_in_q(self) -> None:
        """Phrase OR Term in Q composition."""
        queryset = MockItem.objects.filter(
            Q(description=ParadeDB(Phrase("running shoes")))
            | Q(description=ParadeDB(Term("keyboard")))
        )
        assert queryset.exists()

    def test_chained_filters_all_paradedb(self) -> None:
        """Multiple chained ParadeDB filters (AND semantics)."""
        queryset = MockItem.objects.filter(
            description=ParadeDB(MatchAll("shoes"))
        ).filter(description=ParadeDB(MatchAll("running")))
        for item in queryset:
            assert "shoes" in item.description.lower()
            assert "running" in item.description.lower()
