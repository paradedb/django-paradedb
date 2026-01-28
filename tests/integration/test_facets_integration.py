"""Integration tests for ParadeDB faceted queries."""

from __future__ import annotations

import pytest
from tests.models import MockItem

from paradedb.search import ParadeDB

pytestmark = [
    pytest.mark.integration,
    pytest.mark.django_db(transaction=True),
    pytest.mark.usefixtures("mock_items"),
]


class TestFacetsIntegration:
    """Validate facets() against a real ParadeDB index."""

    def test_facets_only(self) -> None:
        """Aggregate-only facets return a dict payload."""
        facets = MockItem.objects.filter(description=ParadeDB("shoes")).facets(
            "rating",
            include_rows=False,
        )
        assert isinstance(facets, dict)
        assert "buckets" in facets

    def test_facets_with_rows(self) -> None:
        """Windowed facets return both rows and facets."""
        rows, facets = (
            MockItem.objects.filter(description=ParadeDB("shoes"))
            .order_by("rating")[:3]
            .facets("rating")
        )
        assert isinstance(facets, dict)
        assert "buckets" in facets
        assert len(rows) <= 3
        for row in rows:
            assert not hasattr(row, "_paradedb_facets")

    def test_facets_multiple_fields(self) -> None:
        """Multiple field facets return aggregations for each field."""
        rows, facets = (
            MockItem.objects.filter(description=ParadeDB("shoes"))
            .order_by("rating")[:3]
            .facets("rating", "in_stock")
        )
        assert isinstance(facets, dict)
        assert "rating_terms" in facets
        assert "in_stock_terms" in facets
        assert "buckets" in facets["rating_terms"]
        assert "buckets" in facets["in_stock_terms"]
        assert len(rows) <= 3
