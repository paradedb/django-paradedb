"""Integration tests for ParadeDB faceted search."""

from __future__ import annotations

import pytest
from django.db import utils as db_utils
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

    @pytest.mark.parametrize("exact", [None, True, False])
    def test_facets_exact_toggle(self, exact: bool | None) -> None:
        """Facets allow exact defaults, explicit true, and explicit false."""
        queryset = MockItem.objects.filter(description=ParadeDB("shoes")).order_by(
            "rating"
        )[:3]
        if exact is None:
            rows, facets = queryset.facets("rating")
        else:
            rows, facets = queryset.facets("rating", exact=exact)
        assert isinstance(facets, dict)
        assert "buckets" in facets
        assert len(rows) <= 3

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

    def test_facets_json_alias_fields(self) -> None:
        """JSON field facets must use bm25 alias names."""
        rows, facets = (
            MockItem.objects.filter(description=ParadeDB("shoes"))
            .order_by("rating")[:3]
            .facets("metadata_color", "metadata_location")
        )
        assert len(rows) <= 3
        assert "metadata_color_terms" in facets
        assert "metadata_location_terms" in facets
        assert "buckets" in facets["metadata_color_terms"]
        assert "buckets" in facets["metadata_location_terms"]

    @pytest.mark.parametrize(
        "field",
        [
            "metadata.color",
            "metadata->color",
            "metadata->'color'",
            "metadata->>'color'",
            "metadata.color.keyword",
        ],
    )
    def test_facets_rejects_json_path_syntax(self, field: str) -> None:
        """JSON path-like field syntax is not supported by ParadeDB facets."""
        queryset = MockItem.objects.filter(description=ParadeDB("shoes")).order_by(
            "rating"
        )[:3]
        with pytest.raises(db_utils.InternalError, match="invalid field"):
            queryset.facets(field)

    def test_facets_alias_with_keyword_suffix(self) -> None:
        """Alias + .keyword is accepted but yields empty buckets."""
        rows, facets = (
            MockItem.objects.filter(description=ParadeDB("shoes"))
            .order_by("rating")[:3]
            .facets("metadata_color.keyword")
        )
        assert len(rows) <= 3
        assert "buckets" in facets
        assert facets["buckets"] == []
