"""Integration tests for ParadeDB faceted search."""

from __future__ import annotations

import pytest
from django.db import utils as db_utils

from paradedb.search import Match, ParadeDB, Term
from tests.models import MockItem

pytestmark = [
    pytest.mark.integration,
    pytest.mark.django_db(transaction=True),
    pytest.mark.usefixtures("mock_items"),
]


class TestFacetsIntegration:
    """Validate facets() against a real ParadeDB index."""

    def test_facets_only(self) -> None:
        """Aggregate-only facets return a dict payload."""
        facets = MockItem.objects.filter(
            description=ParadeDB(Match("shoes", operator="AND"))
        ).facets(
            "rating",
            include_rows=False,
        )
        assert isinstance(facets, dict)
        assert "buckets" in facets

    def test_facets_with_rows(self) -> None:
        """Windowed facets return both rows and facets."""
        rows, facets = (
            MockItem.objects.filter(
                description=ParadeDB(Match("shoes", operator="AND"))
            )
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
        queryset = MockItem.objects.filter(
            description=ParadeDB(Match("shoes", operator="AND"))
        ).order_by("rating")[:3]
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
            MockItem.objects.filter(
                description=ParadeDB(Match("shoes", operator="AND"))
            )
            .order_by("rating")[:3]
            .facets("rating", "in_stock")
        )
        assert isinstance(facets, dict)
        assert "rating_terms" in facets
        assert "in_stock_terms" in facets
        assert "buckets" in facets["rating_terms"]
        assert "buckets" in facets["in_stock_terms"]
        assert len(rows) <= 3

    def test_facets_json_fields(self) -> None:
        """JSON field facets use json_fields configuration."""
        rows, facets = (
            MockItem.objects.filter(
                description=ParadeDB(Match("shoes", operator="AND"))
            )
            .order_by("rating")[:3]
            .facets("metadata.color", "metadata.location")
        )
        assert len(rows) <= 3
        assert "metadata.color_terms" in facets
        assert "metadata.location_terms" in facets
        assert "buckets" in facets["metadata.color_terms"]
        assert "buckets" in facets["metadata.location_terms"]

    @pytest.mark.parametrize(
        "field",
        [
            "metadata->color",
            "metadata->'color'",
            "metadata->>'color'",
        ],
    )
    def test_facets_rejects_json_operator_syntax(self, field: str) -> None:
        """JSON operator syntax (->, ->>) is not supported by ParadeDB facets."""
        queryset = MockItem.objects.filter(
            description=ParadeDB(Match("shoes", operator="AND"))
        ).order_by("rating")[:3]
        with pytest.raises(db_utils.InternalError, match="invalid field"):
            queryset.facets(field)

    def test_facets_json_with_keyword_suffix(self) -> None:
        """JSON field + .keyword is accepted but yields empty buckets."""
        rows, facets = (
            MockItem.objects.filter(
                description=ParadeDB(Match("shoes", operator="AND"))
            )
            .order_by("rating")[:3]
            .facets("metadata.color.keyword")
        )
        assert len(rows) <= 3
        assert "buckets" in facets
        assert facets["buckets"] == []

    def test_facets_agg_with_term_search(self) -> None:
        """facets(agg=JSON_spec) with Term filter — docs/aggregates/facets.mdx snippet 1."""
        rows, facets = (
            MockItem.objects.filter(category=ParadeDB(Term("electronics")))
            .order_by("-rating")[:3]
            .facets(agg='{"value_count": {"field": "id"}}')
        )
        assert isinstance(rows, list)
        assert len(rows) <= 3
        assert isinstance(facets, dict)
        assert "value" in facets
        assert facets["value"] > 0

    def test_facets_agg_with_match_search(self) -> None:
        """facets(agg=JSON_spec) with Match filter — docs/aggregates/facets.mdx snippet 2."""
        rows, facets = (
            MockItem.objects.filter(
                description=ParadeDB(Match("running shoes", operator="OR"))
            )
            .order_by("rating")[:5]
            .facets(agg='{"value_count": {"field": "id"}}')
        )
        assert isinstance(rows, list)
        assert isinstance(facets, dict)
        assert "value" in facets
        assert facets["value"] > 0
