"""Integration tests for ParadeDB faceted search."""

from __future__ import annotations

import pytest
from django.db import utils as db_utils

from paradedb.search import MatchAll, MatchAny, ParadeDB, Term
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
            description=ParadeDB(MatchAll("shoes"))
        ).facets(
            "rating",
            include_rows=False,
            order="key",
        )
        assert facets == {
            "buckets": [
                {"key": 3, "doc_count": 1},
                {"key": 4, "doc_count": 1},
                {"key": 5, "doc_count": 1},
            ],
            "sum_other_doc_count": 0,
        }

    def test_facets_with_rows(self) -> None:
        """Windowed facets return both rows and facets."""
        rows, facets = (
            MockItem.objects.filter(description=ParadeDB(MatchAll("shoes")))
            .order_by("rating")[:3]
            .facets("rating", order="key")
        )
        assert [row.id for row in rows] == [4, 5, 3]
        assert facets == {
            "buckets": [
                {"key": 3, "doc_count": 1},
                {"key": 4, "doc_count": 1},
                {"key": 5, "doc_count": 1},
            ],
            "sum_other_doc_count": 0,
        }
        for row in rows:
            assert not hasattr(row, "_paradedb_facets")

    def test_facets_exact_toggle(self) -> None:
        """Facets allow exact defaults, explicit true, and explicit false."""
        queryset = MockItem.objects.filter(
            description=ParadeDB(MatchAll("shoes"))
        ).order_by("rating")[:3]
        rows, facets = queryset.facets("rating", exact=True, order="key")
        assert [row.id for row in rows] == [4, 5, 3]
        assert facets == {
            "buckets": [
                {"key": 3, "doc_count": 1},
                {"key": 4, "doc_count": 1},
                {"key": 5, "doc_count": 1},
            ],
            "sum_other_doc_count": 0,
        }

    def test_facets_multiple_fields(self) -> None:
        """Multiple field facets return aggregations for each field."""
        rows, facets = (
            MockItem.objects.filter(description=ParadeDB(MatchAll("shoes")))
            .order_by("rating")[:3]
            .facets("rating", "in_stock", order="key")
        )
        assert [row.id for row in rows] == [4, 5, 3]
        assert facets == {
            "rating_terms": {
                "buckets": [
                    {"key": 3, "doc_count": 1},
                    {"key": 4, "doc_count": 1},
                    {"key": 5, "doc_count": 1},
                ],
                "sum_other_doc_count": 0,
            },
            "in_stock_terms": {
                "buckets": [
                    {"key": 0, "key_as_string": "false", "doc_count": 1},
                    {"key": 1, "key_as_string": "true", "doc_count": 2},
                ],
                "sum_other_doc_count": 0,
            },
        }

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
            description=ParadeDB(MatchAll("shoes"))
        ).order_by("rating")[:3]
        with pytest.raises(db_utils.InternalError, match="invalid field"):
            queryset.facets(field)

    def test_facets_json_with_keyword_suffix(self) -> None:
        """JSON field + .keyword is accepted but yields empty buckets."""
        rows, facets = (
            MockItem.objects.filter(description=ParadeDB(MatchAll("shoes")))
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
        assert [row.id for row in rows] == [12, 1, 2]
        assert facets == {"value": 5.0}

    def test_facets_agg_with_match_search(self) -> None:
        """facets(agg=JSON_spec) with Match filter — docs/aggregates/facets.mdx snippet 2."""
        rows, facets = (
            MockItem.objects.filter(description=ParadeDB(MatchAny("running shoes")))
            .order_by("rating")[:5]
            .facets(agg='{"value_count": {"field": "id"}}')
        )
        assert isinstance(rows, list)
        assert [row.id for row in rows] == [4, 5, 3]
        assert facets == {"value": 3.0}
