"""Integration tests for ParadeDB faceted search."""

from __future__ import annotations

from types import SimpleNamespace

import pytest
from django.db import utils as db_utils

from paradedb.queryset import ParadeDBQuerySet
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


def test_extract_facets_multi_single_alias_dict_rows() -> None:
    rows = [
        {"id": 1, "_paradedb_facets": {"buckets": [{"key": "a"}]}},
        {"id": 2, "_paradedb_facets": {"buckets": [{"key": "a"}]}},
    ]
    facets = ParadeDBQuerySet._extract_facets_multi(rows, ["_paradedb_facets"])
    assert facets == {"buckets": [{"key": "a"}]}
    assert rows == [{"id": 1}, {"id": 2}]


def test_extract_facets_multi_multi_alias_dict_rows() -> None:
    rows = [
        {
            "id": 1,
            "rating_terms": {"buckets": [{"key": 5}]},
            "category_terms": {"buckets": [{"key": "Footwear"}]},
        },
        {
            "id": 2,
            "rating_terms": {"buckets": [{"key": 4}]},
            "category_terms": {"buckets": [{"key": "Electronics"}]},
        },
    ]
    facets = ParadeDBQuerySet._extract_facets_multi(
        rows, ["rating_terms", "category_terms"]
    )
    assert facets == {
        "rating_terms": {"buckets": [{"key": 5}]},
        "category_terms": {"buckets": [{"key": "Footwear"}]},
    }
    assert rows == [{"id": 1}, {"id": 2}]


def test_extract_facets_multi_single_alias_tuple_rows() -> None:
    rows = [(1, {"buckets": [{"key": "a"}]}), (2, {"buckets": [{"key": "a"}]})]
    facets = ParadeDBQuerySet._extract_facets_multi(rows, ["_paradedb_facets"])
    assert facets == {"buckets": [{"key": "a"}]}
    assert rows == [(1,), (2,)]


def test_extract_facets_multi_multi_alias_tuple_rows() -> None:
    rows = [
        (1, "A", {"buckets": [{"key": 5}]}, {"buckets": [{"key": "Footwear"}]}),
        (2, "B", {"buckets": [{"key": 4}]}, {"buckets": [{"key": "Electronics"}]}),
    ]
    facets = ParadeDBQuerySet._extract_facets_multi(
        rows, ["rating_terms", "category_terms"]
    )
    assert facets == {
        "rating_terms": {"buckets": [{"key": 5}]},
        "category_terms": {"buckets": [{"key": "Footwear"}]},
    }
    assert rows == [(1, "A"), (2, "B")]


def test_extract_facets_multi_single_alias_object_rows() -> None:
    rows = [
        SimpleNamespace(id=1, _paradedb_facets={"buckets": [{"key": "x"}]}),
        SimpleNamespace(id=2, _paradedb_facets={"buckets": [{"key": "x"}]}),
    ]
    facets = ParadeDBQuerySet._extract_facets_multi(rows, ["_paradedb_facets"])
    assert facets == {"buckets": [{"key": "x"}]}
    assert not hasattr(rows[0], "_paradedb_facets")
    assert not hasattr(rows[1], "_paradedb_facets")
