from __future__ import annotations

import json

import pytest
from django.db.models import Window

from paradedb.functions import Agg
from paradedb.search import All, MatchAll, ParadeDB, Term
from tests.models import MockItem, Product

pytestmark = [
    pytest.mark.integration,
    pytest.mark.django_db(transaction=True),
    pytest.mark.usefixtures("mock_items"),
]


class TestDjangoORMWithAgg:
    """Test using Agg annotation with standard Django ORM queries."""

    def test_agg_annotation_with_raw_sql(self) -> None:
        """Test that Agg annotation generates correct SQL (unit test validation)."""
        # This validates that the SQL generation works correctly
        json_spec = '{"category":{"terms":{"field":"category","order":{"_count":"desc"},"size":10}}}'
        queryset = Product.objects.filter(
            description=ParadeDB(MatchAll("shoes"))
        ).annotate(facets=Window(expression=Agg(json_spec)))

        # Verify SQL is generated correctly
        sql = str(queryset.query)
        assert "pdb.agg" in sql
        assert "OVER ()" in sql
        assert "facets" in sql


class TestAggregationEdgeCases:
    """Test edge cases and error handling for aggregations."""


def _agg_dict(value: str | dict) -> dict:  # type: ignore[type-arg]
    """Normalize an Agg result — psycopg3 returns JSONB as dict, psycopg2 as str."""
    return value if isinstance(value, dict) else json.loads(value)  # type: ignore[return-value]


class TestDjangoOrmAggExecution:
    """Test Agg Django ORM annotation executes against real DB (docs/aggregates/overview.mdx)."""

    def test_agg_single_value_count(self) -> None:
        """filter(Term).aggregate(agg=Agg(...)) — docs snippet 1."""
        result = MockItem.objects.filter(
            category=ParadeDB(Term("electronics"))
        ).aggregate(agg=Agg('{"value_count": {"field": "id"}}'))
        assert isinstance(result, dict)
        assert "agg" in result
        assert _agg_dict(result["agg"])["value"] > 0

    def test_agg_grouped_by_rating(self) -> None:
        """filter(Term).values('rating').annotate(agg=Agg(...)).order_by()[:5] — docs snippet 2."""
        rows = list(
            MockItem.objects.filter(category=ParadeDB(Term("electronics")))
            .values("rating")
            .annotate(agg=Agg('{"value_count": {"field": "id"}}'))
            .order_by("rating")[:5]
        )
        assert len(rows) > 0
        for row in rows:
            assert "rating" in row
            assert "agg" in row
            assert _agg_dict(row["agg"])["value"] > 0

    def test_agg_multiple_aggregations(self) -> None:
        """filter(Term).aggregate(avg_rating=Agg(...), count=Agg(...)) — docs snippet 3."""
        result = MockItem.objects.filter(
            category=ParadeDB(Term("electronics"))
        ).aggregate(
            avg_rating=Agg('{"avg": {"field": "rating"}}'),
            count=Agg('{"value_count": {"field": "id"}}'),
        )
        assert isinstance(result, dict)
        assert "avg_rating" in result
        assert "count" in result
        assert _agg_dict(result["avg_rating"])["value"] > 0
        assert _agg_dict(result["count"])["value"] > 0

    def test_agg_with_all_query(self) -> None:
        """filter(All()).aggregate(...) — docs snippet 4 (All() matches every document)."""
        result = MockItem.objects.filter(id=ParadeDB(All())).aggregate(
            agg=Agg('{"value_count": {"field": "id"}}')
        )
        assert isinstance(result, dict)
        assert "agg" in result
        assert _agg_dict(result["agg"])["value"] > 0

    def test_agg_filter_conditional_aggregation(self) -> None:
        """Agg(filter=Q(...)) — conditional aggregation with FILTER (WHERE ...)."""
        from django.db.models import Q

        result = MockItem.objects.aggregate(
            electronics_count=Agg(
                '{"value_count": {"field": "id"}}',
                filter=Q(category=ParadeDB(Term("electronics"))),
            ),
            footwear_count=Agg(
                '{"value_count": {"field": "id"}}',
                filter=Q(category=ParadeDB(Term("footwear"))),
            ),
        )
        assert isinstance(result, dict)
        assert "electronics_count" in result
        assert "footwear_count" in result
        assert _agg_dict(result["electronics_count"])["value"] > 0
        assert _agg_dict(result["footwear_count"])["value"] > 0

    def test_agg_terms_on_json_subfield(self) -> None:
        """Agg terms on indexed JSON subfield via ORM aggregate."""
        result = MockItem.objects.filter(id=ParadeDB(All())).aggregate(
            agg=Agg('{"terms": {"field": "metadata.color"}}')
        )
        assert isinstance(result, dict)
        assert "agg" in result
        assert "buckets" in _agg_dict(result["agg"])
