"""Integration tests for ParadeDB aggregations.

ParadeDB's pdb.agg() function provides fast aggregations using the columnar
portion of the BM25 index. Note: Window functions (OVER clause) require a
Top-N query with ORDER BY + LIMIT in pg_search 0.21.8.
"""

from __future__ import annotations

import json

import pytest
from django.db.models import Window
from tests.models import MockItem, Product

from paradedb.functions import Agg
from paradedb.search import All, Match, ParadeDB, Term

pytestmark = [
    pytest.mark.integration,
    pytest.mark.django_db(transaction=True),
    pytest.mark.usefixtures("mock_items"),
]


class TestBasicAggregations:
    """Test basic pdb.agg() aggregations without window functions."""

    def test_value_count_aggregation(self) -> None:
        """Basic value_count aggregation to count matching documents."""
        from django.db import connection

        with connection.cursor() as cursor:
            cursor.execute(
                'SELECT pdb.agg(\'{"value_count": {"field": "id"}}\') FROM mock_items WHERE description @@@ \'shoes\' AND id @@@ pdb.all()'
            )
            row = cursor.fetchone()

        assert row is not None
        agg_value = json.loads(row[0])
        assert agg_value["value"] == 3.0

    def test_avg_aggregation(self) -> None:
        """Average aggregation using pdb.agg()."""
        from django.db import connection

        with connection.cursor() as cursor:
            cursor.execute(
                'SELECT pdb.agg(\'{"avg": {"field": "rating"}}\') FROM mock_items WHERE description @@@ \'shoes\' AND id @@@ pdb.all()'
            )
            row = cursor.fetchone()

        assert row is not None
        agg_value = json.loads(row[0])
        assert 1 <= agg_value["value"] <= 5

    def test_sum_aggregation(self) -> None:
        """Sum aggregation using pdb.agg()."""
        from django.db import connection

        with connection.cursor() as cursor:
            cursor.execute(
                'SELECT pdb.agg(\'{"sum": {"field": "rating"}}\') FROM mock_items WHERE description @@@ \'shoes\' AND id @@@ pdb.all()'
            )
            row = cursor.fetchone()

        assert row is not None
        agg_value = json.loads(row[0])
        assert agg_value["value"] > 0

    def test_min_aggregation(self) -> None:
        """Min aggregation using pdb.agg()."""
        from django.db import connection

        with connection.cursor() as cursor:
            cursor.execute(
                'SELECT pdb.agg(\'{"min": {"field": "rating"}}\') FROM mock_items WHERE description @@@ \'shoes\' AND id @@@ pdb.all()'
            )
            row = cursor.fetchone()

        assert row is not None
        agg_value = json.loads(row[0])
        assert agg_value["value"] >= 1

    def test_max_aggregation(self) -> None:
        """Max aggregation using pdb.agg()."""
        from django.db import connection

        with connection.cursor() as cursor:
            cursor.execute(
                'SELECT pdb.agg(\'{"max": {"field": "rating"}}\') FROM mock_items WHERE description @@@ \'shoes\' AND id @@@ pdb.all()'
            )
            row = cursor.fetchone()

        assert row is not None
        agg_value = json.loads(row[0])
        assert agg_value["value"] <= 5

    def test_histogram_aggregation(self) -> None:
        """Histogram bucket aggregation."""
        from django.db import connection

        with connection.cursor() as cursor:
            cursor.execute(
                'SELECT pdb.agg(\'{"histogram": {"field": "rating", "interval": "1"}}\') FROM mock_items WHERE id @@@ pdb.all()'
            )
            row = cursor.fetchone()

        assert row is not None
        agg_value = json.loads(row[0])
        assert "buckets" in agg_value
        assert len(agg_value["buckets"]) > 0

    def test_range_aggregation(self) -> None:
        """Range bucket aggregation."""
        from django.db import connection

        with connection.cursor() as cursor:
            cursor.execute(
                'SELECT pdb.agg(\'{"range": {"field": "rating", "ranges": [{"to": 3.0}, {"from": 3.0, "to": 6.0}]}}\') FROM mock_items WHERE id @@@ pdb.all()'
            )
            row = cursor.fetchone()

        assert row is not None
        agg_value = json.loads(row[0])
        assert "buckets" in agg_value
        assert len(agg_value["buckets"]) >= 2

    def test_terms_aggregation_literal_field(self) -> None:
        """Terms aggregation by rating (numeric field)."""
        from django.db import connection

        with connection.cursor() as cursor:
            cursor.execute(
                'SELECT rating, pdb.agg(\'{"value_count": {"field": "id"}}\') FROM mock_items WHERE id @@@ pdb.all() GROUP BY rating ORDER BY rating LIMIT 5;'
            )
            results = cursor.fetchall()

        assert len(results) > 0
        for rating, agg_json in results:
            agg_value = json.loads(agg_json)
            assert "value" in agg_value
            assert isinstance(rating, int)


class TestAggregationWithSearch:
    """Test aggregations combined with ParadeDB search queries."""

    def test_aggregate_with_phrase_search(self) -> None:
        """Aggregation with phrase search."""
        from django.db import connection

        with connection.cursor() as cursor:
            cursor.execute(
                'SELECT pdb.agg(\'{"value_count": {"field": "id"}}\') FROM mock_items WHERE description @@@ pdb.phrase(\'running shoes\') AND id @@@ pdb.all()'
            )
            row = cursor.fetchone()

        assert row is not None
        agg_value = json.loads(row[0])
        assert agg_value["value"] == 1.0

    def test_aggregate_with_fuzzy_search(self) -> None:
        """Aggregation with fuzzy search."""
        from django.db import connection

        with connection.cursor() as cursor:
            cursor.execute("SELECT to_regtype('pdb.fuzzy')")
            type_name = cursor.fetchone()[0]
            if type_name is None:
                pytest.skip("pdb.fuzzy type not available in pg_search")
            cursor.execute(
                'SELECT pdb.agg(\'{"value_count": {"field": "id"}}\') FROM mock_items WHERE description ||| \'runnning\'::pdb.fuzzy(1) AND id @@@ pdb.all()'
            )
            row = cursor.fetchone()

        assert row is not None
        agg_value = json.loads(row[0])
        assert agg_value["value"] == 1.0

    def test_aggregate_with_term_search(self) -> None:
        """Aggregation with exact term search."""
        from django.db import connection

        with connection.cursor() as cursor:
            cursor.execute(
                'SELECT pdb.agg(\'{"value_count": {"field": "id"}}\') FROM mock_items WHERE description @@@ pdb.term(\'shoes\') AND id @@@ pdb.all()'
            )
            row = cursor.fetchone()

        assert row is not None
        agg_value = json.loads(row[0])
        # Should match all 3 shoe items
        assert agg_value["value"] == 3.0

    def test_aggregate_with_regex_search(self) -> None:
        """Aggregation with regex search."""
        from django.db import connection

        with connection.cursor() as cursor:
            cursor.execute(
                'SELECT pdb.agg(\'{"value_count": {"field": "id"}}\') FROM mock_items WHERE description @@@ pdb.regex(\'.*shoes.*\') AND id @@@ pdb.all()'
            )
            row = cursor.fetchone()

        assert row is not None
        agg_value = json.loads(row[0])
        assert agg_value["value"] > 0


class TestAggregationWithFilters:
    """Test aggregations combined with additional filters."""

    def test_aggregate_with_in_stock_filter(self) -> None:
        """Aggregation with in_stock filter."""
        from django.db import connection

        with connection.cursor() as cursor:
            cursor.execute(
                'SELECT pdb.agg(\'{"value_count": {"field": "id"}}\') FROM mock_items WHERE description @@@ \'shoes\' AND in_stock = true AND id @@@ pdb.all()'
            )
            row = cursor.fetchone()

        assert row is not None
        agg_value = json.loads(row[0])
        # Only in-stock shoes
        assert 0 < agg_value["value"] <= 3

    def test_aggregate_with_rating_filter(self) -> None:
        """Aggregation with rating filter."""
        from django.db import connection

        with connection.cursor() as cursor:
            cursor.execute(
                'SELECT pdb.agg(\'{"value_count": {"field": "id"}}\') FROM mock_items WHERE description @@@ \'shoes\' AND rating >= 4 AND id @@@ pdb.all()'
            )
            row = cursor.fetchone()

        assert row is not None
        agg_value = json.loads(row[0])
        assert agg_value["value"] > 0

    def test_aggregate_with_category_filter(self) -> None:
        """Aggregation with category filter."""
        from django.db import connection

        with connection.cursor() as cursor:
            cursor.execute(
                'SELECT pdb.agg(\'{"value_count": {"field": "id"}}\') FROM mock_items WHERE category = \'Footwear\' AND id @@@ pdb.all()'
            )
            row = cursor.fetchone()

        assert row is not None
        agg_value = json.loads(row[0])
        # Should count all footwear items
        assert agg_value["value"] > 0


class TestDjangoORMWithAgg:
    """Test using Agg annotation with standard Django ORM queries."""

    def test_agg_annotation_with_raw_sql(self) -> None:
        """Test that Agg annotation generates correct SQL (unit test validation)."""
        # This validates that the SQL generation works correctly
        json_spec = '{"category":{"terms":{"field":"category","order":{"_count":"desc"},"size":10}}}'
        queryset = Product.objects.filter(
            description=ParadeDB(Match("shoes", operator="AND"))
        ).annotate(facets=Window(expression=Agg(json_spec)))

        # Verify SQL is generated correctly
        sql = str(queryset.query)
        assert "pdb.agg" in sql
        assert "OVER ()" in sql
        assert "facets" in sql


class TestAggregationEdgeCases:
    """Test edge cases and error handling for aggregations."""

    def test_empty_result_aggregation(self) -> None:
        """Aggregation on empty result set."""
        from django.db import connection

        with connection.cursor() as cursor:
            cursor.execute(
                'SELECT pdb.agg(\'{"value_count": {"field": "id"}}\') FROM mock_items WHERE description @@@ \'nonexistentterm123\' AND id @@@ pdb.all()'
            )
            row = cursor.fetchone()

        # Result should still exist but with 0 count
        assert row is not None
        agg_value = json.loads(row[0])
        assert agg_value["value"] == 0.0

    def test_multiple_aggregates_same_query(self) -> None:
        """Multiple aggregations in single query."""
        from django.db import connection

        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    pdb.agg('{"avg": {"field": "rating"}}') AS avg_rating,
                    pdb.agg('{"value_count": {"field": "id"}}') AS count,
                    pdb.agg('{"min": {"field": "rating"}}') AS min_rating,
                    pdb.agg('{"max": {"field": "rating"}}') AS max_rating
                FROM mock_items
                WHERE description @@@ 'shoes' AND id @@@ pdb.all()
                """
            )
            row = cursor.fetchone()

        assert row is not None
        avg_rating = json.loads(row[0])
        count = json.loads(row[1])
        min_rating = json.loads(row[2])
        max_rating = json.loads(row[3])

        assert "value" in avg_rating
        assert count["value"] == 3.0
        assert min_rating["value"] <= max_rating["value"]

    def test_aggregation_with_percentiles(self) -> None:
        """Percentile aggregation."""
        from django.db import connection

        with connection.cursor() as cursor:
            cursor.execute(
                'SELECT pdb.agg(\'{"percentiles": {"field": "rating", "percents": [50, 95]}}\') FROM mock_items WHERE id @@@ pdb.all()'
            )
            row = cursor.fetchone()

        assert row is not None
        agg_value = json.loads(row[0])
        # Percentiles return values as key-value pairs
        assert "values" in agg_value or isinstance(agg_value, dict)


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

    def test_agg_terms_on_json_subfield(self) -> None:
        """Agg terms on indexed JSON subfield via ORM aggregate."""
        result = MockItem.objects.filter(id=ParadeDB(All())).aggregate(
            agg=Agg('{"terms": {"field": "metadata.color"}}')
        )
        assert isinstance(result, dict)
        assert "agg" in result
        assert "buckets" in _agg_dict(result["agg"])


class TestJSONFieldAggregations:
    """Test aggregations on JSON subfields (require fast field configuration)."""

    def test_metadata_color_terms_aggregation(self) -> None:
        """AGG-4: Terms aggregation on JSON metadata.color subfield.

        This requires the metadata field to be configured as a fast field
        in the BM25 index using json_fields parameter.
        """
        from django.db import connection

        with connection.cursor() as cursor:
            cursor.execute(
                'SELECT pdb.agg(\'{"terms": {"field": "metadata.color"}}\') FROM mock_items WHERE id @@@ pdb.all()'
            )
            row = cursor.fetchone()

        assert row is not None
        agg_value = json.loads(row[0])
        assert "buckets" in agg_value
        # Should have buckets for different colors
        colors = {bucket["key"] for bucket in agg_value["buckets"]}
        assert len(colors) > 0
