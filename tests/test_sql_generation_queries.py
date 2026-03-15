"""Focused SQL-generation tests for higher-level query helpers and queryset APIs."""

from __future__ import annotations

import json

import pytest
from django.db.models import F, Q, TextField, Value, Window
from django.db.models.functions import Concat, RowNumber

from paradedb.functions import Agg
from paradedb.search import (
    Empty,
    Exists,
    FuzzyTerm,
    Match,
    MoreLikeThis,
    ParadeDB,
    ParseWithField,
    Phrase,
    Range,
    TermSet,
)
from tests.models import Product


class TestMoreLikeThis:
    """Test MoreLikeThis SQL generation."""

    def test_mlt_by_id(self) -> None:
        """MLT by ID: WHERE id @@@ pdb.more_like_this(5)."""
        queryset = Product.objects.filter(MoreLikeThis(product_id=5))
        assert (
            str(queryset.query)
            == 'SELECT "tests_product"."id", "tests_product"."description", "tests_product"."category", "tests_product"."rating", "tests_product"."in_stock", "tests_product"."price", "tests_product"."created_at", "tests_product"."metadata" FROM "tests_product" WHERE "tests_product"."id" @@@ pdb.more_like_this(5)'
        )

    def test_mlt_multiple_ids(self) -> None:
        """MLT with multiple IDs generates OR conditions."""
        queryset = Product.objects.filter(MoreLikeThis(product_ids=[5, 12, 23]))
        assert (
            str(queryset.query)
            == 'SELECT "tests_product"."id", "tests_product"."description", "tests_product"."category", "tests_product"."rating", "tests_product"."in_stock", "tests_product"."price", "tests_product"."created_at", "tests_product"."metadata" FROM "tests_product" WHERE ("tests_product"."id" @@@ pdb.more_like_this(5) OR "tests_product"."id" @@@ pdb.more_like_this(12) OR "tests_product"."id" @@@ pdb.more_like_this(23))'
        )

    def test_mlt_with_parameters(self) -> None:
        """MLT with tuning parameters."""
        queryset = Product.objects.filter(
            MoreLikeThis(
                product_id=5, min_term_freq=2, max_query_terms=10, min_doc_freq=1
            )
        )
        assert (
            str(queryset.query)
            == 'SELECT "tests_product"."id", "tests_product"."description", "tests_product"."category", "tests_product"."rating", "tests_product"."in_stock", "tests_product"."price", "tests_product"."created_at", "tests_product"."metadata" FROM "tests_product" WHERE "tests_product"."id" @@@ pdb.more_like_this(5, min_term_frequency => 2, max_query_terms => 10, min_doc_frequency => 1)'
        )

    def test_mlt_by_document(self) -> None:
        """MLT by document uses JSON input."""
        queryset = Product.objects.filter(
            MoreLikeThis(
                document={
                    "description": "comfortable running shoes",
                    "category": "footwear",
                },
            )
        )
        assert (
            str(queryset.query)
            == 'SELECT "tests_product"."id", "tests_product"."description", "tests_product"."category", "tests_product"."rating", "tests_product"."in_stock", "tests_product"."price", "tests_product"."created_at", "tests_product"."metadata" FROM "tests_product" WHERE "tests_product"."id" @@@ pdb.more_like_this({"description": "comfortable running shoes", "category": "footwear"})'
        )

    def test_mlt_with_word_length(self) -> None:
        """MLT with min/max word length parameters."""
        queryset = Product.objects.filter(
            MoreLikeThis(product_id=5, min_word_length=3, max_word_length=15)
        )
        sql = str(queryset.query)
        assert "min_word_length => 3" in sql
        assert "max_word_length => 15" in sql
        assert "pdb.more_like_this(5," in sql

    def test_mlt_with_stopwords(self) -> None:
        """MLT with stopwords array parameter."""
        queryset = Product.objects.filter(
            MoreLikeThis(product_id=5, stopwords=["the", "a", "an"])
        )
        sql = str(queryset.query)
        assert "stopwords => ARRAY[the, a, an]" in sql

    def test_mlt_with_all_options(self) -> None:
        """MLT with all available options including new ones."""
        queryset = Product.objects.filter(
            MoreLikeThis(
                product_id=5,
                fields=["description"],
                min_term_freq=2,
                max_query_terms=10,
                min_doc_freq=1,
                max_term_freq=100,
                max_doc_freq=1000,
                min_word_length=3,
                max_word_length=20,
                stopwords=["the", "and", "or"],
            )
        )
        sql = str(queryset.query)
        assert "min_term_frequency => 2" in sql
        assert "max_query_terms => 10" in sql
        assert "min_doc_frequency => 1" in sql
        assert "max_term_frequency => 100" in sql
        assert "max_doc_frequency => 1000" in sql
        assert "min_word_length => 3" in sql
        assert "max_word_length => 20" in sql
        assert "stopwords => ARRAY[the, and, or]" in sql
        assert "ARRAY[description]" in sql

    def test_mlt_document_with_new_options(self) -> None:
        """MLT with document input and new word length/stopwords options."""
        queryset = Product.objects.filter(
            MoreLikeThis(
                document={"description": "comfortable running shoes"},
                min_word_length=4,
                max_word_length=12,
                stopwords=["comfortable"],
            )
        )
        sql = str(queryset.query)
        assert "min_word_length => 4" in sql
        assert "max_word_length => 12" in sql
        assert "stopwords => ARRAY[comfortable]" in sql

    def test_mlt_document_should_use_json_format(self) -> None:
        """Document input should generate JSON format."""
        queryset = Product.objects.filter(
            MoreLikeThis(document={"description": "wireless earbuds"})
        )
        sql = str(queryset.query)
        assert 'pdb.more_like_this({"description": "wireless earbuds"})' in sql, (
            "Expected JSON format: "
            'pdb.more_like_this({"description": "wireless earbuds"})\n'
            f"Got SQL: {sql}"
        )
        assert "ARRAY[description]" not in sql, (
            f"Should not use array form for document input\nGot SQL: {sql}"
        )

    def test_mlt_empty_stopwords_should_not_generate_empty_string(self) -> None:
        """Empty stopwords should be omitted."""
        queryset = Product.objects.filter(
            MoreLikeThis(
                product_id=5,
                stopwords=[],
            )
        )
        sql = str(queryset.query)
        assert "stopwords" not in sql, (
            f"Empty stopwords should be omitted entirely\nGot SQL: {sql}"
        )


class TestDjangoIntegration:
    """Test Django ORM integration."""

    def test_paradedb_with_django_q(self) -> None:
        """Combine ParadeDB with Django Q for complex logic."""
        queryset = Product.objects.filter(
            Q(description=ParadeDB(Phrase("running shoes")), rating__gte=4)
            | Q(
                category=ParadeDB(Match("Electronics", operator="AND")),
                description=ParadeDB(Match("wireless", operator="AND")),
            )
        )
        sql = str(queryset.query)
        assert '"tests_product"."description" ### \'running shoes\'' in sql
        assert '"tests_product"."rating" >= 4' in sql
        assert '"tests_product"."category" &&& \'Electronics\'' in sql
        assert '"tests_product"."description" &&& \'wireless\'' in sql
        assert " OR " in sql

    def test_negation_with_q(self) -> None:
        """Negation using ~Q with ParadeDB."""
        queryset = Product.objects.filter(
            Q(description=ParadeDB(Match("running", "athletic", operator="AND"))),
            ~Q(description=ParadeDB(Match("cheap", operator="AND"))),
        )
        sql = str(queryset.query)
        assert (
            "\"tests_product\".\"description\" &&& ARRAY['running', 'athletic']" in sql
        )
        assert 'NOT ("tests_product"."description" &&& \'cheap\')' in sql

    def test_with_standard_filters(self) -> None:
        """ParadeDB search combined with standard ORM filters."""
        queryset = Product.objects.filter(
            description=ParadeDB(Match("shoes", operator="AND")),
            price__lt=100,
            in_stock=True,
            rating__gte=4,
        )
        assert (
            str(queryset.query)
            == 'SELECT "tests_product"."id", "tests_product"."description", "tests_product"."category", "tests_product"."rating", "tests_product"."in_stock", "tests_product"."price", "tests_product"."created_at", "tests_product"."metadata" FROM "tests_product" WHERE ("tests_product"."description" &&& \'shoes\' AND "tests_product"."in_stock" AND "tests_product"."price" < 100 AND "tests_product"."rating" >= 4)'
        )

    def test_with_window_functions(self) -> None:
        """ParadeDB search with Django window functions."""
        queryset = Product.objects.filter(
            description=ParadeDB(Match("shoes", operator="AND"))
        ).annotate(
            rank_in_category=Window(
                expression=RowNumber(),
                partition_by=[F("category")],
                order_by=F("price").desc(),
            )
        )
        assert (
            str(queryset.query)
            == 'SELECT "tests_product"."id", "tests_product"."description", "tests_product"."category", "tests_product"."rating", "tests_product"."in_stock", "tests_product"."price", "tests_product"."created_at", "tests_product"."metadata", ROW_NUMBER() OVER (PARTITION BY "tests_product"."category" ORDER BY "tests_product"."price" DESC) AS "rank_in_category" FROM "tests_product" WHERE "tests_product"."description" &&& \'shoes\''
        )

    def test_with_adhoc_expression_lhs(self) -> None:
        queryset = Product.objects.annotate(
            combined=Concat(
                F("description"),
                Value(" "),
                F("category"),
                output_field=TextField(),
            )
        ).filter(combined=ParadeDB(Match("running", operator="AND")))
        sql = str(queryset.query)
        assert "&&& 'running'" in sql
        assert '"tests_product"."description"' in sql
        assert '"tests_product"."category"' in sql


class TestFacets:
    """Test facets SQL generation helpers."""

    def test_facets_window_annotation(self) -> None:
        """Facets window annotation uses pdb.agg() OVER ()."""
        json_spec = '{"terms":{"field":"category","order":{"_count":"desc"},"size":10}}'
        queryset = Product.objects.filter(
            description=ParadeDB(Match("shoes", operator="AND"))
        ).annotate(facets=Window(expression=Agg(json_spec)))
        sql = str(queryset.query)
        assert (
            'pdb.agg(\'{"terms":{"field":"category","order":{"_count":"desc"},"size":10}}\') OVER () AS "facets"'
            in sql
        )
        assert '"tests_product"."description" &&& \'shoes\'' in sql

    def test_facets_window_annotation_exact_false(self) -> None:
        """Non-exact (exact=False) facets emit the second pdb.agg argument."""
        json_spec = '{"terms":{"field":"category","order":{"_count":"desc"},"size":10}}'
        queryset = Product.objects.filter(
            description=ParadeDB(Match("shoes", operator="AND"))
        ).annotate(facets=Window(expression=Agg(json_spec, exact=False)))
        assert (
            str(queryset.query)
            == 'SELECT "tests_product"."id", "tests_product"."description", "tests_product"."category", "tests_product"."rating", "tests_product"."in_stock", "tests_product"."price", "tests_product"."created_at", "tests_product"."metadata", pdb.agg(\'{"terms":{"field":"category","order":{"_count":"desc"},"size":10}}\', false) OVER () AS "facets" FROM "tests_product" WHERE "tests_product"."description" &&& \'shoes\''
        )

    def test_facets_requires_paradedb_search_condition(self) -> None:
        """facets() raises if no ParadeDB search condition is present."""
        queryset = Product.objects.filter(rating=5).order_by("price")[:5]
        with pytest.raises(ValueError, match="ParadeDB search condition"):
            queryset.facets("category")

    def test_facets_requires_order_by_and_limit(self) -> None:
        """facets() raises if include_rows requires order_by + LIMIT."""
        queryset = Product.objects.filter(
            description=ParadeDB(Match("shoes", operator="AND"))
        )
        with pytest.raises(ValueError, match="order_by\\(\\) and a LIMIT"):
            queryset.facets("category")

    def test_facets_exact_false_requires_window(self) -> None:
        """exact=False facets require windowed aggregations."""
        queryset = Product.objects.filter(
            description=ParadeDB(Match("shoes", operator="AND"))
        )
        with pytest.raises(ValueError, match="exact=False"):
            queryset.facets("category", include_rows=False, exact=False)

    def test_facets_multiple_fields_specs(self) -> None:
        """facets() generates correct specs for multiple fields."""
        queryset = Product.objects.filter(
            description=ParadeDB(Match("shoes", operator="AND"))
        ).order_by("price")[:5]
        specs = queryset._build_agg_specs(
            fields=["category", "rating"],
            size=10,
            order="-count",
            missing=None,
            agg=None,
        )
        assert "category_terms" in specs
        assert "rating_terms" in specs
        assert "category" in specs["category_terms"]
        assert "rating" in specs["rating_terms"]

    def test_facets_single_field_spec_shape(self) -> None:
        """Single field facets use terms as the root aggregation."""
        queryset = Product.objects.filter(
            description=ParadeDB(Match("shoes", operator="AND"))
        ).order_by("price")[:5]
        specs = queryset._build_agg_specs(
            fields=["category"],
            size=5,
            order="-count",
            missing=None,
            agg=None,
        )
        assert list(specs.keys()) == ["_paradedb_facets"]
        assert json.loads(specs["_paradedb_facets"]) == {
            "terms": {"field": "category", "order": {"_count": "desc"}, "size": 5}
        }

    def test_facets_missing_allows_non_string(self) -> None:
        """Missing values accept non-string JSON types."""
        queryset = Product.objects.filter(
            description=ParadeDB(Match("shoes", operator="AND"))
        ).order_by("price")[:5]
        specs = queryset._build_agg_specs(
            fields=["in_stock"],
            size=None,
            order=None,
            missing=False,
            agg=None,
        )
        assert json.loads(specs["_paradedb_facets"]) == {
            "terms": {"field": "in_stock", "missing": False}
        }

    def test_facets_requires_unique_fields(self) -> None:
        """facets() raises when fields are duplicated."""
        queryset = Product.objects.filter(
            description=ParadeDB(Match("shoes", operator="AND"))
        ).order_by("price")[:5]
        with pytest.raises(ValueError, match="unique"):
            queryset.facets("category", "category")


class TestEmptyQuery:
    """Test Empty query SQL generation."""

    def test_empty_basic(self) -> None:
        queryset = Product.objects.filter(description=ParadeDB(Empty()))
        assert "@@@ pdb.empty()" in str(queryset.query)

    def test_empty_with_boost(self) -> None:
        queryset = Product.objects.filter(description=ParadeDB(Empty(boost=2.0)))
        assert "@@@ pdb.empty()::pdb.boost(2.0)" in str(queryset.query)

    def test_empty_with_const(self) -> None:
        queryset = Product.objects.filter(description=ParadeDB(Empty(const=1.0)))
        assert "@@@ pdb.empty()::pdb.const(1.0)" in str(queryset.query)


class TestExistsQuery:
    """Test Exists query SQL generation."""

    def test_exists_basic(self) -> None:
        queryset = Product.objects.filter(description=ParadeDB(Exists()))
        assert "@@@ pdb.exists()" in str(queryset.query)

    def test_exists_with_boost(self) -> None:
        queryset = Product.objects.filter(description=ParadeDB(Exists(boost=3.0)))
        assert "@@@ pdb.exists()::pdb.boost(3.0)" in str(queryset.query)

    def test_exists_with_const(self) -> None:
        queryset = Product.objects.filter(description=ParadeDB(Exists(const=1.0)))
        assert "@@@ pdb.exists()::pdb.const(1.0)" in str(queryset.query)


class TestFuzzyTermQuery:
    """Test FuzzyTerm query SQL generation."""

    def test_fuzzy_term_with_value(self) -> None:
        queryset = Product.objects.filter(
            description=ParadeDB(FuzzyTerm(value="shoes"))
        )
        assert "@@@ pdb.fuzzy_term('shoes')" in str(queryset.query)

    def test_fuzzy_term_no_value(self) -> None:
        queryset = Product.objects.filter(description=ParadeDB(FuzzyTerm()))
        assert "@@@ pdb.fuzzy_term()" in str(queryset.query)

    def test_fuzzy_term_with_distance(self) -> None:
        queryset = Product.objects.filter(
            description=ParadeDB(FuzzyTerm(value="shoes", distance=2))
        )
        assert "@@@ pdb.fuzzy_term('shoes')::pdb.fuzzy(2)" in str(queryset.query)

    def test_fuzzy_term_with_boost(self) -> None:
        queryset = Product.objects.filter(
            description=ParadeDB(FuzzyTerm(value="shoes", boost=1.5))
        )
        assert "@@@ pdb.fuzzy_term('shoes')::pdb.boost(1.5)" in str(queryset.query)

    def test_fuzzy_term_with_distance_and_const(self) -> None:
        queryset = Product.objects.filter(
            description=ParadeDB(FuzzyTerm(value="shoes", distance=1, const=2.0))
        )
        assert (
            "@@@ pdb.fuzzy_term('shoes')::pdb.fuzzy(1)::pdb.query::pdb.const(2.0)"
            in str(queryset.query)
        )


class TestParseWithFieldQuery:
    """Test ParseWithField query SQL generation."""

    def test_parse_with_field_basic(self) -> None:
        queryset = Product.objects.filter(
            description=ParadeDB(ParseWithField(query="shoes"))
        )
        assert "@@@ pdb.parse_with_field('shoes')" in str(queryset.query)

    def test_parse_with_field_lenient(self) -> None:
        queryset = Product.objects.filter(
            description=ParadeDB(ParseWithField(query="shoes", lenient=True))
        )
        assert "@@@ pdb.parse_with_field('shoes', lenient => true)" in str(
            queryset.query
        )

    def test_parse_with_field_conjunction_mode(self) -> None:
        queryset = Product.objects.filter(
            description=ParadeDB(ParseWithField(query="shoes", conjunction_mode=True))
        )
        assert "@@@ pdb.parse_with_field('shoes', conjunction_mode => true)" in str(
            queryset.query
        )

    def test_parse_with_field_with_boost(self) -> None:
        queryset = Product.objects.filter(
            description=ParadeDB(ParseWithField(query="shoes", boost=2.0))
        )
        assert "@@@ pdb.parse_with_field('shoes')::pdb.boost(2.0)" in str(
            queryset.query
        )


class TestRangeQuery:
    """Test Range query SQL generation."""

    def test_range_int4range(self) -> None:
        queryset = Product.objects.filter(
            description=ParadeDB(Range(range="[1, 10]", range_type="int4range"))
        )
        assert "@@@ pdb.range('[1, 10]'::int4range)" in str(queryset.query)

    def test_range_numrange(self) -> None:
        queryset = Product.objects.filter(
            description=ParadeDB(Range(range="(0, 100)", range_type="numrange"))
        )
        assert "@@@ pdb.range('(0, 100)'::numrange)" in str(queryset.query)

    def test_range_with_boost(self) -> None:
        queryset = Product.objects.filter(
            description=ParadeDB(
                Range(range="[1, 10]", range_type="int4range", boost=2.0)
            )
        )
        assert "@@@ pdb.range('[1, 10]'::int4range)::pdb.boost(2.0)" in str(
            queryset.query
        )


class TestTermSetQuery:
    """Test TermSet query SQL generation."""

    def test_term_set_strings(self) -> None:
        queryset = Product.objects.filter(
            description=ParadeDB(TermSet("shoes", "boots"))
        )
        assert "@@@ pdb.term_set(ARRAY['shoes', 'boots']::text[])" in str(
            queryset.query
        )

    def test_term_set_integers(self) -> None:
        queryset = Product.objects.filter(description=ParadeDB(TermSet(1, 2, 3)))
        assert "@@@ pdb.term_set(ARRAY[1, 2, 3]::bigint[])" in str(queryset.query)

    def test_term_set_floats(self) -> None:
        queryset = Product.objects.filter(description=ParadeDB(TermSet(1.0, 2.5)))
        assert "@@@ pdb.term_set(ARRAY[1.0, 2.5]::float8[])" in str(queryset.query)

    def test_term_set_booleans(self) -> None:
        queryset = Product.objects.filter(description=ParadeDB(TermSet(True, False)))
        assert "@@@ pdb.term_set(ARRAY[true, false]::boolean[])" in str(queryset.query)

    def test_term_set_with_boost(self) -> None:
        queryset = Product.objects.filter(
            description=ParadeDB(TermSet("a", "b", boost=2.0))
        )
        assert "@@@ pdb.term_set(ARRAY['a', 'b']::text[])::pdb.boost(2.0)" in str(
            queryset.query
        )
