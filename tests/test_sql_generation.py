"""Tests for SQL generation."""

import pytest
from django.db import connection
from django.db.models import Q, Window
from django.db.models.functions import Coalesce

from paradedb.functions import Agg, Score, Snippet, SnippetPositions, Snippets
from paradedb.search import (
    All,
    Boost,
    Const,
    Exists,
    Fuzzy,
    FuzzyTerm,
    MatchAll,
    MatchAny,
    MoreLikeThis,
    ParadeDB,
    Parse,
    Phrase,
    PhrasePrefix,
    Proximity,
    ProxRegex,
    RangeTerm,
    Regex,
    RegexPhrase,
    Slop,
    Term,
    TermSet,
    Tokenized,
    Tokenizer,
)
from tests.models import MockItem, Product

pytestmark = [
    pytest.mark.integration,
    pytest.mark.django_db(transaction=True),
]


@pytest.fixture(autouse=True)
def product_search_index() -> None:
    with connection.cursor() as cursor:
        cursor.execute("CREATE EXTENSION IF NOT EXISTS pg_search;")
        cursor.execute("DROP INDEX IF EXISTS product_search_idx;")
        cursor.execute(
            """
            CREATE INDEX product_search_idx ON tests_product USING bm25 (
                id,
                description,
                category,
                rating,
                in_stock,
                metadata
            ) WITH (key_field='id', json_fields='{"metadata":{"fast":true}}');
            """
        )


def _run_query(queryset) -> None:
    sql, params = queryset.query.sql_with_params()
    with connection.cursor() as cursor:
        cursor.execute(sql, params)


class TestAggAnnotation:
    """Test Agg annotation SQL generation."""

    pytestmark = pytest.mark.usefixtures("mock_items")

    def test_agg_annotation_with_raw_sql(self) -> None:
        json_spec = '{"value_count": {"field": "id"}}'
        queryset = Product.objects.filter(
            description=ParadeDB(MatchAll("shoes"))
        ).annotate(facets=Window(expression=Agg(json_spec)))[:10]
        assert (
            str(queryset.query)
            == 'SELECT "tests_product"."id", "tests_product"."description", "tests_product"."category", "tests_product"."rating", "tests_product"."in_stock", "tests_product"."price", "tests_product"."created_at", "tests_product"."metadata", pdb.agg(\'{"value_count": {"field": "id"}}\') OVER () AS "facets" FROM "tests_product" WHERE "tests_product"."description" &&& \'shoes\' LIMIT 10'
        )
        _run_query(queryset)

    def test_agg_single_value_count(self) -> None:
        queryset = MockItem.objects.filter(
            category=ParadeDB(Term("electronics"))
        ).annotate(agg=Window(expression=Agg('{"value_count": {"field": "id"}}')))[:1]
        assert (
            str(queryset.query)
            == 'SELECT "mock_items"."id", "mock_items"."description", "mock_items"."category", "mock_items"."rating", "mock_items"."in_stock", "mock_items"."created_at", "mock_items"."metadata", pdb.agg(\'{"value_count": {"field": "id"}}\') OVER () AS "agg" FROM "mock_items" WHERE "mock_items"."category" @@@ pdb.term(\'electronics\') LIMIT 1'
        )
        _run_query(queryset)

    def test_agg_grouped_by_rating(self) -> None:
        queryset = (
            MockItem.objects.filter(category=ParadeDB(Term("electronics")))
            .values("rating")
            .annotate(agg=Agg('{"value_count": {"field": "id"}}'))
            .order_by("rating")[:5]
        )
        assert (
            str(queryset.query)
            == 'SELECT "mock_items"."rating" AS "rating", pdb.agg(\'{"value_count": {"field": "id"}}\') AS "agg" FROM "mock_items" WHERE "mock_items"."category" @@@ pdb.term(\'electronics\') GROUP BY 1 ORDER BY 1 ASC LIMIT 5'
        )
        _run_query(queryset)

    def test_agg_multiple_aggregations(self) -> None:
        queryset = MockItem.objects.filter(
            category=ParadeDB(Term("electronics"))
        ).annotate(
            avg_rating=Window(expression=Agg('{"avg": {"field": "rating"}}')),
            count=Window(expression=Agg('{"value_count": {"field": "id"}}')),
        )[:1]
        assert (
            str(queryset.query)
            == 'SELECT "mock_items"."id", "mock_items"."description", "mock_items"."category", "mock_items"."rating", "mock_items"."in_stock", "mock_items"."created_at", "mock_items"."metadata", pdb.agg(\'{"avg": {"field": "rating"}}\') OVER () AS "avg_rating", pdb.agg(\'{"value_count": {"field": "id"}}\') OVER () AS "count" FROM "mock_items" WHERE "mock_items"."category" @@@ pdb.term(\'electronics\') LIMIT 1'
        )
        _run_query(queryset)

    def test_agg_with_all_query(self) -> None:
        queryset = MockItem.objects.filter(id=ParadeDB(All())).annotate(
            agg=Window(expression=Agg('{"value_count": {"field": "id"}}'))
        )[:1]
        assert (
            str(queryset.query)
            == 'SELECT "mock_items"."id", "mock_items"."description", "mock_items"."category", "mock_items"."rating", "mock_items"."in_stock", "mock_items"."created_at", "mock_items"."metadata", pdb.agg(\'{"value_count": {"field": "id"}}\') OVER () AS "agg" FROM "mock_items" WHERE "mock_items"."id" @@@ pdb.all() LIMIT 1'
        )
        _run_query(queryset)

    def test_agg_terms_on_json_subfield(self) -> None:
        queryset = MockItem.objects.filter(id=ParadeDB(All())).annotate(
            agg=Window(expression=Agg('{"terms": {"field": "metadata.color"}}'))
        )[:1]
        assert (
            str(queryset.query)
            == 'SELECT "mock_items"."id", "mock_items"."description", "mock_items"."category", "mock_items"."rating", "mock_items"."in_stock", "mock_items"."created_at", "mock_items"."metadata", pdb.agg(\'{"terms": {"field": "metadata.color"}}\') OVER () AS "agg" FROM "mock_items" WHERE "mock_items"."id" @@@ pdb.all() LIMIT 1'
        )
        _run_query(queryset)


class TestFacets:
    """Test facets SQL generation helpers."""

    def test_facets_window_annotation_exact_false(self) -> None:
        json_spec = '{"terms":{"field":"category","order":{"_count":"desc"},"size":10}}'
        queryset = Product.objects.filter(description=ParadeDB(MatchAll("shoes")))[
            :10
        ].annotate(facets=Window(expression=Agg(json_spec, exact=False)))
        assert (
            str(queryset.query)
            == 'SELECT "tests_product"."id", "tests_product"."description", "tests_product"."category", "tests_product"."rating", "tests_product"."in_stock", "tests_product"."price", "tests_product"."created_at", "tests_product"."metadata", pdb.agg(\'{"terms":{"field":"category","order":{"_count":"desc"},"size":10}}\', false) OVER () AS "facets" FROM "tests_product" WHERE "tests_product"."description" &&& \'shoes\' LIMIT 10'
        )
        _run_query(queryset)

    def test_facets_requires_paradedb_search_condition(self) -> None:
        queryset = Product.objects.filter(rating=5).order_by("price")[:5]
        with pytest.raises(ValueError, match="ParadeDB search condition"):
            queryset.facets("category")

    def test_facets_requires_order_by_and_limit(self) -> None:
        queryset = Product.objects.filter(description=ParadeDB(MatchAll("shoes")))
        with pytest.raises(ValueError, match=r"order_by\(\) and a LIMIT"):
            queryset.facets("category")

    def test_facets_exact_false_requires_window(self) -> None:
        queryset = Product.objects.filter(description=ParadeDB(MatchAll("shoes")))
        with pytest.raises(ValueError, match="exact=False"):
            queryset.facets("category", include_rows=False, exact=False)

    def test_facets_multiple_fields_specs(self) -> None:
        queryset = Product.objects.filter(
            description=ParadeDB(MatchAll("shoes"))
        ).order_by("price")[:5]
        specs = queryset._build_agg_specs(
            fields=["category", "rating"],
            size=10,
            order="-count",
            missing=None,
            agg=None,
        )
        assert specs == {
            "category_terms": '{"terms":{"field":"category","order":{"_count":"desc"},"size":10}}',
            "rating_terms": '{"terms":{"field":"rating","order":{"_count":"desc"},"size":10}}',
        }

    def test_facets_single_field_spec_shape(self) -> None:
        queryset = Product.objects.filter(
            description=ParadeDB(MatchAll("shoes"))
        ).order_by("price")[:5]
        specs = queryset._build_agg_specs(
            fields=["category"],
            size=5,
            order="-count",
            missing=None,
            agg=None,
        )
        assert specs == {
            "_paradedb_facets": '{"terms":{"field":"category","order":{"_count":"desc"},"size":5}}'
        }

    def test_facets_missing_allows_non_string(self) -> None:
        queryset = Product.objects.filter(
            description=ParadeDB(MatchAll("shoes"))
        ).order_by("price")[:5]
        specs = queryset._build_agg_specs(
            fields=["in_stock"],
            size=None,
            order=None,
            missing=False,
            agg=None,
        )
        assert specs == {
            "_paradedb_facets": '{"terms":{"field":"in_stock","missing":false}}'
        }

    def test_facets_requires_unique_fields(self) -> None:
        queryset = Product.objects.filter(
            description=ParadeDB(MatchAll("shoes"))
        ).order_by("price")[:5]
        with pytest.raises(ValueError, match="unique"):
            queryset.facets("category", "category")


class TestParadeDBLookup:
    """Test ParadeDB lookup SQL generation."""

    def test_single_term_search(self) -> None:
        """Single term generates: WHERE description &&& 'shoes'."""
        queryset = Product.objects.filter(description=ParadeDB(MatchAll("shoes")))
        assert (
            str(queryset.query)
            == 'SELECT "tests_product"."id", "tests_product"."description", "tests_product"."category", "tests_product"."rating", "tests_product"."in_stock", "tests_product"."price", "tests_product"."created_at", "tests_product"."metadata" FROM "tests_product" WHERE "tests_product"."description" &&& \'shoes\''
        )
        _run_query(queryset)

    def test_and_search_multiple_terms(self) -> None:
        """Multiple terms generate: WHERE description &&& ARRAY[...]"""
        queryset = Product.objects.filter(
            description=ParadeDB(MatchAll("running", "shoes"))
        )
        assert (
            str(queryset.query)
            == 'SELECT "tests_product"."id", "tests_product"."description", "tests_product"."category", "tests_product"."rating", "tests_product"."in_stock", "tests_product"."price", "tests_product"."created_at", "tests_product"."metadata" FROM "tests_product" WHERE "tests_product"."description" &&& ARRAY[\'running\', \'shoes\']'
        )
        _run_query(queryset)

    def test_and_search_three_terms(self) -> None:
        """Edge case: three terms for AND search."""
        queryset = Product.objects.filter(
            description=ParadeDB(MatchAll("running", "shoes", "lightweight"))
        )
        assert (
            str(queryset.query)
            == 'SELECT "tests_product"."id", "tests_product"."description", "tests_product"."category", "tests_product"."rating", "tests_product"."in_stock", "tests_product"."price", "tests_product"."created_at", "tests_product"."metadata" FROM "tests_product" WHERE "tests_product"."description" &&& ARRAY[\'running\', \'shoes\', \'lightweight\']'
        )
        _run_query(queryset)


class TestMoreLikeThis:
    """Test MoreLikeThis SQL generation."""

    def test_mlt_by_id(self) -> None:
        queryset = Product.objects.filter(id=ParadeDB(MoreLikeThis(id=5)))
        assert (
            str(queryset.query)
            == 'SELECT "tests_product"."id", "tests_product"."description", "tests_product"."category", "tests_product"."rating", "tests_product"."in_stock", "tests_product"."price", "tests_product"."created_at", "tests_product"."metadata" FROM "tests_product" WHERE "tests_product"."id" @@@ pdb.more_like_this(5)'
        )
        _run_query(queryset)

    def test_mlt_multiple_ids(self) -> None:
        queryset = Product.objects.filter(id=ParadeDB(MoreLikeThis(ids=[5, 12, 23])))
        assert (
            str(queryset.query)
            == 'SELECT "tests_product"."id", "tests_product"."description", "tests_product"."category", "tests_product"."rating", "tests_product"."in_stock", "tests_product"."price", "tests_product"."created_at", "tests_product"."metadata" FROM "tests_product" WHERE ("tests_product"."id" @@@ pdb.more_like_this(5) OR "tests_product"."id" @@@ pdb.more_like_this(12) OR "tests_product"."id" @@@ pdb.more_like_this(23))'
        )
        _run_query(queryset)

    def test_mlt_with_options(self) -> None:
        queryset = Product.objects.filter(
            id=ParadeDB(
                MoreLikeThis(
                    id=5,
                    fields=["description"],
                    min_term_freq=2,
                    max_query_terms=10,
                    min_doc_freq=1,
                    min_word_length=3,
                    max_word_length=20,
                    stopwords=["the", "and", "or"],
                )
            )
        )
        assert (
            str(queryset.query)
            == 'SELECT "tests_product"."id", "tests_product"."description", "tests_product"."category", "tests_product"."rating", "tests_product"."in_stock", "tests_product"."price", "tests_product"."created_at", "tests_product"."metadata" FROM "tests_product" WHERE "tests_product"."id" @@@ pdb.more_like_this(5, ARRAY[description]::text[], min_term_frequency => 2, max_query_terms => 10, min_doc_frequency => 1, min_word_length => 3, max_word_length => 20, stopwords => ARRAY[the, and, or])'
        )
        _run_query(queryset)


class TestExactLiteralDisjunction:
    """Test exact literal OR search SQL generation."""

    def test_single_string_or(self) -> None:
        queryset = Product.objects.filter(
            description=ParadeDB(MatchAny("running shoes"))
        )
        assert (
            str(queryset.query)
            == 'SELECT "tests_product"."id", "tests_product"."description", "tests_product"."category", "tests_product"."rating", "tests_product"."in_stock", "tests_product"."price", "tests_product"."created_at", "tests_product"."metadata" FROM "tests_product" WHERE "tests_product"."description" ||| \'running shoes\''
        )
        _run_query(queryset)

    def test_multiple_strings_or(self) -> None:
        queryset = Product.objects.filter(
            description=ParadeDB(MatchAny("shoes", "boots"))
        )
        assert (
            str(queryset.query)
            == 'SELECT "tests_product"."id", "tests_product"."description", "tests_product"."category", "tests_product"."rating", "tests_product"."in_stock", "tests_product"."price", "tests_product"."created_at", "tests_product"."metadata" FROM "tests_product" WHERE "tests_product"."description" ||| ARRAY[\'shoes\', \'boots\']'
        )
        _run_query(queryset)


class TestPhraseSearch:
    """Test Phrase search SQL generation."""

    def test_phrase_search(self) -> None:
        """Phrase generates: WHERE description ### 'wireless bluetooth'."""
        queryset = Product.objects.filter(
            description=ParadeDB(Phrase("wireless bluetooth"))
        )
        assert (
            str(queryset.query)
            == 'SELECT "tests_product"."id", "tests_product"."description", "tests_product"."category", "tests_product"."rating", "tests_product"."in_stock", "tests_product"."price", "tests_product"."created_at", "tests_product"."metadata" FROM "tests_product" WHERE "tests_product"."description" ### \'wireless bluetooth\''
        )
        _run_query(queryset)

    def test_phrase_with_slop(self) -> None:
        """Phrase with slop: WHERE description ### 'running shoes'::pdb.slop(1)."""
        queryset = Product.objects.filter(
            description=ParadeDB(Phrase(Slop("running shoes", 1)))
        )
        assert (
            str(queryset.query)
            == 'SELECT "tests_product"."id", "tests_product"."description", "tests_product"."category", "tests_product"."rating", "tests_product"."in_stock", "tests_product"."price", "tests_product"."created_at", "tests_product"."metadata" FROM "tests_product" WHERE "tests_product"."description" ### \'running shoes\'::pdb.slop(1)'
        )
        _run_query(queryset)

    def test_phrase_with_multiple_terms(self) -> None:
        queryset = Product.objects.filter(
            description=ParadeDB(Phrase("running shoes", "sneakers"))
        )
        assert (
            str(queryset.query)
            == 'SELECT "tests_product"."id", "tests_product"."description", "tests_product"."category", "tests_product"."rating", "tests_product"."in_stock", "tests_product"."price", "tests_product"."created_at", "tests_product"."metadata" FROM "tests_product" WHERE "tests_product"."description" ### ARRAY[\'running shoes\', \'sneakers\']'
        )
        _run_query(queryset)

    def test_phrase_with_multiple_terms_and_slop(self) -> None:
        queryset = Product.objects.filter(
            description=ParadeDB(Phrase(Slop(("running shoes", "sneakers"), 7)))
        )
        assert (
            str(queryset.query)
            == 'SELECT "tests_product"."id", "tests_product"."description", "tests_product"."category", "tests_product"."rating", "tests_product"."in_stock", "tests_product"."price", "tests_product"."created_at", "tests_product"."metadata" FROM "tests_product" WHERE "tests_product"."description" ### ARRAY[\'running shoes\', \'sneakers\']::pdb.slop(7)'
        )
        _run_query(queryset)


class TestProximitySearch:
    """Test Proximity search SQL generation."""

    def test_proximity_unordered(self) -> None:
        queryset = Product.objects.filter(
            description=ParadeDB(Proximity("running").within(2, "shoes"))
        )
        assert (
            str(queryset.query)
            == 'SELECT "tests_product"."id", "tests_product"."description", "tests_product"."category", "tests_product"."rating", "tests_product"."in_stock", "tests_product"."price", "tests_product"."created_at", "tests_product"."metadata" FROM "tests_product" WHERE "tests_product"."description" @@@ (\'running\' ## 2 ## \'shoes\')'
        )
        _run_query(queryset)

    def test_proximity_ordered(self) -> None:
        queryset = Product.objects.filter(
            description=ParadeDB(Proximity("running").within(2, "shoes", ordered=True))
        )
        assert (
            str(queryset.query)
            == 'SELECT "tests_product"."id", "tests_product"."description", "tests_product"."category", "tests_product"."rating", "tests_product"."in_stock", "tests_product"."price", "tests_product"."created_at", "tests_product"."metadata" FROM "tests_product" WHERE "tests_product"."description" @@@ (\'running\' ##> 2 ##> \'shoes\')'
        )
        _run_query(queryset)

    def test_proximity_with_boost(self) -> None:
        queryset = Product.objects.filter(
            description=ParadeDB(Boost(Proximity("running").within(2, "shoes"), 1.5))
        )
        assert (
            str(queryset.query)
            == 'SELECT "tests_product"."id", "tests_product"."description", "tests_product"."category", "tests_product"."rating", "tests_product"."in_stock", "tests_product"."price", "tests_product"."created_at", "tests_product"."metadata" FROM "tests_product" WHERE "tests_product"."description" @@@ (\'running\' ## 2 ## \'shoes\')::pdb.boost(1.5)'
        )
        _run_query(queryset)

    def test_proximity_with_const(self) -> None:
        queryset = Product.objects.filter(
            description=ParadeDB(Const(Proximity("running").within(2, "shoes"), 1.0))
        )
        assert (
            str(queryset.query)
            == 'SELECT "tests_product"."id", "tests_product"."description", "tests_product"."category", "tests_product"."rating", "tests_product"."in_stock", "tests_product"."price", "tests_product"."created_at", "tests_product"."metadata" FROM "tests_product" WHERE "tests_product"."description" @@@ (\'running\' ## 2 ## \'shoes\')::pdb.const(1.0)'
        )
        _run_query(queryset)

    def test_proximity_three_terms_chain(self) -> None:
        queryset = Product.objects.filter(
            description=ParadeDB(
                Proximity("running").within(2, "shoes").within(2, "lightweight")
            )
        )
        assert (
            str(queryset.query)
            == 'SELECT "tests_product"."id", "tests_product"."description", "tests_product"."category", "tests_product"."rating", "tests_product"."in_stock", "tests_product"."price", "tests_product"."created_at", "tests_product"."metadata" FROM "tests_product" WHERE "tests_product"."description" @@@ (\'running\' ## 2 ## \'shoes\' ## 2 ## \'lightweight\')'
        )
        _run_query(queryset)

    def test_proximity_distance_negative_rejected(self) -> None:
        with pytest.raises(
            ValueError, match=r"Proximity distance must be zero or positive\."
        ):
            Proximity("running").within(-1, "shoes")

    def test_proximity_without_nodes_rejected(self) -> None:
        queryset = Product.objects.filter(description=ParadeDB(Proximity("running")))
        with pytest.raises(
            TypeError,
            match=r"Unsupported ParadeDB term type",
        ):
            _ = str(queryset.query)


class TestDistanceOption:
    def test_match_distance_single(self) -> None:
        queryset = Product.objects.filter(
            description=ParadeDB(MatchAny(Fuzzy("sheos", 1)))
        )
        assert (
            str(queryset.query)
            == 'SELECT "tests_product"."id", "tests_product"."description", "tests_product"."category", "tests_product"."rating", "tests_product"."in_stock", "tests_product"."price", "tests_product"."created_at", "tests_product"."metadata" FROM "tests_product" WHERE "tests_product"."description" ||| \'sheos\'::pdb.fuzzy(1)'
        )
        _run_query(queryset)

    def test_match_distance_multiple_terms(self) -> None:
        queryset = Product.objects.filter(
            description=ParadeDB(MatchAny(Fuzzy(("runnning", "shoez"), 1)))
        )
        # Fuzzy is applied to the whole array, not per-element
        assert (
            str(queryset.query)
            == 'SELECT "tests_product"."id", "tests_product"."description", "tests_product"."category", "tests_product"."rating", "tests_product"."in_stock", "tests_product"."price", "tests_product"."created_at", "tests_product"."metadata" FROM "tests_product" WHERE "tests_product"."description" ||| ARRAY[\'runnning\', \'shoez\']::pdb.fuzzy(1)'
        )
        _run_query(queryset)


class TestTokenizerOverride:
    def test_match_with_tokenizer(self) -> None:
        queryset = Product.objects.filter(
            description=ParadeDB(
                MatchAll(
                    Tokenized("running shoes", Tokenizer.whitespace()),
                )
            )
        )
        assert (
            str(queryset.query)
            == 'SELECT "tests_product"."id", "tests_product"."description", "tests_product"."category", "tests_product"."rating", "tests_product"."in_stock", "tests_product"."price", "tests_product"."created_at", "tests_product"."metadata" FROM "tests_product" WHERE "tests_product"."description" &&& \'running shoes\'::pdb.whitespace'
        )
        _run_query(queryset)

    def test_match_or_with_tokenizer(self) -> None:
        queryset = Product.objects.filter(
            description=ParadeDB(
                MatchAny(Tokenized("running shoes", Tokenizer.whitespace()))
            )
        )
        assert (
            str(queryset.query)
            == 'SELECT "tests_product"."id", "tests_product"."description", "tests_product"."category", "tests_product"."rating", "tests_product"."in_stock", "tests_product"."price", "tests_product"."created_at", "tests_product"."metadata" FROM "tests_product" WHERE "tests_product"."description" ||| \'running shoes\'::pdb.whitespace'
        )
        _run_query(queryset)

    def test_phrase_with_tokenizer(self) -> None:
        queryset = Product.objects.filter(
            description=ParadeDB(
                Phrase(Tokenized("running shoes", Tokenizer.whitespace()))
            )
        )
        assert (
            str(queryset.query)
            == 'SELECT "tests_product"."id", "tests_product"."description", "tests_product"."category", "tests_product"."rating", "tests_product"."in_stock", "tests_product"."price", "tests_product"."created_at", "tests_product"."metadata" FROM "tests_product" WHERE "tests_product"."description" ### \'running shoes\'::pdb.whitespace'
        )
        _run_query(queryset)

    def test_match_with_tokenizer_args(self) -> None:
        queryset = Product.objects.filter(
            description=ParadeDB(
                MatchAll(
                    Tokenized(
                        "running shoes",
                        Tokenizer.whitespace(options={"lowercase": False}),
                    ),
                )
            )
        )
        assert (
            str(queryset.query)
            == 'SELECT "tests_product"."id", "tests_product"."description", "tests_product"."category", "tests_product"."rating", "tests_product"."in_stock", "tests_product"."price", "tests_product"."created_at", "tests_product"."metadata" FROM "tests_product" WHERE "tests_product"."description" &&& \'running shoes\'::pdb.whitespace(\'lowercase=false\')'
        )
        _run_query(queryset)

    def test_match_with_tokenizer_multi_args(self) -> None:
        queryset = Product.objects.filter(
            description=ParadeDB(
                MatchAny(
                    Tokenized(
                        "wireless keyboard",
                        Tokenizer.simple(
                            options={"lowercase": False, "remove_long": 20}
                        ),
                    ),
                )
            )
        )
        assert (
            str(queryset.query)
            == 'SELECT "tests_product"."id", "tests_product"."description", "tests_product"."category", "tests_product"."rating", "tests_product"."in_stock", "tests_product"."price", "tests_product"."created_at", "tests_product"."metadata" FROM "tests_product" WHERE "tests_product"."description" ||| \'wireless keyboard\'::pdb.simple(\'lowercase=false\',\'remove_long=20\')'
        )
        _run_query(queryset)

    def test_match_with_tokenizer_positional_args(self) -> None:
        queryset = Product.objects.filter(
            description=ParadeDB(
                MatchAll(Tokenized("running shoes", Tokenizer.ngram(3, 3)))
            )
        )
        assert (
            str(queryset.query)
            == 'SELECT "tests_product"."id", "tests_product"."description", "tests_product"."category", "tests_product"."rating", "tests_product"."in_stock", "tests_product"."price", "tests_product"."created_at", "tests_product"."metadata" FROM "tests_product" WHERE "tests_product"."description" &&& \'running shoes\'::pdb.ngram(3,3)'
        )
        _run_query(queryset)

    def test_tokenizer_with_boost(self) -> None:
        queryset = Product.objects.filter(
            description=ParadeDB(
                MatchAll(
                    Boost(Tokenized("shoes", Tokenizer.whitespace()), 2.0),
                )
            )
        )
        assert (
            str(queryset.query)
            == 'SELECT "tests_product"."id", "tests_product"."description", "tests_product"."category", "tests_product"."rating", "tests_product"."in_stock", "tests_product"."price", "tests_product"."created_at", "tests_product"."metadata" FROM "tests_product" WHERE "tests_product"."description" &&& \'shoes\'::pdb.whitespace::pdb.boost(2.0)'
        )
        _run_query(queryset)


class TestParseQuery:
    """Test Parse query SQL generation."""

    def test_parse_query(self) -> None:
        """Parse generates: WHERE description @@@ pdb.parse(..., lenient => true)."""
        queryset = Product.objects.filter(
            description=ParadeDB(Parse("running AND shoes", lenient=True))
        )
        assert (
            str(queryset.query)
            == 'SELECT "tests_product"."id", "tests_product"."description", "tests_product"."category", "tests_product"."rating", "tests_product"."in_stock", "tests_product"."price", "tests_product"."created_at", "tests_product"."metadata" FROM "tests_product" WHERE "tests_product"."description" @@@ pdb.parse(\'running AND shoes\', lenient => true)'
        )
        _run_query(queryset)

    def test_parse_query_with_conjunction_mode(self) -> None:
        queryset = Product.objects.filter(
            description=ParadeDB(Parse("running shoes", conjunction_mode=True))
        )
        assert (
            str(queryset.query)
            == 'SELECT "tests_product"."id", "tests_product"."description", "tests_product"."category", "tests_product"."rating", "tests_product"."in_stock", "tests_product"."price", "tests_product"."created_at", "tests_product"."metadata" FROM "tests_product" WHERE "tests_product"."description" @@@ pdb.parse(\'running shoes\', conjunction_mode => true)'
        )
        _run_query(queryset)


class TestPhrasePrefixQuery:
    """Test phrase_prefix query SQL generation."""

    def test_phrase_prefix_query(self) -> None:
        queryset = Product.objects.filter(
            description=ParadeDB(PhrasePrefix("running", "sh"))
        )
        assert (
            str(queryset.query)
            == 'SELECT "tests_product"."id", "tests_product"."description", "tests_product"."category", "tests_product"."rating", "tests_product"."in_stock", "tests_product"."price", "tests_product"."created_at", "tests_product"."metadata" FROM "tests_product" WHERE "tests_product"."description" @@@ pdb.phrase_prefix(ARRAY[\'running\', \'sh\'])'
        )
        _run_query(queryset)

    def test_phrase_prefix_with_max_expansion(self) -> None:
        queryset = Product.objects.filter(
            description=ParadeDB(PhrasePrefix("running", "sh", max_expansion=50))
        )
        assert (
            str(queryset.query)
            == 'SELECT "tests_product"."id", "tests_product"."description", "tests_product"."category", "tests_product"."rating", "tests_product"."in_stock", "tests_product"."price", "tests_product"."created_at", "tests_product"."metadata" FROM "tests_product" WHERE "tests_product"."description" @@@ pdb.phrase_prefix(ARRAY[\'running\', \'sh\'], max_expansion => 50)'
        )
        _run_query(queryset)

    def test_phrase_prefix_with_boost(self) -> None:
        queryset = Product.objects.filter(
            description=ParadeDB(Boost(PhrasePrefix("running", "sh"), 2.0))
        )
        assert (
            str(queryset.query)
            == 'SELECT "tests_product"."id", "tests_product"."description", "tests_product"."category", "tests_product"."rating", "tests_product"."in_stock", "tests_product"."price", "tests_product"."created_at", "tests_product"."metadata" FROM "tests_product" WHERE "tests_product"."description" @@@ pdb.phrase_prefix(ARRAY[\'running\', \'sh\'])::pdb.boost(2.0)'
        )
        _run_query(queryset)

    def test_phrase_prefix_with_const(self) -> None:
        queryset = Product.objects.filter(
            description=ParadeDB(Const(PhrasePrefix("running", "sh"), 1.0))
        )
        assert (
            str(queryset.query)
            == 'SELECT "tests_product"."id", "tests_product"."description", "tests_product"."category", "tests_product"."rating", "tests_product"."in_stock", "tests_product"."price", "tests_product"."created_at", "tests_product"."metadata" FROM "tests_product" WHERE "tests_product"."description" @@@ pdb.phrase_prefix(ARRAY[\'running\', \'sh\'])::pdb.const(1.0)'
        )
        _run_query(queryset)

    def test_phrase_prefix_requires_terms(self) -> None:
        with pytest.raises(
            ValueError, match=r"PhrasePrefix requires at least one phrase term\."
        ):
            PhrasePrefix()


class TestRegexPhraseQuery:
    def test_regex_phrase_query(self) -> None:
        queryset = Product.objects.filter(
            description=ParadeDB(RegexPhrase("run.*", "sho.*"))
        )
        assert (
            str(queryset.query)
            == 'SELECT "tests_product"."id", "tests_product"."description", "tests_product"."category", "tests_product"."rating", "tests_product"."in_stock", "tests_product"."price", "tests_product"."created_at", "tests_product"."metadata" FROM "tests_product" WHERE "tests_product"."description" @@@ pdb.regex_phrase(ARRAY[\'run.*\', \'sho.*\'])'
        )
        _run_query(queryset)

    def test_regex_phrase_with_options(self) -> None:
        queryset = Product.objects.filter(
            description=ParadeDB(
                RegexPhrase("run.*", "sho.*", slop=2, max_expansions=100)
            )
        )
        assert (
            str(queryset.query)
            == 'SELECT "tests_product"."id", "tests_product"."description", "tests_product"."category", "tests_product"."rating", "tests_product"."in_stock", "tests_product"."price", "tests_product"."created_at", "tests_product"."metadata" FROM "tests_product" WHERE "tests_product"."description" @@@ pdb.regex_phrase(ARRAY[\'run.*\', \'sho.*\'], slop => 2, max_expansions => 100)'
        )
        _run_query(queryset)


class TestProximityAdvancedQuery:
    def test_proximity_regex_query(self) -> None:
        queryset = Product.objects.filter(
            description=ParadeDB(Proximity("running").within(1, ProxRegex("sho.*")))
        )
        assert (
            str(queryset.query)
            == 'SELECT "tests_product"."id", "tests_product"."description", "tests_product"."category", "tests_product"."rating", "tests_product"."in_stock", "tests_product"."price", "tests_product"."created_at", "tests_product"."metadata" FROM "tests_product" WHERE "tests_product"."description" @@@ (\'running\' ## 1 ## pdb.prox_regex(\'sho.*\'))'
        )
        _run_query(queryset)

    def test_proximity_array_query(self) -> None:
        queryset = Product.objects.filter(
            description=ParadeDB(Proximity(["sleek", "running"]).within(1, "shoes"))
        )
        assert (
            str(queryset.query)
            == 'SELECT "tests_product"."id", "tests_product"."description", "tests_product"."category", "tests_product"."rating", "tests_product"."in_stock", "tests_product"."price", "tests_product"."created_at", "tests_product"."metadata" FROM "tests_product" WHERE "tests_product"."description" @@@ (pdb.prox_array(\'sleek\', \'running\') ## 1 ## \'shoes\')'
        )
        _run_query(queryset)

    def test_proximity_array_with_prox_regex_items(self) -> None:
        queryset = Product.objects.filter(
            description=ParadeDB(
                Proximity(["chicken", ProxRegex("r..s")]).within(1, "delicious")
            )
        )
        assert (
            str(queryset.query)
            == 'SELECT "tests_product"."id", "tests_product"."description", "tests_product"."category", "tests_product"."rating", "tests_product"."in_stock", "tests_product"."price", "tests_product"."created_at", "tests_product"."metadata" FROM "tests_product" WHERE "tests_product"."description" @@@ (pdb.prox_array(\'chicken\', pdb.prox_regex(\'r..s\')) ## 1 ## \'delicious\')'
        )
        _run_query(queryset)

    def test_proximity_array_with_prox_regex_custom_expansions(self) -> None:
        queryset = Product.objects.filter(
            description=ParadeDB(
                Proximity([ProxRegex("sl.*", max_expansions=100), "white"]).within(
                    1,
                    "shoes",
                )
            )
        )
        assert (
            str(queryset.query)
            == 'SELECT "tests_product"."id", "tests_product"."description", "tests_product"."category", "tests_product"."rating", "tests_product"."in_stock", "tests_product"."price", "tests_product"."created_at", "tests_product"."metadata" FROM "tests_product" WHERE "tests_product"."description" @@@ (pdb.prox_array(pdb.prox_regex(\'sl.*\', 100), \'white\') ## 1 ## \'shoes\')'
        )
        _run_query(queryset)

    def test_proximity_array_regex_rhs_query(self) -> None:
        queryset = Product.objects.filter(
            description=ParadeDB(
                Proximity("running").within(
                    1,
                    ProxRegex("sho.*", max_expansions=80),
                )
            )
        )
        assert (
            str(queryset.query)
            == 'SELECT "tests_product"."id", "tests_product"."description", "tests_product"."category", "tests_product"."rating", "tests_product"."in_stock", "tests_product"."price", "tests_product"."created_at", "tests_product"."metadata" FROM "tests_product" WHERE "tests_product"."description" @@@ (\'running\' ## 1 ## pdb.prox_regex(\'sho.*\', 80))'
        )
        _run_query(queryset)

    def test_proximity_array_list_rhs_query(self) -> None:
        queryset = Product.objects.filter(
            description=ParadeDB(
                Proximity("running").within(
                    1,
                    ["shoes", ProxRegex("boot.*", max_expansions=80)],
                )
            )
        )
        assert (
            str(queryset.query)
            == 'SELECT "tests_product"."id", "tests_product"."description", "tests_product"."category", "tests_product"."rating", "tests_product"."in_stock", "tests_product"."price", "tests_product"."created_at", "tests_product"."metadata" FROM "tests_product" WHERE "tests_product"."description" @@@ (\'running\' ## 1 ## pdb.prox_array(\'shoes\', pdb.prox_regex(\'boot.*\', 80)))'
        )
        _run_query(queryset)

    def test_proximity_chain_mixed_ordering_query(self) -> None:
        queryset = Product.objects.filter(
            description=ParadeDB(
                Proximity(ProxRegex("sho.*"))
                .within(1, ["history", "science"], ordered=True)
                .within(5, "hardcover")
            )
        )
        assert (
            str(queryset.query)
            == 'SELECT "tests_product"."id", "tests_product"."description", "tests_product"."category", "tests_product"."rating", "tests_product"."in_stock", "tests_product"."price", "tests_product"."created_at", "tests_product"."metadata" FROM "tests_product" WHERE "tests_product"."description" @@@ (pdb.prox_regex(\'sho.*\') ##> 1 ##> pdb.prox_array(\'history\', \'science\') ## 5 ## \'hardcover\')'
        )
        _run_query(queryset)

    def test_proximity_nested_child_constructor_query(self) -> None:
        queryset = Product.objects.filter(
            description=ParadeDB(
                Proximity("running")
                .within(
                    2,
                    [ProxRegex("sho.*", max_expansions=80), "boots"],
                )
                .within(4, "hardcover", ordered=True)
            )
        )
        assert (
            str(queryset.query)
            == 'SELECT "tests_product"."id", "tests_product"."description", "tests_product"."category", "tests_product"."rating", "tests_product"."in_stock", "tests_product"."price", "tests_product"."created_at", "tests_product"."metadata" FROM "tests_product" WHERE "tests_product"."description" @@@ (\'running\' ## 2 ## pdb.prox_array(pdb.prox_regex(\'sho.*\', 80), \'boots\') ##> 4 ##> \'hardcover\')'
        )
        _run_query(queryset)

    def test_proximity_associativity_parenthesizes_grouped_rhs(self) -> None:
        queryset = Product.objects.filter(
            description=ParadeDB(
                Proximity("running").within(
                    2, Proximity("shoes").within(4, "hardcover", ordered=True)
                )
            )
        )
        assert (
            str(queryset.query)
            == 'SELECT "tests_product"."id", "tests_product"."description", "tests_product"."category", "tests_product"."rating", "tests_product"."in_stock", "tests_product"."price", "tests_product"."created_at", "tests_product"."metadata" FROM "tests_product" WHERE "tests_product"."description" @@@ (\'running\' ## 2 ## (\'shoes\' ##> 4 ##> \'hardcover\'))'
        )
        _run_query(queryset)

    def test_deeply_nested_query(self) -> None:
        queryset = Product.objects.filter(
            description=ParadeDB(
                Boost(
                    Proximity("running")
                    .within(
                        2,
                        Proximity(
                            [
                                "shoes",
                                ProxRegex("sho.*"),
                                ["shoe", [ProxRegex("shoe")]],
                            ]
                        )
                        .within(4, "hardcover", ordered=True)
                        .within(100, "foo"),
                    )
                    .within(2, ProxRegex("bar")),
                    1.2,
                )
            )
        )
        assert (
            str(queryset.query)
            == 'SELECT "tests_product"."id", "tests_product"."description", "tests_product"."category", "tests_product"."rating", "tests_product"."in_stock", "tests_product"."price", "tests_product"."created_at", "tests_product"."metadata" FROM "tests_product" WHERE "tests_product"."description" @@@ (\'running\' ## 2 ## (pdb.prox_array(\'shoes\', pdb.prox_regex(\'sho.*\'), pdb.prox_array(\'shoe\', pdb.prox_array(pdb.prox_regex(\'shoe\')))) ##> 4 ##> \'hardcover\' ## 100 ## \'foo\') ## 2 ## pdb.prox_regex(\'bar\'))::pdb.boost(1.2)'
        )
        _run_query(queryset)


class TestRangeTermQuery:
    def test_range_term_scalar_query(self) -> None:
        queryset = Product.objects.filter(description=ParadeDB(RangeTerm(10)))
        assert (
            str(queryset.query)
            == 'SELECT "tests_product"."id", "tests_product"."description", "tests_product"."category", "tests_product"."rating", "tests_product"."in_stock", "tests_product"."price", "tests_product"."created_at", "tests_product"."metadata" FROM "tests_product" WHERE "tests_product"."description" @@@ pdb.range_term(10)'
        )
        _run_query(queryset)

    def test_range_term_relation_query(self) -> None:
        queryset = Product.objects.filter(
            description=ParadeDB(
                RangeTerm("(10, 12]", relation="Intersects", range_type="int4range")
            )
        )
        assert (
            str(queryset.query)
            == 'SELECT "tests_product"."id", "tests_product"."description", "tests_product"."category", "tests_product"."rating", "tests_product"."in_stock", "tests_product"."price", "tests_product"."created_at", "tests_product"."metadata" FROM "tests_product" WHERE "tests_product"."description" @@@ pdb.range_term(\'(10, 12]\'::int4range, \'Intersects\')'
        )
        _run_query(queryset)

    def test_range_term_relation_requires_range_type(self) -> None:
        with pytest.raises(ValueError, match=r"RangeTerm relation requires range_type"):
            RangeTerm("(10, 12]", relation="Intersects")

    def test_range_term_range_type_requires_relation(self) -> None:
        with pytest.raises(
            ValueError, match=r"RangeTerm range_type is only valid when relation"
        ):
            RangeTerm("(10, 12]", range_type="int4range")

    def test_range_term_invalid_range_type(self) -> None:
        with pytest.raises(ValueError, match=r"Range type must be one of"):
            RangeTerm("(10, 12]", relation="Intersects", range_type="badtype")


class TestTermQuery:
    """Test Term query SQL generation."""

    def test_term_query(self) -> None:
        """Term generates: WHERE description @@@ pdb.term('shoes')."""
        queryset = Product.objects.filter(description=ParadeDB(Term("shoes")))
        assert (
            str(queryset.query)
            == 'SELECT "tests_product"."id", "tests_product"."description", "tests_product"."category", "tests_product"."rating", "tests_product"."in_stock", "tests_product"."price", "tests_product"."created_at", "tests_product"."metadata" FROM "tests_product" WHERE "tests_product"."description" @@@ pdb.term(\'shoes\')'
        )
        _run_query(queryset)


class TestRegexQuery:
    """Test Regex query SQL generation."""

    def test_regex_query(self) -> None:
        """Regex generates: WHERE description @@@ pdb.regex('run.*shoes')."""
        queryset = Product.objects.filter(description=ParadeDB(Regex("run.*shoes")))
        assert (
            str(queryset.query)
            == 'SELECT "tests_product"."id", "tests_product"."description", "tests_product"."category", "tests_product"."rating", "tests_product"."in_stock", "tests_product"."price", "tests_product"."created_at", "tests_product"."metadata" FROM "tests_product" WHERE "tests_product"."description" @@@ pdb.regex(\'run.*shoes\')'
        )
        _run_query(queryset)


class TestExistsQuery:
    def test_exists_basic(self) -> None:
        queryset = Product.objects.filter(description=ParadeDB(Exists()))
        assert (
            str(queryset.query)
            == 'SELECT "tests_product"."id", "tests_product"."description", "tests_product"."category", "tests_product"."rating", "tests_product"."in_stock", "tests_product"."price", "tests_product"."created_at", "tests_product"."metadata" FROM "tests_product" WHERE "tests_product"."description" @@@ pdb.exists()'
        )
        _run_query(queryset)


class TestFuzzyTermQuery:
    """Test FuzzyTerm query SQL generation."""

    def test_fuzzy_term_with_value(self) -> None:
        queryset = Product.objects.filter(description=ParadeDB(FuzzyTerm("shoes")))
        assert (
            str(queryset.query)
            == 'SELECT "tests_product"."id", "tests_product"."description", "tests_product"."category", "tests_product"."rating", "tests_product"."in_stock", "tests_product"."price", "tests_product"."created_at", "tests_product"."metadata" FROM "tests_product" WHERE "tests_product"."description" @@@ pdb.fuzzy_term(\'shoes\')'
        )
        _run_query(queryset)

    def test_fuzzy_term_with_distance(self) -> None:
        queryset = Product.objects.filter(
            description=ParadeDB(Fuzzy(FuzzyTerm("shoes"), 2))
        )
        assert (
            str(queryset.query)
            == 'SELECT "tests_product"."id", "tests_product"."description", "tests_product"."category", "tests_product"."rating", "tests_product"."in_stock", "tests_product"."price", "tests_product"."created_at", "tests_product"."metadata" FROM "tests_product" WHERE "tests_product"."description" @@@ pdb.fuzzy_term(\'shoes\')::pdb.fuzzy(2)'
        )
        _run_query(queryset)


class TestTermSetQuery:
    """Test TermSet query SQL generation."""

    def test_term_set_strings(self) -> None:
        queryset = Product.objects.filter(
            description=ParadeDB(TermSet("shoes", "boots"))
        )
        assert (
            str(queryset.query)
            == 'SELECT "tests_product"."id", "tests_product"."description", "tests_product"."category", "tests_product"."rating", "tests_product"."in_stock", "tests_product"."price", "tests_product"."created_at", "tests_product"."metadata" FROM "tests_product" WHERE "tests_product"."description" @@@ pdb.term_set(ARRAY[\'shoes\', \'boots\']::text[])'
        )
        _run_query(queryset)

    def test_term_set_integers(self) -> None:
        queryset = Product.objects.filter(rating=ParadeDB(TermSet(1, 2, 3)))
        assert (
            str(queryset.query)
            == 'SELECT "tests_product"."id", "tests_product"."description", "tests_product"."category", "tests_product"."rating", "tests_product"."in_stock", "tests_product"."price", "tests_product"."created_at", "tests_product"."metadata" FROM "tests_product" WHERE "tests_product"."rating" @@@ pdb.term_set(ARRAY[1, 2, 3]::bigint[])'
        )
        _run_query(queryset)

    def test_term_set_booleans(self) -> None:
        queryset = Product.objects.filter(in_stock=ParadeDB(TermSet(True, False)))
        assert (
            str(queryset.query)
            == 'SELECT "tests_product"."id", "tests_product"."description", "tests_product"."category", "tests_product"."rating", "tests_product"."in_stock", "tests_product"."price", "tests_product"."created_at", "tests_product"."metadata" FROM "tests_product" WHERE "tests_product"."in_stock" @@@ pdb.term_set(ARRAY[true, false]::boolean[])'
        )
        _run_query(queryset)


class TestScoreAnnotation:
    """Test Score annotation SQL generation."""

    def test_score_annotation(self) -> None:
        """Score generates: SELECT ..., pdb.score(id) AS search_score."""
        queryset = Product.objects.filter(
            description=ParadeDB(MatchAll("running", "shoes"))
        ).annotate(search_score=Score())
        assert (
            str(queryset.query)
            == 'SELECT "tests_product"."id", "tests_product"."description", "tests_product"."category", "tests_product"."rating", "tests_product"."in_stock", "tests_product"."price", "tests_product"."created_at", "tests_product"."metadata", pdb.score("tests_product"."id") AS "search_score" FROM "tests_product" WHERE "tests_product"."description" &&& ARRAY[\'running\', \'shoes\']'
        )
        _run_query(queryset)

    def test_score_with_ordering(self) -> None:
        """Score with ORDER BY search_score DESC."""
        queryset = (
            Product.objects.filter(description=ParadeDB(MatchAll("shoes")))
            .annotate(search_score=Score())
            .order_by("-search_score")
        )
        assert (
            str(queryset.query)
            == 'SELECT "tests_product"."id", "tests_product"."description", "tests_product"."category", "tests_product"."rating", "tests_product"."in_stock", "tests_product"."price", "tests_product"."created_at", "tests_product"."metadata", pdb.score("tests_product"."id") AS "search_score" FROM "tests_product" WHERE "tests_product"."description" &&& \'shoes\' ORDER BY 9 DESC'
        )
        _run_query(queryset)

    def test_score_filter(self) -> None:
        """Filter by score: WHERE pdb.score(id) > 0."""
        queryset = (
            Product.objects.filter(description=ParadeDB(MatchAll("shoes")))
            .annotate(search_score=Score())
            .filter(search_score__gt=0)
        )
        assert (
            str(queryset.query)
            == 'SELECT "tests_product"."id", "tests_product"."description", "tests_product"."category", "tests_product"."rating", "tests_product"."in_stock", "tests_product"."price", "tests_product"."created_at", "tests_product"."metadata", pdb.score("tests_product"."id") AS "search_score" FROM "tests_product" WHERE ("tests_product"."description" &&& \'shoes\' AND pdb.score("tests_product"."id") > 0.0)'
        )
        _run_query(queryset)

    @pytest.mark.usefixtures("mock_items")
    def test_score_filter_range(self) -> None:
        queryset = (
            MockItem.objects.filter(description=ParadeDB(MatchAll("shoes")))
            .annotate(search_score=Score())
            .filter(search_score__gte=0.1, search_score__lte=100)
        )
        assert (
            str(queryset.query)
            == 'SELECT "mock_items"."id", "mock_items"."description", "mock_items"."category", "mock_items"."rating", "mock_items"."in_stock", "mock_items"."created_at", "mock_items"."metadata", pdb.score("mock_items"."id") AS "search_score" FROM "mock_items" WHERE ("mock_items"."description" &&& \'shoes\' AND pdb.score("mock_items"."id") >= 0.1 AND pdb.score("mock_items"."id") <= 100.0)'
        )
        _run_query(queryset)

    @pytest.mark.usefixtures("mock_items")
    def test_score_with_coalesce(self) -> None:
        queryset = MockItem.objects.filter(
            description=ParadeDB(MatchAll("shoes"))
        ).annotate(
            search_score=Score(),
            safe_score=Coalesce(Score(), 0.0),
        )
        assert (
            str(queryset.query)
            == 'SELECT "mock_items"."id", "mock_items"."description", "mock_items"."category", "mock_items"."rating", "mock_items"."in_stock", "mock_items"."created_at", "mock_items"."metadata", pdb.score("mock_items"."id") AS "search_score", COALESCE(pdb.score("mock_items"."id"), 0.0) AS "safe_score" FROM "mock_items" WHERE "mock_items"."description" &&& \'shoes\''
        )
        _run_query(queryset)


class TestComplexQComposition:
    """Test complex Q object boolean composition SQL generation."""

    pytestmark = pytest.mark.usefixtures("mock_items")

    def test_triple_or_paradedb(self) -> None:
        queryset = MockItem.objects.filter(
            Q(description=ParadeDB(MatchAll("shoes")))
            | Q(description=ParadeDB(MatchAll("keyboard")))
            | Q(description=ParadeDB(MatchAll("earbuds")))
        )
        assert (
            str(queryset.query)
            == 'SELECT "mock_items"."id", "mock_items"."description", "mock_items"."category", "mock_items"."rating", "mock_items"."in_stock", "mock_items"."created_at", "mock_items"."metadata" FROM "mock_items" WHERE ("mock_items"."description" &&& \'shoes\' OR "mock_items"."description" &&& \'keyboard\' OR "mock_items"."description" &&& \'earbuds\')'
        )
        _run_query(queryset)

    def test_deeply_nested_q(self) -> None:
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
        assert (
            str(queryset.query)
            == 'SELECT "mock_items"."id", "mock_items"."description", "mock_items"."category", "mock_items"."rating", "mock_items"."in_stock", "mock_items"."created_at", "mock_items"."metadata" FROM "mock_items" WHERE ((("mock_items"."description" &&& \'shoes\' OR "mock_items"."description" &&& \'boots\') AND "mock_items"."rating" >= 3) OR "mock_items"."category" = Electronics)'
        )
        _run_query(queryset)

    def test_q_not_with_or(self) -> None:
        queryset = MockItem.objects.filter(
            (
                Q(description=ParadeDB(MatchAll("shoes")))
                | Q(description=ParadeDB(MatchAll("boots")))
            )
            & ~Q(description=ParadeDB(MatchAll("running")))
        )
        assert (
            str(queryset.query)
            == 'SELECT "mock_items"."id", "mock_items"."description", "mock_items"."category", "mock_items"."rating", "mock_items"."in_stock", "mock_items"."created_at", "mock_items"."metadata" FROM "mock_items" WHERE (("mock_items"."description" &&& \'shoes\' OR "mock_items"."description" &&& \'boots\') AND NOT ("mock_items"."description" &&& \'running\'))'
        )
        _run_query(queryset)


class TestMultipleSearchTypes:
    """Test combining different ParadeDB search types SQL generation."""

    pytestmark = pytest.mark.usefixtures("mock_items")

    def test_phrase_with_term_in_q(self) -> None:
        queryset = MockItem.objects.filter(
            Q(description=ParadeDB(Phrase("running shoes")))
            | Q(description=ParadeDB(Term("keyboard")))
        )
        assert (
            str(queryset.query)
            == 'SELECT "mock_items"."id", "mock_items"."description", "mock_items"."category", "mock_items"."rating", "mock_items"."in_stock", "mock_items"."created_at", "mock_items"."metadata" FROM "mock_items" WHERE ("mock_items"."description" ### \'running shoes\' OR "mock_items"."description" @@@ pdb.term(\'keyboard\'))'
        )
        _run_query(queryset)

    def test_chained_filters_all_paradedb(self) -> None:
        queryset = MockItem.objects.filter(
            description=ParadeDB(MatchAll("shoes"))
        ).filter(description=ParadeDB(MatchAll("running")))
        assert (
            str(queryset.query)
            == 'SELECT "mock_items"."id", "mock_items"."description", "mock_items"."category", "mock_items"."rating", "mock_items"."in_stock", "mock_items"."created_at", "mock_items"."metadata" FROM "mock_items" WHERE ("mock_items"."description" &&& \'shoes\' AND "mock_items"."description" &&& \'running\')'
        )
        _run_query(queryset)


class TestSnippetAnnotation:
    """Test Snippet annotation SQL generation."""

    def test_snippet_annotation(self) -> None:
        """Snippet generates: pdb.snippet(description) AS snippet."""
        queryset = Product.objects.filter(
            description=ParadeDB(MatchAny("wireless", "bluetooth"))
        ).annotate(snippet=Snippet("description"))
        assert (
            str(queryset.query)
            == 'SELECT "tests_product"."id", "tests_product"."description", "tests_product"."category", "tests_product"."rating", "tests_product"."in_stock", "tests_product"."price", "tests_product"."created_at", "tests_product"."metadata", pdb.snippet("tests_product"."description") AS "snippet" FROM "tests_product" WHERE "tests_product"."description" ||| ARRAY[\'wireless\', \'bluetooth\']'
        )
        _run_query(queryset)

    def test_snippet_with_custom_formatting(self) -> None:
        """Custom snippet: pdb.snippet(description, '<mark>', '</mark>', 100)."""
        queryset = Product.objects.filter(
            description=ParadeDB(MatchAll("shoes"))
        ).annotate(
            snippet=Snippet(
                "description",
                start_sel="<mark>",
                stop_sel="</mark>",
                max_num_chars=100,
            )
        )
        assert (
            str(queryset.query)
            == 'SELECT "tests_product"."id", "tests_product"."description", "tests_product"."category", "tests_product"."rating", "tests_product"."in_stock", "tests_product"."price", "tests_product"."created_at", "tests_product"."metadata", pdb.snippet("tests_product"."description", \'<mark>\', \'</mark>\', 100) AS "snippet" FROM "tests_product" WHERE "tests_product"."description" &&& \'shoes\''
        )
        _run_query(queryset)


class TestSnippetsAnnotation:
    """Test Snippets annotation SQL generation."""

    def test_snippets_basic(self) -> None:
        """Snippets generates: pdb.snippets(description) AS snippets."""
        queryset = Product.objects.filter(
            description=ParadeDB(MatchAll("artistic", "vase"))
        ).annotate(snippets=Snippets("description"))
        assert (
            str(queryset.query)
            == 'SELECT "tests_product"."id", "tests_product"."description", "tests_product"."category", "tests_product"."rating", "tests_product"."in_stock", "tests_product"."price", "tests_product"."created_at", "tests_product"."metadata", pdb.snippets("tests_product"."description") AS "snippets" FROM "tests_product" WHERE "tests_product"."description" &&& ARRAY[\'artistic\', \'vase\']'
        )
        _run_query(queryset)

    def test_snippets_with_max_num_chars(self) -> None:
        """Snippets with max_num_chars only."""
        queryset = Product.objects.filter(
            description=ParadeDB(MatchAll("artistic", "vase"))
        ).annotate(snippets=Snippets("description", max_num_chars=15))
        assert (
            str(queryset.query)
            == 'SELECT "tests_product"."id", "tests_product"."description", "tests_product"."category", "tests_product"."rating", "tests_product"."in_stock", "tests_product"."price", "tests_product"."created_at", "tests_product"."metadata", pdb.snippets("tests_product"."description", max_num_chars => 15) AS "snippets" FROM "tests_product" WHERE "tests_product"."description" &&& ARRAY[\'artistic\', \'vase\']'
        )
        _run_query(queryset)

    def test_snippets_with_limit_and_offset(self) -> None:
        """Snippets with limit and offset uses double-quoted SQL reserved words."""
        queryset = Product.objects.filter(
            description=ParadeDB(MatchAll("running"))
        ).annotate(
            snippets=Snippets("description", max_num_chars=15, limit=1, offset=1)
        )
        assert (
            str(queryset.query)
            == 'SELECT "tests_product"."id", "tests_product"."description", "tests_product"."category", "tests_product"."rating", "tests_product"."in_stock", "tests_product"."price", "tests_product"."created_at", "tests_product"."metadata", pdb.snippets("tests_product"."description", max_num_chars => 15, "limit" => 1, "offset" => 1) AS "snippets" FROM "tests_product" WHERE "tests_product"."description" &&& \'running\''
        )
        _run_query(queryset)

    def test_snippets_with_sort_by(self) -> None:
        """Snippets with sort_by parameter."""
        queryset = Product.objects.filter(
            description=ParadeDB(MatchAll("artistic", "vase"))
        ).annotate(
            snippets=Snippets("description", max_num_chars=15, sort_by="position")
        )
        assert (
            str(queryset.query)
            == 'SELECT "tests_product"."id", "tests_product"."description", "tests_product"."category", "tests_product"."rating", "tests_product"."in_stock", "tests_product"."price", "tests_product"."created_at", "tests_product"."metadata", pdb.snippets("tests_product"."description", max_num_chars => 15, sort_by => \'position\') AS "snippets" FROM "tests_product" WHERE "tests_product"."description" &&& ARRAY[\'artistic\', \'vase\']'
        )
        _run_query(queryset)

    def test_snippets_with_all_params(self) -> None:
        """Snippets with all named parameters."""
        queryset = Product.objects.filter(
            description=ParadeDB(MatchAll("shoes"))
        ).annotate(
            snippets=Snippets(
                "description",
                start_tag="<mark>",
                end_tag="</mark>",
                max_num_chars=30,
                limit=2,
                offset=0,
                sort_by="score",
            )
        )
        assert (
            str(queryset.query)
            == 'SELECT "tests_product"."id", "tests_product"."description", "tests_product"."category", "tests_product"."rating", "tests_product"."in_stock", "tests_product"."price", "tests_product"."created_at", "tests_product"."metadata", pdb.snippets("tests_product"."description", start_tag => \'<mark>\', end_tag => \'</mark>\', max_num_chars => 30, "limit" => 2, "offset" => 0, sort_by => \'score\') AS "snippets" FROM "tests_product" WHERE "tests_product"."description" &&& \'shoes\''
        )
        _run_query(queryset)


class TestSnippetPositionsAnnotation:
    """Test SnippetPositions annotation SQL generation."""

    def test_snippet_positions_basic(self) -> None:
        """SnippetPositions generates: pdb.snippet_positions(description)."""
        queryset = Product.objects.filter(
            description=ParadeDB(MatchAll("shoes"))
        ).annotate(positions=SnippetPositions("description"))
        assert (
            str(queryset.query)
            == 'SELECT "tests_product"."id", "tests_product"."description", "tests_product"."category", "tests_product"."rating", "tests_product"."in_stock", "tests_product"."price", "tests_product"."created_at", "tests_product"."metadata", pdb.snippet_positions("tests_product"."description") AS "positions" FROM "tests_product" WHERE "tests_product"."description" &&& \'shoes\''
        )
        _run_query(queryset)

    def test_snippet_positions_with_snippet(self) -> None:
        """SnippetPositions alongside Snippet."""
        queryset = Product.objects.filter(
            description=ParadeDB(MatchAll("shoes"))
        ).annotate(
            snippet=Snippet("description"),
            positions=SnippetPositions("description"),
        )
        assert (
            str(queryset.query)
            == 'SELECT "tests_product"."id", "tests_product"."description", "tests_product"."category", "tests_product"."rating", "tests_product"."in_stock", "tests_product"."price", "tests_product"."created_at", "tests_product"."metadata", pdb.snippet("tests_product"."description") AS "snippet", pdb.snippet_positions("tests_product"."description") AS "positions" FROM "tests_product" WHERE "tests_product"."description" &&& \'shoes\''
        )
        _run_query(queryset)
