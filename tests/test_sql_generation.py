"""Tests for SQL generation.

This module tests SQL string generation only - no database required.
"""

import json
from unittest.mock import Mock

import pytest
from django.db.backends.base.schema import BaseDatabaseSchemaEditor
from django.db.models import F, Q, TextField, Value, Window
from django.db.models.functions import Concat, RowNumber

from paradedb.functions import Agg, Score, Snippet, SnippetPositions, Snippets
from paradedb.indexes import BM25Index
from paradedb.search import (
    PQ,
    Fuzzy,
    MoreLikeThis,
    ParadeDB,
    Parse,
    Phrase,
    PhrasePrefix,
    Proximity,
    ProximityArray,
    ProximityRegex,
    RangeTerm,
    Regex,
    RegexPhrase,
    Term,
)
from tests.models import Product


class DummySchemaEditor(BaseDatabaseSchemaEditor):
    """Minimal schema editor for SQL string generation."""

    def __init__(self) -> None:
        connection = Mock()
        connection.features.uses_case_insensitive_names = False
        super().__init__(connection, collect_sql=False)

    def quote_name(self, name: str) -> str:
        return f'"{name}"'


class TestParadeDBLookup:
    """Test ParadeDB lookup SQL generation."""

    def test_single_term_search(self) -> None:
        """Single term generates: WHERE description &&& 'shoes'."""
        queryset = Product.objects.filter(description=ParadeDB("shoes"))
        assert (
            str(queryset.query)
            == 'SELECT "tests_product"."id", "tests_product"."description", "tests_product"."category", "tests_product"."rating", "tests_product"."in_stock", "tests_product"."price", "tests_product"."created_at", "tests_product"."metadata" FROM "tests_product" WHERE "tests_product"."description" &&& \'shoes\''
        )

    def test_and_search_multiple_terms(self) -> None:
        """Multiple terms generate: WHERE description &&& ARRAY[...]"""
        queryset = Product.objects.filter(description=ParadeDB("running", "shoes"))
        assert (
            str(queryset.query)
            == 'SELECT "tests_product"."id", "tests_product"."description", "tests_product"."category", "tests_product"."rating", "tests_product"."in_stock", "tests_product"."price", "tests_product"."created_at", "tests_product"."metadata" FROM "tests_product" WHERE "tests_product"."description" &&& ARRAY[\'running\', \'shoes\']'
        )

    def test_and_search_three_terms(self) -> None:
        """Edge case: three terms for AND search."""
        queryset = Product.objects.filter(
            description=ParadeDB("running", "shoes", "lightweight")
        )
        assert (
            str(queryset.query)
            == 'SELECT "tests_product"."id", "tests_product"."description", "tests_product"."category", "tests_product"."rating", "tests_product"."in_stock", "tests_product"."price", "tests_product"."created_at", "tests_product"."metadata" FROM "tests_product" WHERE "tests_product"."description" &&& ARRAY[\'running\', \'shoes\', \'lightweight\']'
        )


class TestExactLiteralDisjunction:
    """Test exact literal OR search SQL generation."""

    def test_single_string_or(self) -> None:
        queryset = Product.objects.filter(
            description=ParadeDB("running shoes", operator="OR")
        )
        assert (
            str(queryset.query)
            == 'SELECT "tests_product"."id", "tests_product"."description", "tests_product"."category", "tests_product"."rating", "tests_product"."in_stock", "tests_product"."price", "tests_product"."created_at", "tests_product"."metadata" FROM "tests_product" WHERE "tests_product"."description" ||| \'running shoes\''
        )

    def test_multiple_strings_or(self) -> None:
        queryset = Product.objects.filter(
            description=ParadeDB("shoes", "boots", operator="OR")
        )
        assert (
            str(queryset.query)
            == 'SELECT "tests_product"."id", "tests_product"."description", "tests_product"."category", "tests_product"."rating", "tests_product"."in_stock", "tests_product"."price", "tests_product"."created_at", "tests_product"."metadata" FROM "tests_product" WHERE "tests_product"."description" ||| ARRAY[\'shoes\', \'boots\']'
        )

    def test_term_operator_single(self) -> None:
        queryset = Product.objects.filter(
            description=ParadeDB("shoes", operator="TERM")
        )
        assert (
            str(queryset.query)
            == 'SELECT "tests_product"."id", "tests_product"."description", "tests_product"."category", "tests_product"."rating", "tests_product"."in_stock", "tests_product"."price", "tests_product"."created_at", "tests_product"."metadata" FROM "tests_product" WHERE "tests_product"."description" === \'shoes\''
        )

    def test_term_operator_multiple(self) -> None:
        queryset = Product.objects.filter(
            description=ParadeDB("shoes", "boots", operator="TERM")
        )
        assert (
            str(queryset.query)
            == 'SELECT "tests_product"."id", "tests_product"."description", "tests_product"."category", "tests_product"."rating", "tests_product"."in_stock", "tests_product"."price", "tests_product"."created_at", "tests_product"."metadata" FROM "tests_product" WHERE "tests_product"."description" === ARRAY[\'shoes\', \'boots\']'
        )

    def test_default_operator_unchanged(self) -> None:
        queryset = Product.objects.filter(description=ParadeDB("a", "b"))
        assert (
            str(queryset.query)
            == 'SELECT "tests_product"."id", "tests_product"."description", "tests_product"."category", "tests_product"."rating", "tests_product"."in_stock", "tests_product"."price", "tests_product"."created_at", "tests_product"."metadata" FROM "tests_product" WHERE "tests_product"."description" &&& ARRAY[\'a\', \'b\']'
        )

    def test_operator_invalid_with_pq(self) -> None:
        with pytest.raises(
            ValueError,
            match=r"ParadeDB operator is only supported with plain string terms\.",
        ):
            _ = ParadeDB(PQ("a") | PQ("b"), operator="OR")

    def test_operator_invalid_with_phrase(self) -> None:
        with pytest.raises(
            ValueError,
            match=r"ParadeDB operator is only supported with plain string terms\.",
        ):
            _ = ParadeDB(Phrase("text"), operator="OR")

    def test_operator_invalid_value(self) -> None:
        with pytest.raises(
            ValueError,
            match=r"ParadeDB operator must be 'AND', 'OR', or 'TERM'\.",
        ):
            _ = ParadeDB("shoes", operator="BAD")


class TestPQObject:
    """Test PQ SQL generation."""

    def test_or_search(self) -> None:
        """PQ OR generates: WHERE description ||| ARRAY[...]"""
        queryset = Product.objects.filter(
            description=ParadeDB(PQ("wireless") | PQ("bluetooth"))
        )
        assert (
            str(queryset.query)
            == 'SELECT "tests_product"."id", "tests_product"."description", "tests_product"."category", "tests_product"."rating", "tests_product"."in_stock", "tests_product"."price", "tests_product"."created_at", "tests_product"."metadata" FROM "tests_product" WHERE "tests_product"."description" ||| ARRAY[\'wireless\', \'bluetooth\']'
        )

    def test_or_search_chained(self) -> None:
        """Edge case: chaining multiple OR terms."""
        queryset = Product.objects.filter(
            description=ParadeDB(PQ("wireless") | PQ("bluetooth") | PQ("speaker"))
        )
        assert (
            str(queryset.query)
            == 'SELECT "tests_product"."id", "tests_product"."description", "tests_product"."category", "tests_product"."rating", "tests_product"."in_stock", "tests_product"."price", "tests_product"."created_at", "tests_product"."metadata" FROM "tests_product" WHERE "tests_product"."description" ||| ARRAY[\'wireless\', \'bluetooth\', \'speaker\']'
        )

    def test_pq_and_search(self) -> None:
        """PQ AND generates: WHERE description &&& ARRAY[...]"""
        queryset = Product.objects.filter(
            description=ParadeDB(PQ("shoes") & PQ("sandals"))
        )
        assert (
            str(queryset.query)
            == 'SELECT "tests_product"."id", "tests_product"."description", "tests_product"."category", "tests_product"."rating", "tests_product"."in_stock", "tests_product"."price", "tests_product"."created_at", "tests_product"."metadata" FROM "tests_product" WHERE "tests_product"."description" &&& ARRAY[\'shoes\', \'sandals\']'
        )


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

    def test_phrase_with_slop(self) -> None:
        """Phrase with slop: WHERE description ### 'running shoes'::pdb.slop(1)."""
        queryset = Product.objects.filter(
            description=ParadeDB(Phrase("running shoes", slop=1))
        )
        assert (
            str(queryset.query)
            == 'SELECT "tests_product"."id", "tests_product"."description", "tests_product"."category", "tests_product"."rating", "tests_product"."in_stock", "tests_product"."price", "tests_product"."created_at", "tests_product"."metadata" FROM "tests_product" WHERE "tests_product"."description" ### \'running shoes\'::pdb.slop(1)'
        )


class TestProximitySearch:
    """Test Proximity search SQL generation."""

    def test_proximity_unordered(self) -> None:
        queryset = Product.objects.filter(
            description=ParadeDB(Proximity("running shoes", distance=2))
        )
        assert (
            str(queryset.query)
            == 'SELECT "tests_product"."id", "tests_product"."description", "tests_product"."category", "tests_product"."rating", "tests_product"."in_stock", "tests_product"."price", "tests_product"."created_at", "tests_product"."metadata" FROM "tests_product" WHERE "tests_product"."description" @@@ pdb.proximity(\'running\' ## 2 ## \'shoes\')'
        )

    def test_proximity_ordered(self) -> None:
        queryset = Product.objects.filter(
            description=ParadeDB(Proximity("running shoes", distance=2, ordered=True))
        )
        assert (
            str(queryset.query)
            == 'SELECT "tests_product"."id", "tests_product"."description", "tests_product"."category", "tests_product"."rating", "tests_product"."in_stock", "tests_product"."price", "tests_product"."created_at", "tests_product"."metadata" FROM "tests_product" WHERE "tests_product"."description" @@@ pdb.proximity(\'running\' ##> 2 ##> \'shoes\')'
        )

    def test_proximity_with_boost(self) -> None:
        queryset = Product.objects.filter(
            description=ParadeDB(Proximity("running shoes", distance=2, boost=1.5))
        )
        assert (
            str(queryset.query)
            == 'SELECT "tests_product"."id", "tests_product"."description", "tests_product"."category", "tests_product"."rating", "tests_product"."in_stock", "tests_product"."price", "tests_product"."created_at", "tests_product"."metadata" FROM "tests_product" WHERE "tests_product"."description" @@@ pdb.proximity(\'running\' ## 2 ## \'shoes\')::pdb.boost(1.5)'
        )

    def test_proximity_with_const(self) -> None:
        queryset = Product.objects.filter(
            description=ParadeDB(Proximity("running shoes", distance=2, const=1.0))
        )
        assert (
            str(queryset.query)
            == 'SELECT "tests_product"."id", "tests_product"."description", "tests_product"."category", "tests_product"."rating", "tests_product"."in_stock", "tests_product"."price", "tests_product"."created_at", "tests_product"."metadata" FROM "tests_product" WHERE "tests_product"."description" @@@ pdb.proximity(\'running\' ## 2 ## \'shoes\')::pdb.const(1.0)'
        )

    def test_proximity_three_terms_chain(self) -> None:
        queryset = Product.objects.filter(
            description=ParadeDB(Proximity("running shoes lightweight", distance=2))
        )
        assert (
            str(queryset.query)
            == 'SELECT "tests_product"."id", "tests_product"."description", "tests_product"."category", "tests_product"."rating", "tests_product"."in_stock", "tests_product"."price", "tests_product"."created_at", "tests_product"."metadata" FROM "tests_product" WHERE "tests_product"."description" @@@ pdb.proximity(\'running\' ## 2 ## \'shoes\' ## 2 ## \'lightweight\')'
        )

    def test_multiple_proximity_terms_rejected(self) -> None:
        queryset = Product.objects.filter(
            description=ParadeDB(
                Proximity("running shoes", distance=2),
                Proximity("lightweight design", distance=2),
            )
        )
        with pytest.raises(
            ValueError,
            match=r"Proximity queries must be a single term\. Proximity arrays are not supported yet\.",
        ):
            _ = str(queryset.query)

    def test_proximity_distance_negative_rejected(self) -> None:
        with pytest.raises(
            ValueError, match=r"Proximity distance must be zero or positive\."
        ):
            Proximity("running shoes", distance=-1)

    def test_proximity_single_word_rejected(self) -> None:
        queryset = Product.objects.filter(
            description=ParadeDB(Proximity("running", distance=2))
        )
        with pytest.raises(
            ValueError,
            match=r"Proximity text must include at least two whitespace-separated terms\.",
        ):
            _ = str(queryset.query)


class TestFuzzySearch:
    """Test Fuzzy search SQL generation."""

    def test_fuzzy_search(self) -> None:
        """Fuzzy generates: WHERE description ||| 'sheos'::pdb.fuzzy(1)."""
        queryset = Product.objects.filter(
            description=ParadeDB(Fuzzy("sheos", distance=1))
        )
        assert (
            str(queryset.query)
            == 'SELECT "tests_product"."id", "tests_product"."description", "tests_product"."category", "tests_product"."rating", "tests_product"."in_stock", "tests_product"."price", "tests_product"."created_at", "tests_product"."metadata" FROM "tests_product" WHERE "tests_product"."description" ||| \'sheos\'::pdb.fuzzy(1)'
        )

    def test_multiple_fuzzy_terms(self) -> None:
        """Multiple fuzzy terms generate OR array."""
        queryset = Product.objects.filter(
            description=ParadeDB(
                Fuzzy("runnning", distance=1), Fuzzy("shoez", distance=1)
            )
        )
        assert (
            str(queryset.query)
            == 'SELECT "tests_product"."id", "tests_product"."description", "tests_product"."category", "tests_product"."rating", "tests_product"."in_stock", "tests_product"."price", "tests_product"."created_at", "tests_product"."metadata" FROM "tests_product" WHERE "tests_product"."description" ||| ARRAY[\'runnning\'::pdb.fuzzy(1), \'shoez\'::pdb.fuzzy(1)]'
        )


class TestFuzzyConjunction:
    def test_fuzzy_conjunction_single(self) -> None:
        queryset = Product.objects.filter(
            description=ParadeDB(Fuzzy("runing shose", distance=2, operator="AND"))
        )
        assert (
            str(queryset.query)
            == 'SELECT "tests_product"."id", "tests_product"."description", "tests_product"."category", "tests_product"."rating", "tests_product"."in_stock", "tests_product"."price", "tests_product"."created_at", "tests_product"."metadata" FROM "tests_product" WHERE "tests_product"."description" &&& \'runing shose\'::pdb.fuzzy(2)'
        )

    def test_fuzzy_conjunction_multiple(self) -> None:
        queryset = Product.objects.filter(
            description=ParadeDB(
                Fuzzy("term1", distance=1, operator="AND"),
                Fuzzy("term2", distance=1, operator="AND"),
            )
        )
        assert (
            str(queryset.query)
            == 'SELECT "tests_product"."id", "tests_product"."description", "tests_product"."category", "tests_product"."rating", "tests_product"."in_stock", "tests_product"."price", "tests_product"."created_at", "tests_product"."metadata" FROM "tests_product" WHERE "tests_product"."description" &&& ARRAY[\'term1\'::pdb.fuzzy(1), \'term2\'::pdb.fuzzy(1)]'
        )


class TestFuzzyTerm:
    def test_fuzzy_term_single(self) -> None:
        queryset = Product.objects.filter(
            description=ParadeDB(Fuzzy("shose", distance=2, operator="TERM"))
        )
        assert (
            str(queryset.query)
            == 'SELECT "tests_product"."id", "tests_product"."description", "tests_product"."category", "tests_product"."rating", "tests_product"."in_stock", "tests_product"."price", "tests_product"."created_at", "tests_product"."metadata" FROM "tests_product" WHERE "tests_product"."description" === \'shose\'::pdb.fuzzy(2)'
        )


class TestFuzzyPrefix:
    def test_fuzzy_prefix(self) -> None:
        queryset = Product.objects.filter(
            description=ParadeDB(
                Fuzzy("rann", distance=1, prefix=True, operator="TERM")
            )
        )
        assert (
            str(queryset.query)
            == 'SELECT "tests_product"."id", "tests_product"."description", "tests_product"."category", "tests_product"."rating", "tests_product"."in_stock", "tests_product"."price", "tests_product"."created_at", "tests_product"."metadata" FROM "tests_product" WHERE "tests_product"."description" === \'rann\'::pdb.fuzzy(1, t)'
        )

    def test_fuzzy_prefix_with_match(self) -> None:
        queryset = Product.objects.filter(
            description=ParadeDB(
                Fuzzy("slee rann", distance=1, prefix=True, operator="AND")
            )
        )
        assert (
            str(queryset.query)
            == 'SELECT "tests_product"."id", "tests_product"."description", "tests_product"."category", "tests_product"."rating", "tests_product"."in_stock", "tests_product"."price", "tests_product"."created_at", "tests_product"."metadata" FROM "tests_product" WHERE "tests_product"."description" &&& \'slee rann\'::pdb.fuzzy(1, t)'
        )


class TestFuzzyTransposition:
    def test_fuzzy_transposition_cost_one(self) -> None:
        queryset = Product.objects.filter(
            description=ParadeDB(
                Fuzzy(
                    "shose",
                    distance=1,
                    transposition_cost_one=True,
                    operator="TERM",
                )
            )
        )
        assert (
            str(queryset.query)
            == 'SELECT "tests_product"."id", "tests_product"."description", "tests_product"."category", "tests_product"."rating", "tests_product"."in_stock", "tests_product"."price", "tests_product"."created_at", "tests_product"."metadata" FROM "tests_product" WHERE "tests_product"."description" === \'shose\'::pdb.fuzzy(1, f, t)'
        )

    def test_fuzzy_prefix_and_transposition(self) -> None:
        queryset = Product.objects.filter(
            description=ParadeDB(
                Fuzzy(
                    "text",
                    distance=1,
                    prefix=True,
                    transposition_cost_one=True,
                    operator="TERM",
                )
            )
        )
        assert (
            str(queryset.query)
            == 'SELECT "tests_product"."id", "tests_product"."description", "tests_product"."category", "tests_product"."rating", "tests_product"."in_stock", "tests_product"."price", "tests_product"."created_at", "tests_product"."metadata" FROM "tests_product" WHERE "tests_product"."description" === \'text\'::pdb.fuzzy(1, t, t)'
        )


class TestFuzzyOperatorValidation:
    def test_mixed_fuzzy_operators_rejected(self) -> None:
        queryset = Product.objects.filter(
            description=ParadeDB(
                Fuzzy("a", operator="AND"),
                Fuzzy("b", operator="TERM"),
            )
        )
        with pytest.raises(
            ValueError, match=r"All Fuzzy terms must use the same operator\."
        ):
            _ = str(queryset.query)


class TestTokenizerOverride:
    def test_match_with_tokenizer(self) -> None:
        queryset = Product.objects.filter(
            description=ParadeDB("running shoes", tokenizer="whitespace")
        )
        assert (
            str(queryset.query)
            == 'SELECT "tests_product"."id", "tests_product"."description", "tests_product"."category", "tests_product"."rating", "tests_product"."in_stock", "tests_product"."price", "tests_product"."created_at", "tests_product"."metadata" FROM "tests_product" WHERE "tests_product"."description" &&& \'running shoes\'::pdb.whitespace'
        )

    def test_match_or_with_tokenizer(self) -> None:
        queryset = Product.objects.filter(
            description=ParadeDB(
                "running shoes",
                operator="OR",
                tokenizer="whitespace",
            )
        )
        assert (
            str(queryset.query)
            == 'SELECT "tests_product"."id", "tests_product"."description", "tests_product"."category", "tests_product"."rating", "tests_product"."in_stock", "tests_product"."price", "tests_product"."created_at", "tests_product"."metadata" FROM "tests_product" WHERE "tests_product"."description" ||| \'running shoes\'::pdb.whitespace'
        )

    def test_phrase_with_tokenizer(self) -> None:
        queryset = Product.objects.filter(
            description=ParadeDB(Phrase("running shoes", tokenizer="whitespace"))
        )
        assert (
            str(queryset.query)
            == 'SELECT "tests_product"."id", "tests_product"."description", "tests_product"."category", "tests_product"."rating", "tests_product"."in_stock", "tests_product"."price", "tests_product"."created_at", "tests_product"."metadata" FROM "tests_product" WHERE "tests_product"."description" ### \'running shoes\'::pdb.whitespace'
        )

    def test_phrase_with_slop_and_tokenizer(self) -> None:
        queryset = Product.objects.filter(
            description=ParadeDB(
                Phrase("running shoes", slop=2, tokenizer="whitespace")
            )
        )
        assert (
            str(queryset.query)
            == 'SELECT "tests_product"."id", "tests_product"."description", "tests_product"."category", "tests_product"."rating", "tests_product"."in_stock", "tests_product"."price", "tests_product"."created_at", "tests_product"."metadata" FROM "tests_product" WHERE "tests_product"."description" ### \'running shoes\'::pdb.slop(2)::pdb.whitespace'
        )

    def test_tokenizer_with_boost(self) -> None:
        queryset = Product.objects.filter(
            description=ParadeDB("shoes", tokenizer="whitespace", boost=2.0)
        )
        assert (
            str(queryset.query)
            == 'SELECT "tests_product"."id", "tests_product"."description", "tests_product"."category", "tests_product"."rating", "tests_product"."in_stock", "tests_product"."price", "tests_product"."created_at", "tests_product"."metadata" FROM "tests_product" WHERE "tests_product"."description" &&& \'shoes\'::pdb.whitespace::pdb.boost(2.0)'
        )

    def test_tokenizer_invalid_with_pq(self) -> None:
        with pytest.raises(
            ValueError,
            match=r"ParadeDB tokenizer is only supported with plain string terms\.",
        ):
            _ = ParadeDB(PQ("a") | PQ("b"), tokenizer="whitespace")

    def test_tokenizer_invalid_with_fuzzy(self) -> None:
        with pytest.raises(
            ValueError,
            match=r"ParadeDB tokenizer is only supported with plain string terms\.",
        ):
            _ = ParadeDB(Fuzzy("x"), tokenizer="whitespace")


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

    def test_parse_query_with_conjunction_mode(self) -> None:
        queryset = Product.objects.filter(
            description=ParadeDB(Parse("running shoes", conjunction_mode=True))
        )
        assert (
            str(queryset.query)
            == 'SELECT "tests_product"."id", "tests_product"."description", "tests_product"."category", "tests_product"."rating", "tests_product"."in_stock", "tests_product"."price", "tests_product"."created_at", "tests_product"."metadata" FROM "tests_product" WHERE "tests_product"."description" @@@ pdb.parse(\'running shoes\', conjunction_mode => true)'
        )


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

    def test_phrase_prefix_with_max_expansion(self) -> None:
        queryset = Product.objects.filter(
            description=ParadeDB(PhrasePrefix("running", "sh", max_expansion=50))
        )
        assert (
            str(queryset.query)
            == 'SELECT "tests_product"."id", "tests_product"."description", "tests_product"."category", "tests_product"."rating", "tests_product"."in_stock", "tests_product"."price", "tests_product"."created_at", "tests_product"."metadata" FROM "tests_product" WHERE "tests_product"."description" @@@ pdb.phrase_prefix(ARRAY[\'running\', \'sh\'], max_expansion => 50)'
        )

    def test_phrase_prefix_with_boost(self) -> None:
        queryset = Product.objects.filter(
            description=ParadeDB(PhrasePrefix("running", "sh", boost=2.0))
        )
        assert (
            str(queryset.query)
            == 'SELECT "tests_product"."id", "tests_product"."description", "tests_product"."category", "tests_product"."rating", "tests_product"."in_stock", "tests_product"."price", "tests_product"."created_at", "tests_product"."metadata" FROM "tests_product" WHERE "tests_product"."description" @@@ pdb.phrase_prefix(ARRAY[\'running\', \'sh\'])::pdb.boost(2.0)'
        )

    def test_phrase_prefix_with_const(self) -> None:
        queryset = Product.objects.filter(
            description=ParadeDB(PhrasePrefix("running", "sh", const=1.0))
        )
        assert (
            str(queryset.query)
            == 'SELECT "tests_product"."id", "tests_product"."description", "tests_product"."category", "tests_product"."rating", "tests_product"."in_stock", "tests_product"."price", "tests_product"."created_at", "tests_product"."metadata" FROM "tests_product" WHERE "tests_product"."description" @@@ pdb.phrase_prefix(ARRAY[\'running\', \'sh\'])::pdb.const(1.0)'
        )

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


class TestProximityAdvancedQuery:
    def test_proximity_regex_query(self) -> None:
        queryset = Product.objects.filter(
            description=ParadeDB(ProximityRegex("running", "sho.*", distance=1))
        )
        assert (
            str(queryset.query)
            == 'SELECT "tests_product"."id", "tests_product"."description", "tests_product"."category", "tests_product"."rating", "tests_product"."in_stock", "tests_product"."price", "tests_product"."created_at", "tests_product"."metadata" FROM "tests_product" WHERE "tests_product"."description" @@@ pdb.proximity(\'running\' ## 1 ## pdb.prox_regex(\'sho.*\', 50))'
        )

    def test_proximity_array_query(self) -> None:
        queryset = Product.objects.filter(
            description=ParadeDB(
                ProximityArray("sleek", "running", right_term="shoes", distance=1)
            )
        )
        assert (
            str(queryset.query)
            == 'SELECT "tests_product"."id", "tests_product"."description", "tests_product"."category", "tests_product"."rating", "tests_product"."in_stock", "tests_product"."price", "tests_product"."created_at", "tests_product"."metadata" FROM "tests_product" WHERE "tests_product"."description" @@@ pdb.proximity(pdb.prox_array(\'sleek\', \'running\') ## 1 ## \'shoes\')'
        )

    def test_proximity_array_regex_rhs_query(self) -> None:
        queryset = Product.objects.filter(
            description=ParadeDB(
                ProximityArray(
                    "running",
                    right_term="unused",
                    right_pattern="sho.*",
                    distance=1,
                    max_expansions=80,
                )
            )
        )
        assert (
            str(queryset.query)
            == 'SELECT "tests_product"."id", "tests_product"."description", "tests_product"."category", "tests_product"."rating", "tests_product"."in_stock", "tests_product"."price", "tests_product"."created_at", "tests_product"."metadata" FROM "tests_product" WHERE "tests_product"."description" @@@ pdb.proximity(pdb.prox_array(\'running\') ## 1 ## pdb.prox_regex(\'sho.*\', 80))'
        )


class TestRangeTermQuery:
    def test_range_term_scalar_query(self) -> None:
        queryset = Product.objects.filter(description=ParadeDB(RangeTerm(10)))
        assert (
            str(queryset.query)
            == 'SELECT "tests_product"."id", "tests_product"."description", "tests_product"."category", "tests_product"."rating", "tests_product"."in_stock", "tests_product"."price", "tests_product"."created_at", "tests_product"."metadata" FROM "tests_product" WHERE "tests_product"."description" @@@ pdb.range_term(10)'
        )

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

    def test_range_term_relation_requires_range_type(self) -> None:
        with pytest.raises(ValueError, match=r"RangeTerm relation requires range_type"):
            RangeTerm("(10, 12]", relation="Intersects")

    def test_range_term_range_type_requires_relation(self) -> None:
        with pytest.raises(
            ValueError, match=r"RangeTerm range_type is only valid when relation"
        ):
            RangeTerm("(10, 12]", range_type="int4range")


class TestTermQuery:
    """Test Term query SQL generation."""

    def test_term_query(self) -> None:
        """Term generates: WHERE description @@@ pdb.term('shoes')."""
        queryset = Product.objects.filter(description=ParadeDB(Term("shoes")))
        assert (
            str(queryset.query)
            == 'SELECT "tests_product"."id", "tests_product"."description", "tests_product"."category", "tests_product"."rating", "tests_product"."in_stock", "tests_product"."price", "tests_product"."created_at", "tests_product"."metadata" FROM "tests_product" WHERE "tests_product"."description" @@@ pdb.term(\'shoes\')'
        )


class TestRegexQuery:
    """Test Regex query SQL generation."""

    def test_regex_query(self) -> None:
        """Regex generates: WHERE description @@@ pdb.regex('run.*shoes')."""
        queryset = Product.objects.filter(description=ParadeDB(Regex("run.*shoes")))
        assert (
            str(queryset.query)
            == 'SELECT "tests_product"."id", "tests_product"."description", "tests_product"."category", "tests_product"."rating", "tests_product"."in_stock", "tests_product"."price", "tests_product"."created_at", "tests_product"."metadata" FROM "tests_product" WHERE "tests_product"."description" @@@ pdb.regex(\'run.*shoes\')'
        )


class TestBoosting:
    """Test boost SQL generation and validation."""

    def test_boost_plain_string(self) -> None:
        queryset = Product.objects.filter(description=ParadeDB("shoes", boost=2.0))
        assert (
            str(queryset.query)
            == 'SELECT "tests_product"."id", "tests_product"."description", "tests_product"."category", "tests_product"."rating", "tests_product"."in_stock", "tests_product"."price", "tests_product"."created_at", "tests_product"."metadata" FROM "tests_product" WHERE "tests_product"."description" &&& \'shoes\'::pdb.boost(2.0)'
        )

    def test_boost_fuzzy(self) -> None:
        queryset = Product.objects.filter(
            description=ParadeDB(Fuzzy("shose", distance=2, boost=2.0))
        )
        assert (
            str(queryset.query)
            == 'SELECT "tests_product"."id", "tests_product"."description", "tests_product"."category", "tests_product"."rating", "tests_product"."in_stock", "tests_product"."price", "tests_product"."created_at", "tests_product"."metadata" FROM "tests_product" WHERE "tests_product"."description" ||| \'shose\'::pdb.fuzzy(2)::pdb.boost(2.0)'
        )

    def test_boost_phrase(self) -> None:
        queryset = Product.objects.filter(
            description=ParadeDB(Phrase("running shoes", boost=1.5))
        )
        assert (
            str(queryset.query)
            == 'SELECT "tests_product"."id", "tests_product"."description", "tests_product"."category", "tests_product"."rating", "tests_product"."in_stock", "tests_product"."price", "tests_product"."created_at", "tests_product"."metadata" FROM "tests_product" WHERE "tests_product"."description" ### \'running shoes\'::pdb.boost(1.5)'
        )

    def test_boost_regex(self) -> None:
        queryset = Product.objects.filter(
            description=ParadeDB(Regex("key.*", boost=2.0))
        )
        assert (
            str(queryset.query)
            == 'SELECT "tests_product"."id", "tests_product"."description", "tests_product"."category", "tests_product"."rating", "tests_product"."in_stock", "tests_product"."price", "tests_product"."created_at", "tests_product"."metadata" FROM "tests_product" WHERE "tests_product"."description" @@@ pdb.regex(\'key.*\')::pdb.boost(2.0)'
        )

    def test_boost_parse(self) -> None:
        queryset = Product.objects.filter(
            description=ParadeDB(Parse("shoes", boost=3.0))
        )
        assert (
            str(queryset.query)
            == 'SELECT "tests_product"."id", "tests_product"."description", "tests_product"."category", "tests_product"."rating", "tests_product"."in_stock", "tests_product"."price", "tests_product"."created_at", "tests_product"."metadata" FROM "tests_product" WHERE "tests_product"."description" @@@ pdb.parse(\'shoes\')::pdb.boost(3.0)'
        )

    def test_boost_term(self) -> None:
        queryset = Product.objects.filter(
            description=ParadeDB(Term("shoes", boost=2.0))
        )
        assert (
            str(queryset.query)
            == 'SELECT "tests_product"."id", "tests_product"."description", "tests_product"."category", "tests_product"."rating", "tests_product"."in_stock", "tests_product"."price", "tests_product"."created_at", "tests_product"."metadata" FROM "tests_product" WHERE "tests_product"."description" @@@ pdb.term(\'shoes\')::pdb.boost(2.0)'
        )

    def test_boost_none_default(self) -> None:
        queryset = Product.objects.filter(description=ParadeDB(Fuzzy("x")))
        assert "::pdb.boost" not in str(queryset.query)


class TestConstantScoring:
    """Test constant scoring SQL generation."""

    def test_const_plain_string(self) -> None:
        queryset = Product.objects.filter(description=ParadeDB("shoes", const=1.0))
        assert (
            str(queryset.query)
            == 'SELECT "tests_product"."id", "tests_product"."description", "tests_product"."category", "tests_product"."rating", "tests_product"."in_stock", "tests_product"."price", "tests_product"."created_at", "tests_product"."metadata" FROM "tests_product" WHERE "tests_product"."description" &&& \'shoes\'::pdb.const(1.0)'
        )

    def test_const_fuzzy_passthrough(self) -> None:
        queryset = Product.objects.filter(
            description=ParadeDB(Fuzzy("shose", distance=2, const=5.0))
        )
        assert (
            str(queryset.query)
            == 'SELECT "tests_product"."id", "tests_product"."description", "tests_product"."category", "tests_product"."rating", "tests_product"."in_stock", "tests_product"."price", "tests_product"."created_at", "tests_product"."metadata" FROM "tests_product" WHERE "tests_product"."description" ||| \'shose\'::pdb.fuzzy(2)::pdb.const(5.0)'
        )

    def test_const_phrase(self) -> None:
        queryset = Product.objects.filter(
            description=ParadeDB(Phrase("running shoes", const=1.0))
        )
        assert (
            str(queryset.query)
            == 'SELECT "tests_product"."id", "tests_product"."description", "tests_product"."category", "tests_product"."rating", "tests_product"."in_stock", "tests_product"."price", "tests_product"."created_at", "tests_product"."metadata" FROM "tests_product" WHERE "tests_product"."description" ### \'running shoes\'::pdb.const(1.0)'
        )

    def test_const_regex(self) -> None:
        queryset = Product.objects.filter(
            description=ParadeDB(Regex("key.*", const=1.0))
        )
        assert (
            str(queryset.query)
            == 'SELECT "tests_product"."id", "tests_product"."description", "tests_product"."category", "tests_product"."rating", "tests_product"."in_stock", "tests_product"."price", "tests_product"."created_at", "tests_product"."metadata" FROM "tests_product" WHERE "tests_product"."description" @@@ pdb.regex(\'key.*\')::pdb.const(1.0)'
        )

    def test_const_term(self) -> None:
        queryset = Product.objects.filter(
            description=ParadeDB(Term("shoes", const=2.0))
        )
        assert (
            str(queryset.query)
            == 'SELECT "tests_product"."id", "tests_product"."description", "tests_product"."category", "tests_product"."rating", "tests_product"."in_stock", "tests_product"."price", "tests_product"."created_at", "tests_product"."metadata" FROM "tests_product" WHERE "tests_product"."description" @@@ pdb.term(\'shoes\')::pdb.const(2.0)'
        )

    def test_const_none_default(self) -> None:
        queryset = Product.objects.filter(description=ParadeDB(Fuzzy("x")))
        assert "::pdb.const" not in str(queryset.query)

    def test_boost_and_const_mutually_exclusive_phrase(self) -> None:
        # pdb.boost -> pdb.const cast does not exist in PostgreSQL; the two are
        # mutually exclusive scoring modifiers.
        with pytest.raises(ValueError, match="mutually exclusive"):
            ParadeDB(Phrase("x", boost=2.0, const=1.0))

    def test_boost_and_const_mutually_exclusive_plain(self) -> None:
        with pytest.raises(ValueError, match="mutually exclusive"):
            ParadeDB("shoes", boost=2.0, const=1.0)

    def test_boost_multi_term_plain_strings(self) -> None:
        # boost must be cast on the ARRAY as a whole, not per-element;
        # ARRAY['a'::pdb.boost(N), 'b'::pdb.boost(N)] produces pdb.boost[] which
        # has no matching &&& operator.
        queryset = Product.objects.filter(
            description=ParadeDB("shoes", "boots", boost=2.0)
        )
        assert (
            str(queryset.query)
            == 'SELECT "tests_product"."id", "tests_product"."description", "tests_product"."category", "tests_product"."rating", "tests_product"."in_stock", "tests_product"."price", "tests_product"."created_at", "tests_product"."metadata" FROM "tests_product" WHERE "tests_product"."description" &&& ARRAY[\'shoes\', \'boots\']::pdb.boost(2.0)'
        )

    def test_const_multi_term_plain_strings(self) -> None:
        queryset = Product.objects.filter(
            description=ParadeDB("shoes", "boots", operator="OR", const=1.5)
        )
        assert (
            str(queryset.query)
            == 'SELECT "tests_product"."id", "tests_product"."description", "tests_product"."category", "tests_product"."rating", "tests_product"."in_stock", "tests_product"."price", "tests_product"."created_at", "tests_product"."metadata" FROM "tests_product" WHERE "tests_product"."description" ||| ARRAY[\'shoes\', \'boots\']::pdb.const(1.5)'
        )


class TestScoreAnnotation:
    """Test Score annotation SQL generation."""

    def test_score_annotation(self) -> None:
        """Score generates: SELECT ..., pdb.score(id) AS search_score."""
        queryset = Product.objects.filter(
            description=ParadeDB("running", "shoes")
        ).annotate(search_score=Score())
        assert (
            str(queryset.query)
            == 'SELECT "tests_product"."id", "tests_product"."description", "tests_product"."category", "tests_product"."rating", "tests_product"."in_stock", "tests_product"."price", "tests_product"."created_at", "tests_product"."metadata", pdb.score("tests_product"."id") AS "search_score" FROM "tests_product" WHERE "tests_product"."description" &&& ARRAY[\'running\', \'shoes\']'
        )

    def test_score_with_ordering(self) -> None:
        """Score with ORDER BY search_score DESC."""
        queryset = (
            Product.objects.filter(description=ParadeDB("shoes"))
            .annotate(search_score=Score())
            .order_by("-search_score")
        )
        assert (
            str(queryset.query)
            == 'SELECT "tests_product"."id", "tests_product"."description", "tests_product"."category", "tests_product"."rating", "tests_product"."in_stock", "tests_product"."price", "tests_product"."created_at", "tests_product"."metadata", pdb.score("tests_product"."id") AS "search_score" FROM "tests_product" WHERE "tests_product"."description" &&& \'shoes\' ORDER BY 9 DESC'
        )

    def test_score_filter(self) -> None:
        """Filter by score: WHERE pdb.score(id) > 0."""
        queryset = (
            Product.objects.filter(description=ParadeDB("shoes"))
            .annotate(search_score=Score())
            .filter(search_score__gt=0)
        )
        assert (
            str(queryset.query)
            == 'SELECT "tests_product"."id", "tests_product"."description", "tests_product"."category", "tests_product"."rating", "tests_product"."in_stock", "tests_product"."price", "tests_product"."created_at", "tests_product"."metadata", pdb.score("tests_product"."id") AS "search_score" FROM "tests_product" WHERE ("tests_product"."description" &&& \'shoes\' AND pdb.score("tests_product"."id") > 0.0)'
        )


class TestSnippetAnnotation:
    """Test Snippet annotation SQL generation."""

    def test_snippet_annotation(self) -> None:
        """Snippet generates: pdb.snippet(description) AS snippet."""
        queryset = Product.objects.filter(
            description=ParadeDB(PQ("wireless") | PQ("bluetooth"))
        ).annotate(snippet=Snippet("description"))
        assert (
            str(queryset.query)
            == 'SELECT "tests_product"."id", "tests_product"."description", "tests_product"."category", "tests_product"."rating", "tests_product"."in_stock", "tests_product"."price", "tests_product"."created_at", "tests_product"."metadata", pdb.snippet("tests_product"."description") AS "snippet" FROM "tests_product" WHERE "tests_product"."description" ||| ARRAY[\'wireless\', \'bluetooth\']'
        )

    def test_snippet_with_custom_formatting(self) -> None:
        """Custom snippet: pdb.snippet(description, '<mark>', '</mark>', 100)."""
        queryset = Product.objects.filter(description=ParadeDB("shoes")).annotate(
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


class TestSnippetsAnnotation:
    """Test Snippets annotation SQL generation."""

    def test_snippets_basic(self) -> None:
        """Snippets generates: pdb.snippets(description) AS snippets."""
        queryset = Product.objects.filter(
            description=ParadeDB("artistic", "vase")
        ).annotate(snippets=Snippets("description"))
        assert (
            str(queryset.query)
            == 'SELECT "tests_product"."id", "tests_product"."description", "tests_product"."category", "tests_product"."rating", "tests_product"."in_stock", "tests_product"."price", "tests_product"."created_at", "tests_product"."metadata", pdb.snippets("tests_product"."description") AS "snippets" FROM "tests_product" WHERE "tests_product"."description" &&& ARRAY[\'artistic\', \'vase\']'
        )

    def test_snippets_with_max_num_chars(self) -> None:
        """Snippets with max_num_chars only."""
        queryset = Product.objects.filter(
            description=ParadeDB("artistic", "vase")
        ).annotate(snippets=Snippets("description", max_num_chars=15))
        assert (
            str(queryset.query)
            == 'SELECT "tests_product"."id", "tests_product"."description", "tests_product"."category", "tests_product"."rating", "tests_product"."in_stock", "tests_product"."price", "tests_product"."created_at", "tests_product"."metadata", pdb.snippets("tests_product"."description", max_num_chars => 15) AS "snippets" FROM "tests_product" WHERE "tests_product"."description" &&& ARRAY[\'artistic\', \'vase\']'
        )

    def test_snippets_with_limit_and_offset(self) -> None:
        """Snippets with limit and offset uses double-quoted SQL reserved words."""
        queryset = Product.objects.filter(description=ParadeDB("running")).annotate(
            snippets=Snippets("description", max_num_chars=15, limit=1, offset=1)
        )
        assert (
            str(queryset.query)
            == 'SELECT "tests_product"."id", "tests_product"."description", "tests_product"."category", "tests_product"."rating", "tests_product"."in_stock", "tests_product"."price", "tests_product"."created_at", "tests_product"."metadata", pdb.snippets("tests_product"."description", max_num_chars => 15, "limit" => 1, "offset" => 1) AS "snippets" FROM "tests_product" WHERE "tests_product"."description" &&& \'running\''
        )

    def test_snippets_with_sort_by(self) -> None:
        """Snippets with sort_by parameter."""
        queryset = Product.objects.filter(
            description=ParadeDB("artistic", "vase")
        ).annotate(
            snippets=Snippets("description", max_num_chars=15, sort_by="position")
        )
        assert (
            str(queryset.query)
            == 'SELECT "tests_product"."id", "tests_product"."description", "tests_product"."category", "tests_product"."rating", "tests_product"."in_stock", "tests_product"."price", "tests_product"."created_at", "tests_product"."metadata", pdb.snippets("tests_product"."description", max_num_chars => 15, sort_by => \'position\') AS "snippets" FROM "tests_product" WHERE "tests_product"."description" &&& ARRAY[\'artistic\', \'vase\']'
        )

    def test_snippets_with_all_params(self) -> None:
        """Snippets with all named parameters."""
        queryset = Product.objects.filter(description=ParadeDB("shoes")).annotate(
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


class TestSnippetPositionsAnnotation:
    """Test SnippetPositions annotation SQL generation."""

    def test_snippet_positions_basic(self) -> None:
        """SnippetPositions generates: pdb.snippet_positions(description)."""
        queryset = Product.objects.filter(description=ParadeDB("shoes")).annotate(
            positions=SnippetPositions("description")
        )
        assert (
            str(queryset.query)
            == 'SELECT "tests_product"."id", "tests_product"."description", "tests_product"."category", "tests_product"."rating", "tests_product"."in_stock", "tests_product"."price", "tests_product"."created_at", "tests_product"."metadata", pdb.snippet_positions("tests_product"."description") AS "positions" FROM "tests_product" WHERE "tests_product"."description" &&& \'shoes\''
        )

    def test_snippet_positions_with_snippet(self) -> None:
        """SnippetPositions alongside Snippet."""
        queryset = Product.objects.filter(description=ParadeDB("shoes")).annotate(
            snippet=Snippet("description"),
            positions=SnippetPositions("description"),
        )
        assert (
            str(queryset.query)
            == 'SELECT "tests_product"."id", "tests_product"."description", "tests_product"."category", "tests_product"."rating", "tests_product"."in_stock", "tests_product"."price", "tests_product"."created_at", "tests_product"."metadata", pdb.snippet("tests_product"."description") AS "snippet", pdb.snippet_positions("tests_product"."description") AS "positions" FROM "tests_product" WHERE "tests_product"."description" &&& \'shoes\''
        )


class TestBM25Index:
    """Test BM25 index SQL generation."""

    def test_basic_index_sql(self) -> None:
        """Basic BM25 index DDL generation."""
        index = BM25Index(
            fields={"id": {}, "description": {}},
            key_field="id",
            name="product_search_idx",
        )
        schema_editor = DummySchemaEditor()
        sql = str(index.create_sql(model=Product, schema_editor=schema_editor))
        assert (
            sql
            == 'CREATE INDEX "product_search_idx" ON "tests_product"\nUSING bm25 (\n    "id",\n    "description"\n)\nWITH (key_field=\'id\')'
        )

    def test_index_with_tokenizer(self) -> None:
        """Index with tokenizer configuration."""
        index = BM25Index(
            fields={
                "id": {},
                "description": {
                    "tokenizer": "simple",
                    "filters": ["lowercase", "stemmer"],
                    "stemmer": "english",
                },
            },
            key_field="id",
            name="product_search_idx",
        )
        schema_editor = DummySchemaEditor()
        sql = str(index.create_sql(model=Product, schema_editor=schema_editor))
        assert (
            sql
            == 'CREATE INDEX "product_search_idx" ON "tests_product"\nUSING bm25 (\n    "id",\n    ("description"::pdb.simple(\'lowercase=true,stemmer=english\'))\n)\nWITH (key_field=\'id\')'
        )

    def test_index_with_tokenizer_only(self) -> None:
        """Index with tokenizer only."""
        index = BM25Index(
            fields={
                "id": {},
                "description": {"tokenizer": "simple"},
            },
            key_field="id",
            name="product_search_idx",
        )
        schema_editor = DummySchemaEditor()
        sql = str(index.create_sql(model=Product, schema_editor=schema_editor))
        assert (
            sql
            == 'CREATE INDEX "product_search_idx" ON "tests_product"\nUSING bm25 (\n    "id",\n    ("description"::pdb.simple)\n)\nWITH (key_field=\'id\')'
        )

    def test_json_field_index(self) -> None:
        """JSON field with json_keys configuration."""
        index = BM25Index(
            fields={
                "id": {},
                "metadata": {
                    "json_keys": {
                        "title": {"tokenizer": "simple", "filters": ["lowercase"]},
                        "brand": {"tokenizer": "simple"},
                    }
                },
            },
            key_field="id",
            name="product_search_idx",
        )
        schema_editor = DummySchemaEditor()
        sql = str(index.create_sql(model=Product, schema_editor=schema_editor))
        assert (
            sql
            == "CREATE INDEX \"product_search_idx\" ON \"tests_product\"\nUSING bm25 (\n    \"id\",\n    ((\"metadata\"->>'title')::pdb.simple('alias=metadata_title,lowercase=true')),\n    ((\"metadata\"->>'brand')::pdb.simple('alias=metadata_brand'))\n)\nWITH (key_field='id')"
        )

    def test_field_filters_without_tokenizer_raises(self) -> None:
        """Specifying filters or stemmer without a tokenizer raises ValueError."""
        index = BM25Index(
            fields={
                "id": {},
                "description": {"stemmer": "english"},
            },
            key_field="id",
            name="product_search_idx",
        )
        schema_editor = DummySchemaEditor()
        with pytest.raises(ValueError, match="no tokenizer"):
            index.create_sql(model=Product, schema_editor=schema_editor)

    def test_field_filters_only_without_tokenizer_raises(self) -> None:
        """Specifying only filters without a tokenizer raises ValueError."""
        index = BM25Index(
            fields={
                "id": {},
                "description": {"filters": ["lowercase"]},
            },
            key_field="id",
            name="product_search_idx",
        )
        schema_editor = DummySchemaEditor()
        with pytest.raises(ValueError, match="no tokenizer"):
            index.create_sql(model=Product, schema_editor=schema_editor)

    def test_json_key_without_tokenizer_raises(self) -> None:
        """JSON keys without an explicit tokenizer raise ValueError."""
        index = BM25Index(
            fields={
                "id": {},
                "metadata": {
                    "json_keys": {
                        "color": {},
                    }
                },
            },
            key_field="id",
            name="product_search_idx",
        )
        schema_editor = DummySchemaEditor()
        with pytest.raises(ValueError, match="requires an explicit tokenizer"):
            index.create_sql(model=Product, schema_editor=schema_editor)

    def test_json_field_literal_alias(self) -> None:
        """JSON subfields can be indexed with literal tokenizer aliases."""
        index = BM25Index(
            fields={
                "id": {},
                "description": {},
                "metadata": {
                    "json_keys": {
                        "color": {"tokenizer": "literal"},
                        "location": {"tokenizer": "literal"},
                    }
                },
            },
            key_field="id",
            name="product_search_idx",
        )
        schema_editor = DummySchemaEditor()
        sql = str(index.create_sql(model=Product, schema_editor=schema_editor))
        assert (
            sql
            == 'CREATE INDEX "product_search_idx" ON "tests_product"\nUSING bm25 (\n    "id",\n    "description",\n    (("metadata"->>\'color\')::pdb.literal(\'alias=metadata_color\')),\n    (("metadata"->>\'location\')::pdb.literal(\'alias=metadata_location\'))\n)\nWITH (key_field=\'id\')'
        )

    def test_field_with_multiple_tokenizers(self) -> None:
        """A field can include multiple tokenizer expressions."""
        index = BM25Index(
            fields={
                "id": {},
                "description": {
                    "tokenizers": [
                        {"tokenizer": "literal"},
                        {
                            "tokenizer": "simple",
                            "filters": ["lowercase"],
                            "alias": "description_simple",
                        },
                    ]
                },
            },
            key_field="id",
            name="product_search_idx",
        )
        schema_editor = DummySchemaEditor()
        sql = str(index.create_sql(model=Product, schema_editor=schema_editor))
        assert (
            sql
            == 'CREATE INDEX "product_search_idx" ON "tests_product"\nUSING bm25 (\n    "id",\n    ("description"::pdb.literal),\n    ("description"::pdb.simple(\'alias=description_simple,lowercase=true\'))\n)\nWITH (key_field=\'id\')'
        )

    def test_multiple_tokenizers_allows_secondary_entries_without_alias(self) -> None:
        """Thin wrapper mode allows tokenizer entries without alias."""
        index = BM25Index(
            fields={
                "id": {},
                "description": {
                    "tokenizers": [
                        {"tokenizer": "literal"},
                        {"tokenizer": "simple"},
                    ]
                },
            },
            key_field="id",
            name="product_search_idx",
        )
        schema_editor = DummySchemaEditor()
        sql = str(index.create_sql(model=Product, schema_editor=schema_editor))
        assert (
            sql
            == 'CREATE INDEX "product_search_idx" ON "tests_product"\nUSING bm25 (\n    "id",\n    ("description"::pdb.literal),\n    ("description"::pdb.simple)\n)\nWITH (key_field=\'id\')'
        )

    def test_multiple_tokenizers_cannot_mix_with_single_tokenizer_keys(self) -> None:
        """The list syntax cannot be combined with top-level tokenizer keys."""
        index = BM25Index(
            fields={
                "id": {},
                "description": {
                    "tokenizers": [{"tokenizer": "literal"}],
                    "tokenizer": "simple",
                },
            },
            key_field="id",
            name="product_search_idx",
        )
        schema_editor = DummySchemaEditor()
        with pytest.raises(ValueError, match="cannot mix 'tokenizers'"):
            index.create_sql(model=Product, schema_editor=schema_editor)

    def test_structured_ngram_args_and_named_args_in_multi_tokenizer_dsl(self) -> None:
        """Supports positional ngram args plus named args in DSL."""
        index = BM25Index(
            fields={
                "id": {},
                "description": {
                    "tokenizers": [
                        {"tokenizer": "literal"},
                        {
                            "tokenizer": "ngram",
                            "args": [3, 3],
                            "named_args": {
                                "prefix_only": True,
                                "positions": True,
                            },
                            "alias": "description_ngram",
                        },
                    ]
                },
            },
            key_field="id",
            name="product_search_idx",
        )
        schema_editor = DummySchemaEditor()
        sql = str(index.create_sql(model=Product, schema_editor=schema_editor))
        assert (
            sql
            == 'CREATE INDEX "product_search_idx" ON "tests_product"\nUSING bm25 (\n    "id",\n    ("description"::pdb.literal),\n    ("description"::pdb.ngram(3,3,\'alias=description_ngram,prefix_only=true,positions=true\'))\n)\nWITH (key_field=\'id\')'
        )

    def test_structured_regex_pattern_and_alias_in_multi_tokenizer_dsl(self) -> None:
        """Supports regex_pattern positional args with alias in DSL."""
        index = BM25Index(
            fields={
                "id": {},
                "description": {
                    "tokenizers": [
                        {"tokenizer": "literal"},
                        {
                            "tokenizer": "regex_pattern",
                            "args": [r"(?i)\bh\w*"],
                            "alias": "description_regex",
                        },
                    ]
                },
            },
            key_field="id",
            name="product_search_idx",
        )
        schema_editor = DummySchemaEditor()
        sql = str(index.create_sql(model=Product, schema_editor=schema_editor))
        assert (
            sql
            == 'CREATE INDEX "product_search_idx" ON "tests_product"\nUSING bm25 (\n    "id",\n    ("description"::pdb.literal),\n    ("description"::pdb.regex_pattern(\'(?i)\\bh\\w*\',\'alias=description_regex\'))\n)\nWITH (key_field=\'id\')'
        )

    def test_structured_lindera_dictionary_argument_in_multi_tokenizer_dsl(
        self,
    ) -> None:
        """Supports lindera dictionary positional arg in DSL."""
        index = BM25Index(
            fields={
                "id": {},
                "description": {
                    "tokenizers": [
                        {"tokenizer": "literal"},
                        {
                            "tokenizer": "lindera",
                            "args": ["japanese"],
                            "alias": "description_jp",
                        },
                    ]
                },
            },
            key_field="id",
            name="product_search_idx",
        )
        schema_editor = DummySchemaEditor()
        sql = str(index.create_sql(model=Product, schema_editor=schema_editor))
        assert (
            sql
            == 'CREATE INDEX "product_search_idx" ON "tests_product"\nUSING bm25 (\n    "id",\n    ("description"::pdb.literal),\n    ("description"::pdb.lindera(\'japanese\',\'alias=description_jp\'))\n)\nWITH (key_field=\'id\')'
        )

    def test_value_based_token_filter_named_args(self) -> None:
        """Supports non-boolean token filter named args in DSL."""
        index = BM25Index(
            fields={
                "id": {},
                "description": {
                    "tokenizer": "simple",
                    "named_args": {
                        "lowercase": False,
                        "stopwords_language": "English,French",
                        "remove_long": 20,
                        "remove_short": 2,
                    },
                    "stemmer": "english",
                },
            },
            key_field="id",
            name="product_search_idx",
        )
        schema_editor = DummySchemaEditor()
        sql = str(index.create_sql(model=Product, schema_editor=schema_editor))
        assert (
            sql
            == 'CREATE INDEX "product_search_idx" ON "tests_product"\nUSING bm25 (\n    "id",\n    ("description"::pdb.simple(\'lowercase=false,stopwords_language=English,French,remove_long=20,remove_short=2,stemmer=english\'))\n)\nWITH (key_field=\'id\')'
        )

    def test_legacy_options_key_raises(self) -> None:
        """Legacy 'options' key is no longer accepted."""
        index = BM25Index(
            fields={
                "id": {},
                "description": {
                    "tokenizer": "simple",
                    "options": {"remove_long": 20},
                },
            },
            key_field="id",
            name="product_search_idx",
        )
        schema_editor = DummySchemaEditor()
        with pytest.raises(ValueError, match="deprecated 'options'"):
            index.create_sql(model=Product, schema_editor=schema_editor)


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
        # With parameterized SQL, Django's string representation doesn't show quotes
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
        # With parameterized SQL, stopwords appear without quotes in string representation
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
        # With parameterized SQL, stopwords appear without quotes in string representation
        assert "stopwords => ARRAY[the, and, or]" in sql
        # Fields also appear without quotes
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
        # With parameterized SQL, stopwords appear without quotes in string representation
        assert "stopwords => ARRAY[comfortable]" in sql

    def test_mlt_document_should_use_json_format(self) -> None:
        """Document input should generate JSON format."""
        queryset = Product.objects.filter(
            MoreLikeThis(document={"description": "wireless earbuds"})
        )
        sql = str(queryset.query)

        # With parameterized SQL, JSON appears without quotes in string representation
        assert 'pdb.more_like_this({"description": "wireless earbuds"})' in sql, (
            "Expected JSON format: "
            'pdb.more_like_this({"description": "wireless earbuds"})\n'
            f"Got SQL: {sql}"
        )

        # Should NOT contain array form
        # With parameterized SQL, would appear as ARRAY[description] not ARRAY['description']
        assert "ARRAY[description]" not in sql, (
            f"Should not use array form for document input\nGot SQL: {sql}"
        )

    def test_mlt_empty_stopwords_should_not_generate_empty_string(self) -> None:
        """Empty stopwords should be omitted."""
        queryset = Product.objects.filter(
            MoreLikeThis(
                product_id=5,
                stopwords=[],  # Empty list
            )
        )
        sql = str(queryset.query)

        # Should NOT have stopwords at all (parameterized or not)
        assert "stopwords" not in sql, (
            f"Empty stopwords should be omitted entirely\nGot SQL: {sql}"
        )


class TestDjangoIntegration:
    """Test Django ORM integration."""

    def test_paradedb_with_django_q(self) -> None:
        """Combine ParadeDB with Django Q for complex logic."""
        queryset = Product.objects.filter(
            Q(description=ParadeDB(Phrase("running shoes")), rating__gte=4)
            | Q(category=ParadeDB("Electronics"), description=ParadeDB("wireless"))
        )
        assert (
            str(queryset.query)
            == 'SELECT "tests_product"."id", "tests_product"."description", "tests_product"."category", "tests_product"."rating", "tests_product"."in_stock", "tests_product"."price", "tests_product"."created_at", "tests_product"."metadata" FROM "tests_product" WHERE (("tests_product"."description" ### \'running shoes\' AND "tests_product"."rating" >= 4) OR ("tests_product"."category" &&& \'Electronics\' AND "tests_product"."description" &&& \'wireless\'))'
        )

    def test_negation_with_q(self) -> None:
        """Negation using ~Q with ParadeDB."""
        queryset = Product.objects.filter(
            Q(description=ParadeDB("running", "athletic")),
            ~Q(description=ParadeDB("cheap")),
        )
        assert (
            str(queryset.query)
            == 'SELECT "tests_product"."id", "tests_product"."description", "tests_product"."category", "tests_product"."rating", "tests_product"."in_stock", "tests_product"."price", "tests_product"."created_at", "tests_product"."metadata" FROM "tests_product" WHERE ("tests_product"."description" &&& ARRAY[\'running\', \'athletic\'] AND NOT ("tests_product"."description" &&& \'cheap\'))'
        )

    def test_with_standard_filters(self) -> None:
        """ParadeDB search combined with standard ORM filters."""
        queryset = Product.objects.filter(
            description=ParadeDB("shoes"),
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
        queryset = Product.objects.filter(description=ParadeDB("shoes")).annotate(
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
        ).filter(combined=ParadeDB("running"))
        sql = str(queryset.query)
        assert "&&& 'running'" in sql
        assert '"tests_product"."description"' in sql
        assert '"tests_product"."category"' in sql


class TestFacets:
    """Test facets SQL generation helpers."""

    def test_facets_window_annotation(self) -> None:
        """Facets window annotation uses pdb.agg() OVER ()."""
        json_spec = '{"terms":{"field":"category","order":{"_count":"desc"},"size":10}}'
        queryset = Product.objects.filter(description=ParadeDB("shoes")).annotate(
            facets=Window(expression=Agg(json_spec))
        )
        assert (
            str(queryset.query)
            == 'SELECT "tests_product"."id", "tests_product"."description", "tests_product"."category", "tests_product"."rating", "tests_product"."in_stock", "tests_product"."price", "tests_product"."created_at", "tests_product"."metadata", pdb.agg(\'{"terms":{"field":"category","order":{"_count":"desc"},"size":10}}\') OVER () AS "facets" FROM "tests_product" WHERE "tests_product"."description" &&& \'shoes\''
        )

    def test_facets_requires_paradedb_operator(self) -> None:
        """facets() raises if no ParadeDB operator is present."""
        queryset = Product.objects.filter(rating=5).order_by("price")[:5]
        with pytest.raises(ValueError, match="ParadeDB operator"):
            queryset.facets("category")

    def test_facets_requires_order_by_and_limit(self) -> None:
        """facets() raises if include_rows requires order_by + LIMIT."""
        queryset = Product.objects.filter(description=ParadeDB("shoes"))
        with pytest.raises(ValueError, match="order_by\\(\\) and a LIMIT"):
            queryset.facets("category")

    def test_facets_multiple_fields_specs(self) -> None:
        """facets() generates correct specs for multiple fields."""
        queryset = Product.objects.filter(description=ParadeDB("shoes")).order_by(
            "price"
        )[:5]
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
        queryset = Product.objects.filter(description=ParadeDB("shoes")).order_by(
            "price"
        )[:5]
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
        queryset = Product.objects.filter(description=ParadeDB("shoes")).order_by(
            "price"
        )[:5]
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
        queryset = Product.objects.filter(description=ParadeDB("shoes")).order_by(
            "price"
        )[:5]
        with pytest.raises(ValueError, match="unique"):
            queryset.facets("category", "category")

    def test_mlt_document_should_use_json_format(self) -> None:
        """Document input should generate JSON format."""
        queryset = Product.objects.filter(
            MoreLikeThis(document={"description": "wireless earbuds"})
        )
        sql = str(queryset.query)

        # With parameterized SQL, JSON appears without quotes in string representation
        assert 'pdb.more_like_this({"description": "wireless earbuds"})' in sql, (
            "Expected JSON format: "
            'pdb.more_like_this({"description": "wireless earbuds"})\n'
            f"Got SQL: {sql}"
        )

        # Should NOT contain array form
        # With parameterized SQL, would appear as ARRAY[description] not ARRAY['description']
        assert "ARRAY[description]" not in sql, (
            f"Should not use array form for document input\nGot SQL: {sql}"
        )

    def test_mlt_empty_stopwords_should_not_generate_empty_string(self) -> None:
        """Empty stopwords should be omitted."""
        queryset = Product.objects.filter(
            MoreLikeThis(
                product_id=5,
                stopwords=[],  # Empty list
            )
        )
        sql = str(queryset.query)

        # Should NOT have stopwords at all (parameterized or not)
        assert "stopwords" not in sql, (
            f"Empty stopwords should be omitted entirely\nGot SQL: {sql}"
        )
