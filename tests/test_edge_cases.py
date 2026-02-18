"""Edge case tests for ParadeDB Django integration.

Tests for special characters, validation, boundary conditions, and unusual inputs.
These are unit tests that don't require a database.
"""

from __future__ import annotations

from unittest.mock import Mock

import pytest
from django.db.models import Value

from paradedb.functions import Score, Snippet, SnippetPositions, Snippets
from paradedb.indexes import BM25Index
from paradedb.search import (
    PQ,
    Fuzzy,
    MoreLikeThis,
    ParadeDB,
    Parse,
    Phrase,
    Regex,
    Term,
)
from tests.models import Product


class TestSpecialCharacterEscaping:
    """Test SQL injection prevention and special character handling."""

    def test_single_quote_in_search_term(self) -> None:
        """Single quotes are escaped to prevent SQL injection."""
        queryset = Product.objects.filter(description=ParadeDB("it's"))
        sql = str(queryset.query)
        assert "it''s" in sql

    def test_double_single_quotes(self) -> None:
        """Multiple single quotes are all escaped."""
        queryset = Product.objects.filter(description=ParadeDB("don''t"))
        sql = str(queryset.query)
        assert "don''''t" in sql

    def test_backslash_in_search_term(self) -> None:
        """Backslashes are preserved in search terms."""
        queryset = Product.objects.filter(description=ParadeDB("path\\to\\file"))
        sql = str(queryset.query)
        assert "path\\to\\file" in sql

    def test_unicode_characters(self) -> None:
        """Unicode characters work in search terms."""
        queryset = Product.objects.filter(description=ParadeDB("日本語"))
        sql = str(queryset.query)
        assert "日本語" in sql

    def test_emoji_in_search(self) -> None:
        """Emoji characters work in search terms."""
        queryset = Product.objects.filter(description=ParadeDB("👟 shoes"))
        sql = str(queryset.query)
        assert "👟 shoes" in sql

    def test_special_sql_keywords(self) -> None:
        """SQL keywords in search terms are quoted safely."""
        queryset = Product.objects.filter(description=ParadeDB("SELECT * FROM"))
        sql = str(queryset.query)
        assert "'SELECT * FROM'" in sql

    def test_phrase_with_quotes(self) -> None:
        """Phrase containing quotes is escaped."""
        queryset = Product.objects.filter(
            description=ParadeDB(Phrase('it\'s a "test"'))
        )
        sql = str(queryset.query)
        assert "it''s" in sql

    def test_regex_special_chars_preserved(self) -> None:
        """Regex special characters are preserved."""
        queryset = Product.objects.filter(
            description=ParadeDB(Regex("test.*[a-z]+\\d{2,3}"))
        )
        sql = str(queryset.query)
        assert "test.*[a-z]+\\d{2,3}" in sql


class TestParadeDBValidation:
    """Test input validation for ParadeDB wrapper."""

    def test_empty_terms_raises_error(self) -> None:
        """ParadeDB with no terms raises ValueError."""
        with pytest.raises(ValueError, match="requires at least one"):
            ParadeDB()

    def test_pq_must_be_sole_input(self) -> None:
        """PQ mixed with other terms raises ValueError on SQL generation."""
        pdb = ParadeDB(PQ("a") | PQ("b"), "extra")
        queryset = Product.objects.filter(description=pdb)
        with pytest.raises(ValueError, match="sole ParadeDB input"):
            str(queryset.query)

    def test_parse_must_be_single(self) -> None:
        """Multiple Parse objects raise ValueError on SQL generation."""
        pdb = ParadeDB(Parse("a"), Parse("b"))
        queryset = Product.objects.filter(description=pdb)
        with pytest.raises(ValueError, match="single term"):
            str(queryset.query)

    def test_term_must_be_single(self) -> None:
        """Multiple Term objects raise ValueError on SQL generation."""
        pdb = ParadeDB(Term("a"), Term("b"))
        queryset = Product.objects.filter(description=pdb)
        with pytest.raises(ValueError, match="single term"):
            str(queryset.query)

    def test_regex_must_be_single(self) -> None:
        """Multiple Regex objects raise ValueError on SQL generation."""
        pdb = ParadeDB(Regex("a"), Regex("b"))
        queryset = Product.objects.filter(description=pdb)
        with pytest.raises(ValueError, match="single term"):
            str(queryset.query)

    def test_phrase_cannot_mix_with_string(self) -> None:
        """Phrase mixed with string raises TypeError on SQL generation."""
        pdb = ParadeDB(Phrase("a"), "b")
        queryset = Product.objects.filter(description=pdb)
        with pytest.raises(TypeError, match="only accept Phrase"):
            str(queryset.query)

    def test_fuzzy_cannot_mix_with_string(self) -> None:
        """Fuzzy mixed with string raises TypeError on SQL generation."""
        pdb = ParadeDB(Fuzzy("a"), "b")
        queryset = Product.objects.filter(description=pdb)
        with pytest.raises(TypeError, match="only accept Fuzzy"):
            str(queryset.query)

    def test_paradedb_invalid_tokenizer_raises(self) -> None:
        """Invalid tokenizer identifiers raise ValueError."""
        with pytest.raises(ValueError, match="tokenizer must be a valid identifier"):
            ParadeDB("shoes", tokenizer="bad-tokenizer;")


class TestPhraseValidation:
    """Test Phrase dataclass validation."""

    def test_phrase_slop_zero_is_valid(self) -> None:
        """Slop of 0 is valid."""
        phrase = Phrase("test", slop=0)
        assert phrase.slop == 0

    def test_phrase_negative_slop_raises(self) -> None:
        """Negative slop raises ValueError."""
        with pytest.raises(ValueError, match="zero or positive"):
            Phrase("test", slop=-1)

    def test_phrase_large_slop(self) -> None:
        """Large slop values work."""
        phrase = Phrase("test", slop=100)
        assert phrase.slop == 100

    def test_phrase_invalid_tokenizer_raises(self) -> None:
        """Invalid phrase tokenizers raise ValueError."""
        with pytest.raises(
            ValueError, match="Phrase tokenizer must be a valid identifier"
        ):
            Phrase("test", tokenizer="bad tokenizer")


class TestFuzzyValidation:
    """Test Fuzzy dataclass validation."""

    def test_fuzzy_default_distance(self) -> None:
        """Default distance is 1."""
        fuzzy = Fuzzy("test")
        assert fuzzy.distance == 1

    def test_fuzzy_distance_zero_is_valid(self) -> None:
        """Distance of 0 is valid (exact match)."""
        fuzzy = Fuzzy("test", distance=0)
        assert fuzzy.distance == 0

    def test_fuzzy_negative_distance_raises(self) -> None:
        """Negative distance raises ValueError."""
        with pytest.raises(ValueError, match="zero or positive"):
            Fuzzy("test", distance=-1)

    def test_fuzzy_large_distance(self) -> None:
        """Distance values > 2 raise ValueError."""
        with pytest.raises(ValueError, match="<= 2"):
            Fuzzy("test", distance=10)


class TestPQValidation:
    """Test PQ object validation and operations."""

    def test_pq_combine_with_non_pq_raises(self) -> None:
        """Combining PQ with non-PQ raises TypeError."""
        pq = PQ("test")
        with pytest.raises(TypeError, match="PQ instances"):
            pq | "string"  # type: ignore[operator]

    def test_pq_mixed_operators_raises(self) -> None:
        """Mixing AND and OR operators raises ValueError."""
        pq_or = PQ("a") | PQ("b")
        with pytest.raises(ValueError, match="Mixed PQ operators"):
            pq_or & PQ("c")

    def test_pq_single_term_no_operator(self) -> None:
        """Single PQ has no operator."""
        pq = PQ("test")
        assert pq.operator is None
        assert pq.terms == ("test",)

    def test_pq_chained_or(self) -> None:
        """Chained OR preserves all terms."""
        pq = PQ("a") | PQ("b") | PQ("c") | PQ("d")
        assert pq.terms == ("a", "b", "c", "d")
        assert pq.operator == "OR"

    def test_pq_chained_and(self) -> None:
        """Chained AND preserves all terms."""
        pq = PQ("a") & PQ("b") & PQ("c")
        assert pq.terms == ("a", "b", "c")
        assert pq.operator == "AND"


class TestMoreLikeThisValidation:
    """Test MoreLikeThis validation."""

    def test_mlt_requires_one_input(self) -> None:
        """MLT with no inputs raises ValueError."""
        with pytest.raises(ValueError, match="exactly one input"):
            MoreLikeThis()

    def test_mlt_multiple_inputs_raises(self) -> None:
        """MLT with multiple inputs raises ValueError."""
        with pytest.raises(ValueError, match="exactly one input"):
            MoreLikeThis(product_id=1, document={"description": "test"})

    def test_mlt_empty_product_ids_raises(self) -> None:
        """MLT with empty product_ids raises ValueError."""
        with pytest.raises(ValueError, match="cannot be empty"):
            MoreLikeThis(product_ids=[])

    def test_mlt_single_product_id(self) -> None:
        """MLT with single product_id works."""
        mlt = MoreLikeThis(product_id=1)
        assert mlt.product_id == 1

    def test_mlt_product_ids_list(self) -> None:
        """MLT with product_ids list works."""
        mlt = MoreLikeThis(product_ids=[1, 2, 3])
        assert mlt.product_ids == [1, 2, 3]

    def test_mlt_document_dict(self) -> None:
        """MLT with document dict works."""
        mlt = MoreLikeThis(document={"description": "running shoes"})
        assert mlt.document == '{"description": "running shoes"}'

    def test_mlt_document_with_fields_raises(self) -> None:
        """MLT with document and fields raises ValueError."""
        with pytest.raises(ValueError, match="fields are only valid"):
            MoreLikeThis(
                document={"description": "running shoes"}, fields=["description"]
            )

    def test_mlt_with_all_options(self) -> None:
        """MLT with all tuning options works."""
        mlt = MoreLikeThis(
            product_id=1,
            fields=["description"],
            min_term_freq=2,
            max_query_terms=10,
            min_doc_freq=1,
            max_term_freq=100,
            max_doc_freq=1000,
            min_word_length=3,
            max_word_length=15,
            stopwords=["the", "a"],
        )
        assert mlt.min_term_freq == 2
        assert mlt.max_query_terms == 10
        assert mlt.min_word_length == 3
        assert mlt.max_word_length == 15
        assert mlt.stopwords == ["the", "a"]

    def test_mlt_stopwords_empty_list(self) -> None:
        """MLT with empty stopwords list works."""
        mlt = MoreLikeThis(product_id=1, stopwords=[])
        assert mlt.stopwords == []

    def test_mlt_stopwords_tuple(self) -> None:
        """MLT with stopwords as tuple converts to list."""
        mlt = MoreLikeThis(product_id=1, stopwords=("the", "a", "an"))
        assert mlt.stopwords == ["the", "a", "an"]

    def test_mlt_word_length_validation(self) -> None:
        """MLT word length parameters accept integers."""
        mlt = MoreLikeThis(
            product_id=1,
            min_word_length=2,
            max_word_length=20,
        )
        assert isinstance(mlt.min_word_length, int)
        assert isinstance(mlt.max_word_length, int)

    def test_mlt_numeric_validation(self) -> None:
        """MLT validates that numeric parameters are positive integers."""
        # Test min_term_freq
        with pytest.raises(ValueError, match="min_term_freq must be >= 1"):
            MoreLikeThis(product_id=1, min_term_freq=0)

        with pytest.raises(ValueError, match="min_term_freq must be >= 1"):
            MoreLikeThis(product_id=1, min_term_freq=-1)

        # Test max_query_terms
        with pytest.raises(ValueError, match="max_query_terms must be >= 1"):
            MoreLikeThis(product_id=1, max_query_terms=0)

        # Test min_word_length
        with pytest.raises(ValueError, match="min_word_length must be >= 1"):
            MoreLikeThis(product_id=1, min_word_length=0)

        # Test type validation
        with pytest.raises(TypeError, match="min_term_freq must be an integer"):
            MoreLikeThis(product_id=1, min_term_freq="5")  # type: ignore[arg-type]

        # Valid values should work
        mlt = MoreLikeThis(
            product_id=1,
            min_term_freq=1,
            max_query_terms=100,
            min_word_length=1,
        )
        assert mlt.min_term_freq == 1
        assert mlt.max_query_terms == 100
        assert mlt.min_word_length == 1

    def test_mlt_custom_key_field(self) -> None:
        """MLT accepts custom key_field parameter."""
        mlt = MoreLikeThis(product_id=1, key_field="custom_id")
        assert mlt.key_field == "custom_id"

        # Default should be None
        mlt = MoreLikeThis(product_id=1)
        assert mlt.key_field is None


class TestScoreEdgeCases:
    """Test Score annotation edge cases."""

    def test_score_with_custom_key_field(self) -> None:
        """Score can use custom key field."""
        score = Score(key_field="custom_id")
        assert score is not None

    def test_score_default_uses_pk(self) -> None:
        """Score defaults to pk field."""
        queryset = Product.objects.filter(description=ParadeDB("shoes")).annotate(
            s=Score()
        )
        sql = str(queryset.query)
        assert "pdb.score" in sql


class TestSnippetEdgeCases:
    """Test Snippet annotation edge cases."""

    def test_snippet_partial_formatting(self) -> None:
        """Snippet with only start_sel."""
        queryset = Product.objects.filter(description=ParadeDB("shoes")).annotate(
            s=Snippet("description", start_sel="<b>")
        )
        sql = str(queryset.query)
        assert "<b>" in sql

    def test_snippet_only_max_chars(self) -> None:
        """Snippet with only max_num_chars."""
        queryset = Product.objects.filter(description=ParadeDB("shoes")).annotate(
            s=Snippet("description", max_num_chars=50)
        )
        sql = str(queryset.query)
        assert "50" in sql


class TestBM25IndexEdgeCases:
    """Test BM25Index edge cases."""

    def test_index_single_field(self) -> None:
        """Index with single field works."""
        index = BM25Index(
            fields={"id": {}},
            key_field="id",
            name="test_idx",
        )
        assert index.key_field == "id"

    def test_index_many_fields(self) -> None:
        """Index with many fields works."""
        index = BM25Index(
            fields={
                "id": {},
                "description": {},
                "category": {},
                "rating": {},
                "metadata": {},
            },
            key_field="id",
            name="test_idx",
        )
        assert len(index.fields_config) == 5

    def test_index_deconstruct(self) -> None:
        """Index deconstruct for migrations works."""
        index = BM25Index(
            fields={"id": {}, "description": {}},
            key_field="id",
            name="test_idx",
        )
        path, _args, kwargs = index.deconstruct()
        assert "BM25Index" in path
        assert kwargs["key_field"] == "id"
        assert kwargs["name"] == "test_idx"


class TestParseOptions:
    """Test Parse query options."""

    def test_parse_lenient_true(self) -> None:
        """Parse with lenient=True generates correct SQL."""
        queryset = Product.objects.filter(
            description=ParadeDB(Parse("test", lenient=True))
        )
        sql = str(queryset.query)
        assert "lenient => true" in sql

    def test_parse_lenient_false(self) -> None:
        """Parse with lenient=False generates correct SQL."""
        queryset = Product.objects.filter(
            description=ParadeDB(Parse("test", lenient=False))
        )
        sql = str(queryset.query)
        assert "lenient => false" in sql

    def test_parse_no_lenient(self) -> None:
        """Parse without lenient omits the option."""
        queryset = Product.objects.filter(description=ParadeDB(Parse("test")))
        sql = str(queryset.query)
        assert "lenient" not in sql


class TestEmptyAndWhitespaceInputs:
    """Test handling of empty and whitespace inputs."""

    def test_empty_string_search(self) -> None:
        """Empty string search term works (ParadeDB handles it)."""
        queryset = Product.objects.filter(description=ParadeDB(""))
        sql = str(queryset.query)
        assert "&&& ''" in sql

    def test_whitespace_only_search(self) -> None:
        """Whitespace-only search term works."""
        queryset = Product.objects.filter(description=ParadeDB("   "))
        sql = str(queryset.query)
        assert "'   '" in sql

    def test_phrase_empty_string(self) -> None:
        """Phrase with empty string works."""
        queryset = Product.objects.filter(description=ParadeDB(Phrase("")))
        sql = str(queryset.query)
        assert "### ''" in sql

    def test_fuzzy_empty_string(self) -> None:
        """Fuzzy with empty string works."""
        queryset = Product.objects.filter(description=ParadeDB(Fuzzy("")))
        sql = str(queryset.query)
        assert "''::pdb.fuzzy" in sql


class TestLongInputs:
    """Test handling of very long inputs."""

    def test_very_long_search_term(self) -> None:
        """Very long search term is handled."""
        long_term = "a" * 10000
        queryset = Product.objects.filter(description=ParadeDB(long_term))
        sql = str(queryset.query)
        assert long_term in sql

    def test_many_or_terms(self) -> None:
        """Many OR terms work."""
        pq = PQ("term1")
        for i in range(2, 101):
            pq = pq | PQ(f"term{i}")
        queryset = Product.objects.filter(description=ParadeDB(pq))
        sql = str(queryset.query)
        assert "term100" in sql
        assert "|||" in sql

    def test_many_and_terms(self) -> None:
        """Many AND terms work."""
        queryset = Product.objects.filter(
            description=ParadeDB(*[f"term{i}" for i in range(100)])
        )
        sql = str(queryset.query)
        assert "term99" in sql
        assert "&&&" in sql


class TestParameterizedFieldValidation:
    """Test validation of parameterized fields in snippet functions."""

    @pytest.mark.parametrize("func_class", [Snippet, Snippets, SnippetPositions])
    def test_parameterized_field_raises(self, func_class) -> None:
        """Verify parameterized field check in as_sql for various functions."""
        instance = func_class("description")
        instance.source_expressions = [Value("parameterized")]
        compiler = Mock()
        compiler.compile.return_value = ("'parameterized'", ["parameterized"])
        with pytest.raises(ValueError, match="does not support parameterized fields"):
            instance.as_sql(compiler, Mock())


class TestSnippetsValidation:
    """Test validation specifically for Snippets function."""

    def test_snippets_invalid_sort_by_raises(self) -> None:
        """Verify sort_by validation."""
        with pytest.raises(ValueError, match="sort_by must be one of"):
            Snippets("description", sort_by="invalid")  # type: ignore[arg-type]
