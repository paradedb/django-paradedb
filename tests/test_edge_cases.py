"""Edge case tests for ParadeDB Django integration.

Tests for special characters, validation, boundary conditions, and unusual inputs.
These are unit tests that don't require a database.
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from unittest.mock import Mock

import pytest
from django.db.models import Value

import paradedb
from paradedb.functions import Score, Snippet, SnippetPositions, Snippets
from paradedb.indexes import BM25Index
from paradedb.search import (
    Empty,
    Exists,
    FuzzyTerm,
    Match,
    MoreLikeThis,
    ParadeDB,
    Parse,
    ParseWithField,
    Phrase,
    PhrasePrefix,
    Proximity,
    ProximityArray,
    ProximityRegex,
    ProxRegex,
    Range,
    Regex,
    RegexPhrase,
    Term,
    TermSet,
)
from tests.models import Product


class TestSpecialCharacterEscaping:
    """Test SQL injection prevention and special character handling."""

    def test_single_quote_in_search_term(self) -> None:
        """Single quotes are escaped to prevent SQL injection."""
        queryset = Product.objects.filter(
            description=ParadeDB(Match("it's", operator="AND"))
        )
        sql = str(queryset.query)
        assert "it''s" in sql

    def test_double_single_quotes(self) -> None:
        """Multiple single quotes are all escaped."""
        queryset = Product.objects.filter(
            description=ParadeDB(Match("don''t", operator="AND"))
        )
        sql = str(queryset.query)
        assert "don''''t" in sql

    def test_backslash_in_search_term(self) -> None:
        """Backslashes are preserved in search terms."""
        queryset = Product.objects.filter(
            description=ParadeDB(Match("path\\to\\file", operator="AND"))
        )
        sql = str(queryset.query)
        assert "path\\to\\file" in sql

    def test_unicode_characters(self) -> None:
        """Unicode characters work in search terms."""
        queryset = Product.objects.filter(
            description=ParadeDB(Match("日本語", operator="AND"))
        )
        sql = str(queryset.query)
        assert "日本語" in sql

    def test_emoji_in_search(self) -> None:
        """Emoji characters work in search terms."""
        queryset = Product.objects.filter(
            description=ParadeDB(Match("👟 shoes", operator="AND"))
        )
        sql = str(queryset.query)
        assert "👟 shoes" in sql

    def test_special_sql_keywords(self) -> None:
        """SQL keywords in search terms are quoted safely."""
        queryset = Product.objects.filter(
            description=ParadeDB(Match("SELECT * FROM", operator="AND"))
        )
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

    def test_phrase_cannot_mix_with_match(self) -> None:
        """Phrase mixed with Match raises TypeError on SQL generation."""
        pdb = ParadeDB(Phrase("a"), Match("b", operator="AND"))
        queryset = Product.objects.filter(description=pdb)
        with pytest.raises(ValueError, match="Match queries must be a single term"):
            str(queryset.query)

    def test_paradedb_invalid_tokenizer_deferred_to_database(self) -> None:
        """Tokenizer names are quoted in SQL; validity is deferred to database execution."""
        queryset = Product.objects.filter(
            description=ParadeDB(
                Match("shoes", operator="AND", tokenizer="bad-tokenizer;")
            )
        )
        assert '::pdb."bad-tokenizer;"' in str(queryset.query)


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

    def test_phrase_text_must_be_string(self) -> None:
        with pytest.raises(TypeError, match="Phrase text must be a string"):
            Phrase(123)  # type: ignore[arg-type]

    def test_phrase_slop_must_be_integer(self) -> None:
        with pytest.raises(TypeError, match="Phrase slop must be an integer"):
            Phrase("test", slop=True)  # type: ignore[arg-type]

    def test_phrase_tokenizer_must_be_string(self) -> None:
        with pytest.raises(TypeError, match="Phrase tokenizer must be a string"):
            Phrase("test", tokenizer=1)  # type: ignore[arg-type]

    def test_phrase_invalid_tokenizer_deferred_to_database(self) -> None:
        """Phrase tokenizer names are quoted in SQL; validity is deferred to database execution."""
        queryset = Product.objects.filter(
            description=ParadeDB(Phrase("test", tokenizer="bad tokenizer"))
        )
        assert '::pdb."bad tokenizer"' in str(queryset.query)


class TestDistanceValidation:
    """Test distance validation on Match and Term."""

    def test_match_default_distance(self) -> None:
        match = Match("test", operator="AND")
        assert match.distance is None

    def test_match_distance_zero_is_valid(self) -> None:
        match = Match("test", operator="AND", distance=0)
        assert match.distance == 0

    def test_match_negative_distance_raises(self) -> None:
        with pytest.raises(ValueError, match="between 0 and 2, inclusive"):
            Match("test", operator="AND", distance=-1)

    def test_match_large_distance_raises(self) -> None:
        with pytest.raises(ValueError, match="between 0 and 2, inclusive"):
            Match("test", operator="AND", distance=10)

    def test_match_distance_must_be_integer(self) -> None:
        with pytest.raises(TypeError, match="Distance must be an integer"):
            Match("test", operator="AND", distance=True)  # type: ignore[arg-type]

    def test_term_distance_validation(self) -> None:
        term = Term("test", distance=1)
        assert term.distance == 1

    def test_term_distance_must_be_integer(self) -> None:
        with pytest.raises(TypeError, match="Distance must be an integer"):
            Term("test", distance=True)  # type: ignore[arg-type]

    def test_match_tokenizer_and_fuzzy_options_rejected(self) -> None:
        with pytest.raises(
            ValueError, match="Match tokenizer cannot be combined with fuzzy options"
        ):
            Match("test", operator="AND", tokenizer="whitespace", distance=1)

    def test_match_multi_term_fuzzy_with_boost_rejected(self) -> None:
        with pytest.raises(
            ValueError, match="Multi-term fuzzy Match does not support boost or const"
        ):
            Match("a", "b", operator="OR", distance=1, boost=2.0)

    def test_match_multi_term_fuzzy_with_const_rejected(self) -> None:
        with pytest.raises(
            ValueError, match="Multi-term fuzzy Match does not support boost or const"
        ):
            Match("a", "b", operator="OR", distance=1, const=1.0)

    def test_term_non_string_rejected(self) -> None:
        with pytest.raises(TypeError, match="Term text must be a string"):
            Term(123)  # type: ignore[arg-type]

    def test_match_non_string_terms_rejected(self) -> None:
        with pytest.raises(TypeError, match="Match terms must be strings"):
            Match("ok", 123, operator="AND")  # type: ignore[arg-type]

    def test_match_non_string_tokenizer_rejected(self) -> None:
        with pytest.raises(TypeError, match="Match tokenizer must be a string"):
            Match("ok", operator="AND", tokenizer=123)  # type: ignore[arg-type]

    def test_match_prefix_must_be_boolean(self) -> None:
        with pytest.raises(TypeError, match="Match prefix must be a boolean"):
            Match("ok", operator="AND", prefix=1)  # type: ignore[arg-type]

    def test_term_prefix_must_be_boolean(self) -> None:
        with pytest.raises(TypeError, match="Term prefix must be a boolean"):
            Term("ok", prefix=1)  # type: ignore[arg-type]


class TestExpressionValidation:
    """Validation coverage for expression dataclasses."""

    def test_proximity_boost_and_const_deferred_to_database(self) -> None:
        proximity = Proximity("a b", distance=1, boost=1.0, const=1.0)
        assert proximity.boost == 1.0
        assert proximity.const == 1.0

    def test_proximity_text_must_be_string(self) -> None:
        with pytest.raises(TypeError, match="Proximity text must be a string"):
            Proximity(1, distance=1)  # type: ignore[arg-type]

    def test_proximity_distance_must_be_integer(self) -> None:
        with pytest.raises(TypeError, match="Proximity distance must be an integer"):
            Proximity("a b", distance=True)  # type: ignore[arg-type]

    def test_proximity_ordered_must_be_boolean(self) -> None:
        with pytest.raises(TypeError, match="Proximity ordered must be a boolean"):
            Proximity("a b", distance=1, ordered=1)  # type: ignore[arg-type]

    def test_parse_boost_and_const_deferred_to_database(self) -> None:
        parsed = Parse("query", boost=1.0, const=1.0)
        assert parsed.boost == 1.0
        assert parsed.const == 1.0

    def test_parse_query_must_be_string(self) -> None:
        with pytest.raises(TypeError, match="Parse query must be a string"):
            Parse(123)  # type: ignore[arg-type]

    def test_parse_lenient_must_be_boolean(self) -> None:
        with pytest.raises(TypeError, match="Parse lenient must be a boolean"):
            Parse("query", lenient="yes")  # type: ignore[arg-type]

    def test_regex_phrase_requires_terms(self) -> None:
        with pytest.raises(ValueError, match="requires at least one regex term"):
            RegexPhrase()

    def test_regex_phrase_negative_slop_raises(self) -> None:
        with pytest.raises(ValueError, match="slop must be zero or positive"):
            RegexPhrase("a.*", slop=-1)

    def test_regex_phrase_terms_must_be_strings(self) -> None:
        with pytest.raises(TypeError, match="RegexPhrase regex must be a string"):
            RegexPhrase(123)  # type: ignore[arg-type]

    def test_regex_phrase_max_expansions_must_be_integer(self) -> None:
        with pytest.raises(
            TypeError, match="RegexPhrase max_expansions must be an integer"
        ):
            RegexPhrase("a.*", max_expansions=True)  # type: ignore[arg-type]

    def test_regex_phrase_boost_and_const_deferred_to_database(self) -> None:
        regex_phrase = RegexPhrase("a.*", boost=1.0, const=1.0)
        assert regex_phrase.boost == 1.0
        assert regex_phrase.const == 1.0

    def test_phrase_prefix_terms_must_be_strings(self) -> None:
        with pytest.raises(TypeError, match="PhrasePrefix phrase must be a string"):
            PhrasePrefix("ok", 1)  # type: ignore[arg-type]

    def test_phrase_prefix_max_expansion_must_be_integer(self) -> None:
        with pytest.raises(
            TypeError, match="PhrasePrefix max_expansion must be an integer"
        ):
            PhrasePrefix("ok", "term", max_expansion=True)  # type: ignore[arg-type]

    def test_regex_pattern_must_be_string(self) -> None:
        with pytest.raises(TypeError, match="Regex pattern must be a string"):
            Regex(123)  # type: ignore[arg-type]

    def test_proximity_regex_negative_distance_raises(self) -> None:
        with pytest.raises(ValueError, match="distance must be zero or positive"):
            ProximityRegex("left", "right.*", distance=-1)

    def test_proximity_regex_negative_max_expansions_raises(self) -> None:
        with pytest.raises(ValueError, match="max_expansions must be zero or positive"):
            ProximityRegex("left", "right.*", distance=1, max_expansions=-1)

    def test_proximity_regex_boost_and_const_deferred_to_database(self) -> None:
        proximity_regex = ProximityRegex(
            "left", "right.*", distance=1, boost=1.0, const=1.0
        )
        assert proximity_regex.boost == 1.0
        assert proximity_regex.const == 1.0

    def test_proximity_array_requires_left_terms(self) -> None:
        with pytest.raises(ValueError, match="requires at least one left-side term"):
            ProximityArray(right_term="right", distance=1)

    def test_proximity_array_negative_distance_raises(self) -> None:
        with pytest.raises(ValueError, match="distance must be zero or positive"):
            ProximityArray("left", right_term="right", distance=-1)

    def test_proximity_array_negative_max_expansions_raises(self) -> None:
        with pytest.raises(ValueError, match="max_expansions must be zero or positive"):
            ProximityArray("left", right_term="right", distance=1, max_expansions=-1)

    def test_proximity_array_boost_and_const_deferred_to_database(self) -> None:
        proximity_array = ProximityArray(
            "left", right_term="right", distance=1, boost=1.0, const=1.0
        )
        assert proximity_array.boost == 1.0
        assert proximity_array.const == 1.0

    def test_proximity_array_accepts_prox_regex_items(self) -> None:
        proximity_array = ProximityArray(
            "chicken", ProxRegex("r..s"), right_term="delicious", distance=1
        )
        assert len(proximity_array.left_terms) == 2
        assert proximity_array.left_terms[0] == "chicken"
        assert isinstance(proximity_array.left_terms[1], ProxRegex)
        assert proximity_array.left_terms[1].pattern == "r..s"

    def test_prox_regex_negative_max_expansions_raises(self) -> None:
        with pytest.raises(ValueError, match="max_expansions must be zero or positive"):
            ProxRegex("pattern", max_expansions=-1)

    def test_prox_regex_max_expansions_must_be_integer(self) -> None:
        with pytest.raises(TypeError, match="max_expansions must be an integer"):
            ProxRegex("pattern", max_expansions=1.5)  # type: ignore[arg-type]

    def test_prox_regex_pattern_must_be_string(self) -> None:
        with pytest.raises(TypeError, match="pattern must be a string"):
            ProxRegex(123)  # type: ignore[arg-type]

    def test_prox_regex_defaults(self) -> None:
        prox_regex = ProxRegex("pattern")
        assert prox_regex.pattern == "pattern"
        assert prox_regex.max_expansions == 50

    def test_proximity_array_left_terms_must_be_strings_or_proxregex(self) -> None:
        with pytest.raises(
            TypeError,
            match="left_terms must be strings or ProxRegex instances",
        ):
            ProximityArray(123, right_term="right", distance=1)  # type: ignore[arg-type]


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

    def test_mlt_product_id_must_be_integer(self) -> None:
        with pytest.raises(TypeError, match="product_id must be an integer"):
            MoreLikeThis(product_id="1")  # type: ignore[arg-type]

        with pytest.raises(TypeError, match="product_id must be an integer"):
            MoreLikeThis(product_id=True)  # type: ignore[arg-type]

    def test_mlt_product_ids_list(self) -> None:
        """MLT with product_ids list works."""
        mlt = MoreLikeThis(product_ids=[1, 2, 3])
        assert mlt.product_ids == [1, 2, 3]

    def test_mlt_product_ids_must_contain_integers(self) -> None:
        with pytest.raises(TypeError, match="product_ids must contain integers"):
            MoreLikeThis(product_ids=[1, "2"])  # type: ignore[list-item]

        with pytest.raises(TypeError, match="product_ids must contain integers"):
            MoreLikeThis(product_ids=[1, True])  # type: ignore[list-item]

    def test_mlt_document_dict(self) -> None:
        """MLT with document dict works."""
        mlt = MoreLikeThis(document={"description": "running shoes"})
        assert mlt.document == '{"description": "running shoes"}'

    def test_mlt_document_string_must_be_valid_json_object(self) -> None:
        with pytest.raises(ValueError, match="must decode to an object"):
            MoreLikeThis(document='["not", "an", "object"]')

        with pytest.raises(ValueError, match="must be valid JSON"):
            MoreLikeThis(document="{bad json}")

    def test_mlt_document_with_fields_raises(self) -> None:
        """MLT with document and fields raises ValueError."""
        with pytest.raises(ValueError, match="fields are only valid"):
            MoreLikeThis(
                document={"description": "running shoes"}, fields=["description"]
            )

    def test_mlt_fields_must_be_non_empty_strings(self) -> None:
        with pytest.raises(ValueError, match="fields cannot be empty"):
            MoreLikeThis(product_id=1, fields=[])

        with pytest.raises(TypeError, match="fields must contain strings"):
            MoreLikeThis(product_id=1, fields=[1])  # type: ignore[list-item]

        with pytest.raises(ValueError, match="fields cannot contain empty names"):
            MoreLikeThis(product_id=1, fields=[""])

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

    def test_mlt_stopwords_must_be_strings(self) -> None:
        with pytest.raises(TypeError, match="stopwords must contain strings"):
            MoreLikeThis(product_id=1, stopwords=["the", 1])  # type: ignore[list-item]

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

        with pytest.raises(TypeError, match="min_term_freq must be an integer"):
            MoreLikeThis(product_id=1, min_term_freq=True)  # type: ignore[arg-type]

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

    def test_mlt_key_field_validation(self) -> None:
        with pytest.raises(TypeError, match="key_field must be a string"):
            MoreLikeThis(product_id=1, key_field=1)  # type: ignore[arg-type]

        with pytest.raises(ValueError, match="key_field cannot be empty"):
            MoreLikeThis(product_id=1, key_field="")


class TestScoreEdgeCases:
    """Test Score annotation edge cases."""

    def test_score_with_custom_key_field(self) -> None:
        """Score can use custom key field."""
        score = Score(key_field="custom_id")
        assert score is not None

    def test_score_default_uses_pk(self) -> None:
        """Score defaults to pk field."""
        queryset = Product.objects.filter(
            description=ParadeDB(Match("shoes", operator="AND"))
        ).annotate(s=Score())
        sql = str(queryset.query)
        assert "pdb.score" in sql


class TestSnippetEdgeCases:
    """Test Snippet annotation edge cases."""

    def test_snippet_partial_formatting(self) -> None:
        """Snippet with only start_sel."""
        queryset = Product.objects.filter(
            description=ParadeDB(Match("shoes", operator="AND"))
        ).annotate(s=Snippet("description", start_sel="<b>"))
        sql = str(queryset.query)
        assert "<b>" in sql

    def test_snippet_only_max_chars(self) -> None:
        """Snippet with only max_num_chars."""
        queryset = Product.objects.filter(
            description=ParadeDB(Match("shoes", operator="AND"))
        ).annotate(s=Snippet("description", max_num_chars=50))
        sql = str(queryset.query)
        assert "50" in sql

    def test_snippet_max_num_chars_must_be_integer(self) -> None:
        with pytest.raises(TypeError, match="max_num_chars must be an integer"):
            Snippet("description", max_num_chars="50")  # type: ignore[arg-type]

    def test_snippets_limit_must_be_integer(self) -> None:
        with pytest.raises(TypeError, match="limit must be an integer"):
            Snippets("description", limit="1")  # type: ignore[arg-type]

    def test_snippets_offset_must_be_integer(self) -> None:
        with pytest.raises(TypeError, match="offset must be an integer"):
            Snippets("description", offset="1")  # type: ignore[arg-type]


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
        queryset = Product.objects.filter(
            description=ParadeDB(Match("", operator="AND"))
        )
        sql = str(queryset.query)
        assert "&&& ''" in sql

    def test_whitespace_only_search(self) -> None:
        """Whitespace-only search term works."""
        queryset = Product.objects.filter(
            description=ParadeDB(Match("   ", operator="AND"))
        )
        sql = str(queryset.query)
        assert "'   '" in sql

    def test_phrase_empty_string(self) -> None:
        """Phrase with empty string works."""
        queryset = Product.objects.filter(description=ParadeDB(Phrase("")))
        sql = str(queryset.query)
        assert "### ''" in sql

    def test_match_distance_empty_string(self) -> None:
        """Match with distance and empty string works."""
        queryset = Product.objects.filter(
            description=ParadeDB(Match("", operator="OR", distance=1))
        )
        sql = str(queryset.query)
        assert "''::pdb.fuzzy" in sql


class TestLongInputs:
    """Test handling of very long inputs."""

    def test_very_long_search_term(self) -> None:
        """Very long search term is handled."""
        long_term = "a" * 10000
        queryset = Product.objects.filter(
            description=ParadeDB(Match(long_term, operator="AND"))
        )
        sql = str(queryset.query)
        assert long_term in sql

    def test_many_or_terms(self) -> None:
        """Many OR terms work."""
        queryset = Product.objects.filter(
            description=ParadeDB(
                Match(*[f"term{i}" for i in range(1, 101)], operator="OR")
            )
        )
        sql = str(queryset.query)
        assert "term100" in sql
        assert "|||" in sql

    def test_many_and_terms(self) -> None:
        """Many AND terms work."""
        queryset = Product.objects.filter(
            description=ParadeDB(
                Match(*[f"term{i}" for i in range(100)], operator="AND")
            )
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


class TestEmptyExpression:
    """Test Empty dataclass construction."""

    def test_empty_defaults(self) -> None:
        expr = Empty()
        assert expr.boost is None
        assert expr.const is None

    def test_empty_with_boost(self) -> None:
        expr = Empty(boost=1.5)
        assert expr.boost == 1.5

    def test_empty_with_const(self) -> None:
        expr = Empty(const=2.0)
        assert expr.const == 2.0

    def test_empty_with_boost_and_const(self) -> None:
        expr = Empty(boost=1.5, const=2.0)
        assert expr.boost == 1.5
        assert expr.const == 2.0

    def test_empty_is_frozen(self) -> None:
        expr = Empty()
        with pytest.raises(AttributeError):
            expr.boost = 1.0  # type: ignore[misc]


class TestExistsExpression:
    """Test Exists dataclass construction."""

    def test_exists_defaults(self) -> None:
        expr = Exists()
        assert expr.boost is None
        assert expr.const is None

    def test_exists_with_boost(self) -> None:
        expr = Exists(boost=3.0)
        assert expr.boost == 3.0

    def test_exists_with_const(self) -> None:
        expr = Exists(const=1.0)
        assert expr.const == 1.0

    def test_exists_with_boost_and_const(self) -> None:
        expr = Exists(boost=3.0, const=1.0)
        assert expr.boost == 3.0
        assert expr.const == 1.0

    def test_exists_is_frozen(self) -> None:
        expr = Exists()
        with pytest.raises(AttributeError):
            expr.const = 1.0  # type: ignore[misc]


class TestFuzzyTermExpression:
    """Test FuzzyTerm dataclass construction and validation."""

    def test_fuzzy_term_defaults(self) -> None:
        expr = FuzzyTerm()
        assert expr.value is None
        assert expr.distance is None
        assert expr.transposition_cost_one is None
        assert expr.prefix is None
        assert expr.boost is None
        assert expr.const is None

    def test_fuzzy_term_with_value(self) -> None:
        expr = FuzzyTerm(value="shoes")
        assert expr.value == "shoes"

    def test_fuzzy_term_value_must_be_string(self) -> None:
        with pytest.raises(TypeError, match="FuzzyTerm value must be a string"):
            FuzzyTerm(value=123)  # type: ignore[arg-type]

    def test_fuzzy_term_with_all_options(self) -> None:
        expr = FuzzyTerm(
            value="shoes",
            distance=2,
            transposition_cost_one=True,
            prefix=True,
            boost=1.5,
            const=2.0,
        )
        assert expr.value == "shoes"
        assert expr.distance == 2
        assert expr.transposition_cost_one is True
        assert expr.prefix is True
        assert expr.boost == 1.5
        assert expr.const == 2.0

    def test_fuzzy_term_distance_zero_is_valid(self) -> None:
        expr = FuzzyTerm(value="test", distance=0)
        assert expr.distance == 0

    def test_fuzzy_term_distance_one_is_valid(self) -> None:
        expr = FuzzyTerm(value="test", distance=1)
        assert expr.distance == 1

    def test_fuzzy_term_distance_two_is_valid(self) -> None:
        expr = FuzzyTerm(value="test", distance=2)
        assert expr.distance == 2

    def test_fuzzy_term_negative_distance_raises(self) -> None:
        with pytest.raises(ValueError, match="between 0 and 2, inclusive"):
            FuzzyTerm(value="test", distance=-1)

    def test_fuzzy_term_distance_three_raises(self) -> None:
        with pytest.raises(ValueError, match="between 0 and 2, inclusive"):
            FuzzyTerm(value="test", distance=3)

    def test_fuzzy_term_large_distance_raises(self) -> None:
        with pytest.raises(ValueError, match="between 0 and 2, inclusive"):
            FuzzyTerm(value="test", distance=100)

    def test_fuzzy_term_distance_must_be_integer(self) -> None:
        with pytest.raises(TypeError, match="Distance must be an integer"):
            FuzzyTerm(value="test", distance=True)  # type: ignore[arg-type]

    def test_fuzzy_term_prefix_must_be_boolean(self) -> None:
        with pytest.raises(TypeError, match="FuzzyTerm prefix must be a boolean"):
            FuzzyTerm(value="test", prefix=1)  # type: ignore[arg-type]

    def test_fuzzy_term_is_frozen(self) -> None:
        expr = FuzzyTerm(value="shoes")
        with pytest.raises(AttributeError):
            expr.value = "boots"  # type: ignore[misc]


class TestParseWithFieldExpression:
    """Test ParseWithField dataclass construction."""

    def test_parse_with_field_required_query(self) -> None:
        expr = ParseWithField(query="running AND shoes")
        assert expr.query == "running AND shoes"
        assert expr.lenient is None
        assert expr.conjunction_mode is None
        assert expr.boost is None
        assert expr.const is None

    def test_parse_with_field_all_options(self) -> None:
        expr = ParseWithField(
            query="shoes",
            lenient=True,
            conjunction_mode=True,
            boost=2.0,
            const=1.0,
        )
        assert expr.query == "shoes"
        assert expr.lenient is True
        assert expr.conjunction_mode is True
        assert expr.boost == 2.0
        assert expr.const == 1.0

    def test_parse_with_field_lenient_false(self) -> None:
        expr = ParseWithField(query="test", lenient=False)
        assert expr.lenient is False

    def test_parse_with_field_conjunction_mode_false(self) -> None:
        expr = ParseWithField(query="test", conjunction_mode=False)
        assert expr.conjunction_mode is False

    def test_parse_with_field_query_must_be_string(self) -> None:
        with pytest.raises(TypeError, match="ParseWithField query must be a string"):
            ParseWithField(query=1)  # type: ignore[arg-type]

    def test_parse_with_field_lenient_must_be_boolean(self) -> None:
        with pytest.raises(TypeError, match="ParseWithField lenient must be a boolean"):
            ParseWithField(query="test", lenient=1)  # type: ignore[arg-type]

    def test_parse_with_field_is_frozen(self) -> None:
        expr = ParseWithField(query="test")
        with pytest.raises(AttributeError):
            expr.query = "other"  # type: ignore[misc]

    def test_parse_with_field_missing_query_raises(self) -> None:
        with pytest.raises(TypeError):
            ParseWithField()  # type: ignore[call-arg]


class TestRangeExpression:
    """Test Range dataclass construction and validation."""

    def test_range_int4range(self) -> None:
        expr = Range(range="[1, 10]", range_type="int4range")
        assert expr.range == "[1, 10]"
        assert expr.range_type == "int4range"
        assert expr.boost is None
        assert expr.const is None

    def test_range_all_valid_types(self) -> None:
        for rt in (
            "int4range",
            "int8range",
            "numrange",
            "daterange",
            "tsrange",
            "tstzrange",
        ):
            expr = Range(range="[1, 10]", range_type=rt)  # type: ignore[arg-type]
            assert expr.range_type == rt

    def test_range_invalid_type_raises(self) -> None:
        with pytest.raises(ValueError, match="Range type must be one of"):
            Range(range="[1, 10]", range_type="badtype")  # type: ignore[arg-type]

    def test_range_literal_must_be_string(self) -> None:
        with pytest.raises(TypeError, match="Range range must be a string"):
            Range(range=10, range_type="int4range")  # type: ignore[arg-type]

    def test_range_with_boost_and_const(self) -> None:
        expr = Range(range="(0, 100)", range_type="numrange", boost=1.5, const=2.0)
        assert expr.boost == 1.5
        assert expr.const == 2.0

    def test_range_is_frozen(self) -> None:
        expr = Range(range="[1, 10]", range_type="int4range")
        with pytest.raises(AttributeError):
            expr.range = "[2, 20]"  # type: ignore[misc]

    def test_range_missing_args_raises(self) -> None:
        with pytest.raises(TypeError):
            Range()  # type: ignore[call-arg]

    def test_range_missing_range_type_raises(self) -> None:
        with pytest.raises(TypeError):
            Range(range="[1, 10]")  # type: ignore[call-arg]


class TestTermSetExpression:
    """Test TermSet dataclass construction and validation."""

    def test_term_set_single_string(self) -> None:
        expr = TermSet("shoes")
        assert expr.terms == ("shoes",)

    def test_term_set_multiple_strings(self) -> None:
        expr = TermSet("shoes", "boots", "sandals")
        assert expr.terms == ("shoes", "boots", "sandals")

    def test_term_set_integers(self) -> None:
        expr = TermSet(1, 2, 3)
        assert expr.terms == (1, 2, 3)

    def test_term_set_floats(self) -> None:
        expr = TermSet(1.0, 2.5, 3.7)
        assert expr.terms == (1.0, 2.5, 3.7)

    def test_term_set_booleans(self) -> None:
        expr = TermSet(True, False)
        assert expr.terms == (True, False)

    def test_term_set_empty_raises(self) -> None:
        with pytest.raises(ValueError, match="requires at least one term"):
            TermSet()

    def test_term_set_with_boost(self) -> None:
        expr = TermSet("a", "b", boost=2.0)
        assert expr.boost == 2.0
        assert expr.terms == ("a", "b")

    def test_term_set_with_const(self) -> None:
        expr = TermSet("a", const=1.0)
        assert expr.const == 1.0

    def test_term_set_with_boost_and_const(self) -> None:
        expr = TermSet("a", "b", boost=2.0, const=1.0)
        assert expr.boost == 2.0
        assert expr.const == 1.0

    def test_term_set_is_frozen(self) -> None:
        expr = TermSet("a", "b")
        with pytest.raises(AttributeError):
            expr.terms = ("c",)  # type: ignore[misc]

    def test_term_set_terms_is_tuple(self) -> None:
        expr = TermSet("a", "b", "c")
        assert isinstance(expr.terms, tuple)

    def test_term_set_mixed_types_raise(self) -> None:
        with pytest.raises(TypeError, match="must all have the same type"):
            TermSet("a", 1)  # type: ignore[arg-type]

    def test_term_set_mixed_date_datetime_raise(self) -> None:
        with pytest.raises(TypeError, match="must all have the same type"):
            TermSet(date.today(), datetime.now(tz=timezone.utc))


class TestNewQueryTypeValidation:
    """Test that new query types pass through _resolve_terms without TypeError."""

    def test_empty_does_not_raise_type_error(self) -> None:
        """ParadeDB(Empty()) should not raise TypeError from _resolve_terms."""
        queryset = Product.objects.filter(description=ParadeDB(Empty()))
        sql = str(queryset.query)
        assert "pdb.empty()" in sql

    def test_exists_does_not_raise_type_error(self) -> None:
        queryset = Product.objects.filter(description=ParadeDB(Exists()))
        sql = str(queryset.query)
        assert "pdb.exists()" in sql

    def test_fuzzy_term_does_not_raise_type_error(self) -> None:
        queryset = Product.objects.filter(description=ParadeDB(FuzzyTerm(value="test")))
        sql = str(queryset.query)
        assert "pdb.fuzzy_term" in sql

    def test_parse_with_field_does_not_raise_type_error(self) -> None:
        queryset = Product.objects.filter(
            description=ParadeDB(ParseWithField(query="test"))
        )
        sql = str(queryset.query)
        assert "pdb.parse_with_field" in sql

    def test_range_does_not_raise_type_error(self) -> None:
        queryset = Product.objects.filter(
            description=ParadeDB(Range(range="[1, 10]", range_type="int4range"))
        )
        sql = str(queryset.query)
        assert "pdb.range" in sql

    def test_term_set_does_not_raise_type_error(self) -> None:
        queryset = Product.objects.filter(description=ParadeDB(TermSet("a", "b")))
        sql = str(queryset.query)
        assert "pdb.term_set" in sql


class TestScoringValidation:
    def test_boost_rejects_non_numeric_input(self) -> None:
        queryset = Product.objects.filter(
            description=ParadeDB(
                Match("shoes", operator="AND", boost="1.0)::pdb.const(10")  # type: ignore[arg-type]
            )
        )
        with pytest.raises(TypeError, match="boost must be an int or float"):
            _ = str(queryset.query)

    def test_const_rejects_non_finite_input(self) -> None:
        queryset = Product.objects.filter(
            description=ParadeDB(Match("shoes", operator="AND", const=float("inf")))
        )
        with pytest.raises(ValueError, match="const must be finite"):
            _ = str(queryset.query)


class TestSnippetsValidation:
    """Test validation specifically for Snippets function."""

    def test_snippets_invalid_sort_by_raises(self) -> None:
        """Verify sort_by validation."""
        with pytest.raises(ValueError, match="sort_by must be one of"):
            Snippets("description", sort_by="invalid")  # type: ignore[arg-type]


class TestTopLevelExports:
    def test_query_and_manager_exports_are_available(self) -> None:
        expected_exports = [
            "Empty",
            "Exists",
            "FuzzyTerm",
            "ParseWithField",
            "Range",
            "RangeRelation",
            "RangeType",
            "TermSet",
            "ParadeOperator",
            "ParadeDBManager",
            "ParadeDBQuerySet",
        ]
        for name in expected_exports:
            assert hasattr(paradedb, name)
