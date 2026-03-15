"""Edge case tests for ParadeDB Django integration.

Tests for special characters, validation, boundary conditions, and unusual inputs.
These are unit tests that don't require a database.
"""

from __future__ import annotations

import pytest

from paradedb.search import (
    Match,
    MoreLikeThis,
    ParadeDB,
    Parse,
    Phrase,
    PhrasePrefix,
    Proximity,
    ProximityArray,
    ProximityRegex,
    ProxRegex,
    Regex,
    RegexPhrase,
    Term,
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
