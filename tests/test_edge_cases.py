"""Edge case tests for ParadeDB Django integration.

Tests for special characters, validation, boundary conditions, and unusual inputs.
These are unit tests that don't require a database.
"""

from __future__ import annotations

import pytest

from paradedb.search import (
    Boost,
    Const,
    MatchAll,
    MoreLikeThis,
    ParadeDB,
    Parse,
    Phrase,
    PhrasePrefix,
    Proximity,
    ProximityNode,
    ProxRegex,
    Regex,
    RegexPhrase,
)
from tests.models import MockItem


class TestSpecialCharacterEscaping:
    """Test SQL injection prevention and special character handling."""

    def test_single_quote_in_search_term(self) -> None:
        """Single quotes are escaped to prevent SQL injection."""
        queryset = MockItem.objects.filter(description=ParadeDB(MatchAll("it's")))
        sql = str(queryset.query)
        assert "it''s" in sql

    def test_double_single_quotes(self) -> None:
        """Multiple single quotes are all escaped."""
        queryset = MockItem.objects.filter(description=ParadeDB(MatchAll("don''t")))
        sql = str(queryset.query)
        assert "don''''t" in sql

    def test_backslash_in_search_term(self) -> None:
        """Backslashes are preserved in search terms."""
        queryset = MockItem.objects.filter(
            description=ParadeDB(MatchAll("path\\to\\file"))
        )
        sql = str(queryset.query)
        assert "path\\to\\file" in sql

    def test_unicode_characters(self) -> None:
        """Unicode characters work in search terms."""
        queryset = MockItem.objects.filter(description=ParadeDB(MatchAll("日本語")))
        sql = str(queryset.query)
        assert "日本語" in sql

    def test_emoji_in_search(self) -> None:
        """Emoji characters work in search terms."""
        queryset = MockItem.objects.filter(description=ParadeDB(MatchAll("👟 shoes")))
        sql = str(queryset.query)
        assert "👟 shoes" in sql

    def test_special_sql_keywords(self) -> None:
        """SQL keywords in search terms are quoted safely."""
        queryset = MockItem.objects.filter(
            description=ParadeDB(MatchAll("SELECT * FROM"))
        )
        sql = str(queryset.query)
        assert "'SELECT * FROM'" in sql

    def test_phrase_with_quotes(self) -> None:
        """Phrase containing quotes is escaped."""
        queryset = MockItem.objects.filter(
            description=ParadeDB(Phrase('it\'s a "test"'))
        )
        sql = str(queryset.query)
        assert "it''s" in sql

    def test_regex_special_chars_preserved(self) -> None:
        """Regex special characters are preserved."""
        queryset = MockItem.objects.filter(
            description=ParadeDB(Regex("test.*[a-z]+\\d{2,3}"))
        )
        sql = str(queryset.query)
        assert "test.*[a-z]+\\d{2,3}" in sql


class TestExpressionValidation:
    """Validation coverage for expression dataclasses."""

    def test_proximity_boost_and_const_wrap_query_node(self) -> None:
        proximity = Proximity("a").within(1, "b")
        boosted = Boost(proximity, 1.0)
        constant = Const(proximity, 1.0)
        assert boosted.value == proximity
        assert boosted.factor == 1.0
        assert constant.value == proximity
        assert constant.score == 1.0

    def test_proximity_start_must_be_string_or_proxregex(self) -> None:
        with pytest.raises(
            TypeError, match="Proximity term must be strings or ProxRegex instances"
        ):
            Proximity(1)  # type: ignore[arg-type]

    def test_parse_boost_and_const_deferred_to_database(self) -> None:
        parsed = Const(Boost(Parse("query"), 1.0), 1.0)
        assert parsed.value.factor == 1.0
        assert parsed.score == 1.0

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
        regex_phrase = Const(Boost(RegexPhrase("a.*"), 1.0), 1.0)
        assert regex_phrase.value.factor == 1.0
        assert regex_phrase.score == 1.0

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

    def test_proximity_negative_distance_raises(self) -> None:
        with pytest.raises(
            ValueError, match="Proximity distance must be zero or positive"
        ):
            Proximity("left").within(-1, ProxRegex("right.*"))

    def test_prox_regex_negative_max_expansions_in_chain_raises(self) -> None:
        with pytest.raises(ValueError, match="max_expansions must be zero or positive"):
            Proximity("left").within(1, ProxRegex("right.*", max_expansions=-1))

    def test_proximity_with_prox_regex_boost_and_const_wrap_query_node(self) -> None:
        proximity = Proximity("left").within(1, ProxRegex("right.*"))
        assert Boost(proximity, 1.0).factor == 1.0
        assert Const(proximity, 1.0).score == 1.0

    def test_proximity_allows_empty_start_term_list(self) -> None:
        proximity = Proximity([])
        assert proximity.term == []

    def test_proximity_then_allows_empty_term_list(self) -> None:
        proximity = Proximity("left").within(1, [])
        assert proximity.right == []

    def test_proximity_then_negative_distance_raises(self) -> None:
        with pytest.raises(
            ValueError, match="Proximity distance must be zero or positive"
        ):
            Proximity("left").within(-1, "right")

    def test_proximity_then_preserves_non_boolean_ordered_value(self) -> None:
        proximity = Proximity("left").within(1, "right", ordered=1)  # type: ignore[arg-type]
        assert proximity.ordered == 1

    def test_proximity_boost_and_const_with_step_wrap_query_node(self) -> None:
        proximity = Proximity("left").within(1, "right")
        assert Boost(proximity, 1.0).value == proximity
        assert Const(proximity, 1.0).value == proximity

    def test_proximity_accepts_prox_regex_items_in_start(self) -> None:
        proximity = Proximity(["chicken", ProxRegex("r..s")]).within(1, "delicious")
        assert isinstance(proximity.left, list)
        assert len(proximity.left) == 2
        assert proximity.left[0] == "chicken"
        assert isinstance(proximity.left[1], ProxRegex)
        assert proximity.left[1].pattern == "r..s"

    def test_proximity_accepts_term_lists_in_step(self) -> None:
        proximity = Proximity("chicken").within(1, ["delicious", ProxRegex("cris.*")])
        assert isinstance(proximity.right, list)
        assert len(proximity.right) == 2
        assert proximity.right[0] == "delicious"
        assert isinstance(proximity.right[1], ProxRegex)
        assert proximity.right[1].pattern == "cris.*"

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
        assert prox_regex.max_expansions is None

    def test_proximity_start_must_be_strings_or_proxregex(self) -> None:
        with pytest.raises(
            TypeError,
            match="term must be strings or ProxRegex instances",
        ):
            Proximity(123)  # type: ignore[arg-type]

    def test_proximity_step_preserves_unvalidated_term_value(self) -> None:
        proximity = Proximity("left").within(1, 123)  # type: ignore[arg-type]
        assert proximity.right == 123

    def test_proximity_chain_builder_preserves_ordering(self) -> None:
        chain = Proximity("left").within(
            1,
            ["middle", ProxRegex("r.*")],
            ordered=True,
        )
        assert chain.ordered is True

    def test_then_accepts_explicit_grouped_proximity_child(self) -> None:
        grouped = ProximityNode(
            1,
            False,
            "middle",
            ProximityNode(2, True, "right", "tail"),
        )
        chain = Proximity("left").within(3, grouped)
        assert chain.right == grouped
        assert chain.left == "left"
        assert chain.distance == 3
        assert chain.ordered is False

    def test_proximity_node_accepts_nested_proximity_children(self) -> None:
        child = ProximityNode(2, True, "right", "tail")
        node = ProximityNode(1, False, "left", child)
        assert node.left == "left"
        assert node.right == child
        assert node.distance == 1

    def test_root_proximity_rejects_step_only_options(self) -> None:
        with pytest.raises(TypeError):
            Proximity("left", ordered=True)  # type: ignore[call-arg]

    def test_proximity_step_requires_distance(self) -> None:
        with pytest.raises(TypeError):
            ProximityNode("left")  # type: ignore[call-arg]

    def test_top_level_proximity_step_renders_directly(self) -> None:
        queryset = MockItem.objects.filter(
            description=ParadeDB(ProximityNode(1, False, "right", "tail"))
        )
        assert (
            str(queryset.query)
            == 'SELECT "mock_items"."id", "mock_items"."description", "mock_items"."category", "mock_items"."rating", "mock_items"."in_stock", "mock_items"."created_at", "mock_items"."metadata" FROM "mock_items" WHERE "mock_items"."description" @@@ (\'right\' ## 1 ## \'tail\')'
        )


class TestMoreLikeThisValidation:
    """Test MoreLikeThis validation."""

    def test_mlt_requires_one_input(self) -> None:
        """MLT with no inputs raises ValueError."""
        with pytest.raises(ValueError, match="exactly one input"):
            MoreLikeThis()

    def test_mlt_multiple_inputs_raises(self) -> None:
        """MLT with multiple inputs raises ValueError."""
        with pytest.raises(ValueError, match="exactly one input"):
            MoreLikeThis(id=1, document={"description": "test"})

    def test_mlt_single_id(self) -> None:
        """MLT with single id works."""
        mlt = MoreLikeThis(id=1)
        assert mlt.id == 1

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
            MoreLikeThis(id=1, fields=[])

        with pytest.raises(TypeError, match="fields must contain strings"):
            MoreLikeThis(id=1, fields=[1])  # type: ignore[list-item]

        with pytest.raises(ValueError, match="fields cannot contain empty names"):
            MoreLikeThis(id=1, fields=[""])

    def test_mlt_with_all_options(self) -> None:
        """MLT with all tuning options works."""
        mlt = MoreLikeThis(
            id=1,
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
        mlt = MoreLikeThis(id=1, stopwords=[])
        assert mlt.stopwords == []

    def test_mlt_stopwords_tuple(self) -> None:
        """MLT with stopwords as tuple converts to list."""
        mlt = MoreLikeThis(id=1, stopwords=("the", "a", "an"))
        assert mlt.stopwords == ["the", "a", "an"]

    def test_mlt_stopwords_must_be_strings(self) -> None:
        with pytest.raises(TypeError, match="stopwords must contain strings"):
            MoreLikeThis(id=1, stopwords=["the", 1])  # type: ignore[list-item]

    def test_mlt_word_length_validation(self) -> None:
        """MLT word length parameters accept integers."""
        mlt = MoreLikeThis(
            id=1,
            min_word_length=2,
            max_word_length=20,
        )
        assert isinstance(mlt.min_word_length, int)
        assert isinstance(mlt.max_word_length, int)

    def test_mlt_numeric_validation(self) -> None:
        """MLT validates that numeric parameters are positive integers."""
        # Test min_term_freq
        with pytest.raises(ValueError, match="min_term_freq must be >= 1"):
            MoreLikeThis(id=1, min_term_freq=0)

        with pytest.raises(ValueError, match="min_term_freq must be >= 1"):
            MoreLikeThis(id=1, min_term_freq=-1)

        # Test max_query_terms
        with pytest.raises(ValueError, match="max_query_terms must be >= 1"):
            MoreLikeThis(id=1, max_query_terms=0)

        # Test min_word_length
        with pytest.raises(ValueError, match="min_word_length must be >= 1"):
            MoreLikeThis(id=1, min_word_length=0)

        # Test type validation
        with pytest.raises(TypeError, match="min_term_freq must be an integer"):
            MoreLikeThis(id=1, min_term_freq="5")  # type: ignore[arg-type]

        with pytest.raises(TypeError, match="min_term_freq must be an integer"):
            MoreLikeThis(id=1, min_term_freq=True)  # type: ignore[arg-type]

        # Valid values should work
        mlt = MoreLikeThis(
            id=1,
            min_term_freq=1,
            max_query_terms=100,
            min_word_length=1,
        )
        assert mlt.min_term_freq == 1
        assert mlt.max_query_terms == 100
        assert mlt.min_word_length == 1

    def test_mlt_custom_key_field(self) -> None:
        """MLT accepts custom key_field parameter."""
        mlt = MoreLikeThis(id=1, key_field="custom_id")
        assert mlt.key_field == "custom_id"

        # Default should be None
        mlt = MoreLikeThis(id=1)
        assert mlt.key_field is None

    def test_mlt_key_field_validation(self) -> None:
        with pytest.raises(TypeError, match="key_field must be a string"):
            MoreLikeThis(id=1, key_field=1)  # type: ignore[arg-type]

        with pytest.raises(ValueError, match="key_field cannot be empty"):
            MoreLikeThis(id=1, key_field="")
