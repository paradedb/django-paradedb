"""Additional edge case coverage for annotations, query expressions, and indexes."""

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
    ParadeDB,
    Parse,
    ParseWithField,
    Phrase,
    Range,
    TermSet,
)
from tests.models import Product


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
