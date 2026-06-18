"""ParadeDB search expressions and lookups."""

from __future__ import annotations

import json
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Literal, TypeAlias

from django.db.backends.base.base import BaseDatabaseWrapper
from django.db.models import (
    AutoField,
    BigAutoField,
    BigIntegerField,
    BooleanField,
    CharField,
    Field,
    IntegerField,
    SmallIntegerField,
    TextField,
    UUIDField,
)
from django.db.models.expressions import Expression
from django.db.models.lookups import Exact
from django.db.models.sql.compiler import SQLCompiler
from django.utils.deconstruct import deconstructible

from paradedb.api import (
    FN_ALL,
    FN_EXISTS,
    FN_FUZZY_TERM,
    FN_MORE_LIKE_THIS,
    FN_PARSE,
    FN_PHRASE_PREFIX,
    FN_PROX_ARRAY,
    FN_PROX_REGEX,
    FN_RANGE_TERM,
    FN_REGEX,
    FN_REGEX_PHRASE,
    FN_TERM,
    FN_TERM_SET,
    OP_AND,
    OP_OR,
    OP_PHRASE,
    OP_PROXIMITY,
    OP_PROXIMITY_ORD,
    OP_SEARCH,
    PDB_TYPE_BOOST,
    PDB_TYPE_CONST,
    PDB_TYPE_FUZZY,
    PDB_TYPE_SLOP,
    PDB_TYPE_TOKENIZER_CHINESE_COMPATIBLE,
    PDB_TYPE_TOKENIZER_EDGE_NGRAM,
    PDB_TYPE_TOKENIZER_ICU,
    PDB_TYPE_TOKENIZER_JIEBA,
    PDB_TYPE_TOKENIZER_LINDERA,
    PDB_TYPE_TOKENIZER_LITERAL,
    PDB_TYPE_TOKENIZER_LITERAL_NORMALIZED,
    PDB_TYPE_TOKENIZER_NGRAM,
    PDB_TYPE_TOKENIZER_REGEX,
    PDB_TYPE_TOKENIZER_SIMPLE,
    PDB_TYPE_TOKENIZER_SOURCE_CODE,
    PDB_TYPE_TOKENIZER_UNICODE_WORDS,
    PDB_TYPE_TOKENIZER_WHITESPACE,
)

SearchValue: TypeAlias = "str | list[str] | tuple[str, ...] | Modifier | Expression"
Modifiable: TypeAlias = "SearchValue | QueryExpression"


@dataclass(frozen=True)
class Modifier:
    value: Modifiable


@dataclass(frozen=True)
class Boost(Modifier):
    factor: float


@dataclass(frozen=True)
class Const(Modifier):
    score: float


@dataclass(frozen=True)
class Fuzzy(Modifier):
    distance: int
    prefix: bool | None = None
    transposition_cost_one: bool | None = None

    def __post_init__(self) -> None:
        _validate_fuzzy_distance(self.distance)
        _validate_optional_bool("fuzzy prefix", self.prefix)
        _validate_optional_bool(
            "fuzzy transposition_cost_one", self.transposition_cost_one
        )


@dataclass(frozen=True)
class Slop(Modifier):
    distance: int

    def __post_init__(self) -> None:
        _validate_non_negative_int("slop distance", self.distance)


@dataclass(frozen=True)
class Tokenized(Modifier):
    tokenizer: Tokenizer


def _validate_fuzzy_distance(distance: int | None) -> None:
    if distance is None:
        return
    if isinstance(distance, bool) or not isinstance(distance, int):
        raise TypeError("Distance must be an integer between 0 and 2, inclusive.")
    if distance < 0 or distance > 2:
        raise ValueError("Distance must be between 0 and 2, inclusive.")


def _validate_non_negative_int(name: str, value: int) -> None:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{name} must be an integer.")
    if value < 0:
        raise ValueError(f"{name} must be zero or positive.")


def _validate_string(name: str, value: str) -> None:
    if not isinstance(value, str):
        raise TypeError(f"{name} must be a string.")


def _validate_optional_string(name: str, value: str | None) -> None:
    if value is not None:
        _validate_string(name, value)


def _validate_optional_bool(name: str, value: bool | None) -> None:
    if value is not None and not isinstance(value, bool):
        raise TypeError(f"{name} must be a boolean.")


_TOKENIZER_OPTIONS = dict[str, bool | int | float | str]


@deconstructible(path="paradedb.search.Tokenizer")
class Tokenizer:
    """A ParadeDB Tokenizer call"""

    name: str
    positional_arguments: tuple[bool | int | float | str, ...] | None
    options: _TOKENIZER_OPTIONS | None

    def __init__(
        self,
        tokenizer: str,
        params: tuple[bool | int | float | str, ...] | None = None,
        options: _TOKENIZER_OPTIONS | None = None,
    ) -> None:
        self.name = tokenizer
        self.positional_arguments = params
        self.options = options

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Tokenizer):
            return NotImplemented
        return (
            self.name == other.name
            and self.positional_arguments == other.positional_arguments
            and self.options == other.options
        )

    @staticmethod
    def unicode_words(
        options: _TOKENIZER_OPTIONS | None = None,
    ) -> Tokenizer:
        return Tokenizer(PDB_TYPE_TOKENIZER_UNICODE_WORDS, options=options)

    @staticmethod
    def simple(options: _TOKENIZER_OPTIONS | None = None) -> Tokenizer:
        return Tokenizer(PDB_TYPE_TOKENIZER_SIMPLE, options=options)

    @staticmethod
    def whitespace(
        options: _TOKENIZER_OPTIONS | None = None,
    ) -> Tokenizer:
        return Tokenizer(PDB_TYPE_TOKENIZER_WHITESPACE, None, options)

    @staticmethod
    def icu(options: _TOKENIZER_OPTIONS | None = None) -> Tokenizer:
        return Tokenizer(PDB_TYPE_TOKENIZER_ICU, options=options)

    @staticmethod
    def chinese_compatible(
        options: _TOKENIZER_OPTIONS | None = None,
    ) -> Tokenizer:
        return Tokenizer(PDB_TYPE_TOKENIZER_CHINESE_COMPATIBLE, options=options)

    @staticmethod
    def jieba(options: _TOKENIZER_OPTIONS | None = None) -> Tokenizer:
        return Tokenizer(PDB_TYPE_TOKENIZER_JIEBA, options=options)

    @staticmethod
    def literal(
        options: _TOKENIZER_OPTIONS | None = None,
    ) -> Tokenizer:
        return Tokenizer(PDB_TYPE_TOKENIZER_LITERAL, options=options)

    @staticmethod
    def literal_normalized(
        options: _TOKENIZER_OPTIONS | None = None,
    ) -> Tokenizer:
        return Tokenizer(PDB_TYPE_TOKENIZER_LITERAL_NORMALIZED, options=options)

    @staticmethod
    def ngram(
        min_gram: int,
        max_gram: int,
        options: _TOKENIZER_OPTIONS | None = None,
    ) -> Tokenizer:
        return Tokenizer(PDB_TYPE_TOKENIZER_NGRAM, (min_gram, max_gram), options)

    @staticmethod
    def edge_ngram(
        min_gram: int,
        max_gram: int,
        options: _TOKENIZER_OPTIONS | None = None,
    ) -> Tokenizer:
        return Tokenizer(PDB_TYPE_TOKENIZER_EDGE_NGRAM, (min_gram, max_gram), options)

    @staticmethod
    def lindera(
        dictionary: str,
        options: _TOKENIZER_OPTIONS | None = None,
    ) -> Tokenizer:
        return Tokenizer(PDB_TYPE_TOKENIZER_LINDERA, (dictionary,), options)

    @staticmethod
    def regex_pattern(
        pattern: str,
        options: _TOKENIZER_OPTIONS | None = None,
    ) -> Tokenizer:
        return Tokenizer(PDB_TYPE_TOKENIZER_REGEX, (pattern,), options)

    @staticmethod
    def source_code(
        options: _TOKENIZER_OPTIONS | None = None,
    ) -> Tokenizer:
        return Tokenizer(PDB_TYPE_TOKENIZER_SOURCE_CODE, options=options)

    def render(self: Tokenizer) -> str:
        if self.positional_arguments is None and self.options is None:
            return self.name

        arguments: list[str] = []

        if self.positional_arguments is not None:
            for param in self.positional_arguments:
                if isinstance(param, bool):
                    arguments.append("true" if param else "false")
                elif isinstance(param, int | float):
                    arguments.append(str(param))
                elif isinstance(param, str):
                    arguments.append(_quote_term(param))
                else:
                    raise TypeError(
                        f"Unsupported self arg type: {type(param).__name__}"
                    )

        if self.options:
            for key, value in self.options.items():
                if isinstance(value, bool):
                    rendered = "true" if value else "false"
                elif isinstance(value, int | float):
                    rendered = str(value)
                elif isinstance(value, str):
                    rendered = value
                else:
                    raise TypeError(
                        f"Unsupported self named arg type: {type(value).__name__}"
                    )
                arguments.append(_quote_term(f"{key}={rendered}"))

        return f"{self.name}({','.join(arguments)})"


@dataclass(frozen=True)
class Phrase:
    """Phrase search expression.

    Note: The slop parameter controls the maximum number of intervening unmatched
    tokens allowed between words in a phrase. Higher values increase query flexibility
    but may impact performance. Commonly used values are 0-10.
    See: https://docs.paradedb.com/documentation/full-text/phrase
    """

    terms: tuple[SearchValue, ...]

    def __init__(
        self,
        *terms: SearchValue,
    ) -> None:
        object.__setattr__(self, "terms", terms)


@dataclass(frozen=True)
class Parse:
    """Parse query expression."""

    query: str
    lenient: bool | None = None
    conjunction_mode: bool | None = None

    def __post_init__(self) -> None:
        _validate_string("Parse query", self.query)
        _validate_optional_bool("Parse lenient", self.lenient)
        _validate_optional_bool("Parse conjunction_mode", self.conjunction_mode)


@dataclass(frozen=True)
class PhrasePrefix:
    """Phrase prefix query expression."""

    phrases: tuple[str, ...]
    max_expansion: int | None = None

    def __init__(
        self,
        *phrases: str,
        max_expansion: int | None = None,
    ) -> None:
        if not phrases:
            raise ValueError("PhrasePrefix requires at least one phrase term.")
        for phrase in phrases:
            _validate_string("PhrasePrefix phrase", phrase)
        if max_expansion is not None:
            _validate_non_negative_int("PhrasePrefix max_expansion", max_expansion)
        object.__setattr__(self, "phrases", tuple(phrases))
        object.__setattr__(self, "max_expansion", max_expansion)


@dataclass(frozen=True)
class RegexPhrase:
    """Regex phrase query expression."""

    regexes: tuple[str, ...]
    slop: int | None = None
    max_expansions: int | None = None

    def __init__(
        self,
        *regexes: str,
        slop: int | None = None,
        max_expansions: int | None = None,
    ) -> None:
        if not regexes:
            raise ValueError("RegexPhrase requires at least one regex term.")
        for regex in regexes:
            _validate_string("RegexPhrase regex", regex)
        if slop is not None:
            _validate_non_negative_int("RegexPhrase slop", slop)
        if max_expansions is not None:
            _validate_non_negative_int("RegexPhrase max_expansions", max_expansions)
        object.__setattr__(self, "regexes", tuple(regexes))
        object.__setattr__(self, "slop", slop)
        object.__setattr__(self, "max_expansions", max_expansions)


@dataclass(frozen=True)
class ProxRegex:
    """Regex clause for use inside a proximity expression."""

    pattern: str
    max_expansions: int | None = None

    def __post_init__(self) -> None:
        _validate_string("ProxRegex pattern", self.pattern)
        if self.max_expansions is not None:
            _validate_non_negative_int("ProxRegex max_expansions", self.max_expansions)


ProximityTerm: TypeAlias = str | ProxRegex | list["ProximityTerm"]


@dataclass(frozen=True)
class Proximity:
    term: ProximityTerm

    def __init__(
        self,
        term: ProximityTerm,
    ) -> None:
        if not isinstance(term, (str, list, ProxRegex)):
            raise TypeError("Proximity term must be strings or ProxRegex instances")
        object.__setattr__(self, "term", term)

    def within(
        self,
        distance: int,
        term: ProximityNode | ProximityTerm,
        *,
        ordered: bool = False,
    ) -> ProximityNode:
        return ProximityNode(distance, ordered, self.term, term)


@dataclass(frozen=True)
class ProximityNode:
    distance: int
    ordered: bool
    left: ProximityNode | ProximityTerm
    right: ProximityNode | ProximityTerm

    def __init__(
        self,
        distance: int,
        ordered: bool,
        left: ProximityNode | ProximityTerm,
        right: ProximityNode | ProximityTerm,
    ) -> None:
        if distance < 0:
            raise ValueError(
                f"Proximity distance must be zero or positive. Received: {distance}"
            )
        object.__setattr__(self, "distance", distance)
        object.__setattr__(self, "ordered", ordered)
        object.__setattr__(self, "left", left)
        object.__setattr__(self, "right", right)

    def within(
        self,
        distance: int,
        term: ProximityNode | ProximityTerm,
        *,
        ordered: bool = False,
    ) -> ProximityNode:
        return ProximityNode(distance, ordered, self, term)


RangeRelation = Literal["Intersects", "Contains", "Within"]
RangeType = Literal[
    "int4range",
    "int8range",
    "numrange",
    "daterange",
    "tsrange",
    "tstzrange",
]
_RANGE_TYPES: set[str] = {
    "int4range",
    "int8range",
    "numrange",
    "daterange",
    "tsrange",
    "tstzrange",
}


def _validate_range_type(range_type: str) -> str:
    if range_type not in _RANGE_TYPES:
        valid = ", ".join(sorted(_RANGE_TYPES))
        raise ValueError(f"Range type must be one of: {valid}.")
    return range_type


@dataclass(frozen=True)
class RangeTerm:
    """Range-term query expression."""

    value: int | float | str | date | datetime
    relation: RangeRelation | None = None
    range_type: RangeType | None = None

    def __post_init__(self) -> None:
        if self.relation is None and self.range_type is not None:
            raise ValueError(
                "RangeTerm range_type is only valid when relation is provided."
            )
        if self.relation is not None and self.range_type is None:
            raise ValueError(
                "RangeTerm relation requires range_type for explicit range casting."
            )
        if self.range_type is not None:
            _validate_range_type(self.range_type)


@dataclass(frozen=True)
class Term:
    """Term query expression."""

    value: SearchValue


@dataclass(frozen=True)
class Regex:
    """Regex query expression."""

    pattern: str

    def __post_init__(self) -> None:
        _validate_string("Regex pattern", self.pattern)


@dataclass(frozen=True)
class All:
    """Match-all query expression."""


@dataclass(frozen=True)
class Exists:
    """Field existence check — matches documents where the LHS field has any indexed value."""


@dataclass(frozen=True)
class FuzzyTerm:
    """Fuzzy term search against the LHS field."""

    value: str | None = None

    def __post_init__(self) -> None:
        _validate_optional_string("FuzzyTerm value", self.value)


@dataclass(frozen=True)
class TermSet:
    """Match any term from a set against the LHS field.

    Terms must all be the same Python type (str, int, float, bool, date, or
    datetime). The type is used to pick the correct PostgreSQL array cast.
    """

    terms: tuple[str | int | float | bool | date | datetime, ...]

    def __init__(
        self,
        *terms: str | int | float | bool | date | datetime,
    ) -> None:
        if not terms:
            raise ValueError("TermSet requires at least one term.")
        first_kind = self._term_kind(terms[0])
        for term in terms[1:]:
            term_kind = self._term_kind(term)
            if term_kind != first_kind:
                raise TypeError(
                    "TermSet terms must all have the same type "
                    "(str, int, float, bool, date, or datetime)."
                )
        object.__setattr__(self, "terms", tuple(terms))

    @staticmethod
    def _term_kind(value: str | int | float | bool | date | datetime) -> str:
        if isinstance(value, bool):
            return "bool"
        if isinstance(value, int):
            return "int"
        if isinstance(value, float):
            return "float"
        if isinstance(value, datetime):
            return "datetime"
        if isinstance(value, date):
            return "date"
        if isinstance(value, str):
            return "str"
        raise TypeError(
            "TermSet terms must be str, int, float, bool, date, or datetime."
        )


@dataclass(frozen=True)
class MatchAny:
    """Explicit text-match query expression using OR semantics."""

    terms: tuple[SearchValue, ...]

    def __init__(
        self,
        *terms: SearchValue,
    ) -> None:
        object.__setattr__(self, "terms", tuple(terms))


@dataclass(frozen=True)
class MatchAll:
    """Explicit text-match query expression using AND semantics."""

    terms: tuple[SearchValue, ...]

    def __init__(
        self,
        *terms: SearchValue,
    ) -> None:
        object.__setattr__(self, "terms", tuple(terms))


class MoreLikeThis(Expression):
    conditional = True
    output_field = BooleanField()

    def __init__(
        self,
        *,
        id: object | None = None,
        document: dict[str, Any] | str | None = None,
        fields: Iterable[str] | None = None,
        key_field: str | None = None,
        min_term_freq: int | None = None,
        max_query_terms: int | None = None,
        min_doc_freq: int | None = None,
        max_term_freq: int | None = None,
        max_doc_freq: int | None = None,
        min_word_length: int | None = None,
        max_word_length: int | None = None,
        stopwords: Iterable[str] | None = None,
    ) -> None:
        super().__init__()
        self.id = id
        self.document: str | None = None
        self._document_input = document
        self.fields = list(fields) if fields is not None else None
        self.key_field = key_field
        self.min_term_freq = min_term_freq
        self.max_query_terms = max_query_terms
        self.min_doc_freq = min_doc_freq
        self.max_term_freq = max_term_freq
        self.max_doc_freq = max_doc_freq
        self.min_word_length = min_word_length
        self.max_word_length = max_word_length
        self.stopwords = list(stopwords) if stopwords is not None else None
        self._validate()

    def _validate(self) -> None:
        # Check exactly one input source is provided
        if self._count_inputs() != 1:
            raise ValueError("MoreLikeThis requires exactly one input source.")

        self._validate_key_field()
        self._validate_fields()
        self._validate_stopwords()

        # Validate fields only with ID-based queries
        if self._document_input is not None and self.fields:
            raise ValueError("MoreLikeThis fields are only valid with id(s).")

        # Validate document type and convert to JSON string
        self._validate_document_input()
        self._validate_numeric_params()

    def _count_inputs(self) -> int:
        return sum(
            [
                self.id is not None,
                self._document_input is not None,
            ]
        )

    def _validate_key_field(self) -> None:
        if self.key_field is not None and not isinstance(self.key_field, str):
            raise TypeError("MoreLikeThis key_field must be a string.")
        if isinstance(self.key_field, str) and not self.key_field.strip():
            raise ValueError("MoreLikeThis key_field cannot be empty.")

    def _validate_fields(self) -> None:
        if self.fields is None:
            return
        if not self.fields:
            raise ValueError("MoreLikeThis fields cannot be empty.")
        for field in self.fields:
            if not isinstance(field, str):
                raise TypeError("MoreLikeThis fields must contain strings.")
            if not field.strip():
                raise ValueError("MoreLikeThis fields cannot contain empty names.")

    def _validate_stopwords(self) -> None:
        if self.stopwords is None:
            return
        for stopword in self.stopwords:
            if not isinstance(stopword, str):
                raise TypeError("MoreLikeThis stopwords must contain strings.")

    def _validate_document_input(self) -> None:
        if self._document_input is None:
            return
        if not isinstance(self._document_input, dict | str):
            raise ValueError("MoreLikeThis document must be a dict or JSON string.")
        if isinstance(self._document_input, dict):
            self.document = json.dumps(self._document_input)
            return

        try:
            parsed_document = json.loads(self._document_input)
        except json.JSONDecodeError as exc:
            raise ValueError(
                "MoreLikeThis document JSON string must be valid JSON."
            ) from exc

        if not isinstance(parsed_document, dict):
            raise ValueError(
                "MoreLikeThis document JSON string must decode to an object."
            )
        self.document = self._document_input

    def _validate_numeric_params(self) -> None:
        numeric_params = {
            "min_term_freq": self.min_term_freq,
            "max_query_terms": self.max_query_terms,
            "min_doc_freq": self.min_doc_freq,
            "max_term_freq": self.max_term_freq,
            "max_doc_freq": self.max_doc_freq,
            "min_word_length": self.min_word_length,
            "max_word_length": self.max_word_length,
        }
        for param_name, param_value in numeric_params.items():
            if param_value is None:
                continue
            if isinstance(param_value, bool) or not isinstance(param_value, int):
                raise TypeError(f"MoreLikeThis {param_name} must be an integer.")
            if param_value < 1:
                raise ValueError(f"MoreLikeThis {param_name} must be >= 1.")


def _render_more_like_this_call(
    term: MoreLikeThis, value: object
) -> tuple[str, list[Any]]:
    args: list[str] = []
    params: list[Any] = []

    args.append("%s")
    params.append(value)

    if term.fields:
        # Build parameterized array
        field_placeholders = ", ".join("%s" for _ in term.fields)
        args.append(f"ARRAY[{field_placeholders}]::text[]")
        params.extend(term.fields)

    options, option_params = _render_options(
        {
            "min_term_frequency": term.min_term_freq,
            "max_query_terms": term.max_query_terms,
            "min_doc_frequency": term.min_doc_freq,
            "max_term_frequency": term.max_term_freq,
            "max_doc_frequency": term.max_doc_freq,
            "min_word_length": term.min_word_length,
            "max_word_length": term.max_word_length,
            "stopwords": term.stopwords,
        }
    )
    params.extend(option_params)
    return f"{FN_MORE_LIKE_THIS}({', '.join(args)}{options})", params


def _render_options(options: dict[str, object | None]) -> tuple[str, list[Any]]:
    """Render options for more_like_this function with parameterized values.

    Returns:
        Tuple of (sql_string, parameters_list)
    """
    rendered: list[str] = []
    params: list[Any] = []

    for key, value in options.items():
        if value is None:
            continue
        # Handle stopwords array with parameterization
        if key == "stopwords" and isinstance(value, list):
            if not value:
                continue
            stopword_placeholders = ", ".join("%s" for _ in value)
            rendered.append(f"{key} => ARRAY[{stopword_placeholders}]")
            params.extend(value)
        else:
            # Numeric values are safe to inline as they're validated
            rendered.append(f"{key} => {value}")

    if not rendered:
        return "", []
    return ", " + ", ".join(rendered), params


QueryExpression: TypeAlias = (
    MatchAny
    | MatchAll
    | Exists
    | FuzzyTerm
    | TermSet
    | Phrase
    | ProximityNode
    | Parse
    | PhrasePrefix
    | RegexPhrase
    | RangeTerm
    | Term
    | Regex
    | All
)
TermType: TypeAlias = QueryExpression | MoreLikeThis | Modifier


class ParadeDB:
    """Wrapper for ParadeDB search terms.

    Usage:
        # Explicit literal match query
        ParadeDB(MatchAll('running', 'shoes'))

        # Complex mixed AND/OR logic is expressed by combining multiple
        # ParadeDB(MatchAny(...))/ParadeDB(MatchAll(...)) clauses with Django Q objects.

        # Query expressions (SINGLE expression only)
        ParadeDB(Parse('query'))             # ✅ Valid
        ParadeDB(Parse('a'), Parse('b'))     # ❌ Error - only one allowed

        # Phrase search (multiple phrases allowed, no mixing with strings)
        ParadeDB(Phrase('exact match'))
        ParadeDB(Phrase('a'), Phrase('b'))   # ✅ Valid
        ParadeDB(Phrase('a'), 'b')           # ❌ Error - no mixing

    Raises:
        ValueError: If Parse/Term/Regex/All is not provided as a single term
        TypeError: If query term types are mixed
    """

    contains_aggregate = False
    contains_over_clause = False
    contains_column_references = False

    def __init__(self, term: TermType) -> None:
        self._term = term

    def resolve_expression(
        self,
        query: Any = None,  # noqa: ARG002
        allow_joins: bool = True,  # noqa: ARG002
        reuse: set[str] | None = None,  # noqa: ARG002
        summarize: bool = False,  # noqa: ARG002
        for_save: bool = False,  # noqa: ARG002
    ) -> ParadeDB:
        return self

    def as_sql(
        self,
        compiler: SQLCompiler,
        _connection: BaseDatabaseWrapper,
        lhs_sql: str,
    ) -> tuple[str, list[object]]:
        rendered, params = self._render_term(self._term, compiler)
        return f"{lhs_sql} {rendered}", params

    @staticmethod
    def _unwrap_term(term: TermType) -> TermType:
        while isinstance(term, Boost | Const | Fuzzy | Slop | Tokenized):
            term = term.value  # type: ignore[assignment]
        return term

    @staticmethod
    def _quote_range_literal(
        value: int | float | str | date | datetime, range_type: RangeType
    ) -> str:
        if isinstance(value, date | datetime):
            literal = value.isoformat()
        else:
            literal = str(value)
        safe_range_type = _validate_range_type(range_type)
        return f"{_quote_term(literal)}::{safe_range_type}"

    @staticmethod
    def _render_search_value(
        value: object, compiler: SQLCompiler
    ) -> tuple[str, list[Any]]:
        if isinstance(value, Boost):
            rendered, params = ParadeDB._render_search_value(value.value, compiler)
            return f"{rendered}::{PDB_TYPE_BOOST}({value.factor})", params
        if isinstance(value, Const):
            rendered, params = ParadeDB._render_search_value(value.value, compiler)
            return f"{rendered}::{PDB_TYPE_CONST}({value.score})", params
        if isinstance(value, Fuzzy):
            rendered, params = ParadeDB._render_search_value(value.value, compiler)
            fuzzy_args = [str(value.distance)]
            if value.prefix is not None:
                fuzzy_args.append("t" if value.prefix else "f")
            if value.transposition_cost_one is not None:
                fuzzy_args.append("t" if value.transposition_cost_one else "f")
            return f"{rendered}::{PDB_TYPE_FUZZY}({', '.join(fuzzy_args)})", params
        if isinstance(value, Slop):
            rendered, params = ParadeDB._render_search_value(value.value, compiler)
            return f"{rendered}::{PDB_TYPE_SLOP}({value.distance})", params
        if isinstance(value, Tokenized):
            rendered, params = ParadeDB._render_search_value(value.value, compiler)
            return f"{rendered}::{value.tokenizer.render()}", params
        if isinstance(value, QueryExpression):
            return ParadeDB(value)._render_term(value, compiler)
        if isinstance(value, Expression):
            expression = value.resolve_expression(compiler.query)
            sql, expression_params = compiler.compile(expression)
            return sql, list(expression_params)
        if isinstance(value, str):
            return _quote_term(value), []
        if isinstance(value, list | tuple):
            quoted = [_quote_term(item) for item in value]
            return f"ARRAY[{', '.join(quoted)}]", []
        raise TypeError(f"Unsupported search value type. {value}")

    def _render_proximity_node(self, node: ProximityNode) -> str:
        return f"{OP_SEARCH} ({self._render_proximity(node)})"

    def _render_proximity(self, item: ProximityNode | ProximityTerm) -> str:
        if isinstance(item, ProximityNode):
            operator = OP_PROXIMITY_ORD if item.ordered else OP_PROXIMITY
            lhs = self._render_proximity(item.left)
            rhs = self._render_proximity(item.right)
            # if the right side is a node we need to wrap the final expression in parens
            # to produce the correct associativity
            if isinstance(item.right, ProximityNode):
                return f"{lhs} {operator} {item.distance} {operator} ({rhs})"
            else:
                return f"{lhs} {operator} {item.distance} {operator} {rhs}"
        return self._render_proximity_term(item)

    def _render_proximity_term(self, term: ProximityTerm) -> str:
        if isinstance(term, str):
            return _quote_term(term)
        if isinstance(term, ProxRegex):
            if term.max_expansions is None:
                return f"{FN_PROX_REGEX}({_quote_term(term.pattern)})"
            return (
                f"{FN_PROX_REGEX}({_quote_term(term.pattern)}, {term.max_expansions})"
            )
        if isinstance(term, list):
            parts = [self._render_proximity_term(x) for x in term]
            return f"{FN_PROX_ARRAY}({', '.join(parts)})"
        raise AssertionError(f"Unhandled proximity term: {term!r}")

    def _render_term(
        self, term: TermType, compiler: SQLCompiler
    ) -> tuple[str, list[Any]]:
        if isinstance(term, Boost | Const | Fuzzy | Slop | Tokenized):
            return self._render_search_value(term, compiler)
        if isinstance(term, Phrase):
            rendered, params = self._render_search_value(
                term.terms[0] if len(term.terms) == 1 else term.terms, compiler
            )
            return f"{OP_PHRASE} {rendered}", params
        if isinstance(term, ProximityNode):
            return self._render_proximity_node(term), []
        if isinstance(term, Parse):
            rendered = (
                f"{OP_SEARCH} {FN_PARSE}({_quote_term(term.query)}"
                f"{self._render_options({'lenient': term.lenient, 'conjunction_mode': term.conjunction_mode})})"
            )
            return rendered, []
        if isinstance(term, PhrasePrefix):
            phrases_sql = ", ".join(_quote_term(phrase) for phrase in term.phrases)
            return (
                f"{OP_SEARCH} {FN_PHRASE_PREFIX}(ARRAY[{phrases_sql}]"
                f"{self._render_options({'max_expansion': term.max_expansion})})"
            ), []
        if isinstance(term, RegexPhrase):
            regex_sql = ", ".join(_quote_term(regex) for regex in term.regexes)
            return (
                f"{OP_SEARCH} {FN_REGEX_PHRASE}(ARRAY[{regex_sql}]"
                f"{self._render_options({'slop': term.slop, 'max_expansions': term.max_expansions})})"
            ), []
        if isinstance(term, RangeTerm):
            if term.relation is None:
                return (
                    f"{OP_SEARCH} {FN_RANGE_TERM}({self._render_value(term.value)})",
                    [],
                )
            else:
                assert term.range_type is not None
                return (
                    f"{OP_SEARCH} {FN_RANGE_TERM}("
                    f"{self._quote_range_literal(term.value, term.range_type)}, "
                    f"{_quote_term(term.relation)}"
                    ")"
                ), []
        if isinstance(term, Term):
            rendered, params = self._render_search_value(term.value, compiler)
            return f"{OP_SEARCH} {FN_TERM}({rendered})", params
        if isinstance(term, Regex):
            return f"{OP_SEARCH} {FN_REGEX}({_quote_term(term.pattern)})", []
        if isinstance(term, Exists):
            return f"{OP_SEARCH} {FN_EXISTS}()", []
        if isinstance(term, FuzzyTerm):
            if term.value is not None:
                return f"{OP_SEARCH} {FN_FUZZY_TERM}({_quote_term(term.value)})", []
            else:
                return f"{OP_SEARCH} {FN_FUZZY_TERM}()", []
        if isinstance(term, TermSet):
            array_sql = self._render_term_set_array(term.terms)
            return f"{OP_SEARCH} {FN_TERM_SET}({array_sql})", []
        if isinstance(term, All):
            return f"{OP_SEARCH} {FN_ALL}()", []
        if isinstance(term, MatchAll):
            rendered, params = self._render_search_value(
                term.terms[0] if len(term.terms) == 1 else term.terms, compiler
            )
            return f"{OP_AND} {rendered}", params
        if isinstance(term, MatchAny):
            rendered, params = self._render_search_value(
                term.terms[0] if len(term.terms) == 1 else term.terms, compiler
            )
            return f"{OP_OR} {rendered}", params
        if isinstance(term, MoreLikeThis):
            if term.id is not None:
                mlt_sql, mlt_params = _render_more_like_this_call(term, term.id)
                return f"{OP_SEARCH} {mlt_sql}", mlt_params
            else:
                mlt_sql, mlt_params = _render_more_like_this_call(term, term.document)
                return f"{OP_SEARCH} {mlt_sql}", mlt_params

        raise TypeError(f"Unsupported ParadeDB term type. {term}")

    @staticmethod
    def _render_term_set_array(
        terms: tuple[str | int | float | bool | date | datetime, ...],
    ) -> str:
        """Render a TermSet terms tuple as a typed SQL ARRAY literal.

        The first term's Python type determines the PostgreSQL element type.
        bool is checked before int (bool subclasses int).
        datetime is checked before date (datetime subclasses date).
        """
        first = terms[0]
        if isinstance(first, bool):
            return f"ARRAY[{', '.join('true' if t else 'false' for t in terms)}]::boolean[]"
        if isinstance(first, int):
            return f"ARRAY[{', '.join(str(t) for t in terms)}]::bigint[]"
        if isinstance(first, float):
            return f"ARRAY[{', '.join(str(t) for t in terms)}]::float8[]"
        if isinstance(first, datetime):
            datetime_terms: list[datetime] = []
            for term in terms:
                if not isinstance(term, datetime):
                    raise TypeError("TermSet terms must all be datetime values.")
                datetime_terms.append(term)
            return f"ARRAY[{', '.join(_quote_term(term.isoformat()) for term in datetime_terms)}]::timestamptz[]"
        if isinstance(first, date):
            date_terms: list[date] = []
            for term in terms:
                if not isinstance(term, date) or isinstance(term, datetime):
                    raise TypeError("TermSet terms must all be date values.")
                date_terms.append(term)
            return f"ARRAY[{', '.join(_quote_term(term.isoformat()) for term in date_terms)}]::date[]"
        return f"ARRAY[{', '.join(_quote_term(str(t)) for t in terms)}]::text[]"

    @staticmethod
    def _render_options(options: dict[str, object | None]) -> str:
        rendered: list[str] = []
        for key, value in options.items():
            if value is None:
                continue
            rendered.append(f"{key} => {ParadeDB._render_value(value)}")
        if not rendered:
            return ""
        return ", " + ", ".join(rendered)

    @staticmethod
    def _render_value(value: object) -> str:
        if isinstance(value, bool):
            return "true" if value else "false"
        if isinstance(value, int | float):
            return str(value)
        if isinstance(value, date | datetime):
            return _quote_term(value.isoformat())
        if isinstance(value, str):
            return _quote_term(value)
        raise TypeError("Unsupported option value type.")


def _quote_term(term: str) -> str:
    if not isinstance(term, str):
        raise TypeError("Search term literal must be a string.")
    escaped = term.replace("'", "''")
    return f"'{escaped}'"


class ParadeDBExact(Exact):  # type: ignore[type-arg]
    """Exact lookup override to emit ParadeDB operators."""

    lookup_name = "exact"

    def as_sql(
        self, compiler: SQLCompiler, connection: BaseDatabaseWrapper
    ) -> tuple[str, list[Any]]:
        if isinstance(self.rhs, ParadeDB):
            lhs_sql, lhs_params = self.process_lhs(compiler, connection)
            rhs_sql, rhs_params = self.rhs.as_sql(compiler, connection, lhs_sql)
            return rhs_sql, [*lhs_params, *rhs_params]

        result = super().as_sql(compiler, connection)
        return result[0], list(result[1])


Field.register_lookup(ParadeDBExact)
TextField.register_lookup(ParadeDBExact)
CharField.register_lookup(ParadeDBExact)
IntegerField.register_lookup(ParadeDBExact)
SmallIntegerField.register_lookup(ParadeDBExact)
BigIntegerField.register_lookup(ParadeDBExact)
AutoField.register_lookup(ParadeDBExact)
BigAutoField.register_lookup(ParadeDBExact)
UUIDField.register_lookup(ParadeDBExact)

__all__ = [
    "All",
    "Boost",
    "Const",
    "Exists",
    "Fuzzy",
    "FuzzyTerm",
    "MatchAll",
    "MatchAny",
    "MoreLikeThis",
    "ParadeDB",
    "Parse",
    "Phrase",
    "PhrasePrefix",
    "ProxRegex",
    "Proximity",
    "RangeRelation",
    "RangeTerm",
    "RangeType",
    "Regex",
    "RegexPhrase",
    "Slop",
    "Term",
    "TermSet",
    "Tokenized",
    "Tokenizer",
]
