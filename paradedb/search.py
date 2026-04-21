"""ParadeDB search expressions and lookups."""

from __future__ import annotations

import json
import math
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
    PDB_TYPE_QUERY,
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

ParadeOperator = Literal["OR", "AND"]


def _is_fuzzy_enabled(
    *,
    distance: int | None,
    prefix: bool,
    transposition_cost_one: bool,
) -> bool:
    return distance is not None or prefix or transposition_cost_one


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

    terms: list[str]
    slop: int | None = None
    tokenizer: Tokenizer | None = None
    boost: float | None = None
    const: float | None = None

    def __init__(
        self,
        *terms: str,
        slop: int | None = None,
        tokenizer: Tokenizer | None = None,
        boost: float | None = None,
        const: float | None = None,
    ) -> None:
        if len(terms) == 0:
            raise ValueError("Phrase requires at least one term")
        for term in terms:
            _validate_string("Phrase term", term)
        if slop is not None:
            _validate_non_negative_int("Phrase slop", slop)

        object.__setattr__(self, "terms", terms)
        object.__setattr__(self, "slop", slop)
        object.__setattr__(self, "tokenizer", tokenizer)
        object.__setattr__(self, "boost", boost)
        object.__setattr__(self, "const", const)


@dataclass(frozen=True)
class Parse:
    """Parse query expression."""

    query: str
    lenient: bool | None = None
    conjunction_mode: bool | None = None
    boost: float | None = None
    const: float | None = None

    def __post_init__(self) -> None:
        _validate_string("Parse query", self.query)
        _validate_optional_bool("Parse lenient", self.lenient)
        _validate_optional_bool("Parse conjunction_mode", self.conjunction_mode)


@dataclass(frozen=True)
class PhrasePrefix:
    """Phrase prefix query expression."""

    phrases: tuple[str, ...]
    max_expansion: int | None = None
    boost: float | None = None
    const: float | None = None

    def __init__(
        self,
        *phrases: str,
        max_expansion: int | None = None,
        boost: float | None = None,
        const: float | None = None,
    ) -> None:
        if not phrases:
            raise ValueError("PhrasePrefix requires at least one phrase term.")
        for phrase in phrases:
            _validate_string("PhrasePrefix phrase", phrase)
        if max_expansion is not None:
            _validate_non_negative_int("PhrasePrefix max_expansion", max_expansion)
        object.__setattr__(self, "phrases", tuple(phrases))
        object.__setattr__(self, "max_expansion", max_expansion)
        object.__setattr__(self, "boost", boost)
        object.__setattr__(self, "const", const)


@dataclass(frozen=True)
class RegexPhrase:
    """Regex phrase query expression."""

    regexes: tuple[str, ...]
    slop: int | None = None
    max_expansions: int | None = None
    boost: float | None = None
    const: float | None = None

    def __init__(
        self,
        *regexes: str,
        slop: int | None = None,
        max_expansions: int | None = None,
        boost: float | None = None,
        const: float | None = None,
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
        object.__setattr__(self, "boost", boost)
        object.__setattr__(self, "const", const)


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

    def boost(self, value: float) -> ProximityQuery:
        return ProximityQuery(self, Boost(value))

    def const(self, value: float) -> ProximityQuery:
        return ProximityQuery(self, Const(value))


@dataclass(frozen=True)
class Boost:
    value: float


@dataclass(frozen=True)
class Const:
    value: float


@dataclass(frozen=True)
class ProximityQuery:
    node: ProximityNode
    # encoding boost & const like this makes it clear that at most one of them can be set at once
    relevance_modifier: Boost | Const | None


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
    boost: float | None = None
    const: float | None = None

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

    text: str
    distance: int | None = None
    prefix: bool = False
    transposition_cost_one: bool = False
    boost: float | None = None
    const: float | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.text, str):
            raise TypeError("Term text must be a string.")
        if not isinstance(self.prefix, bool):
            raise TypeError("Term prefix must be a boolean.")
        if not isinstance(self.transposition_cost_one, bool):
            raise TypeError("Term transposition_cost_one must be a boolean.")
        _validate_fuzzy_distance(self.distance)


@dataclass(frozen=True)
class Regex:
    """Regex query expression."""

    pattern: str
    boost: float | None = None
    const: float | None = None

    def __post_init__(self) -> None:
        _validate_string("Regex pattern", self.pattern)


@dataclass(frozen=True)
class All:
    """Match-all query expression."""


@dataclass(frozen=True)
class Exists:
    """Field existence check — matches documents where the LHS field has any indexed value."""

    boost: float | None = None
    const: float | None = None


@dataclass(frozen=True)
class FuzzyTerm:
    """Fuzzy term search against the LHS field."""

    value: str | None = None
    distance: int | None = None
    transposition_cost_one: bool | None = None
    prefix: bool | None = None
    boost: float | None = None
    const: float | None = None

    def __post_init__(self) -> None:
        _validate_optional_string("FuzzyTerm value", self.value)
        _validate_optional_bool(
            "FuzzyTerm transposition_cost_one", self.transposition_cost_one
        )
        _validate_optional_bool("FuzzyTerm prefix", self.prefix)
        _validate_fuzzy_distance(self.distance)


@dataclass(frozen=True)
class TermSet:
    """Match any term from a set against the LHS field.

    Terms must all be the same Python type (str, int, float, bool, date, or
    datetime). The type is used to pick the correct PostgreSQL array cast.
    """

    terms: tuple[str | int | float | bool | date | datetime, ...]
    boost: float | None = None
    const: float | None = None

    def __init__(
        self,
        *terms: str | int | float | bool | date | datetime,
        boost: float | None = None,
        const: float | None = None,
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
        object.__setattr__(self, "boost", boost)
        object.__setattr__(self, "const", const)

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
class Match:
    """Explicit text-match query expression."""

    terms: tuple[str, ...]
    operator: ParadeOperator
    tokenizer: Tokenizer | None = None
    distance: int | None = None
    prefix: bool = False
    transposition_cost_one: bool = False
    boost: float | None = None
    const: float | None = None

    def __init__(
        self,
        *terms: str,
        operator: ParadeOperator,
        tokenizer: Tokenizer | None = None,
        distance: int | None = None,
        prefix: bool = False,
        transposition_cost_one: bool = False,
        boost: float | None = None,
        const: float | None = None,
    ) -> None:
        if not terms:
            raise ValueError("Match requires at least one search term.")
        if operator not in ("AND", "OR"):
            raise ValueError("Match operator must be 'AND' or 'OR'.")
        if not all(isinstance(term, str) for term in terms):
            raise TypeError("Match terms must be strings.")
        if not isinstance(prefix, bool):
            raise TypeError("Match prefix must be a boolean.")
        if not isinstance(transposition_cost_one, bool):
            raise TypeError("Match transposition_cost_one must be a boolean.")

        _validate_fuzzy_distance(distance)
        fuzzy_enabled = _is_fuzzy_enabled(
            distance=distance,
            prefix=prefix,
            transposition_cost_one=transposition_cost_one,
        )
        if tokenizer is not None and fuzzy_enabled:
            raise ValueError("Match tokenizer cannot be combined with fuzzy options.")
        if (
            len(terms) > 1
            and fuzzy_enabled
            and (boost is not None or const is not None)
        ):
            raise ValueError("Multi-term fuzzy Match does not support boost or const.")

        object.__setattr__(self, "terms", tuple(terms))
        object.__setattr__(self, "operator", operator)
        object.__setattr__(self, "tokenizer", tokenizer)
        object.__setattr__(self, "distance", distance)
        object.__setattr__(self, "prefix", prefix)
        object.__setattr__(self, "transposition_cost_one", transposition_cost_one)
        object.__setattr__(self, "boost", boost)
        object.__setattr__(self, "const", const)


class MoreLikeThis(Expression):
    """More Like This query filter.

    Provide exactly one of id, ids, or document (dict/JSON string).
    Fields are only valid with id/ids.

    Args:
        id: Single document ID for similarity search
        ids: Multiple document IDs for similarity search (OR'd together)
        document: Custom JSON document (dict or JSON string) for similarity search
        fields: List of fields to consider (only valid with id/ids)
        key_field: Field name to use for comparison (defaults to model's primary key)
        min_term_freq: Minimum term frequency (must be >= 1)
        max_query_terms: Maximum query terms (must be >= 1)
        min_doc_freq: Minimum document frequency (must be >= 1)
        max_term_freq: Maximum term frequency (must be >= 1)
        max_doc_freq: Maximum document frequency (must be >= 1)
        min_word_length: Minimum word length (must be >= 1)
        max_word_length: Maximum word length (must be >= 1)
        stopwords: List of stopwords to exclude

    See: https://docs.paradedb.com/documentation/query-builder/specialized/more-like-this
    """

    conditional = True
    output_field = BooleanField()

    def __init__(
        self,
        *,
        id: int | None = None,
        ids: Iterable[int] | None = None,
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
        self.ids = list(ids) if ids is not None else None
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

        self._validate_id_inputs()
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
                self.ids is not None,
                self._document_input is not None,
            ]
        )

    def _validate_id_inputs(self) -> None:
        if self.id is not None and (
            isinstance(self.id, bool) or not isinstance(self.id, int)
        ):
            raise TypeError("MoreLikeThis id must be an integer.")

        if self.ids is not None and not self.ids:
            raise ValueError("MoreLikeThis ids cannot be empty.")

        if self.ids is not None and any(
            isinstance(item_id, bool) or not isinstance(item_id, int)
            for item_id in self.ids
        ):
            raise TypeError("MoreLikeThis ids must contain integers.")

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


def render_more_like_this(term: MoreLikeThis, lhs_sql: str) -> tuple[str, list[Any]]:
    params: list[Any] = []

    if term.ids is not None:
        expressions = []
        for value in term.ids:
            mlt_sql, mlt_params = _render_more_like_this_call(term, value)
            expressions.append(f"{lhs_sql} {OP_SEARCH} {mlt_sql}")
            params.extend(mlt_params)
        joined = " OR ".join(expressions)
        return f"({joined})", params

    if term.id is not None:
        mlt_sql, mlt_params = _render_more_like_this_call(term, term.id)
        params.extend(mlt_params)
        return f"{lhs_sql} {OP_SEARCH} {mlt_sql}", params

    mlt_sql, mlt_params = _render_more_like_this_call(term, term.document)
    params.extend(mlt_params)
    return f"{lhs_sql} {OP_SEARCH} {mlt_sql}", params


def _render_more_like_this_call(
    term: MoreLikeThis, value: int | str | None
) -> tuple[str, list[Any]]:
    args: list[str] = []
    params: list[Any] = []

    if isinstance(value, str):
        # This is a JSON document - use parameter
        args.append("%s")
        params.append(value)
    else:
        # This is an integer ID - use parameter for safety
        args.append("%s")
        params.append(value)

    if term.fields and not isinstance(value, str):
        # Fields array only valid for ID-based queries
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


TermType = (
    Match
    | Exists
    | FuzzyTerm
    | MoreLikeThis
    | TermSet
    | Phrase
    | ProximityNode
    | ProximityQuery
    | Parse
    | PhrasePrefix
    | RegexPhrase
    | RangeTerm
    | Term
    | Regex
    | All
)


class ParadeDB:
    """Wrapper for ParadeDB search terms.

    Usage:
        # Explicit literal match query
        ParadeDB(Match('running', 'shoes', operator='AND'))

        # Complex mixed AND/OR logic is expressed by combining multiple
        # ParadeDB(Match(...)) clauses with Django Q objects.

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
        _compiler: SQLCompiler,
        _connection: BaseDatabaseWrapper,
        lhs_sql: str,
    ) -> tuple[str, list[object]]:
        if isinstance(self._term, MoreLikeThis):
            return render_more_like_this(self._term, lhs_sql)
        rendered = self._render_term(self._term)
        return f"{lhs_sql} {self._lookup_operator()} {rendered}", []

    def _lookup_operator(self) -> str:
        if isinstance(self._term, Match):
            if self._term.operator == "OR":
                return OP_OR
            elif self._term.operator == "AND":
                return OP_AND
            else:
                raise ValueError("Match operator must be 'AND' or 'OR'.")
        if isinstance(self._term, Phrase):
            return OP_PHRASE
        return OP_SEARCH

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
    def _render_scoring_number(value: float | None, *, name: str) -> str | None:
        if value is None:
            return None
        if isinstance(value, bool) or not isinstance(value, int | float):
            raise TypeError(f"{name} must be an int or float.")
        numeric = float(value)
        if not math.isfinite(numeric):
            raise ValueError(f"{name} must be finite.")
        return str(value)

    @staticmethod
    def _append_scoring(sql: str, *, boost: float | None, const: float | None) -> str:
        boost_sql = ParadeDB._render_scoring_number(boost, name="boost")
        if boost_sql is not None:
            sql = f"{sql}::{PDB_TYPE_BOOST}({boost_sql})"
        const_sql = ParadeDB._render_scoring_number(const, name="const")
        if const_sql is not None:
            sql = f"{sql}::{PDB_TYPE_CONST}({const_sql})"
        return sql

    @staticmethod
    def _append_fuzzy(
        sql: str,
        *,
        distance: int | None,
        prefix: bool,
        transposition_cost_one: bool,
    ) -> str:
        if not _is_fuzzy_enabled(
            distance=distance,
            prefix=prefix,
            transposition_cost_one=transposition_cost_one,
        ):
            return sql

        fuzzy_distance = 1 if distance is None else distance
        fuzzy_args: list[str] = [str(fuzzy_distance)]
        if transposition_cost_one:
            fuzzy_args.extend(["t" if prefix else "f", "t"])
        elif prefix:
            fuzzy_args.append("t")
        return f"{sql}::{PDB_TYPE_FUZZY}({', '.join(fuzzy_args)})"

    def _render_proximity_node(self, node: ProximityNode) -> str:
        return f"({self._render_proximity(node)})"

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

    def _render_term(self, term: TermType) -> str:
        if isinstance(term, Phrase):
            if len(term.terms) == 1:
                rendered = _quote_term(term.terms[0])
            else:
                quoted = [_quote_term(item) for item in term.terms]
                rendered = f"ARRAY[{', '.join(quoted)}]"
            if term.slop is not None:
                rendered = f"{rendered}::{PDB_TYPE_SLOP}({term.slop})"
                # pdb.slop has no direct cast to pdb.const; bridge via pdb.query.
                # Once this is fixed in the DB we can remove this
                if term.const is not None:
                    rendered = f"{rendered}::{PDB_TYPE_QUERY}"
            if term.tokenizer is not None:
                rendered = f"{rendered}::{term.tokenizer.render()}"
            return self._append_scoring(rendered, boost=term.boost, const=term.const)
        if isinstance(term, ProximityNode):
            return self._render_proximity_node(term)
        if isinstance(term, ProximityQuery):
            rendered = self._render_proximity_node(term.node)
            boost = (
                term.relevance_modifier.value
                if isinstance(term.relevance_modifier, Boost)
                else None
            )
            const = (
                term.relevance_modifier.value
                if isinstance(term.relevance_modifier, Const)
                else None
            )
            return self._append_scoring(rendered, boost=boost, const=const)
        if isinstance(term, Parse):
            rendered = (
                f"{FN_PARSE}({_quote_term(term.query)}"
                f"{self._render_options({'lenient': term.lenient, 'conjunction_mode': term.conjunction_mode})})"
            )
            return self._append_scoring(rendered, boost=term.boost, const=term.const)
        if isinstance(term, PhrasePrefix):
            phrases_sql = ", ".join(_quote_term(phrase) for phrase in term.phrases)
            rendered = (
                f"{FN_PHRASE_PREFIX}(ARRAY[{phrases_sql}]"
                f"{self._render_options({'max_expansion': term.max_expansion})})"
            )
            return self._append_scoring(rendered, boost=term.boost, const=term.const)
        if isinstance(term, RegexPhrase):
            regex_sql = ", ".join(_quote_term(regex) for regex in term.regexes)
            rendered = (
                f"{FN_REGEX_PHRASE}(ARRAY[{regex_sql}]"
                f"{self._render_options({'slop': term.slop, 'max_expansions': term.max_expansions})})"
            )
            return self._append_scoring(rendered, boost=term.boost, const=term.const)
        if isinstance(term, RangeTerm):
            if term.relation is None:
                rendered = f"{FN_RANGE_TERM}({self._render_value(term.value)})"
            else:
                assert term.range_type is not None
                rendered = (
                    f"{FN_RANGE_TERM}("
                    f"{self._quote_range_literal(term.value, term.range_type)}, "
                    f"{_quote_term(term.relation)}"
                    ")"
                )
            return self._append_scoring(rendered, boost=term.boost, const=term.const)
        if isinstance(term, Term):
            rendered = f"{FN_TERM}({_quote_term(term.text)})"
            rendered = self._append_fuzzy(
                rendered,
                distance=term.distance,
                prefix=term.prefix,
                transposition_cost_one=term.transposition_cost_one,
            )
            if (
                _is_fuzzy_enabled(
                    distance=term.distance,
                    prefix=term.prefix,
                    transposition_cost_one=term.transposition_cost_one,
                )
                and term.const is not None
            ):
                # pdb.fuzzy has no direct cast to pdb.const; bridge via pdb.query.
                rendered = f"{rendered}::{PDB_TYPE_QUERY}"
            return self._append_scoring(rendered, boost=term.boost, const=term.const)
        if isinstance(term, Regex):
            rendered = f"{FN_REGEX}({_quote_term(term.pattern)})"
            return self._append_scoring(rendered, boost=term.boost, const=term.const)
        if isinstance(term, Exists):
            rendered = f"{FN_EXISTS}()"
            return self._append_scoring(rendered, boost=term.boost, const=term.const)
        if isinstance(term, FuzzyTerm):
            if term.value is not None:
                rendered = f"{FN_FUZZY_TERM}({_quote_term(term.value)})"
            else:
                rendered = f"{FN_FUZZY_TERM}()"
            rendered = self._append_fuzzy(
                rendered,
                distance=term.distance,
                prefix=term.prefix or False,
                transposition_cost_one=term.transposition_cost_one or False,
            )
            if (
                _is_fuzzy_enabled(
                    distance=term.distance,
                    prefix=term.prefix or False,
                    transposition_cost_one=term.transposition_cost_one or False,
                )
                and term.const is not None
            ):
                rendered = f"{rendered}::{PDB_TYPE_QUERY}"
            return self._append_scoring(rendered, boost=term.boost, const=term.const)
        if isinstance(term, TermSet):
            array_sql = self._render_term_set_array(term.terms)
            rendered = f"{FN_TERM_SET}({array_sql})"
            return self._append_scoring(rendered, boost=term.boost, const=term.const)
        if isinstance(term, All):
            return f"{FN_ALL}()"
        if isinstance(term, Match):
            if len(term.terms) == 1:
                rendered = _quote_term(term.terms[0])
            else:
                quoted = [_quote_term(item) for item in term.terms]
                rendered = f"ARRAY[{', '.join(quoted)}]"
            if term.tokenizer is not None:
                rendered = f"{rendered}::{term.tokenizer.render()}"
            rendered = self._append_fuzzy(
                rendered,
                distance=term.distance,
                prefix=term.prefix,
                transposition_cost_one=term.transposition_cost_one,
            )
            if (
                _is_fuzzy_enabled(
                    distance=term.distance,
                    prefix=term.prefix,
                    transposition_cost_one=term.transposition_cost_one,
                )
                and term.const is not None
            ):
                # pdb.fuzzy has no direct cast to pdb.const; bridge via pdb.query.
                # Casting fuzzy to const should be supported in ParadeDB. Once it is we can remove this
                rendered = f"{rendered}::pdb.query"
            rendered = self._append_scoring(
                rendered, boost=term.boost, const=term.const
            )
            return rendered
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
    "Exists",
    "FuzzyTerm",
    "Match",
    "MoreLikeThis",
    "ParadeDB",
    "ParadeOperator",
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
    "Term",
    "TermSet",
    "Tokenizer",
]
