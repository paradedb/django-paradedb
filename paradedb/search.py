"""ParadeDB search expressions and lookups."""

from __future__ import annotations

import json
import math
import re
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Literal, overload

from django.core.exceptions import FieldDoesNotExist
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

from paradedb.api import (
    FN_ALL,
    FN_EMPTY,
    FN_EXISTS,
    FN_FUZZY_TERM,
    FN_MORE_LIKE_THIS,
    FN_PARSE,
    FN_PARSE_WITH_FIELD,
    FN_PHRASE_PREFIX,
    FN_PROX_ARRAY,
    FN_PROX_REGEX,
    FN_PROXIMITY,
    FN_RANGE,
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
    PDB_TYPE_TOKENIZER_ALIAS,
    PDB_TYPE_TOKENIZER_CHINESE_COMPATIBLE,
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


# Regex to detect simple PostgreSQL identifiers (no quoting needed) vs complex ones.
# Simple identifiers can be used directly as `pdb.name`. We also support tokenizer
# invocation syntax such as `whitespace('lowercase=false')`.
_SIMPLE_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_TOKENIZER_CALL_RE = re.compile(
    r"^(?P<name>[A-Za-z_][A-Za-z0-9_]*)\((?P<args>.*)\)$", re.DOTALL
)
# Tokenizer args must be SQL string literals: 'k=v'[, 'k=v', ...]
_TOKENIZER_CALL_ARGS_RE = re.compile(
    r"^\s*(?:'(?:[^']|'')*'\s*(?:,\s*'(?:[^']|'')*'\s*)*)?$"
)

# Maps the bare tokenizer name (e.g. "whitespace") to its qualified pdb type constant.
# Built from the PDB_TYPE_TOKENIZER_* constants so the lookup is tied to api.py.
_KNOWN_TOKENIZERS: dict[str, str] = {
    t[len("pdb.") :]: t
    for t in (
        PDB_TYPE_TOKENIZER_ALIAS,
        PDB_TYPE_TOKENIZER_CHINESE_COMPATIBLE,
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
}


def _tokenizer_cast(name: str) -> str:
    """Return safe ``pdb.<tokenizer>`` SQL for tokenizer casts.

    Supported forms:
    - ``tokenizer``
    - ``tokenizer('k=v')``
    - ``tokenizer('k=v', 'k2=v2')``

    Any other form is treated as an identifier and quoted to avoid injection.
    """
    if _SIMPLE_IDENTIFIER_RE.match(name):
        return _KNOWN_TOKENIZERS.get(name, f"pdb.{name}")

    tokenizer_call = _TOKENIZER_CALL_RE.match(name)
    if tokenizer_call is not None:
        tokenizer_name = tokenizer_call.group("name")
        tokenizer_args = tokenizer_call.group("args")
        if _TOKENIZER_CALL_ARGS_RE.match(tokenizer_args):
            qualified = _KNOWN_TOKENIZERS.get(tokenizer_name, f"pdb.{tokenizer_name}")
            return f"{qualified}({tokenizer_args.strip()})"

    escaped = name.replace('"', '""')
    return f'pdb."{escaped}"'


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


@dataclass(frozen=True)
class Phrase:
    """Phrase search expression.

    Note: The slop parameter controls the maximum number of intervening unmatched
    tokens allowed between words in a phrase. Higher values increase query flexibility
    but may impact performance. Commonly used values are 0-10.
    See: https://docs.paradedb.com/documentation/full-text/phrase
    """

    text: str
    slop: int | None = None
    tokenizer: str | None = None
    boost: float | None = None
    const: float | None = None

    def __post_init__(self) -> None:
        _validate_string("Phrase text", self.text)
        if self.slop is not None:
            _validate_non_negative_int("Phrase slop", self.slop)
        _validate_optional_string("Phrase tokenizer", self.tokenizer)


@dataclass(frozen=True)
class Proximity:
    """Proximity search expression."""

    text: str
    distance: int
    ordered: bool = False
    boost: float | None = None
    const: float | None = None

    def __post_init__(self) -> None:
        _validate_string("Proximity text", self.text)
        _validate_non_negative_int("Proximity distance", self.distance)
        if not isinstance(self.ordered, bool):
            raise TypeError("Proximity ordered must be a boolean.")


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
class ProximityRegex:
    """Proximity regex query expression."""

    left_term: str
    pattern: str
    distance: int
    ordered: bool = False
    max_expansions: int = 50
    boost: float | None = None
    const: float | None = None

    def __post_init__(self) -> None:
        _validate_string("ProximityRegex left_term", self.left_term)
        _validate_string("ProximityRegex pattern", self.pattern)
        _validate_non_negative_int("ProximityRegex distance", self.distance)
        _validate_non_negative_int("ProximityRegex max_expansions", self.max_expansions)


@dataclass(frozen=True)
class ProxRegex:
    """Regex clause for use inside :class:`ProximityArray`.

    Wraps ``pdb.prox_regex(pattern, max_expansions)`` so that regex items can
    be mixed with plain-string terms inside a ``prox_array`` call.
    """

    pattern: str
    max_expansions: int = 50

    def __post_init__(self) -> None:
        _validate_string("ProxRegex pattern", self.pattern)
        _validate_non_negative_int("ProxRegex max_expansions", self.max_expansions)


@dataclass(frozen=True)
class ProximityArray:
    """Proximity array query expression."""

    left_terms: tuple[str | ProxRegex, ...]
    right_term: str
    distance: int
    ordered: bool = False
    right_pattern: str | None = None
    max_expansions: int = 50
    boost: float | None = None
    const: float | None = None

    def __init__(
        self,
        *left_terms: str | ProxRegex,
        right_term: str,
        distance: int,
        ordered: bool = False,
        right_pattern: str | None = None,
        max_expansions: int = 50,
        boost: float | None = None,
        const: float | None = None,
    ) -> None:
        if not left_terms:
            raise ValueError("ProximityArray requires at least one left-side term.")
        for left_term in left_terms:
            if not isinstance(left_term, str | ProxRegex):
                raise TypeError(
                    "ProximityArray left_terms must be strings or ProxRegex instances."
                )
        _validate_string("ProximityArray right_term", right_term)
        _validate_non_negative_int("ProximityArray distance", distance)
        _validate_non_negative_int("ProximityArray max_expansions", max_expansions)
        if right_pattern is not None:
            _validate_string("ProximityArray right_pattern", right_pattern)
        object.__setattr__(self, "left_terms", tuple(left_terms))
        object.__setattr__(self, "right_term", right_term)
        object.__setattr__(self, "distance", distance)
        object.__setattr__(self, "ordered", ordered)
        object.__setattr__(self, "right_pattern", right_pattern)
        object.__setattr__(self, "max_expansions", max_expansions)
        object.__setattr__(self, "boost", boost)
        object.__setattr__(self, "const", const)


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
class Empty:
    """Match-nothing query expression (opposite of All)."""

    boost: float | None = None
    const: float | None = None


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
class ParseWithField:
    """Query string parser scoped to the LHS field."""

    query: str
    lenient: bool | None = None
    conjunction_mode: bool | None = None
    boost: float | None = None
    const: float | None = None

    def __post_init__(self) -> None:
        _validate_string("ParseWithField query", self.query)
        _validate_optional_bool("ParseWithField lenient", self.lenient)
        _validate_optional_bool(
            "ParseWithField conjunction_mode", self.conjunction_mode
        )


@dataclass(frozen=True)
class Range:
    """Range query against the LHS field.

    ``range`` is a PostgreSQL range literal (e.g. ``'[1, 10]'``) and
    ``range_type`` is one of the supported PostgreSQL range types
    (``int4range``, ``int8range``, ``numrange``, ``daterange``, ``tsrange``,
    ``tstzrange``).
    """

    range: str
    range_type: RangeType
    boost: float | None = None
    const: float | None = None

    def __post_init__(self) -> None:
        _validate_string("Range range", self.range)
        _validate_range_type(self.range_type)


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
    tokenizer: str | None = None
    distance: int | None = None
    prefix: bool = False
    transposition_cost_one: bool = False
    boost: float | None = None
    const: float | None = None

    def __init__(
        self,
        *terms: str,
        operator: ParadeOperator,
        tokenizer: str | None = None,
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
        if tokenizer is not None and not isinstance(tokenizer, str):
            raise TypeError("Match tokenizer must be a string.")
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

    Provide exactly one of product_id, product_ids, or document (dict/JSON string).
    Fields are only valid with product_id/product_ids.

    Args:
        product_id: Single document ID for similarity search
        product_ids: Multiple document IDs for similarity search (OR'd together)
        document: Custom JSON document (dict or JSON string) for similarity search
        fields: List of fields to consider (only valid with product_id/product_ids)
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
        product_id: int | None = None,
        product_ids: Iterable[int] | None = None,
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
        self.product_id = product_id
        self.product_ids = list(product_ids) if product_ids is not None else None
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

        self._validate_product_inputs()
        self._validate_key_field()
        self._validate_fields()
        self._validate_stopwords()

        # Validate fields only with ID-based queries
        if self._document_input is not None and self.fields:
            raise ValueError("MoreLikeThis fields are only valid with product_id(s).")

        # Validate document type and convert to JSON string
        self._validate_document_input()
        self._validate_numeric_params()

    def _count_inputs(self) -> int:
        return sum(
            [
                self.product_id is not None,
                self.product_ids is not None,
                self._document_input is not None,
            ]
        )

    def _validate_product_inputs(self) -> None:
        if self.product_id is not None and (
            isinstance(self.product_id, bool) or not isinstance(self.product_id, int)
        ):
            raise TypeError("MoreLikeThis product_id must be an integer.")

        if self.product_ids is not None and not self.product_ids:
            raise ValueError("MoreLikeThis product_ids cannot be empty.")

        if self.product_ids is not None and any(
            isinstance(product_id, bool) or not isinstance(product_id, int)
            for product_id in self.product_ids
        ):
            raise TypeError("MoreLikeThis product_ids must contain integers.")

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

    def resolve_expression(
        self,
        query: Any = None,
        allow_joins: bool = True,
        reuse: set[str] | None = None,
        summarize: bool = False,
        for_save: bool = False,
    ) -> MoreLikeThis:
        return super().resolve_expression(
            query=query,
            allow_joins=allow_joins,
            reuse=reuse,
            summarize=summarize,
            for_save=for_save,
        )

    def get_source_expressions(self) -> list[object]:
        return []

    def as_sql(
        self, compiler: SQLCompiler, connection: BaseDatabaseWrapper
    ) -> tuple[str, list[Any]]:
        pk_sql = self._pk_sql(compiler, connection)
        params: list[Any] = []

        if self.product_ids is not None:
            expressions = []
            for value in self.product_ids:
                mlt_sql, mlt_params = self._render_mlt_call(value)
                expressions.append(f"{pk_sql} {OP_SEARCH} {mlt_sql}")
                params.extend(mlt_params)
            joined = " OR ".join(expressions)
            return f"({joined})", params

        if self.product_id is not None:
            mlt_sql, mlt_params = self._render_mlt_call(self.product_id)
            params.extend(mlt_params)
            return f"{pk_sql} {OP_SEARCH} {mlt_sql}", params

        mlt_sql, mlt_params = self._render_mlt_call(self.document)
        params.extend(mlt_params)
        return f"{pk_sql} {OP_SEARCH} {mlt_sql}", params

    def _pk_sql(self, compiler: SQLCompiler, connection: BaseDatabaseWrapper) -> str:
        query = compiler.query
        alias = query.get_initial_alias()
        model = query.model
        assert model is not None

        # Use custom key_field if provided, otherwise default to primary key
        if self.key_field:
            # Find the field in the model to get its column name
            try:
                field = model._meta.get_field(self.key_field)
            except FieldDoesNotExist as e:
                raise ValueError(
                    f"MoreLikeThis key_field '{self.key_field}' not found in model {model.__name__}"
                ) from e
            # Check if field has column attribute (exclude relations)
            if not hasattr(field, "column") or field.column is None:
                raise ValueError(
                    f"MoreLikeThis key_field '{self.key_field}' must be a concrete field with a column"
                )
            key_column: str = field.column
        else:
            key_column = model._meta.pk.column

        return f"{connection.ops.quote_name(alias)}.{connection.ops.quote_name(key_column)}"

    def _render_mlt_call(self, value: int | str | None) -> tuple[str, list[Any]]:
        """Render the more_like_this function call with parameterized values.

        Returns:
            Tuple of (sql_string, parameters_list)
        """
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

        if self.fields and not isinstance(value, str):
            # Fields array only valid for ID-based queries
            # Build parameterized array
            field_placeholders = ", ".join("%s" for _ in self.fields)
            args.append(f"ARRAY[{field_placeholders}]::text[]")
            params.extend(self.fields)

        options, option_params = self._render_options(
            {
                "min_term_frequency": self.min_term_freq,
                "max_query_terms": self.max_query_terms,
                "min_doc_frequency": self.min_doc_freq,
                "max_term_frequency": self.max_term_freq,
                "max_doc_frequency": self.max_doc_freq,
                "min_word_length": self.min_word_length,
                "max_word_length": self.max_word_length,
                "stopwords": self.stopwords,
            }
        )
        params.extend(option_params)
        return f"{FN_MORE_LIKE_THIS}({', '.join(args)}{options})", params

    @staticmethod
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

    @overload
    def __init__(self, __match: Match) -> None:
        """Explicit literal search with required Match operator."""
        ...

    @overload
    def __init__(
        self,
        __expr: Empty
        | Exists
        | FuzzyTerm
        | ParseWithField
        | Range
        | TermSet
        | Parse
        | PhrasePrefix
        | RegexPhrase
        | ProximityRegex
        | ProximityArray
        | RangeTerm
        | Term
        | Regex
        | All,
    ) -> None:
        """Query expression (must be sole argument)."""
        ...

    @overload
    def __init__(self, __phrase1: Phrase, *phrases: Phrase) -> None:
        """Phrase search with one or more Phrase objects."""
        ...

    @overload
    def __init__(self, __proximity: Proximity) -> None:
        """Proximity search with a single Proximity object."""
        ...

    def __init__(
        self,
        *terms: Match
        | Phrase
        | Proximity
        | Empty
        | Exists
        | FuzzyTerm
        | ParseWithField
        | Range
        | TermSet
        | Parse
        | PhrasePrefix
        | RegexPhrase
        | ProximityRegex
        | ProximityArray
        | RangeTerm
        | Term
        | Regex
        | All,
        tokenizer: str | None = None,
        boost: float | None = None,
        const: float | None = None,
    ) -> None:
        if not terms:
            raise ValueError("ParadeDB requires at least one search term.")
        self._terms = terms
        self._tokenizer = tokenizer
        self._distance: int | None = None
        self._prefix = False
        self._transposition_cost_one = False
        self._boost = boost
        self._const = const

        if any(isinstance(term, str) for term in self._terms):
            raise TypeError(
                "Plain string terms are not supported. Use ParadeDB(Match(..., operator=...))."
            )

        if self._tokenizer is not None:
            raise ValueError(
                "ParadeDB tokenizer keyword is only supported via Match(..., tokenizer=...)."
            )
        if self._boost is not None:
            raise ValueError(
                "ParadeDB boost keyword is only supported via Match(..., boost=...)."
            )
        if self._const is not None:
            raise ValueError(
                "ParadeDB const keyword is only supported via Match(..., const=...)."
            )

    def resolve_expression(
        self,
        query: Any = None,  # noqa: ARG002
        allow_joins: bool = True,  # noqa: ARG002
        reuse: set[str] | None = None,  # noqa: ARG002
        summarize: bool = False,  # noqa: ARG002
        for_save: bool = False,  # noqa: ARG002
    ) -> ParadeDB:
        return self

    def get_source_expressions(self) -> list[object]:
        return []

    def as_sql(
        self,
        _compiler: SQLCompiler,
        _connection: BaseDatabaseWrapper,
        lhs_sql: str,
    ) -> tuple[str, list[object]]:
        operator, terms = self._resolve_terms()
        literals = [self._render_term(term) for term in terms]

        if len(literals) == 1:
            # Match-level fuzzy and scoring are applied here after rendering.
            rendered = self._append_fuzzy(
                literals[0],
                distance=self._distance,
                prefix=self._prefix,
                transposition_cost_one=self._transposition_cost_one,
            )
            if (
                _is_fuzzy_enabled(
                    distance=self._distance,
                    prefix=self._prefix,
                    transposition_cost_one=self._transposition_cost_one,
                )
                and self._const is not None
            ):
                # pdb.fuzzy has no direct cast to pdb.const; bridge via pdb.query.
                rendered = f"{rendered}::pdb.query"
            scored = self._append_scoring(
                rendered, boost=self._boost, const=self._const
            )
            return f"{lhs_sql} {operator} {scored}", []

        # Multi-term with fuzzy: pdb.fuzzy[] (per-element cast) has no matching operator.
        # Cast the whole ARRAY: ARRAY['term1', 'term2']::pdb.fuzzy(N) is valid.
        # Use bare quoted terms (not literals, which already have per-element fuzzy applied).
        if all(isinstance(t, str) for t in terms) and _is_fuzzy_enabled(
            distance=self._distance,
            prefix=self._prefix,
            transposition_cost_one=self._transposition_cost_one,
        ):
            bare = [self._quote_term(t) for t in terms if isinstance(t, str)]
            array_sql = f"ARRAY[{', '.join(bare)}]"
            array_sql = self._append_fuzzy(
                array_sql,
                distance=self._distance,
                prefix=self._prefix,
                transposition_cost_one=self._transposition_cost_one,
            )
            scored = self._append_scoring(
                array_sql, boost=self._boost, const=self._const
            )
            return f"{lhs_sql} {operator} {scored}", []

        # Multi-term: cast applied to the whole ARRAY, not per-element.
        # Per-element casts (e.g. ARRAY['a'::pdb.boost(2), 'b'::pdb.boost(2)]) produce
        # pdb.boost[] which has no matching operator. text[]::pdb.boost(N) is correct.
        array_sql = f"ARRAY[{', '.join(literals)}]"
        # Apply fuzzy to the whole array for multi-term queries.
        array_sql = self._append_fuzzy(
            array_sql,
            distance=self._distance,
            prefix=self._prefix,
            transposition_cost_one=self._transposition_cost_one,
        )
        if (
            _is_fuzzy_enabled(
                distance=self._distance,
                prefix=self._prefix,
                transposition_cost_one=self._transposition_cost_one,
            )
            and self._const is not None
        ):
            # pdb.fuzzy has no direct cast to pdb.const; bridge via pdb.query.
            array_sql = f"{array_sql}::pdb.query"
        array_sql = self._append_scoring(
            array_sql, boost=self._boost, const=self._const
        )
        return f"{lhs_sql} {operator} {array_sql}", []

    def _resolve_terms(
        self,
    ) -> tuple[
        str,
        tuple[
            str
            | Empty
            | Exists
            | FuzzyTerm
            | ParseWithField
            | Range
            | TermSet
            | Phrase
            | Proximity
            | Parse
            | PhrasePrefix
            | RegexPhrase
            | ProximityRegex
            | ProximityArray
            | RangeTerm
            | Term
            | Regex
            | All,
            ...,
        ],
    ]:
        if any(
            isinstance(
                term,
                Empty
                | Exists
                | FuzzyTerm
                | ParseWithField
                | Range
                | TermSet
                | Parse
                | PhrasePrefix
                | RegexPhrase
                | ProximityRegex
                | ProximityArray
                | RangeTerm
                | Term
                | Regex
                | All,
            )
            for term in self._terms
        ):
            if len(self._terms) != 1:
                raise ValueError(
                    "Empty/Exists/FuzzyTerm/ParseWithField/Range/TermSet/Parse/PhrasePrefix/RegexPhrase/ProximityRegex/ProximityArray/RangeTerm/Term/Regex/All queries must be a single term."
                )
            term = self._terms[0]
            if not isinstance(
                term,
                Empty
                | Exists
                | FuzzyTerm
                | ParseWithField
                | Range
                | TermSet
                | Parse
                | PhrasePrefix
                | RegexPhrase
                | ProximityRegex
                | ProximityArray
                | RangeTerm
                | Term
                | Regex
                | All,
            ):
                raise TypeError(
                    "Empty/Exists/FuzzyTerm/ParseWithField/Range/TermSet/Parse/PhrasePrefix/RegexPhrase/ProximityRegex/ProximityArray/RangeTerm/Term/Regex/All cannot be mixed with other terms."
                )
            return OP_SEARCH, (term,)

        if any(isinstance(term, Match) for term in self._terms):
            if len(self._terms) != 1:
                raise ValueError("Match queries must be a single term.")
            term = self._terms[0]
            if not isinstance(term, Match):
                raise TypeError("Match queries cannot be mixed with other terms.")
            if term.operator == "OR":
                operator = OP_OR
            elif term.operator == "AND":
                operator = OP_AND
            else:
                raise ValueError("Match operator must be 'AND' or 'OR'.")
            self._tokenizer = term.tokenizer
            self._distance = term.distance
            self._prefix = term.prefix
            self._transposition_cost_one = term.transposition_cost_one
            self._boost = term.boost
            self._const = term.const
            return operator, term.terms

        if any(isinstance(term, Phrase) for term in self._terms):
            phrases: list[Phrase] = []
            for term in self._terms:
                if not isinstance(term, Phrase):
                    raise TypeError("Phrase searches only accept Phrase terms.")
                phrases.append(term)
            return OP_PHRASE, tuple(phrases)

        if any(isinstance(term, Proximity) for term in self._terms):
            if len(self._terms) != 1:
                raise ValueError(
                    "Proximity queries accept a single Proximity term. Use ProximityArray for multiple proximity clauses."
                )
            term = self._terms[0]
            if not isinstance(term, Proximity):
                raise TypeError("Proximity cannot be mixed with other terms.")
            return OP_SEARCH, (term,)

        # Plain string terms are rejected in __init__, so this path is unreachable.
        raise RuntimeError("Unreachable ParadeDB term resolution branch.")

    @staticmethod
    def _quote_term(term: str) -> str:
        if not isinstance(term, str):
            raise TypeError("Search term literal must be a string.")
        escaped = term.replace("'", "''")
        return f"'{escaped}'"

    @staticmethod
    def _quote_range_literal(
        value: int | float | str | date | datetime, range_type: RangeType
    ) -> str:
        if isinstance(value, date | datetime):
            literal = value.isoformat()
        else:
            literal = str(value)
        safe_range_type = _validate_range_type(range_type)
        return f"{ParadeDB._quote_term(literal)}::{safe_range_type}"

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

    def _render_term(
        self,
        term: str
        | Empty
        | Exists
        | FuzzyTerm
        | ParseWithField
        | Range
        | TermSet
        | Phrase
        | Proximity
        | Parse
        | PhrasePrefix
        | RegexPhrase
        | ProximityRegex
        | ProximityArray
        | RangeTerm
        | Term
        | Regex
        | All,
    ) -> str:
        if isinstance(term, Phrase):
            literal = self._quote_term(term.text)
            if term.slop is not None:
                literal = f"{literal}::{PDB_TYPE_SLOP}({term.slop})"
                # pdb.slop has no direct cast to pdb.const; bridge via pdb.query.
                if term.const is not None:
                    literal = f"{literal}::{PDB_TYPE_QUERY}"
            if term.tokenizer is not None:
                literal = f"{literal}::{_tokenizer_cast(term.tokenizer)}"
            return self._append_scoring(literal, boost=term.boost, const=term.const)
        if isinstance(term, Proximity):
            words = [word for word in term.text.split() if word]
            if len(words) < 2:
                raise ValueError(
                    "Proximity text must include at least two whitespace-separated terms."
                )
            operator = OP_PROXIMITY_ORD if term.ordered else OP_PROXIMITY
            clause_sql = self._quote_term(words[0])
            for word in words[1:]:
                clause_sql = (
                    f"{clause_sql} {operator} {term.distance} {operator} "
                    f"{self._quote_term(word)}"
                )
            rendered = f"{FN_PROXIMITY}({clause_sql})"
            return self._append_scoring(rendered, boost=term.boost, const=term.const)
        if isinstance(term, Parse):
            rendered = (
                f"{FN_PARSE}({self._quote_term(term.query)}"
                f"{self._render_options({'lenient': term.lenient, 'conjunction_mode': term.conjunction_mode})})"
            )
            return self._append_scoring(rendered, boost=term.boost, const=term.const)
        if isinstance(term, PhrasePrefix):
            phrases_sql = ", ".join(self._quote_term(phrase) for phrase in term.phrases)
            rendered = (
                f"{FN_PHRASE_PREFIX}(ARRAY[{phrases_sql}]"
                f"{self._render_options({'max_expansion': term.max_expansion})})"
            )
            return self._append_scoring(rendered, boost=term.boost, const=term.const)
        if isinstance(term, RegexPhrase):
            regex_sql = ", ".join(self._quote_term(regex) for regex in term.regexes)
            rendered = (
                f"{FN_REGEX_PHRASE}(ARRAY[{regex_sql}]"
                f"{self._render_options({'slop': term.slop, 'max_expansions': term.max_expansions})})"
            )
            return self._append_scoring(rendered, boost=term.boost, const=term.const)
        if isinstance(term, ProximityRegex):
            operator = OP_PROXIMITY_ORD if term.ordered else OP_PROXIMITY
            rendered = (
                f"{FN_PROXIMITY}("
                f"{self._quote_term(term.left_term)} {operator} {term.distance} {operator} "
                f"{FN_PROX_REGEX}({self._quote_term(term.pattern)}, {term.max_expansions})"
                ")"
            )
            return self._append_scoring(rendered, boost=term.boost, const=term.const)
        if isinstance(term, ProximityArray):
            left_parts: list[str] = []
            for lt in term.left_terms:
                if isinstance(lt, ProxRegex):
                    left_parts.append(
                        f"{FN_PROX_REGEX}({self._quote_term(lt.pattern)}, {lt.max_expansions})"
                    )
                else:
                    left_parts.append(self._quote_term(lt))
            left_sql = ", ".join(left_parts)
            operator = OP_PROXIMITY_ORD if term.ordered else OP_PROXIMITY
            right_sql = self._quote_term(term.right_term)
            if term.right_pattern is not None:
                right_sql = f"{FN_PROX_REGEX}({self._quote_term(term.right_pattern)}, {term.max_expansions})"
            rendered = (
                f"{FN_PROXIMITY}("
                f"{FN_PROX_ARRAY}({left_sql}) {operator} {term.distance} {operator} {right_sql}"
                ")"
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
                    f"{self._quote_term(term.relation)}"
                    ")"
                )
            return self._append_scoring(rendered, boost=term.boost, const=term.const)
        if isinstance(term, Term):
            rendered = f"{FN_TERM}({self._quote_term(term.text)})"
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
            rendered = f"{FN_REGEX}({self._quote_term(term.pattern)})"
            return self._append_scoring(rendered, boost=term.boost, const=term.const)
        if isinstance(term, Empty):
            rendered = f"{FN_EMPTY}()"
            return self._append_scoring(rendered, boost=term.boost, const=term.const)
        if isinstance(term, Exists):
            rendered = f"{FN_EXISTS}()"
            return self._append_scoring(rendered, boost=term.boost, const=term.const)
        if isinstance(term, FuzzyTerm):
            if term.value is not None:
                rendered = f"{FN_FUZZY_TERM}({self._quote_term(term.value)})"
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
        if isinstance(term, ParseWithField):
            rendered = (
                f"{FN_PARSE_WITH_FIELD}({self._quote_term(term.query)}"
                f"{self._render_options({'lenient': term.lenient, 'conjunction_mode': term.conjunction_mode})})"
            )
            return self._append_scoring(rendered, boost=term.boost, const=term.const)
        if isinstance(term, Range):
            rendered = (
                f"{FN_RANGE}({self._quote_range_literal(term.range, term.range_type)})"
            )
            return self._append_scoring(rendered, boost=term.boost, const=term.const)
        if isinstance(term, TermSet):
            array_sql = self._render_term_set_array(term.terms)
            rendered = f"{FN_TERM_SET}({array_sql})"
            return self._append_scoring(rendered, boost=term.boost, const=term.const)
        if isinstance(term, All):
            return f"{FN_ALL}()"
        if not isinstance(term, str):
            raise TypeError("Unsupported ParadeDB term type.")
        # Match(...) resolves into plain string terms, which are rendered here.
        rendered = self._quote_term(term)
        if self._tokenizer is not None:
            rendered = f"{rendered}::{_tokenizer_cast(self._tokenizer)}"
        return rendered

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
            return f"ARRAY[{', '.join(ParadeDB._quote_term(term.isoformat()) for term in datetime_terms)}]::timestamptz[]"
        if isinstance(first, date):
            date_terms: list[date] = []
            for term in terms:
                if not isinstance(term, date) or isinstance(term, datetime):
                    raise TypeError("TermSet terms must all be date values.")
                date_terms.append(term)
            return f"ARRAY[{', '.join(ParadeDB._quote_term(term.isoformat()) for term in date_terms)}]::date[]"
        return (
            f"ARRAY[{', '.join(ParadeDB._quote_term(str(t)) for t in terms)}]::text[]"
        )

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
            return ParadeDB._quote_term(value.isoformat())
        if isinstance(value, str):
            return ParadeDB._quote_term(value)
        raise TypeError("Unsupported option value type.")


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
    "Empty",
    "Exists",
    "FuzzyTerm",
    "Match",
    "MoreLikeThis",
    "ParadeDB",
    "ParadeOperator",
    "Parse",
    "ParseWithField",
    "Phrase",
    "PhrasePrefix",
    "ProxRegex",
    "Proximity",
    "ProximityArray",
    "ProximityRegex",
    "Range",
    "RangeRelation",
    "RangeTerm",
    "RangeType",
    "Regex",
    "RegexPhrase",
    "Term",
    "TermSet",
]
