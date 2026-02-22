"""ParadeDB search expressions and lookups."""

from __future__ import annotations

import json
import re
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date, datetime
from functools import wraps
from typing import Any, Callable, Literal, TypeVar, cast, overload

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

PQOperator = Literal["OR", "AND"]
ParadeOperator = Literal["OR", "AND"]
_DEFAULT_OPERATOR = object()
F = TypeVar("F", bound=Callable[..., Any])


# Regex to detect simple PostgreSQL identifiers (no quoting needed) vs complex ones.
# Simple identifiers can be used directly as `pdb.name`, while complex identifiers
# require pg_sql.Identifier quoting to prevent SQL injection. Using a whitelist of
# ParadeDB tokenizers would require constant updates as new features launch.
_SIMPLE_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _tokenizer_cast(name: str) -> str:
    """Return ``::pdb.<name>`` using quoting only when the name is not a plain identifier."""
    if _SIMPLE_IDENTIFIER_RE.match(name):
        return f"pdb.{name}"
    escaped = name.replace('"', '""')
    return f'pdb."{escaped}"'


def validate_distance(method: F) -> F:
    """Validate a `distance` attribute after init/post-init hooks run."""

    @wraps(method)
    def wrapper(self: Any, *args: Any, **kwargs: Any) -> Any:
        result = method(self, *args, **kwargs)
        distance = getattr(self, "distance", None)
        if distance is None:
            return result
        if distance < 0:
            raise ValueError("Distance must be zero or positive.")
        if distance > 2:
            raise ValueError("Distance must be <= 2.")
        return result

    return cast(F, wrapper)


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
        if self.slop is not None and self.slop < 0:
            raise ValueError("Phrase slop must be zero or positive.")


@dataclass(frozen=True)
class Proximity:
    """Proximity search expression."""

    text: str
    distance: int
    ordered: bool = False
    boost: float | None = None
    const: float | None = None

    def __post_init__(self) -> None:
        if self.distance < 0:
            raise ValueError("Proximity distance must be zero or positive.")


@dataclass(frozen=True)
class Parse:
    """Parse query expression."""

    query: str
    lenient: bool | None = None
    conjunction_mode: bool | None = None
    boost: float | None = None
    const: float | None = None


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
        if slop is not None and slop < 0:
            raise ValueError("RegexPhrase slop must be zero or positive.")
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
        if self.distance < 0:
            raise ValueError("ProximityRegex distance must be zero or positive.")
        if self.max_expansions < 0:
            raise ValueError("ProximityRegex max_expansions must be zero or positive.")


@dataclass(frozen=True)
class ProximityArray:
    """Proximity array query expression."""

    left_terms: tuple[str, ...]
    right_term: str
    distance: int
    ordered: bool = False
    right_pattern: str | None = None
    max_expansions: int = 50
    boost: float | None = None
    const: float | None = None

    def __init__(
        self,
        *left_terms: str,
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
        if distance < 0:
            raise ValueError("ProximityArray distance must be zero or positive.")
        if max_expansions < 0:
            raise ValueError("ProximityArray max_expansions must be zero or positive.")
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
    boost: float | None = None
    const: float | None = None

    @validate_distance
    def __post_init__(self) -> None:
        pass


@dataclass(frozen=True)
class Regex:
    """Regex query expression."""

    pattern: str
    boost: float | None = None
    const: float | None = None


@dataclass(frozen=True)
class All:
    """Match-all query expression."""


@dataclass(frozen=True)
class Match:
    """Explicit text-match query expression."""

    terms: tuple[str, ...]
    operator: ParadeOperator
    tokenizer: str | None = None
    distance: int | None = None
    boost: float | None = None
    const: float | None = None

    @validate_distance
    def __init__(
        self,
        *terms: str,
        operator: ParadeOperator,
        tokenizer: str | None = None,
        distance: int | None = None,
        boost: float | None = None,
        const: float | None = None,
    ) -> None:
        if not terms:
            raise ValueError("Match requires at least one search term.")
        if operator not in ("AND", "OR"):
            raise ValueError("Match operator must be 'AND' or 'OR'.")
        object.__setattr__(self, "terms", tuple(terms))
        object.__setattr__(self, "operator", operator)
        object.__setattr__(self, "tokenizer", tokenizer)
        object.__setattr__(self, "distance", distance)
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
        inputs = [
            self.product_id is not None,
            self.product_ids is not None,
            self._document_input is not None,
        ]
        if sum(inputs) != 1:
            raise ValueError("MoreLikeThis requires exactly one input source.")

        # Validate product_ids not empty
        if self.product_ids is not None and not self.product_ids:
            raise ValueError("MoreLikeThis product_ids cannot be empty.")

        # Validate fields only with ID-based queries
        if self._document_input is not None and self.fields:
            raise ValueError("MoreLikeThis fields are only valid with product_id(s).")

        # Validate document type and convert to JSON string
        if self._document_input is not None:
            if not isinstance(self._document_input, dict | str):
                raise ValueError("MoreLikeThis document must be a dict or JSON string.")
            if isinstance(self._document_input, dict):
                self.document = json.dumps(self._document_input)
            else:
                self.document = self._document_input

        # Validate numeric parameters
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
            if param_value is not None:
                if not isinstance(param_value, int):
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
                expressions.append(f"{pk_sql} @@@ {mlt_sql}")
                params.extend(mlt_params)
            joined = " OR ".join(expressions)
            return f"({joined})", params

        if self.product_id is not None:
            mlt_sql, mlt_params = self._render_mlt_call(self.product_id)
            params.extend(mlt_params)
            return f"{pk_sql} @@@ {mlt_sql}", params

        mlt_sql, mlt_params = self._render_mlt_call(self.document)
        params.extend(mlt_params)
        return f"{pk_sql} @@@ {mlt_sql}", params

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
                # Check if field has column attribute (exclude relations)
                if not hasattr(field, "column") or field.column is None:
                    raise ValueError(
                        f"MoreLikeThis key_field '{self.key_field}' must be a concrete field with a column"
                    )
                key_column: str = field.column
            except Exception as e:
                raise ValueError(
                    f"MoreLikeThis key_field '{self.key_field}' not found in model {model.__name__}"
                ) from e
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
        return f"pdb.more_like_this({', '.join(args)}{options})", params

    @staticmethod
    def _quote_term(value: str) -> str:
        escaped = value.replace("'", "''")
        return f"'{escaped}'"

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


@dataclass(frozen=True)
class PQ:
    """Query object for ParadeDB boolean logic."""

    terms: tuple[str, ...]
    operator: PQOperator | None = None

    def __init__(self, term: str) -> None:
        object.__setattr__(self, "terms", (term,))
        object.__setattr__(self, "operator", None)

    def __or__(self, other: PQ) -> PQ:
        return self._combine("OR", other)

    def __and__(self, other: PQ) -> PQ:
        return self._combine("AND", other)

    def _combine(self, operator: PQOperator, other: PQ) -> PQ:
        if not isinstance(other, PQ):
            raise TypeError("PQ objects can only be combined with PQ instances.")

        if self.operator not in (None, operator):
            raise ValueError("Mixed PQ operators are not supported yet.")
        if other.operator not in (None, operator):
            raise ValueError("Mixed PQ operators are not supported yet.")

        terms = (*self.terms, *other.terms)
        return PQ._from_terms(terms, operator)

    @classmethod
    def _from_terms(cls, terms: Iterable[str], operator: PQOperator) -> PQ:
        instance = cls.__new__(cls)
        object.__setattr__(instance, "terms", tuple(terms))
        object.__setattr__(instance, "operator", operator)
        return instance


class ParadeDB:
    """Wrapper for ParadeDB search terms.

    Usage:
        # Explicit literal match query
        ParadeDB(Match('running', 'shoes', operator='AND'))

        # Boolean logic (PQ must be SOLE argument)
        ParadeDB(PQ('shoes') | PQ('boots'))  # ✅ Valid
        ParadeDB(PQ('a'), 'b')               # ❌ Error - PQ must be alone

        # Query expressions (SINGLE expression only)
        ParadeDB(Parse('query'))             # ✅ Valid
        ParadeDB(Parse('a'), Parse('b'))     # ❌ Error - only one allowed

        # Phrase search (multiple phrases allowed, no mixing with strings)
        ParadeDB(Phrase('exact match'))
        ParadeDB(Phrase('a'), Phrase('b'))   # ✅ Valid
        ParadeDB(Phrase('a'), 'b')           # ❌ Error - no mixing

        # Important: Match(..., operator='OR') and PQ(... | ...) are not equivalent.
        # Match(..., operator='OR') sends the raw string to ParadeDB
        # (e.g. ||| 'running shoes'),
        # which is tokenized using the column's configured tokenizer.
        # PQ('running') | PQ('shoes') emits ||| ARRAY['running', 'shoes'],
        # which bypasses tokenizer-based splitting.

    Raises:
        ValueError: If PQ is mixed with other terms, or Parse/Term/Regex/All
            is not provided as a single term, or operator is passed with
            non-string query expressions
        TypeError: If query term types are mixed
    """

    contains_aggregate = False
    contains_over_clause = False
    contains_column_references = False

    @overload
    def __init__(
        self, __match: Match, *, operator: object = _DEFAULT_OPERATOR
    ) -> None:
        """Explicit literal search with required Match operator."""
        ...

    @overload
    def __init__(self, __pq: PQ, *, operator: None = None) -> None:
        """Boolean logic with PQ object (must be sole argument)."""
        ...

    @overload
    def __init__(
        self,
        __expr: Parse
        | PhrasePrefix
        | RegexPhrase
        | ProximityRegex
        | ProximityArray
        | RangeTerm
        | Term
        | Regex
        | All,
        *,
        operator: None = None,
    ) -> None:
        """Query expression (must be sole argument)."""
        ...

    @overload
    def __init__(
        self, __phrase1: Phrase, *phrases: Phrase, operator: None = None
    ) -> None:
        """Phrase search with one or more Phrase objects."""
        ...

    @overload
    def __init__(
        self, __prox1: Proximity, *prox: Proximity, operator: None = None
    ) -> None:
        """Proximity search with a single Proximity object."""
        ...

    def __init__(
        self,
        *terms: str
        | Match
        | PQ
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
        operator: ParadeOperator | object = _DEFAULT_OPERATOR,
        tokenizer: str | None = None,
        boost: float | None = None,
        const: float | None = None,
    ) -> None:
        if not terms:
            raise ValueError("ParadeDB requires at least one search term.")
        self._terms = terms
        self._tokenizer = tokenizer
        self._distance: int | None = None
        self._boost = boost
        self._const = const
        self._operator: ParadeOperator = "AND"

        if any(isinstance(term, str) for term in self._terms):
            raise TypeError(
                "Plain string terms are not supported. Use ParadeDB(Match(..., operator=...))."
            )
        if operator is not _DEFAULT_OPERATOR:
            raise ValueError(
                "ParadeDB operator keyword is only supported via Match(..., operator=...)."
            )

        if self._tokenizer is not None and any(
            isinstance(
                term,
                PQ
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
            )
            for term in self._terms
        ):
            raise ValueError(
                "ParadeDB tokenizer is only supported with plain string terms."
            )
        if self._boost is not None and any(
            isinstance(
                term,
                PQ
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
            )
            for term in self._terms
        ):
            raise ValueError(
                "ParadeDB boost is only supported with plain string terms."
            )
        if self._const is not None and any(
            isinstance(
                term,
                PQ
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
            )
            for term in self._terms
        ):
            raise ValueError(
                "ParadeDB const is only supported with plain string terms."
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
            # For plain strings, _render_term returns bare literals; scoring is applied here.
            # For structured types, _render_term already bakes in term-level scoring.
            scored = self._append_scoring(
                literals[0], boost=self._boost, const=self._const
            )
            return f"{lhs_sql} {operator} {scored}", []

        # Multi-term: cast applied to the whole ARRAY, not per-element.
        # Per-element casts (e.g. ARRAY['a'::pdb.boost(2), 'b'::pdb.boost(2)]) produce
        # pdb.boost[] which has no matching operator. text[]::pdb.boost(N) is correct.
        array_sql = f"ARRAY[{', '.join(literals)}]"
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
        if len(self._terms) == 1 and isinstance(self._terms[0], PQ):
            pq = self._terms[0]
            operator = "|||" if pq.operator == "OR" else "&&&"
            return operator, pq.terms

        if any(isinstance(term, PQ) for term in self._terms):
            raise ValueError("PQ objects must be provided as the sole ParadeDB input.")

        if any(
            isinstance(
                term,
                Parse
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
                    "Parse/PhrasePrefix/RegexPhrase/ProximityRegex/ProximityArray/RangeTerm/Term/Regex/All queries must be a single term."
                )
            term = self._terms[0]
            if not isinstance(
                term,
                Parse
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
                    "Parse/PhrasePrefix/RegexPhrase/ProximityRegex/ProximityArray/RangeTerm/Term/Regex/All cannot be mixed with other terms."
                )
            return "@@@", (term,)

        if any(isinstance(term, Match) for term in self._terms):
            if len(self._terms) != 1:
                raise ValueError("Match queries must be a single term.")
            term = self._terms[0]
            if term.operator == "OR":
                operator = "|||"
            elif term.operator == "AND":
                operator = "&&&"
            else:
                raise ValueError("Match operator must be 'AND' or 'OR'.")
            self._tokenizer = term.tokenizer
            self._distance = term.distance
            self._boost = term.boost
            self._const = term.const
            return operator, term.terms

        if any(isinstance(term, Phrase) for term in self._terms):
            phrases: list[Phrase] = []
            for term in self._terms:
                if not isinstance(term, Phrase):
                    raise TypeError("Phrase searches only accept Phrase terms.")
                phrases.append(term)
            return "###", tuple(phrases)

        if any(isinstance(term, Proximity) for term in self._terms):
            if len(self._terms) != 1:
                raise ValueError(
                    "Proximity queries must be a single term. Proximity arrays are not supported yet."
                )
            term = self._terms[0]
            if not isinstance(term, Proximity):
                raise TypeError("Proximity cannot be mixed with other terms.")
            return "@@@", (term,)

        terms: list[str] = []
        for term in self._terms:
            if not isinstance(term, str):
                raise TypeError("ParadeDB terms must be strings.")
            terms.append(term)

        operator = "&&&"
        if self._operator == "OR":
            operator = "|||"
        return operator, tuple(terms)

    @staticmethod
    def _quote_term(term: str) -> str:
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
    def _append_scoring(sql: str, *, boost: float | None, const: float | None) -> str:
        if boost is not None:
            sql = f"{sql}::pdb.boost({boost})"
        if const is not None:
            sql = f"{sql}::pdb.const({const})"
        return sql

    def _render_term(
        self,
        term: str
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
                literal = f"{literal}::pdb.slop({term.slop})"
                # pdb.slop has no direct cast to pdb.const; bridge via pdb.query.
                if term.const is not None:
                    literal = f"{literal}::pdb.query"
            if term.tokenizer is not None:
                literal = f"{literal}::{_tokenizer_cast(term.tokenizer)}"
            return self._append_scoring(literal, boost=term.boost, const=term.const)
        if isinstance(term, Proximity):
            words = [word for word in term.text.split() if word]
            if len(words) < 2:
                raise ValueError(
                    "Proximity text must include at least two whitespace-separated terms."
                )
            operator = "##>" if term.ordered else "##"
            clause_sql = self._quote_term(words[0])
            for word in words[1:]:
                clause_sql = (
                    f"{clause_sql} {operator} {term.distance} {operator} "
                    f"{self._quote_term(word)}"
                )
            rendered = f"pdb.proximity({clause_sql})"
            return self._append_scoring(rendered, boost=term.boost, const=term.const)
        if isinstance(term, Parse):
            rendered = (
                f"pdb.parse({self._quote_term(term.query)}"
                f"{self._render_options({'lenient': term.lenient, 'conjunction_mode': term.conjunction_mode})})"
            )
            return self._append_scoring(rendered, boost=term.boost, const=term.const)
        if isinstance(term, PhrasePrefix):
            phrases_sql = ", ".join(self._quote_term(phrase) for phrase in term.phrases)
            rendered = (
                f"pdb.phrase_prefix(ARRAY[{phrases_sql}]"
                f"{self._render_options({'max_expansion': term.max_expansion})})"
            )
            return self._append_scoring(rendered, boost=term.boost, const=term.const)
        if isinstance(term, RegexPhrase):
            regex_sql = ", ".join(self._quote_term(regex) for regex in term.regexes)
            rendered = (
                f"pdb.regex_phrase(ARRAY[{regex_sql}]"
                f"{self._render_options({'slop': term.slop, 'max_expansions': term.max_expansions})})"
            )
            return self._append_scoring(rendered, boost=term.boost, const=term.const)
        if isinstance(term, ProximityRegex):
            operator = "##>" if term.ordered else "##"
            rendered = (
                "pdb.proximity("
                f"{self._quote_term(term.left_term)} {operator} {term.distance} {operator} "
                f"pdb.prox_regex({self._quote_term(term.pattern)}, {term.max_expansions})"
                ")"
            )
            return self._append_scoring(rendered, boost=term.boost, const=term.const)
        if isinstance(term, ProximityArray):
            left_sql = ", ".join(
                self._quote_term(left_term) for left_term in term.left_terms
            )
            operator = "##>" if term.ordered else "##"
            right_sql = self._quote_term(term.right_term)
            if term.right_pattern is not None:
                right_sql = f"pdb.prox_regex({self._quote_term(term.right_pattern)}, {term.max_expansions})"
            rendered = (
                "pdb.proximity("
                f"pdb.prox_array({left_sql}) {operator} {term.distance} {operator} {right_sql}"
                ")"
            )
            return self._append_scoring(rendered, boost=term.boost, const=term.const)
        if isinstance(term, RangeTerm):
            if term.relation is None:
                rendered = f"pdb.range_term({self._render_value(term.value)})"
            else:
                assert term.range_type is not None
                rendered = (
                    "pdb.range_term("
                    f"{self._quote_range_literal(term.value, term.range_type)}, "
                    f"{self._quote_term(term.relation)}"
                    ")"
                )
            return self._append_scoring(rendered, boost=term.boost, const=term.const)
        if isinstance(term, Term):
            rendered = f"pdb.term({self._quote_term(term.text)})"
            if term.distance is not None:
                rendered = f"{rendered}::pdb.fuzzy({term.distance})"
            return self._append_scoring(rendered, boost=term.boost, const=term.const)
        if isinstance(term, Regex):
            rendered = f"pdb.regex({self._quote_term(term.pattern)})"
            return self._append_scoring(rendered, boost=term.boost, const=term.const)
        if isinstance(term, All):
            return "pdb.all()"
        rendered = self._quote_term(term)
        if self._distance is not None:
            rendered = f"{rendered}::pdb.fuzzy({self._distance})"
        if self._tokenizer is not None:
            rendered = f"{rendered}::{_tokenizer_cast(self._tokenizer)}"
        # Scoring is NOT applied here for plain strings: as_sql applies self._boost/self._const
        # at the right level (to the single literal or to the ARRAY as a whole).
        return rendered

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
    "PQ",
    "All",
    "Match",
    "MoreLikeThis",
    "ParadeDB",
    "ParadeOperator",
    "Parse",
    "Phrase",
    "PhrasePrefix",
    "Proximity",
    "ProximityArray",
    "ProximityRegex",
    "RangeRelation",
    "RangeTerm",
    "RangeType",
    "Regex",
    "RegexPhrase",
    "Term",
]
