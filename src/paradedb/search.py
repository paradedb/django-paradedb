"""ParadeDB search expressions and lookups."""

from __future__ import annotations

import json
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any, Literal, overload

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

    def __post_init__(self) -> None:
        if self.slop is not None and self.slop < 0:
            raise ValueError("Phrase slop must be zero or positive.")


@dataclass(frozen=True)
class Fuzzy:
    """Fuzzy search expression.

    Note: Distance parameter is limited to max 2 by ParadeDB. This defines the maximum
    number of character edits allowed (insertions, deletions, substitutions) when matching.
    See: https://docs.paradedb.com/documentation/full-text/fuzzy
    """

    text: str
    distance: int = 1

    def __post_init__(self) -> None:
        if self.distance < 0:
            raise ValueError("Fuzzy distance must be zero or positive.")
        if self.distance > 2:
            raise ValueError("Fuzzy distance must be <= 2.")


@dataclass(frozen=True)
class Parse:
    """Parse query expression."""

    query: str
    lenient: bool | None = None


@dataclass(frozen=True)
class Term:
    """Term query expression."""

    text: str


@dataclass(frozen=True)
class Regex:
    """Regex query expression."""

    pattern: str


@dataclass(frozen=True)
class All:
    """Match-all query expression."""


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
        # Simple AND search (multiple strings allowed)
        ParadeDB('running', 'shoes')

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

        # Fuzzy search (multiple fuzzy allowed, no mixing with strings)
        ParadeDB(Fuzzy('typo'))
        ParadeDB(Fuzzy('a'), Fuzzy('b'))     # ✅ Valid
        ParadeDB(Fuzzy('a'), 'b')            # ❌ Error - no mixing

    Raises:
        ValueError: If PQ is mixed with other terms, or Parse/Term/Regex/All
            is not provided as a single term
        TypeError: If Phrase/Fuzzy terms are mixed with strings
    """

    contains_aggregate = False
    contains_over_clause = False
    contains_column_references = False

    @overload
    def __init__(self, __term1: str, *terms: str) -> None:
        """Simple AND search with multiple string terms."""
        ...

    @overload
    def __init__(self, __pq: PQ) -> None:
        """Boolean logic with PQ object (must be sole argument)."""
        ...

    @overload
    def __init__(self, __expr: Parse | Term | Regex | All) -> None:
        """Query expression (must be sole argument)."""
        ...

    @overload
    def __init__(self, __phrase1: Phrase, *phrases: Phrase) -> None:
        """Phrase search with one or more Phrase objects."""
        ...

    @overload
    def __init__(self, __fuzzy1: Fuzzy, *fuzzy: Fuzzy) -> None:
        """Fuzzy search with one or more Fuzzy objects."""
        ...

    def __init__(
        self, *terms: str | PQ | Phrase | Fuzzy | Parse | Term | Regex | All
    ) -> None:
        if not terms:
            raise ValueError("ParadeDB requires at least one search term.")
        self._terms = terms

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
            return f"{lhs_sql} {operator} {literals[0]}", []

        array_sql = f"ARRAY[{', '.join(literals)}]"
        return f"{lhs_sql} {operator} {array_sql}", []

    def _resolve_terms(
        self,
    ) -> tuple[str, tuple[str | Phrase | Fuzzy | Parse | Term | Regex | All, ...]]:
        if len(self._terms) == 1 and isinstance(self._terms[0], PQ):
            pq = self._terms[0]
            operator = "|||" if pq.operator == "OR" else "&&&"
            return operator, pq.terms

        if any(isinstance(term, PQ) for term in self._terms):
            raise ValueError("PQ objects must be provided as the sole ParadeDB input.")

        if any(isinstance(term, Parse | Term | Regex | All) for term in self._terms):
            if len(self._terms) != 1:
                raise ValueError("Parse/Term/Regex/All queries must be a single term.")
            term = self._terms[0]
            if not isinstance(term, Parse | Term | Regex | All):
                raise TypeError(
                    "Parse/Term/Regex/All cannot be mixed with other terms."
                )
            return "@@@", (term,)

        if any(isinstance(term, Phrase) for term in self._terms):
            phrases: list[Phrase] = []
            for term in self._terms:
                if not isinstance(term, Phrase):
                    raise TypeError("Phrase searches only accept Phrase terms.")
                phrases.append(term)
            return "###", tuple(phrases)

        if any(isinstance(term, Fuzzy) for term in self._terms):
            fuzzies: list[Fuzzy] = []
            for term in self._terms:
                if not isinstance(term, Fuzzy):
                    raise TypeError("Fuzzy searches only accept Fuzzy terms.")
                fuzzies.append(term)
            return "|||", tuple(fuzzies)

        terms: list[str] = []
        for term in self._terms:
            if not isinstance(term, str):
                raise TypeError("ParadeDB terms must be strings.")
            terms.append(term)

        return "&&&", tuple(terms)

    @staticmethod
    def _quote_term(term: str) -> str:
        escaped = term.replace("'", "''")
        return f"'{escaped}'"

    def _render_term(
        self, term: str | Phrase | Fuzzy | Parse | Term | Regex | All
    ) -> str:
        if isinstance(term, Phrase):
            literal = self._quote_term(term.text)
            if term.slop is None:
                return literal
            return f"{literal}::pdb.slop({term.slop})"
        if isinstance(term, Fuzzy):
            literal = self._quote_term(term.text)
            return f"{literal}::pdb.fuzzy({term.distance})"
        if isinstance(term, Parse):
            options = self._render_options({"lenient": term.lenient})
            return f"pdb.parse({self._quote_term(term.query)}{options})"
        if isinstance(term, Term):
            return f"pdb.term({self._quote_term(term.text)})"
        if isinstance(term, Regex):
            return f"pdb.regex({self._quote_term(term.pattern)})"
        if isinstance(term, All):
            return "pdb.all()"
        return self._quote_term(term)

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
    "PQ",
    "Fuzzy",
    "MoreLikeThis",
    "ParadeDB",
    "Parse",
    "Phrase",
    "Regex",
    "Term",
]
