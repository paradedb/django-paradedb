"""ParadeDB search expressions and lookups."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any, Literal

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

    Find documents similar to a reference document or text.

    Args:
        product_id: Find documents similar to this document ID.
        product_ids: Find documents similar to any of these document IDs (OR).
        document: A dict mapping field names to text values for similarity matching.
            Example: {'description': 'comfortable running shoes'}
        text: Plain text for similarity matching. Requires `fields` parameter.
            The text will be matched against all specified fields.
        fields: Field names to consider for matching.
            - With product_id/product_ids: limits which fields are compared.
            - With text: required; specifies which fields the text applies to.
        min_term_freq: Exclude terms appearing fewer than N times in input.
        max_query_terms: Max number of terms to consider (default: 25).
        min_doc_freq: Exclude terms appearing in fewer than N documents.
        max_term_freq: Exclude terms appearing more than N times in input.
        max_doc_freq: Exclude terms appearing in more than N documents.
    """

    conditional = True
    output_field = BooleanField()

    def __init__(
        self,
        *,
        product_id: int | None = None,
        product_ids: Iterable[int] | None = None,
        document: dict[str, str] | None = None,
        text: str | None = None,
        fields: Iterable[str] | None = None,
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
        self.document = document
        self.text = text
        self.fields = list(fields) if fields is not None else None
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
        inputs = [
            self.product_id is not None,
            self.product_ids is not None,
            self.document is not None,
            self.text is not None,
        ]
        if sum(inputs) != 1:
            raise ValueError(
                "MoreLikeThis requires exactly one of: "
                "product_id, product_ids, document, or text."
            )
        if self.product_ids is not None and not self.product_ids:
            raise ValueError("MoreLikeThis product_ids cannot be empty.")
        if self.document is not None and not self.document:
            raise ValueError("MoreLikeThis document cannot be empty.")
        if self.text is not None and not self.fields:
            raise ValueError(
                "MoreLikeThis with text requires fields parameter. "
                "Example: MoreLikeThis(text='running shoes', fields=['description'])"
            )

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

        if self.product_ids is not None:
            expressions = [
                f"{pk_sql} @@@ {self._render_mlt_call_for_id(value)}"
                for value in self.product_ids
            ]
            joined = " OR ".join(expressions)
            return f"({joined})", []

        if self.product_id is not None:
            return f"{pk_sql} @@@ {self._render_mlt_call_for_id(self.product_id)}", []

        if self.document is not None:
            return (
                f"{pk_sql} @@@ {self._render_mlt_call_for_document(self.document)}",
                [],
            )

        # text with fields - build document from text applied to all fields
        assert self.text is not None and self.fields is not None
        doc = dict.fromkeys(self.fields, self.text)
        return f"{pk_sql} @@@ {self._render_mlt_call_for_document(doc)}", []

    def _pk_sql(self, compiler: SQLCompiler, connection: BaseDatabaseWrapper) -> str:
        query = compiler.query
        alias = query.get_initial_alias()
        model = query.model
        assert model is not None
        pk_column = model._meta.pk.column
        return (
            f"{connection.ops.quote_name(alias)}.{connection.ops.quote_name(pk_column)}"
        )

    def _render_mlt_call_for_id(self, value: int) -> str:
        """Render pdb.more_like_this() for a document ID."""
        args: list[str] = [str(value)]

        if self.fields:
            fields_sql = ", ".join(self._quote_term(field) for field in self.fields)
            args.append(f"ARRAY[{fields_sql}]")

        options = self._render_options(self._get_options())
        return f"pdb.more_like_this({', '.join(args)}{options})"

    def _render_mlt_call_for_document(self, doc: dict[str, str]) -> str:
        """Render pdb.more_like_this() for a JSON document."""
        import json

        json_str = json.dumps(doc, separators=(",", ":"))
        args: list[str] = [self._quote_term(json_str)]

        options = self._render_options(self._get_options())
        return f"pdb.more_like_this({', '.join(args)}{options})"

    def _get_options(self) -> dict[str, object | None]:
        return {
            "min_term_frequency": self.min_term_freq,
            "max_query_terms": self.max_query_terms,
            "min_doc_frequency": self.min_doc_freq,
            "max_term_frequency": self.max_term_freq,
            "max_doc_frequency": self.max_doc_freq,
            "min_word_length": self.min_word_length,
            "max_word_length": self.max_word_length,
            "stopwords": self.stopwords,
        }

    @staticmethod
    def _quote_term(value: str) -> str:
        escaped = value.replace("'", "''")
        return f"'{escaped}'"

    @staticmethod
    def _render_options(options: dict[str, object | None]) -> str:
        rendered: list[str] = []
        for key, value in options.items():
            if value is None:
                continue
            # Handle stopwords array
            if key == "stopwords" and isinstance(value, list):
                quoted = "'" + "','".join(w.replace("'", "''") for w in value) + "'"
                rendered.append(f"{key} => ARRAY[{quoted}]")
            else:
                rendered.append(f"{key} => {value}")
        if not rendered:
            return ""
        return ", " + ", ".join(rendered)


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
    """Wrapper for ParadeDB search terms."""

    contains_aggregate = False
    contains_over_clause = False
    contains_column_references = False

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
    "PQ",
    "Fuzzy",
    "MoreLikeThis",
    "ParadeDB",
    "Parse",
    "Phrase",
    "Regex",
    "Term",
]
