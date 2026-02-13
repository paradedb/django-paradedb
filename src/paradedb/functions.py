"""ParadeDB annotation functions (Score, Snippet, etc.)."""

from __future__ import annotations

from typing import Any, Literal

from django.contrib.postgres.fields import ArrayField
from django.db.backends.base.base import BaseDatabaseWrapper
from django.db.models import CharField, F, FloatField, Func, IntegerField, JSONField
from django.db.models.sql.compiler import SQLCompiler


def _quote_term(value: str) -> str:
    escaped = value.replace("'", "''")
    return f"'{escaped}'"


class Score(Func):
    """BM25 score annotation."""

    function = "pdb.score"
    output_field = FloatField()

    def __init__(self, key_field: str | None = None) -> None:
        expression = F(key_field or "pk")
        super().__init__(expression)


class Snippet(Func):
    """Snippet annotation."""

    function = "pdb.snippet"
    output_field = CharField()

    def __init__(
        self,
        field: str,
        *,
        start_sel: str | None = None,
        stop_sel: str | None = None,
        max_num_chars: int | None = None,
    ) -> None:
        self._formatting = (start_sel, stop_sel, max_num_chars)
        super().__init__(F(field))

    def as_sql(  # type: ignore[override]
        self,
        compiler: SQLCompiler,
        _connection: BaseDatabaseWrapper,
        **_extra_context: Any,
    ) -> tuple[str, list[Any]]:
        field_sql, params = compiler.compile(self.source_expressions[0])
        if params:
            raise ValueError("Snippet does not support parameterized fields.")

        args = [field_sql]
        start_sel, stop_sel, max_num_chars = self._formatting
        if start_sel is not None:
            args.append(_quote_term(start_sel))
        if stop_sel is not None:
            args.append(_quote_term(stop_sel))
        if max_num_chars is not None:
            args.append(str(max_num_chars))

        sql = f"{self.function}({', '.join(args)})"
        return sql, []


class Snippets(Func):
    """Multiple-snippets annotation.

    Wraps ``pdb.snippets(column, ...)`` which returns a text array of all
    matching snippet fragments, with named SQL parameters.

    See: https://docs.paradedb.com/documentation/full-text/highlight#multiple-snippets
    """

    function = "pdb.snippets"
    output_field = ArrayField(base_field=CharField())

    _VALID_SORT_BY = ("score", "position")

    def __init__(
        self,
        field: str,
        *,
        start_tag: str | None = None,
        end_tag: str | None = None,
        max_num_chars: int | None = None,
        limit: int | None = None,
        offset: int | None = None,
        sort_by: Literal["score", "position"] | None = None,
    ) -> None:
        if sort_by is not None and sort_by not in self._VALID_SORT_BY:
            raise ValueError(
                f"sort_by must be one of {self._VALID_SORT_BY!r}, got {sort_by!r}"
            )
        self._start_tag = start_tag
        self._end_tag = end_tag
        self._max_num_chars = max_num_chars
        self._limit = limit
        self._offset = offset
        self._sort_by = sort_by
        super().__init__(F(field))

    def as_sql(  # type: ignore[override]
        self,
        compiler: SQLCompiler,
        _connection: BaseDatabaseWrapper,
        **_extra_context: Any,
    ) -> tuple[str, list[Any]]:
        field_sql, params = compiler.compile(self.source_expressions[0])
        if params:
            raise ValueError("Snippets does not support parameterized fields.")

        parts = [field_sql]
        if self._start_tag is not None:
            parts.append(f"start_tag => {_quote_term(self._start_tag)}")
        if self._end_tag is not None:
            parts.append(f"end_tag => {_quote_term(self._end_tag)}")
        if self._max_num_chars is not None:
            parts.append(f"max_num_chars => {self._max_num_chars}")
        if self._limit is not None:
            parts.append(f'"limit" => {self._limit}')
        if self._offset is not None:
            parts.append(f'"offset" => {self._offset}')
        if self._sort_by is not None:
            parts.append(f"sort_by => {_quote_term(self._sort_by)}")

        sql = f"{self.function}({', '.join(parts)})"
        return sql, []


class SnippetPositions(Func):
    """Byte-offset positions annotation.

    Wraps ``pdb.snippet_positions(column)`` which returns start/end byte
    offset pairs for each matching term.

    See: https://docs.paradedb.com/documentation/full-text/highlight#byte-offsets
    """

    function = "pdb.snippet_positions"
    output_field = ArrayField(base_field=ArrayField(base_field=IntegerField()))

    def __init__(self, field: str) -> None:
        super().__init__(F(field))

    def as_sql(  # type: ignore[override]
        self,
        compiler: SQLCompiler,
        _connection: BaseDatabaseWrapper,
        **_extra_context: Any,
    ) -> tuple[str, list[Any]]:
        field_sql, params = compiler.compile(self.source_expressions[0])
        if params:
            raise ValueError("SnippetPositions does not support parameterized fields.")

        sql = f"{self.function}({field_sql})"
        return sql, []


class Agg(Func):
    """Aggregate annotation for ParadeDB facets."""

    function = "pdb.agg"
    output_field = JSONField()
    contains_aggregate = True
    window_compatible = True

    def __init__(self, json_spec: str) -> None:
        self._json_spec = json_spec
        super().__init__()

    def as_sql(  # type: ignore[override]
        self,
        _compiler: SQLCompiler,
        _connection: BaseDatabaseWrapper,
        **_extra_context: Any,
    ) -> tuple[str, list[Any]]:
        json_literal = _quote_term(self._json_spec)
        sql = f"{self.function}({json_literal})"
        return sql, []


__all__ = ["Agg", "Score", "Snippet", "SnippetPositions", "Snippets"]
