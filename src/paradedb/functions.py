"""ParadeDB annotation functions (Score, Snippet, etc.)."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any, Literal

from django.contrib.postgres.fields import ArrayField
from django.db import DEFAULT_DB_ALIAS, connections
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

    def __init__(self, json_spec: str, *, exact: bool | None = None) -> None:
        if exact is not None and not isinstance(exact, bool):
            raise TypeError("Agg exact must be a boolean when provided.")
        self._json_spec = json_spec
        self._exact = exact
        super().__init__()

    def as_sql(  # type: ignore[override]
        self,
        _compiler: SQLCompiler,
        _connection: BaseDatabaseWrapper,
        **_extra_context: Any,
    ) -> tuple[str, list[Any]]:
        json_literal = _quote_term(self._json_spec)
        if self._exact is False:
            sql = f"{self.function}({json_literal}, false)"
        else:
            sql = f"{self.function}({json_literal})"
        return sql, []


def _execute_table_function(
    sql: str,
    params: Sequence[Any],
    *,
    using: str = DEFAULT_DB_ALIAS,
) -> list[dict[str, Any]]:
    with connections[using].cursor() as cursor:
        cursor.execute(sql, params)
        if cursor.description is None:
            return []
        columns = [column[0] for column in cursor.description]
        rows = cursor.fetchall()
    return [dict(zip(columns, row, strict=False)) for row in rows]


def paradedb_indexes(*, using: str = DEFAULT_DB_ALIAS) -> list[dict[str, Any]]:
    """Return metadata for all BM25 indexes from ``pdb.indexes()``."""
    return _execute_table_function("SELECT * FROM pdb.indexes()", (), using=using)


def paradedb_index_segments(
    index: str, *, using: str = DEFAULT_DB_ALIAS
) -> list[dict[str, Any]]:
    """Return segment metadata for a BM25 index from ``pdb.index_segments()``."""
    return _execute_table_function(
        "SELECT * FROM pdb.index_segments(%s::regclass)", (index,), using=using
    )


def paradedb_verify_index(
    index: str,
    *,
    heapallindexed: bool = False,
    sample_rate: float | None = None,
    report_progress: bool = False,
    verbose: bool = False,
    on_error_stop: bool = False,
    segment_ids: Sequence[int] | None = None,
    using: str = DEFAULT_DB_ALIAS,
) -> list[dict[str, Any]]:
    """Run ``pdb.verify_index()`` for one BM25 index."""
    sql = ["SELECT * FROM pdb.verify_index(%s::regclass"]
    params: list[Any] = [index]
    if heapallindexed:
        sql.append(", heapallindexed => %s::boolean")
        params.append(heapallindexed)
    if sample_rate is not None:
        sql.append(", sample_rate => %s::double precision")
        params.append(sample_rate)
    if report_progress:
        sql.append(", report_progress => %s::boolean")
        params.append(report_progress)
    if verbose:
        sql.append(", verbose => %s::boolean")
        params.append(verbose)
    if on_error_stop:
        sql.append(", on_error_stop => %s::boolean")
        params.append(on_error_stop)
    if segment_ids is not None:
        sql.append(", segment_ids => %s::int[]")
        params.append(list(segment_ids))
    sql.append(")")
    return _execute_table_function("".join(sql), params, using=using)


def paradedb_verify_all_indexes(
    *,
    schema_pattern: str | None = None,
    index_pattern: str | None = None,
    heapallindexed: bool = False,
    sample_rate: float | None = None,
    report_progress: bool = False,
    on_error_stop: bool = False,
    using: str = DEFAULT_DB_ALIAS,
) -> list[dict[str, Any]]:
    """Run ``pdb.verify_all_indexes()`` across BM25 indexes."""
    sql = ["SELECT * FROM pdb.verify_all_indexes("]
    params: list[Any] = []
    named_params: list[tuple[str, str, Any]] = []
    if schema_pattern is not None:
        named_params.append(("schema_pattern", "text", schema_pattern))
    if index_pattern is not None:
        named_params.append(("index_pattern", "text", index_pattern))
    if heapallindexed:
        named_params.append(("heapallindexed", "boolean", heapallindexed))
    if sample_rate is not None:
        named_params.append(("sample_rate", "double precision", sample_rate))
    if report_progress:
        named_params.append(("report_progress", "boolean", report_progress))
    if on_error_stop:
        named_params.append(("on_error_stop", "boolean", on_error_stop))

    if named_params:
        sql.append(
            ", ".join(
                f"{parameter_name} => %s::{parameter_type}"
                for parameter_name, parameter_type, _parameter_value in named_params
            )
        )
        params.extend(
            parameter_value
            for _parameter_name, _parameter_type, parameter_value in named_params
        )

    sql.append(")")
    return _execute_table_function("".join(sql), params, using=using)


__all__ = [
    "Agg",
    "Score",
    "Snippet",
    "SnippetPositions",
    "Snippets",
    "paradedb_index_segments",
    "paradedb_indexes",
    "paradedb_verify_all_indexes",
    "paradedb_verify_index",
]
