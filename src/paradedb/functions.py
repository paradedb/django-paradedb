"""ParadeDB annotation functions (Score, Snippet, etc.)."""

from __future__ import annotations

from typing import Any

from django.db.backends.base.base import BaseDatabaseWrapper
from django.db.models import CharField, F, FloatField, Func
from django.db.models.sql.compiler import SQLCompiler


def _quote_term(value: str) -> str:
    escaped = value.replace("'", "''")
    return f"'{escaped}'"


class Score(Func):
    """BM25 score annotation."""

    function = "pdb.score"
    output_field = FloatField()

    def __init__(self, key_field: str | None = None) -> None:
        expression = F(key_field or "id")
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


__all__ = ["Score", "Snippet"]
