"""Composable, field-aware ParadeDB search query inputs."""

from __future__ import annotations

import math
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class SearchQuery:
    _function: str
    _values: tuple[Any, ...]

    def __post_init__(self) -> None:
        if self._function not in ("parse", "boolean", "disjunction_max"):
            raise ValueError("Unsupported query-input function")

    @staticmethod
    def parse(
        query: str, *, lenient: bool = False, conjunction_mode: bool = False
    ) -> SearchQuery:
        if not isinstance(query, str):
            raise TypeError("query must be a string")
        return SearchQuery("parse", (query, lenient, conjunction_mode))

    def as_sql(self) -> tuple[str, list[Any]]:
        if self._function == "parse":
            return "paradedb.parse(%s, %s, %s)", list(self._values)
        args = []
        params: list[Any] = []
        arrays = self._values[:3] if self._function == "boolean" else self._values[:1]
        for clauses in arrays:
            parts = []
            for clause in clauses:
                sql, values = clause.as_sql()
                parts.append(sql)
                params.extend(values)
            args.append(f"ARRAY[{', '.join(parts)}]::paradedb.searchqueryinput[]")
        args.append("%s")
        params.append(self._values[-1])
        return f"paradedb.{self._function}({', '.join(args)})", params


def _queries(values: Sequence[str | SearchQuery]) -> tuple[SearchQuery, ...]:
    if isinstance(values, str):
        raise TypeError("clauses must be a sequence of queries")
    return tuple(
        value if isinstance(value, SearchQuery) else SearchQuery.parse(value)
        for value in values
    )


def BooleanQuery(
    *,
    must: Sequence[str | SearchQuery] = (),
    should: Sequence[str | SearchQuery] = (),
    must_not: Sequence[str | SearchQuery] = (),
    minimum_should_match: int | None = None,
) -> SearchQuery:
    if minimum_should_match is not None and (
        isinstance(minimum_should_match, bool)
        or not isinstance(minimum_should_match, int)
        or minimum_should_match < 0
    ):
        raise ValueError("minimum_should_match must be a non-negative integer")
    return SearchQuery(
        "boolean",
        (_queries(must), _queries(should), _queries(must_not), minimum_should_match),
    )


def DisjunctionMax(
    disjuncts: Sequence[str | SearchQuery], *, tie_breaker: float | None = None
) -> SearchQuery:
    clauses = _queries(disjuncts)
    if not clauses:
        raise ValueError("disjuncts must not be empty")
    if tie_breaker is not None and (
        isinstance(tie_breaker, bool)
        or not isinstance(tie_breaker, (int, float))
        or not math.isfinite(tie_breaker)
        or not 0 <= tie_breaker <= 1
    ):
        raise ValueError("tie_breaker must be between 0 and 1")
    return SearchQuery("disjunction_max", (clauses, tie_breaker))
