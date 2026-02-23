"""Custom QuerySet helpers for ParadeDB integrations."""

from __future__ import annotations

import json
from collections.abc import Iterable
from typing import Any, cast

from django.db import models
from django.db.models import Window
from django.db.models.sql.where import WhereNode

from paradedb.functions import Agg
from paradedb.search import MoreLikeThis, ParadeDB, ParadeDBExact


def _contains_paradedb_operator(where: WhereNode) -> bool:
    for child in where.children:
        if isinstance(child, WhereNode):
            if _contains_paradedb_operator(child):
                return True
            continue
        if isinstance(child, ParadeDBExact) and isinstance(
            getattr(child, "rhs", None), ParadeDB
        ):
            return True
        lhs = getattr(child, "lhs", None)
        if isinstance(lhs, MoreLikeThis):
            return True
        rhs = getattr(child, "rhs", None)
        if isinstance(rhs, ParadeDB):
            return True
    return False


class ParadeDBQuerySet(models.QuerySet[Any]):
    """QuerySet with ParadeDB-specific helpers."""

    def facets(
        self,
        *fields: str,
        size: int | None = 10,
        order: str | None = "-count",
        missing: object | None = None,
        agg: dict[str, object] | str | None = None,
        include_rows: bool = True,
        exact: bool | None = None,
    ) -> dict[str, object] | tuple[list[Any], dict[str, object]]:
        # Faceted queries require pdb.agg() OVER () with ORDER BY + LIMIT and a ParadeDB
        # operator in the WHERE clause to trigger the custom scan.
        #
        # Example:
        # SELECT id, description, pdb.agg('{"value_count":{"field":"id"}}') OVER ()
        # FROM mock_items
        # WHERE description ||| 'running shoes'
        # ORDER BY id
        # LIMIT 5;
        if not fields and agg is None:
            raise ValueError("facets() requires fields or agg.")

        self._require_paradedb_operator()
        if include_rows:
            self._require_order_by_and_limit()
        elif exact is False:
            raise ValueError(
                "facets(exact=False) requires include_rows=True so the aggregation "
                "runs as a window function."
            )

        agg_specs = self._build_agg_specs(
            fields=fields,
            size=size,
            order=order,
            missing=missing,
            agg=agg,
        )

        if include_rows:
            queryset = self._normalized_queryset()
            annotations = {
                alias: Window(expression=Agg(spec, exact=exact))
                for alias, spec in agg_specs.items()
            }
            queryset = queryset.annotate(**annotations)
            rows = list(queryset)
            if not rows:
                facets = self._facets_only_multi(agg_specs, exact=exact)
            else:
                facets = self._extract_facets_multi(rows, list(agg_specs.keys()))
            return rows, facets

        return self._facets_only_multi(agg_specs, exact=exact)

    def _normalized_queryset(self) -> ParadeDBQuerySet:
        return cast(ParadeDBQuerySet, self._chain())  # type: ignore[attr-defined]

    def _facets_only(
        self, json_spec: str, *, exact: bool | None = None
    ) -> dict[str, object]:
        queryset = self._normalized_queryset()
        result = queryset.aggregate(_paradedb_facets=Agg(json_spec, exact=exact))
        return result.get("_paradedb_facets") or {}

    def _facets_only_multi(
        self, agg_specs: dict[str, str], *, exact: bool | None = None
    ) -> dict[str, object]:
        queryset = self._normalized_queryset()
        aggregations = {
            alias: Agg(spec, exact=exact) for alias, spec in agg_specs.items()
        }
        result = queryset.aggregate(**aggregations)
        if len(agg_specs) == 1:
            alias = next(iter(agg_specs.keys()))
            return result.get(alias) or {}
        return {alias: result.get(alias) or {} for alias in agg_specs}

    def _require_paradedb_operator(self) -> None:
        if not _contains_paradedb_operator(self.query.where):
            raise ValueError(
                "facets() requires a ParadeDB operator in the WHERE clause. "
                "Add a ParadeDB search filter before calling facets()."
            )

    def _require_order_by_and_limit(self) -> None:
        query = self.query
        has_ordering = bool(query.order_by or query.extra_order_by)
        if not has_ordering and query.default_ordering:
            has_ordering = bool(self.model._meta.ordering)
        if not has_ordering:
            raise ValueError(
                "facets(include_rows=True) requires order_by() and a LIMIT. "
                "Apply order_by(...) and slice the queryset before calling facets()."
            )
        if not query.is_sliced or query.high_mark is None:
            raise ValueError(
                "facets(include_rows=True) requires a LIMIT. "
                "Slice the queryset (e.g. [:10]) before calling facets()."
            )

    def _build_agg_specs(
        self,
        *,
        fields: Iterable[str],
        size: int | None,
        order: str | None,
        missing: object | None,
        agg: dict[str, object] | str | None,
    ) -> dict[str, str]:
        """Build a dict of {alias: json_spec} for each aggregation."""
        if agg is not None:
            if isinstance(agg, str):
                return {"_paradedb_facets": agg}
            return {
                "_paradedb_facets": json.dumps(
                    agg, separators=(",", ":"), sort_keys=True
                )
            }

        fields = list(fields)
        if not fields:
            raise ValueError("facets() requires at least one field.")
        if len(set(fields)) != len(fields):
            raise ValueError("Facet field names must be unique.")

        terms_order = self._resolve_terms_order(order)
        specs: dict[str, str] = {}
        for field in fields:
            if not isinstance(field, str):
                raise TypeError("Facet field names must be strings.")
            terms: dict[str, object] = {"field": field}
            if size is not None:
                if size < 0:
                    raise ValueError("Facet size must be zero or positive.")
                terms["size"] = size
            if terms_order is not None:
                terms["order"] = terms_order
            if missing is not None:
                terms["missing"] = missing
            alias = f"{field}_terms" if len(fields) > 1 else "_paradedb_facets"
            specs[alias] = json.dumps(
                {"terms": terms}, separators=(",", ":"), sort_keys=True
            )
        return specs

    @staticmethod
    def _resolve_terms_order(order: str | None) -> dict[str, str] | None:
        if order is None:
            return None
        mapping = {
            "count": {"_count": "asc"},
            "-count": {"_count": "desc"},
            "key": {"_key": "asc"},
            "-key": {"_key": "desc"},
        }
        if order not in mapping:
            raise ValueError("Facet order must be count, -count, key, or -key.")
        return mapping[order]

    @staticmethod
    def _extract_facets(rows: list[Any], alias: str) -> dict[str, object]:
        if not rows:
            return {}
        first = rows[0]
        if isinstance(first, dict):
            facets = first.get(alias) or {}
            for row in rows:
                row.pop(alias, None)
            return facets
        if isinstance(first, tuple):
            if not first:
                return {}
            facets = first[-1] if isinstance(first[-1], dict) else {}
            for index, row in enumerate(rows):
                if isinstance(row, tuple) and len(row) > 0:
                    rows[index] = row[:-1]
            return facets
        facets = getattr(first, alias, None) or {}
        for row in rows:
            if hasattr(row, alias):
                delattr(row, alias)
        return facets

    @staticmethod
    def _extract_facets_multi(rows: list[Any], aliases: list[str]) -> dict[str, object]:
        if not rows:
            return {}
        first = rows[0]
        if len(aliases) == 1:
            alias = aliases[0]
            if isinstance(first, dict):
                facets = first.get(alias) or {}
                for row in rows:
                    row.pop(alias, None)
                return facets
            if isinstance(first, tuple):
                if not first:
                    return {}
                facets = first[-1] if isinstance(first[-1], dict) else {}
                for index, row in enumerate(rows):
                    if isinstance(row, tuple) and len(row) > 0:
                        rows[index] = row[:-1]
                return facets
            facets = getattr(first, alias, None) or {}
            for row in rows:
                if hasattr(row, alias):
                    delattr(row, alias)
            return facets

        result: dict[str, object] = {}
        if isinstance(first, dict):
            for alias in aliases:
                result[alias] = first.get(alias) or {}
            for row in rows:
                for alias in aliases:
                    row.pop(alias, None)
            return result
        if isinstance(first, tuple):
            if len(first) < len(aliases):
                return {}
            facet_values = first[-len(aliases) :]
            for alias, value in zip(aliases, facet_values, strict=False):
                result[alias] = value if isinstance(value, dict) else {}
            for index, row in enumerate(rows):
                if isinstance(row, tuple) and len(row) >= len(aliases):
                    rows[index] = row[: -len(aliases)]
            return result
        for alias in aliases:
            result[alias] = getattr(first, alias, None) or {}
        for row in rows:
            for alias in aliases:
                if hasattr(row, alias):
                    delattr(row, alias)
        return result


class ParadeDBManager(models.Manager.from_queryset(ParadeDBQuerySet)):  # type: ignore[misc]
    """Manager that exposes ParadeDBQuerySet helpers."""


__all__ = ["ParadeDBManager", "ParadeDBQuerySet"]
