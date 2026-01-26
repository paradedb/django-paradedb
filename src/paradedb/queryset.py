"""Custom QuerySet helpers for ParadeDB integrations."""

from __future__ import annotations

import json
from collections.abc import Iterable
from typing import Any, cast

from django.db import models
from django.db.models import Window
from django.db.models.query import ModelIterable
from django.db.models.sql.where import WhereNode

from paradedb.functions import Agg
from paradedb.indexes import BM25Index
from paradedb.search import All, MoreLikeThis, ParadeDB, ParadeDBExact


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
        missing: str | None = None,
        agg: dict[str, object] | str | None = None,
        include_rows: bool = True,
    ) -> dict[str, object] | tuple[list[Any], dict[str, object]]:
        if not fields and agg is None:
            raise ValueError("facets() requires fields or agg.")

        json_spec = self._build_agg_json(
            fields=fields,
            size=size,
            order=order,
            missing=missing,
            agg=agg,
        )

        if include_rows:
            queryset = self._normalized_queryset()._with_paradedb_sentinel()
            alias = "_paradedb_facets"
            queryset = queryset.annotate(**{alias: Window(expression=Agg(json_spec))})
            rows = list(queryset)
            if not rows:
                facets = self._facets_only(json_spec)
            else:
                facets = self._extract_facets(rows, alias)
            return rows, facets

        return self._facets_only(json_spec)

    def _normalized_queryset(self) -> ParadeDBQuerySet:
        queryset = cast(ParadeDBQuerySet, self._chain())  # type: ignore[attr-defined]
        if queryset._fields is not None:  # type: ignore[attr-defined]
            queryset._fields = None  # type: ignore[attr-defined]
            queryset._iterable_class = ModelIterable
            queryset.query.set_values(())
        return queryset

    def _facets_only(self, json_spec: str) -> dict[str, object]:
        queryset = self._normalized_queryset()._with_paradedb_sentinel()
        result = queryset.aggregate(_paradedb_facets=Agg(json_spec))
        return result.get("_paradedb_facets") or {}

    def _with_paradedb_sentinel(self) -> ParadeDBQuerySet:
        if _contains_paradedb_operator(self.query.where):
            return self
        key_field = self._resolve_key_field()
        return self.filter(**{key_field: ParadeDB(All())})

    def _resolve_key_field(self) -> str:
        for index in self.model._meta.indexes:
            if isinstance(index, BM25Index):
                return index.key_field
        pk = self.model._meta.pk
        return pk.name if pk else "id"

    def _build_agg_json(
        self,
        *,
        fields: Iterable[str],
        size: int | None,
        order: str | None,
        missing: str | None,
        agg: dict[str, object] | str | None,
    ) -> str:
        if agg is not None:
            if isinstance(agg, str):
                return agg
            return json.dumps(agg, separators=(",", ":"), sort_keys=True)

        terms_order = self._resolve_terms_order(order)
        aggregations: dict[str, object] = {}
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
            aggregations[field] = {"terms": terms}

        return json.dumps(aggregations, separators=(",", ":"), sort_keys=True)

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
        facets = getattr(first, alias, None) or {}
        for row in rows:
            if hasattr(row, alias):
                delattr(row, alias)
        return facets


class ParadeDBManager(models.Manager.from_queryset(ParadeDBQuerySet)):  # type: ignore[misc]
    """Manager that exposes ParadeDBQuerySet helpers."""


__all__ = ["ParadeDBManager", "ParadeDBQuerySet"]
