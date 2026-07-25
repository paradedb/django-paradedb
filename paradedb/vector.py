"""Native pgvector `vector` support for ParadeDB bm25 indexes."""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, Any

from django.db import models
from django.db.backends.base.base import BaseDatabaseWrapper
from django.db.models import FloatField, Func, Value
from django.db.models.expressions import Combinable, Expression, F
from django.db.models.sql.compiler import SQLCompiler

if TYPE_CHECKING:
    BaseField = models.Field[Any, Any]
else:
    BaseField = models.Field

VECTOR_OPCLASSES: dict[str, str] = {
    "l2": "vector_l2_ops",
    "cosine": "vector_cosine_ops",
    "ip": "vector_ip_ops",
}


def vector_opclass(metric: str) -> str:
    opclass = VECTOR_OPCLASSES.get(metric)
    if opclass is None:
        valid = ", ".join(sorted(VECTOR_OPCLASSES))
        raise ValueError(f"Vector metric must be one of: {valid}.")
    return opclass


def serialize_vector(value: Sequence[float]) -> str:
    return "[" + ",".join(str(float(item)) for item in value) + "]"


def parse_vector(value: str) -> list[float]:
    stripped = value.strip()[1:-1]
    if not stripped:
        return []
    return [float(item) for item in stripped.split(",")]


class VectorField(BaseField):
    """pgvector ``vector(n)`` column."""

    description = "Vector"

    def __init__(
        self, *args: Any, dimensions: int | None = None, **kwargs: Any
    ) -> None:
        if dimensions is not None and (
            isinstance(dimensions, bool)
            or not isinstance(dimensions, int)
            or dimensions < 1
        ):
            raise ValueError("VectorField dimensions must be a positive integer.")
        self.dimensions = dimensions
        super().__init__(*args, **kwargs)

    def deconstruct(self) -> tuple[str, str, Sequence[Any], dict[str, Any]]:
        name, path, args, kwargs = super().deconstruct()
        if self.dimensions is not None:
            kwargs["dimensions"] = self.dimensions
        return name, path, args, kwargs

    def db_type(self, connection: BaseDatabaseWrapper) -> str:  # noqa: ARG002
        if self.dimensions is None:
            return "vector"
        return f"vector({self.dimensions})"

    def get_placeholder(
        self,
        value: Any,  # noqa: ARG002
        compiler: SQLCompiler,  # noqa: ARG002
        connection: BaseDatabaseWrapper,  # noqa: ARG002
    ) -> str:
        return "%s::vector"

    def get_prep_value(self, value: Any) -> str | None:
        if value is None or isinstance(value, str):
            return value
        return serialize_vector(value)

    def from_db_value(
        self,
        value: str | None,
        expression: Expression,  # noqa: ARG002
        connection: BaseDatabaseWrapper,  # noqa: ARG002
    ) -> list[float] | None:
        if value is None:
            return None
        return parse_vector(value)

    def to_python(self, value: Any) -> list[float] | None:
        if value is None or isinstance(value, list):
            return value
        return parse_vector(value)


class VectorDistance(Func):
    """Base for pgvector distance operators usable in order_by()/annotate().

    The operator must match the metric of the bm25 index opclass on the
    column (``l2`` ↔ ``<->``, ``cosine`` ↔ ``<=>``, ``ip`` ↔ ``<#>``),
    otherwise ParadeDB falls back to a plain sort instead of Top-K index
    pushdown.
    """

    template = "(%(expressions)s)"
    output_field = FloatField()

    def __init__(
        self,
        expression: Combinable | str,
        vector: Sequence[float] | Combinable,
    ) -> None:
        lhs = F(expression) if isinstance(expression, str) else expression
        rhs = (
            vector
            if isinstance(vector, Combinable)
            else Value(list(vector), output_field=VectorField())
        )
        super().__init__(lhs, rhs)


# pgvector operators reused verbatim by pg_search; not part of the pg_search
# schema, so intentionally absent from api.json5 (schema-compat checks releases).
class L2Distance(VectorDistance):
    arg_joiner = " <-> "


class CosineDistance(VectorDistance):
    arg_joiner = " <=> "


class InnerProduct(VectorDistance):
    arg_joiner = " <#> "


__all__ = [
    "CosineDistance",
    "InnerProduct",
    "L2Distance",
    "VectorField",
]
