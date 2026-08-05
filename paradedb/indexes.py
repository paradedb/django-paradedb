"""ParadeDB index support for Django models."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, cast

from django.db import models
from django.db.backends.base.schema import BaseDatabaseSchemaEditor
from django.db.backends.ddl_references import Statement
from django.db.models.expressions import Expression
from django.utils.deconstruct import deconstructible

from paradedb.api import PDB_TYPE_TOKENIZER_ALIAS
from paradedb.search import Tokenizer
from paradedb.vector import VectorField, vector_opclass

if TYPE_CHECKING:
    ModelField = models.Field[Any, Any]
else:
    ModelField = models.Field


def _quote_term(value: str) -> str:
    escaped = value.replace("'", "''")
    return f"'{escaped}'"


def _render_sql_arg(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int | float):
        return str(value)
    if value is None:
        return "null"
    if isinstance(value, str):
        return _quote_term(value)
    raise TypeError(f"Unsupported tokenizer arg type: {type(value).__name__}")


def _quote_compiled_param(value: Any, schema_editor: BaseDatabaseSchemaEditor) -> str:
    try:
        return schema_editor.quote_value(value)
    except NotImplementedError:
        return _render_sql_arg(value)


def _inline_compiled_params(
    sql: str,
    params: tuple[Any, ...] | list[Any],
    schema_editor: BaseDatabaseSchemaEditor,
) -> str:
    if not params:
        return sql

    sql_parts = sql.split("%s")
    if len(sql_parts) - 1 != len(params):
        raise ValueError(
            "IndexExpression generated unsupported SQL placeholders. "
            "Only simple value placeholders are supported."
        )

    rendered_sql = sql_parts[0]
    for quoted_param, sql_part in zip(
        (_quote_compiled_param(value, schema_editor) for value in params),
        sql_parts[1:],
        strict=True,
    ):
        rendered_sql += quoted_param + sql_part

    return rendered_sql


def _requires_expression_tokenizer(output_field: ModelField | None) -> bool:
    if output_field is None:
        return False
    return isinstance(
        output_field, (models.CharField, models.TextField, models.JSONField)
    )


def _expression_requires_tokenizer(expression: Expression) -> bool:
    output_field = getattr(expression, "output_field", None)
    if output_field is not None:
        return _requires_expression_tokenizer(output_field)

    for source in expression.get_source_expressions():
        if source is not None and _expression_requires_tokenizer(source):
            return True

    return False


def _validate_int_index_option(name: str, value: Any, *, maximum: int) -> None:
    if value is None:
        return
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{name} must be an integer.")
    if not 1 <= value <= maximum:
        raise ValueError(f"{name} must be between 1 and {maximum}, inclusive.")


def _validate_centroid_ratio(value: Any) -> None:
    if value is None:
        return
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise TypeError("centroid_ratio must be a number.")
    if not 0.000001 <= value <= 1.0:
        raise ValueError("centroid_ratio must be between 0.000001 and 1.0, inclusive.")


def _render_native_json_fields_json(json_fields: dict[str, dict[str, Any]]) -> str:
    return json.dumps(json_fields, separators=(",", ":"), sort_keys=True)


def _validate_native_json_field_config(
    *,
    field: ModelField,
    field_name: str,
    json_fields: Any,
) -> dict[str, Any]:
    if not isinstance(field, models.JSONField):
        raise ValueError(
            f"Field {field_name!r} uses 'json_fields' but is not a JSONField."
        )
    if not isinstance(json_fields, dict):
        raise ValueError(
            f"Field {field_name!r} 'json_fields' must be a dictionary of options."
        )
    return cast(dict[str, Any], json_fields)


@deconstructible(path="paradedb.indexes.IndexExpression")
@dataclass
class IndexExpression:
    """Computed expression for ParadeDB indexing.

    Use this class to index Django expressions (like ``Lower('title')``,
    ``F('rating') + 1``, or ``Concat('first', 'last')``) in a ParadeDB index.

    For text expressions, specify a tokenizer. For non-text expressions
    (integers, timestamps, etc.), omit the tokenizer to use ``pdb.alias``.

    Tokenizer configuration (positional args, named args, token filters,
    stemmer, etc.) is supplied directly on the ``tokenizer`` object via
    :class:`~paradedb.search.Tokenizer` factory methods (e.g.
    ``Tokenizer.simple(options={'stemmer': 'english'})``).

    Args:
        expression: A Django expression to index (e.g., ``Lower('title')``).
        alias: Required. The name used to reference this expression in queries.
        tokenizer: Tokenizer for text expressions (e.g., 'simple', 'unicode_words').
            Omit for non-text expressions to use ``pdb.alias``.

    Example::

        from django.db.models import F
        from django.db.models.functions import Lower
        from paradedb.indexes import ParadeDBIndex, IndexExpression

        ParadeDBIndex(
            fields={"id": {}, "description": {}},
            expressions=[
                # Text expression with tokenizer
                IndexExpression(
                    Lower("title"),
                    alias="title_lower",
                    tokenizer="simple",
                ),
                # Non-text expression with pdb.alias
                IndexExpression(
                    F("rating") + 1,
                    alias="rating_plus_one",
                ),
            ],
            key_field="id",
            name="search_idx",
        )
    """

    expression: Expression | str
    alias: str
    tokenizer: Tokenizer | None = None


class ParadeDBIndex(models.Index):
    """ParadeDB index.

    The index is created with ``USING paradedb``, the index access method
    name in pg_search 0.25.0+.

    ``centroid_ratio``, ``training_samples_per_centroid``, and
    ``cluster_replication`` are index-wide vector build options emitted in
    the ``WITH (...)`` clause. They apply to every vector field in the index
    and are meaningful only when the index contains a vector field.
    """

    suffix = "search"

    def __init__(
        self,
        *,
        fields: dict[str, dict[str, Any]],
        key_field: str,
        name: str,
        expressions: list[IndexExpression] | None = None,
        condition: models.Q | None = None,
        centroid_ratio: float | None = None,
        training_samples_per_centroid: int | None = None,
        cluster_replication: int | None = None,
    ) -> None:
        _validate_centroid_ratio(centroid_ratio)
        _validate_int_index_option(
            "training_samples_per_centroid",
            training_samples_per_centroid,
            maximum=100000,
        )
        _validate_int_index_option(
            "cluster_replication", cluster_replication, maximum=2147483647
        )
        self.fields_config = fields
        self.key_field = key_field
        self.index_expressions = list(expressions or [])
        self.centroid_ratio = centroid_ratio
        self.training_samples_per_centroid = training_samples_per_centroid
        self.cluster_replication = cluster_replication
        super().__init__(name=name, fields=list(fields.keys()), condition=condition)

    def deconstruct(self) -> tuple[str, Any, dict[str, Any]]:
        path, args, kwargs = super().deconstruct()
        kwargs["fields"] = self.fields_config
        kwargs["key_field"] = self.key_field
        kwargs["name"] = self.name
        if self.index_expressions:
            kwargs["expressions"] = self.index_expressions
        for option in (
            "centroid_ratio",
            "training_samples_per_centroid",
            "cluster_replication",
        ):
            value = getattr(self, option)
            if value is not None:
                kwargs[option] = value
        return path, args, kwargs

    def create_sql(
        self,
        model: type[models.Model],
        schema_editor: BaseDatabaseSchemaEditor,
        using: str = "",  # noqa: ARG002
        **kwargs: Any,
    ) -> Statement:
        concurrently = kwargs.get("concurrently", False)
        table = schema_editor.quote_name(model._meta.db_table)
        index_name = schema_editor.quote_name(self.name)

        expressions, json_fields = self._build_index_expressions(model, schema_editor)
        expr_sql = ",\n    ".join(expressions)
        storage_params = [f"key_field={_quote_term(self.key_field)}"]
        if json_fields:
            storage_params.append(
                "json_fields="
                + _quote_term(_render_native_json_fields_json(json_fields))
            )
        for option in (
            "centroid_ratio",
            "training_samples_per_centroid",
            "cluster_replication",
        ):
            value = getattr(self, option)
            if value is not None:
                storage_params.append(f"{option}={value}")

        create_stmt = "CREATE INDEX"
        if concurrently:
            create_stmt += " CONCURRENTLY"
        template = (
            f"{create_stmt} %(name)s ON %(table)s\n"
            "USING paradedb (\n"
            "    %(expressions)s\n"
            ")\n"
            f"WITH ({', '.join(storage_params)})"
        )

        condition_sql = self._get_condition_sql(model, schema_editor)  # type: ignore[attr-defined]
        if condition_sql:
            template += f"\nWHERE {condition_sql}"

        return Statement(
            template,
            name=index_name,
            table=table,
            expressions=expr_sql,
        )

    def _build_index_expressions(
        self, model: type[models.Model], schema_editor: BaseDatabaseSchemaEditor
    ) -> tuple[list[str], dict[str, dict[str, Any]]]:
        expressions: list[str] = []
        json_fields: dict[str, dict[str, Any]] = {}
        for field_name, config in self.fields_config.items():
            field = model._meta.get_field(field_name)
            column_name: str = getattr(field, "column")  # noqa: B009
            column: str = schema_editor.quote_name(column_name)

            json_keys = config.get("json_keys")
            tokenizers = config.get("tokenizers")
            tokenizer = config.get("tokenizer")
            alias = config.get("alias")
            native_json_fields = config.get("json_fields")
            metric = config.get("metric")

            if metric is not None:
                if any(
                    option is not None
                    for option in (
                        json_keys,
                        tokenizers,
                        tokenizer,
                        alias,
                        native_json_fields,
                    )
                ):
                    raise ValueError(
                        f"Field {field_name!r} cannot mix 'metric' with tokenizer "
                        f"or JSON options."
                    )
                if not isinstance(field, VectorField):
                    raise ValueError(
                        f"Field {field_name!r} uses 'metric' but is not a VectorField."
                    )
                expressions.append(f"{column} {vector_opclass(metric)}")
                continue

            if native_json_fields is not None:
                expressions.append(column)
                json_fields[field_name] = _validate_native_json_field_config(
                    field=cast(ModelField, field),
                    field_name=field_name,
                    json_fields=native_json_fields,
                )
                continue

            if json_keys:
                expressions.extend(
                    self._build_json_key_expressions(
                        column, field_name, json_keys, schema_editor
                    )
                )
                continue

            if tokenizers is not None:
                if tokenizer is not None or alias is not None:
                    raise ValueError(
                        f"Field {field_name!r} cannot mix 'tokenizers' with "
                        f"'tokenizer' or 'alias'."
                    )
                expressions.extend(
                    self._build_multi_tokenizer_expressions(
                        column=column,
                        field_name=field_name,
                        tokenizers=tokenizers,
                    )
                )
                continue

            if tokenizer is not None:
                if not isinstance(tokenizer, Tokenizer):
                    raise TypeError("tokenizer must be a Tokenizer")
                expressions.append(f"({column}::{tokenizer.render()})")
            else:
                expressions.append(column)

        # Process IndexExpression entries
        for idx_expr in self.index_expressions:
            expressions.append(
                self._build_computed_expression(idx_expr, model, schema_editor)
            )

        return expressions, json_fields

    def _build_computed_expression(
        self,
        idx_expr: IndexExpression,
        model: type[models.Model],
        schema_editor: BaseDatabaseSchemaEditor,
    ) -> str:
        """Build SQL for a computed expression with pdb.alias or tokenizer cast."""
        raw_expr = idx_expr.expression

        # Handle string field references as F() expressions
        if isinstance(raw_expr, str):
            from django.db.models import F

            expr = cast(Expression, F(raw_expr))
        else:
            expr = raw_expr

        # Compile the expression to SQL
        from django.db.models.sql import Query

        query = Query(model)
        compiler = query.get_compiler(connection=schema_editor.connection)

        # Resolve and compile the expression
        resolved = expr.resolve_expression(query, allow_joins=False, for_save=False)
        if idx_expr.tokenizer is None and _expression_requires_tokenizer(resolved):
            raise ValueError(
                f"IndexExpression with alias {idx_expr.alias!r} resolves to a text or "
                f"JSON value. Specify a tokenizer for text/JSON expressions."
            )
        expr_sql, params = compiler.compile(resolved)
        expr_sql = _inline_compiled_params(expr_sql, params, schema_editor)

        # Build the cast based on whether a tokenizer is specified
        if idx_expr.tokenizer is not None:
            return f"(({expr_sql})::{idx_expr.tokenizer.render()})"
        else:
            # Non-text expression with pdb.alias
            alias_quoted = _quote_term(idx_expr.alias)
            return f"(({expr_sql})::{PDB_TYPE_TOKENIZER_ALIAS}({alias_quoted}))"

    def _build_multi_tokenizer_expressions(
        self, *, column: str, field_name: str, tokenizers: Any
    ) -> list[str]:
        if not isinstance(tokenizers, list) or not tokenizers:
            raise ValueError(
                f"Field {field_name!r} must define 'tokenizers' as a non-empty list."
            )

        expressions: list[str] = []
        for idx, config in enumerate(tokenizers):
            tokenizer = config.get("tokenizer")
            if tokenizer is None:
                raise ValueError(
                    f"Field {field_name!r} tokenizer entry at position {idx} "
                    f"requires 'tokenizer'."
                )
            expressions.append(f"({column}::{tokenizer.render()})")

        return expressions

    def _build_json_key_expressions(
        self,
        column: str,
        field_name: str,
        json_keys: dict[str, dict[str, Any]],
        _schema_editor: BaseDatabaseSchemaEditor,
    ) -> list[str]:
        expressions: list[str] = []
        for key, config in json_keys.items():
            tokenizer = config.get("tokenizer")
            if tokenizer is None:
                raise ValueError(
                    f"JSON key {key!r} in field {field_name!r} requires an explicit "
                    f"tokenizer (e.g. 'unicode_words', 'simple', 'literal')."
                )
            if not isinstance(tokenizer, Tokenizer):
                raise TypeError("tokenizer must be a Tokenizer")
            key_literal = _quote_term(key)
            expressions.append(f"(({column}->>{key_literal})::{tokenizer.render()})")
        return expressions


__all__ = ["IndexExpression", "ParadeDBIndex"]
