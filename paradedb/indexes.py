"""BM25 index support for Django models."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, cast

from django.db import models
from django.db.backends.base.schema import BaseDatabaseSchemaEditor
from django.db.backends.ddl_references import Statement
from django.db.models.expressions import Expression
from django.utils.deconstruct import deconstructible

if TYPE_CHECKING:
    ModelField = models.Field[Any, Any]
else:
    ModelField = models.Field


def _quote_term(value: str) -> str:
    escaped = value.replace("'", "''")
    return f"'{escaped}'"


_CONFIG_KEY_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _validate_config_key(key: str, *, context: str) -> str:
    if not isinstance(key, str):
        raise ValueError(f"{context} must be strings.")
    if not _CONFIG_KEY_RE.match(key):
        raise ValueError(
            f"{context} must match {_CONFIG_KEY_RE.pattern!r}; got {key!r}."
        )
    return key


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


def _render_config_value(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int | float):
        return str(value)
    if value is None:
        return "null"
    if isinstance(value, str):
        if "=" in value:
            raise ValueError("Tokenizer named arg string values cannot contain '='.")
        if any(ch in value for ch in ("\x00", "\n", "\r")):
            raise ValueError(
                "Tokenizer named arg string values cannot contain control characters."
            )
        return value
    raise TypeError(f"Unsupported tokenizer named arg type: {type(value).__name__}")


def _build_tokenizer_config(
    *,
    tokenizer: str,
    args: list[Any] | None,
    named_args: dict[str, Any] | None,
    filters: list[str] | None,
    stemmer: str | None,
    alias: str | None = None,
) -> str:
    config_parts: dict[str, Any] = {}
    if alias is not None:
        config_parts["alias"] = alias

    if named_args:
        for key, value in named_args.items():
            safe_key = _validate_config_key(key, context="Tokenizer named arg keys")
            config_parts[safe_key] = value

    if filters:
        for name in filters:
            safe_name = _validate_config_key(name, context="Tokenizer filter names")
            if name == "stemmer":
                if stemmer is None:
                    raise ValueError(
                        "Tokenizer filter 'stemmer' requires a stemmer language."
                    )
                config_parts.setdefault("stemmer", stemmer)
            else:
                config_parts.setdefault(safe_name, True)
    elif stemmer:
        config_parts.setdefault("stemmer", stemmer)

    args_sql: list[str] = []
    if args:
        args_sql = [_render_sql_arg(value) for value in args]

    if config_parts:
        rendered_config = ",".join(
            f"{_validate_config_key(key, context='Tokenizer config keys')}={_render_config_value(value)}"
            for key, value in config_parts.items()
        )
        args_sql.append(_quote_term(rendered_config))

    if not args_sql:
        return tokenizer
    return f"{tokenizer}({','.join(args_sql)})"


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


def _validate_native_json_field_conflicts(
    *,
    field_name: str,
    json_fields: Any,
    json_keys: Any,
    tokenizers: Any,
    tokenizer: Any,
    args: Any,
    named_args: Any,
    filters: Any,
    stemmer: Any,
    alias: Any,
) -> None:
    if json_fields is None:
        return

    if (
        json_keys is not None
        or tokenizers is not None
        or tokenizer is not None
        or args is not None
        or named_args is not None
        or filters is not None
        or stemmer is not None
        or alias is not None
    ):
        raise ValueError(
            f"Field {field_name!r} cannot mix 'json_fields' with "
            f"'json_keys', 'tokenizers', 'tokenizer', 'args', 'named_args', "
            f"'filters', 'stemmer', or 'alias'."
        )


@deconstructible(path="paradedb.indexes.IndexExpression")
@dataclass
class IndexExpression:
    """Computed expression for BM25 indexing.

    Use this class to index Django expressions (like ``Lower('title')``,
    ``F('rating') + 1``, or ``Concat('first', 'last')``) in a BM25 index.

    For text expressions, specify a tokenizer. For non-text expressions
    (integers, timestamps, etc.), omit the tokenizer to use ``pdb.alias``.

    Args:
        expression: A Django expression to index (e.g., ``Lower('title')``).
        alias: Required. The name used to reference this expression in queries.
        tokenizer: Tokenizer for text expressions (e.g., 'simple', 'unicode_words').
            Omit for non-text expressions to use ``pdb.alias``.
        args: Positional arguments for the tokenizer.
        named_args: Named arguments for the tokenizer configuration.
        filters: Token filters (e.g., ['lowercase', 'stemmer']).
        stemmer: Stemmer language (e.g., 'english').

    Example::

        from django.db.models import F
        from django.db.models.functions import Lower
        from paradedb.indexes import BM25Index, IndexExpression

        BM25Index(
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
    tokenizer: str | None = None
    args: list[Any] | None = None
    named_args: dict[str, Any] | None = None
    filters: list[str] | None = None
    stemmer: str | None = None


class BM25Index(models.Index):
    """BM25 index for ParadeDB."""

    suffix = "bm25"

    def __init__(
        self,
        *,
        fields: dict[str, dict[str, Any]],
        key_field: str,
        name: str,
        expressions: list[IndexExpression] | None = None,
        condition: models.Q | None = None,
    ) -> None:
        self.fields_config = fields
        self.key_field = key_field
        self.index_expressions = list(expressions or [])
        super().__init__(name=name, fields=list(fields.keys()), condition=condition)

    def deconstruct(self) -> tuple[str, Any, dict[str, Any]]:
        path, args, kwargs = super().deconstruct()
        kwargs["fields"] = self.fields_config
        kwargs["key_field"] = self.key_field
        kwargs["name"] = self.name
        if self.index_expressions:
            kwargs["expressions"] = self.index_expressions
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

        create_stmt = "CREATE INDEX"
        if concurrently:
            create_stmt += " CONCURRENTLY"
        template = (
            f"{create_stmt} %(name)s ON %(table)s\n"
            "USING bm25 (\n"
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
            args = config.get("args")
            named_args = self._extract_named_args(config, field_name)
            filters = config.get("filters")
            stemmer = config.get("stemmer")
            alias = config.get("alias")
            native_json_fields = config.get("json_fields")

            _validate_native_json_field_conflicts(
                field_name=field_name,
                json_fields=native_json_fields,
                json_keys=json_keys,
                tokenizers=tokenizers,
                tokenizer=tokenizer,
                args=args,
                named_args=named_args,
                filters=filters,
                stemmer=stemmer,
                alias=alias,
            )
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
                if (
                    tokenizer is not None
                    or filters is not None
                    or stemmer is not None
                    or args is not None
                    or named_args is not None
                    or alias is not None
                ):
                    raise ValueError(
                        f"Field {field_name!r} cannot mix 'tokenizers' with "
                        f"'tokenizer', 'args', 'named_args', 'filters', "
                        f"'stemmer', or 'alias'."
                    )
                expressions.extend(
                    self._build_multi_tokenizer_expressions(
                        column=column,
                        field_name=field_name,
                        tokenizers=tokenizers,
                    )
                )
                continue

            if (
                tokenizer is not None
                or filters is not None
                or stemmer is not None
                or args is not None
                or named_args is not None
                or alias is not None
            ):
                if tokenizer is None:
                    raise ValueError(
                        f"Field {field_name!r} specifies tokenizer configuration but "
                        f"no tokenizer. Please set an explicit tokenizer (e.g. "
                        f"'unicode_words', 'simple', 'literal')."
                    )
                tokenizer_sql = _build_tokenizer_config(
                    tokenizer=tokenizer,
                    args=args,
                    named_args=named_args,
                    filters=filters,
                    stemmer=stemmer,
                    alias=alias,
                )
                expressions.append(f"({column}::pdb.{tokenizer_sql})")
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
            # Text expression with tokenizer
            tokenizer_sql = _build_tokenizer_config(
                tokenizer=idx_expr.tokenizer,
                args=idx_expr.args,
                named_args=idx_expr.named_args,
                filters=idx_expr.filters,
                stemmer=idx_expr.stemmer,
                alias=idx_expr.alias,
            )
            return f"(({expr_sql})::pdb.{tokenizer_sql})"
        else:
            # Non-text expression with pdb.alias
            alias_quoted = _quote_term(idx_expr.alias)
            return f"(({expr_sql})::pdb.alias({alias_quoted}))"

    @staticmethod
    def _extract_named_args(
        config: dict[str, Any], field_name: str
    ) -> dict[str, Any] | None:
        named_args = config.get("named_args")
        if "options" in config:
            raise ValueError(
                f"Field {field_name!r} uses deprecated 'options'. "
                f"Use 'named_args' instead."
            )
        if named_args is not None and not isinstance(named_args, dict):
            raise ValueError(f"Field {field_name!r} 'named_args' must be a dictionary.")
        return named_args

    def _build_multi_tokenizer_expressions(
        self, *, column: str, field_name: str, tokenizers: Any
    ) -> list[str]:
        if not isinstance(tokenizers, list) or not tokenizers:
            raise ValueError(
                f"Field {field_name!r} must define 'tokenizers' as a non-empty list."
            )

        expressions: list[str] = []
        for idx, config in enumerate(tokenizers):
            if not isinstance(config, dict):
                raise ValueError(
                    f"Field {field_name!r} tokenizer entry at position {idx} must be "
                    f"a dictionary."
                )

            tokenizer = config.get("tokenizer")
            if tokenizer is None:
                raise ValueError(
                    f"Field {field_name!r} tokenizer entry at position {idx} "
                    f"requires 'tokenizer'."
                )
            args = config.get("args")
            named_args = self._extract_named_args(config, f"{field_name}[{idx}]")
            filters = config.get("filters")
            stemmer = config.get("stemmer")
            alias = config.get("alias")

            tokenizer_sql = _build_tokenizer_config(
                tokenizer=tokenizer,
                args=args,
                named_args=named_args,
                filters=filters,
                stemmer=stemmer,
                alias=alias,
            )
            expressions.append(f"({column}::pdb.{tokenizer_sql})")

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
            args = config.get("args")
            named_args = self._extract_named_args(config, f"{field_name}.{key}")
            filters = config.get("filters")
            stemmer = config.get("stemmer")
            alias = config.get("alias") or f"{field_name}_{key}"
            tokenizer_sql = _build_tokenizer_config(
                tokenizer=tokenizer,
                args=args,
                named_args=named_args,
                filters=filters,
                stemmer=stemmer,
                alias=alias,
            )
            key_literal = _quote_term(key)
            expressions.append(f"(({column}->>{key_literal})::pdb.{tokenizer_sql})")
        return expressions


__all__ = ["BM25Index", "IndexExpression"]
