"""BM25 index support for Django models."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

from django.db import models
from django.db.backends.base.schema import BaseDatabaseSchemaEditor
from django.db.backends.ddl_references import Statement


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


def _render_config_value(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int | float):
        return str(value)
    if value is None:
        return "null"
    if isinstance(value, str):
        return value
    raise TypeError(f"Unsupported tokenizer named arg type: {type(value).__name__}")


def _is_bare_tokenizer_name(tokenizer: str) -> bool:
    return re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", tokenizer) is not None


def _build_tokenizer_config(
    *,
    tokenizer: str,
    args: list[Any] | None,
    named_args: dict[str, Any] | None,
    filters: list[str] | None,
    stemmer: str | None,
    alias: str | None = None,
) -> str:
    config_parts: dict[str, str] = {}
    if alias is not None:
        config_parts["alias"] = alias

    if named_args:
        for key, value in named_args.items():
            config_parts[str(key)] = _render_config_value(value)

    if filters:
        for name in filters:
            if name == "stemmer" and stemmer:
                config_parts.setdefault("stemmer", stemmer)
            else:
                config_parts.setdefault(name, "true")
    elif stemmer:
        config_parts.setdefault("stemmer", stemmer)

    args_sql: list[str] = []
    if args:
        args_sql = [_render_sql_arg(value) for value in args]

    if config_parts:
        rendered_config = ",".join(
            f"{key}={value}" for key, value in config_parts.items()
        )
        args_sql.append(_quote_term(rendered_config))

    if not args_sql:
        return tokenizer
    if not _is_bare_tokenizer_name(tokenizer):
        raise ValueError(
            "Tokenizer with args/named_args/alias must use a bare tokenizer name "
            "(e.g. 'ngram'), not a pre-rendered function string."
        )
    return f"{tokenizer}({','.join(args_sql)})"


@dataclass(frozen=True)
class FieldConfig:
    """Configuration for a BM25 index field."""

    tokenizer: str | None
    filters: list[str] | None
    stemmer: str | None


class BM25Index(models.Index):
    """BM25 index for ParadeDB."""

    suffix = "bm25"

    def __init__(
        self,
        *,
        fields: dict[str, dict[str, Any]],
        key_field: str,
        name: str,
    ) -> None:
        self.fields_config = fields
        self.key_field = key_field
        super().__init__(name=name, fields=list(fields.keys()))

    def deconstruct(self) -> tuple[str, Any, dict[str, Any]]:
        path, args, kwargs = super().deconstruct()
        kwargs["fields"] = self.fields_config
        kwargs["key_field"] = self.key_field
        kwargs["name"] = self.name
        return path, args, kwargs

    def create_sql(
        self,
        model: type[models.Model],
        schema_editor: BaseDatabaseSchemaEditor,
        using: str = "",  # noqa: ARG002
        **kwargs: Any,  # noqa: ARG002
    ) -> Statement:
        table = schema_editor.quote_name(model._meta.db_table)
        index_name = schema_editor.quote_name(self.name)

        expressions = self._build_index_expressions(model, schema_editor)
        expr_sql = ",\n    ".join(expressions)

        template = (
            "CREATE INDEX %(name)s ON %(table)s\n"
            "USING bm25 (\n"
            "    %(expressions)s\n"
            ")\n"
            "WITH (key_field=%(key_field)s)"
        )
        return Statement(
            template,
            name=index_name,
            table=table,
            expressions=expr_sql,
            key_field=_quote_term(self.key_field),
        )

    def _build_index_expressions(
        self, model: type[models.Model], schema_editor: BaseDatabaseSchemaEditor
    ) -> list[str]:
        expressions: list[str] = []
        for field_name, config in self.fields_config.items():
            field = model._meta.get_field(field_name)
            column_name: str = getattr(field, "column")  # noqa: B009
            column: str = schema_editor.quote_name(column_name)

            json_keys = config.get("json_keys")
            if json_keys:
                expressions.extend(
                    self._build_json_key_expressions(
                        column, field_name, json_keys, schema_editor
                    )
                )
                continue

            tokenizers = config.get("tokenizers")
            tokenizer = config.get("tokenizer")
            args = config.get("args")
            named_args, legacy_options = self._extract_named_args(config, field_name)
            filters = config.get("filters")
            stemmer = config.get("stemmer")
            alias = config.get("alias")
            if tokenizers is not None:
                if (
                    tokenizer is not None
                    or filters is not None
                    or stemmer is not None
                    or args is not None
                    or named_args is not None
                    or legacy_options is not None
                    or alias is not None
                ):
                    raise ValueError(
                        f"Field {field_name!r} cannot mix 'tokenizers' with "
                        f"'tokenizer', 'args', 'named_args', 'options', "
                        f"'filters', 'stemmer', or 'alias'."
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
                or legacy_options is not None
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

        return expressions

    @staticmethod
    def _extract_named_args(
        config: dict[str, Any], field_name: str
    ) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
        named_args = config.get("named_args")
        legacy_options = config.get("options")
        if named_args is not None and legacy_options is not None:
            raise ValueError(
                f"Field {field_name!r} cannot specify both 'named_args' and "
                f"'options'. Use only 'named_args'."
            )
        if named_args is not None and not isinstance(named_args, dict):
            raise ValueError(f"Field {field_name!r} 'named_args' must be a dictionary.")
        if legacy_options is not None and not isinstance(legacy_options, dict):
            raise ValueError(f"Field {field_name!r} 'options' must be a dictionary.")
        effective = named_args if named_args is not None else legacy_options
        return effective, legacy_options

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
            named_args, _legacy_options = self._extract_named_args(
                config, f"{field_name}[{idx}]"
            )
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
            named_args, _legacy_options = self._extract_named_args(
                config, f"{field_name}.{key}"
            )
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


__all__ = ["BM25Index"]
