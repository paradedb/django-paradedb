"""BM25 index support for Django models."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from django.db import models
from django.db.backends.base.schema import BaseDatabaseSchemaEditor
from django.db.backends.ddl_references import Statement


def _quote_term(value: str) -> str:
    escaped = value.replace("'", "''")
    return f"'{escaped}'"


def _build_tokenizer_config(
    *,
    tokenizer: str,
    filters: list[str] | None,
    stemmer: str | None,
    alias: str | None = None,
) -> str:
    parts: list[str] = []
    if alias is not None:
        parts.append(f"alias={alias}")
    if filters:
        for name in filters:
            if name == "stemmer" and stemmer:
                parts.append(f"stemmer={stemmer}")
            else:
                parts.append(f"{name}=true")
    elif stemmer:
        parts.append(f"stemmer={stemmer}")
    if not parts:
        return tokenizer
    return f"{tokenizer}({_quote_term(','.join(parts))})"


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

            tokenizer = config.get("tokenizer")
            filters = config.get("filters")
            stemmer = config.get("stemmer")
            if tokenizer or filters or stemmer:
                if tokenizer is None:
                    raise ValueError(
                        f"Field {field_name!r} specifies filters or stemmer but no "
                        f"tokenizer. Please set an explicit tokenizer (e.g. "
                        f"'unicode_words', 'simple', 'literal')."
                    )
                tokenizer_sql = _build_tokenizer_config(
                    tokenizer=tokenizer,
                    filters=filters,
                    stemmer=stemmer,
                )
                expressions.append(f"({column}::pdb.{tokenizer_sql})")
            else:
                expressions.append(column)

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
            filters = config.get("filters")
            stemmer = config.get("stemmer")
            alias = f"{field_name}_{key}"
            tokenizer_sql = _build_tokenizer_config(
                tokenizer=tokenizer,
                filters=filters,
                stemmer=stemmer,
                alias=alias,
            )
            key_literal = _quote_term(key)
            expressions.append(f"(({column}->>{key_literal})::pdb.{tokenizer_sql})")
        return expressions


__all__ = ["BM25Index"]
