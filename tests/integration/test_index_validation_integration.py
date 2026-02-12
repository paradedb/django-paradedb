"""Integration-marked tests for BM25Index config validation errors."""

from __future__ import annotations

from unittest.mock import Mock

import pytest
from django.db.backends.base.schema import BaseDatabaseSchemaEditor
from tests.models import Product

from paradedb.indexes import BM25Index

pytestmark = [pytest.mark.integration]


class DummySchemaEditor(BaseDatabaseSchemaEditor):
    """Minimal schema editor for SQL generation in integration-marked tests."""

    def __init__(self) -> None:
        connection = Mock()
        connection.features.uses_case_insensitive_names = False
        super().__init__(connection, collect_sql=False)

    def quote_name(self, name: str) -> str:
        return f'"{name}"'


@pytest.mark.parametrize(
    ("description_config", "error_match"),
    [
        ({"stemmer": "english"}, "no tokenizer"),
        ({"filters": ["lowercase"]}, "no tokenizer"),
        ({"args": [3, 8]}, "no tokenizer"),
        ({"named_args": {"positions": True}}, "no tokenizer"),
        ({"alias": "description_alias"}, "no tokenizer"),
    ],
)
def test_field_config_without_tokenizer_raises_value_error(
    description_config: dict[str, object], error_match: str
) -> None:
    index = BM25Index(
        fields={
            "id": {},
            "description": description_config,
        },
        key_field="id",
        name="product_search_idx",
    )
    with pytest.raises(ValueError, match=error_match):
        index.create_sql(model=Product, schema_editor=DummySchemaEditor())


def test_legacy_options_key_raises_value_error() -> None:
    index = BM25Index(
        fields={
            "id": {},
            "description": {
                "tokenizer": "simple",
                "options": {"remove_long": 20},
            },
        },
        key_field="id",
        name="product_search_idx",
    )
    with pytest.raises(ValueError, match="deprecated 'options'"):
        index.create_sql(model=Product, schema_editor=DummySchemaEditor())


def test_tokenizers_mixed_with_top_level_tokenizer_config_raises_value_error() -> None:
    index = BM25Index(
        fields={
            "id": {},
            "description": {
                "tokenizers": [{"tokenizer": "literal"}],
                "tokenizer": "simple",
            },
        },
        key_field="id",
        name="product_search_idx",
    )
    with pytest.raises(ValueError, match="cannot mix 'tokenizers'"):
        index.create_sql(model=Product, schema_editor=DummySchemaEditor())


@pytest.mark.parametrize(
    ("tokenizers_value", "error_match"),
    [
        ([], "non-empty list"),
        ("not-a-list", "non-empty list"),
        ([123], "must be a dictionary"),
        ([{}], "requires 'tokenizer'"),
    ],
)
def test_invalid_tokenizers_list_shapes_raise_value_error(
    tokenizers_value: object, error_match: str
) -> None:
    index = BM25Index(
        fields={
            "id": {},
            "description": {
                "tokenizers": tokenizers_value,
            },
        },
        key_field="id",
        name="product_search_idx",
    )
    with pytest.raises(ValueError, match=error_match):
        index.create_sql(model=Product, schema_editor=DummySchemaEditor())


def test_json_key_without_tokenizer_raises_value_error() -> None:
    index = BM25Index(
        fields={
            "id": {},
            "metadata": {
                "json_keys": {
                    "color": {},
                }
            },
        },
        key_field="id",
        name="product_search_idx",
    )
    with pytest.raises(ValueError, match="requires an explicit tokenizer"):
        index.create_sql(model=Product, schema_editor=DummySchemaEditor())


def test_pre_rendered_tokenizer_string_with_named_args_is_rendered_verbatim() -> None:
    index = BM25Index(
        fields={
            "id": {},
            "description": {
                "tokenizer": "ngram(3,8)",
                "named_args": {"prefix_only": True},
            },
        },
        key_field="id",
        name="product_search_idx",
    )
    sql = str(index.create_sql(model=Product, schema_editor=DummySchemaEditor()))
    assert "pdb.ngram(3,8)('prefix_only=true')" in sql
