"""Unit tests for BM25Index configuration validation errors."""

from __future__ import annotations

from unittest.mock import Mock

import pytest
from django.db.backends.base.schema import BaseDatabaseSchemaEditor

from paradedb.indexes import BM25Index
from paradedb.search import Tokenizer
from tests.models import Product


class DummySchemaEditor(BaseDatabaseSchemaEditor):
    """Minimal schema editor for SQL generation in unit tests."""

    def __init__(self) -> None:
        connection = Mock()
        connection.features.uses_case_insensitive_names = False
        super().__init__(connection, collect_sql=False)

    def quote_name(self, name: str) -> str:
        return f'"{name}"'


def test_tokenizers_mixed_with_top_level_tokenizer_config_raises_value_error() -> None:
    index = BM25Index(
        fields={
            "id": {},
            "description": {
                "tokenizers": [{"tokenizer": Tokenizer.literal()}],
                "tokenizer": Tokenizer.simple(),
            },
        },
        key_field="id",
        name="product_search_idx",
    )
    with pytest.raises(ValueError, match="cannot mix 'tokenizers'"):
        index.create_sql(model=Product, schema_editor=DummySchemaEditor())


def test_json_key_without_tokenizer_raises_type_error() -> None:
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
    with pytest.raises(TypeError, match="tokenizer must be a Tokenizer"):
        index.create_sql(model=Product, schema_editor=DummySchemaEditor())


def test_native_json_fields_on_non_json_field_raises_value_error() -> None:
    index = BM25Index(
        fields={
            "id": {},
            "description": {
                "json_fields": {"fast": True},
            },
        },
        key_field="id",
        name="product_search_idx",
    )
    with pytest.raises(ValueError, match="is not a JSONField"):
        index.create_sql(model=Product, schema_editor=DummySchemaEditor())
