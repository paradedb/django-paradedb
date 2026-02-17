"""Tests for fast fields in BM25 index SQL generation.

This module tests the fast fields feature that enables columnar storage.
"""

from unittest.mock import Mock

from django.db.backends.base.schema import BaseDatabaseSchemaEditor

from paradedb.indexes import BM25Index
from tests.models import Product


class DummySchemaEditor(BaseDatabaseSchemaEditor):
    """Minimal schema editor for SQL string generation."""

    def __init__(self):
        connection = Mock()
        connection.features.uses_case_insensitive_names = False
        super().__init__(connection, collect_sql=False)

    def quote_name(self, name):
        return f'"{name}"'


class TestFastFields:
    """Test fast fields SQL generation."""

    def test_fast_field_text(self):
        """Index with fast=true on text field."""
        index = BM25Index(
            fields={
                "id": {},
                "description": {"fast": True},
            },
            key_field="id",
            name="product_search_idx",
        )
        schema_editor = DummySchemaEditor()
        sql = str(index.create_sql(model=Product, schema_editor=schema_editor))
        assert (
            sql
            == 'CREATE INDEX "product_search_idx" ON "tests_product"\nUSING bm25 (\n    "id",\n    "description"\n)\nWITH (key_field=\'id\', text_fields=\'{"description":{"fast":true}}\')'
        )

    def test_fast_field_numeric(self):
        """Index with fast=true on numeric field."""
        index = BM25Index(
            fields={
                "id": {},
                "rating": {"fast": True},
            },
            key_field="id",
            name="product_search_idx",
        )
        schema_editor = DummySchemaEditor()
        sql = str(index.create_sql(model=Product, schema_editor=schema_editor))
        assert (
            sql
            == 'CREATE INDEX "product_search_idx" ON "tests_product"\nUSING bm25 (\n    "id",\n    "rating"\n)\nWITH (key_field=\'id\', numeric_fields=\'{"rating":{"fast":true}}\')'
        )

    def test_fast_field_boolean(self):
        """Index with fast=true on boolean field."""
        index = BM25Index(
            fields={
                "id": {},
                "in_stock": {"fast": True},
            },
            key_field="id",
            name="product_search_idx",
        )
        schema_editor = DummySchemaEditor()
        sql = str(index.create_sql(model=Product, schema_editor=schema_editor))
        assert (
            sql
            == 'CREATE INDEX "product_search_idx" ON "tests_product"\nUSING bm25 (\n    "id",\n    "in_stock"\n)\nWITH (key_field=\'id\', boolean_fields=\'{"in_stock":{"fast":true}}\')'
        )

    def test_fast_field_datetime(self):
        """Index with fast=true on datetime field."""
        index = BM25Index(
            fields={
                "id": {},
                "created_at": {"fast": True},
            },
            key_field="id",
            name="product_search_idx",
        )
        schema_editor = DummySchemaEditor()
        sql = str(index.create_sql(model=Product, schema_editor=schema_editor))
        assert (
            sql
            == 'CREATE INDEX "product_search_idx" ON "tests_product"\nUSING bm25 (\n    "id",\n    "created_at"\n)\nWITH (key_field=\'id\', datetime_fields=\'{"created_at":{"fast":true}}\')'
        )

    def test_fast_field_json(self):
        """Index with fast=true on JSON field."""
        index = BM25Index(
            fields={
                "id": {},
                "metadata": {"fast": True},
            },
            key_field="id",
            name="product_search_idx",
        )
        schema_editor = DummySchemaEditor()
        sql = str(index.create_sql(model=Product, schema_editor=schema_editor))
        assert (
            sql
            == 'CREATE INDEX "product_search_idx" ON "tests_product"\nUSING bm25 (\n    "id",\n    "metadata"\n)\nWITH (key_field=\'id\', json_fields=\'{"metadata":{"fast":true}}\')'
        )

    def test_fast_field_false(self):
        """Index with fast=false on field."""
        index = BM25Index(
            fields={
                "id": {},
                "description": {"fast": False},
            },
            key_field="id",
            name="product_search_idx",
        )
        schema_editor = DummySchemaEditor()
        sql = str(index.create_sql(model=Product, schema_editor=schema_editor))
        assert (
            sql
            == 'CREATE INDEX "product_search_idx" ON "tests_product"\nUSING bm25 (\n    "id",\n    "description"\n)\nWITH (key_field=\'id\', text_fields=\'{"description":{"fast":false}}\')'
        )

    def test_fast_field_multiple_types(self):
        """Index with fast=true on multiple field types."""
        index = BM25Index(
            fields={
                "id": {},
                "description": {"fast": True},
                "rating": {"fast": True},
                "in_stock": {"fast": True},
            },
            key_field="id",
            name="product_search_idx",
        )
        schema_editor = DummySchemaEditor()
        sql = str(index.create_sql(model=Product, schema_editor=schema_editor))
        assert 'text_fields=\'{"description":{"fast":true}}\'' in sql
        assert 'numeric_fields=\'{"rating":{"fast":true}}\'' in sql
        assert 'boolean_fields=\'{"in_stock":{"fast":true}}\'' in sql

    def test_fast_field_skipped_for_json_keys(self):
        """Fast field option is ignored for json_keys."""
        index = BM25Index(
            fields={
                "id": {},
                "metadata": {
                    "fast": True,
                    "json_keys": {
                        "title": {"tokenizer": "simple"},
                    },
                },
            },
            key_field="id",
            name="product_search_idx",
        )
        schema_editor = DummySchemaEditor()
        sql = str(index.create_sql(model=Product, schema_editor=schema_editor))
        # Should not include json_fields with fast since json_keys is present
        assert "json_fields" not in sql

    def test_fast_field_non_boolean_ignored(self):
        """Non-boolean fast values are ignored."""
        index = BM25Index(
            fields={
                "id": {},
                "description": {"fast": "yes"},  # Invalid type
            },
            key_field="id",
            name="product_search_idx",
        )
        schema_editor = DummySchemaEditor()
        sql = str(index.create_sql(model=Product, schema_editor=schema_editor))
        assert (
            sql
            == 'CREATE INDEX "product_search_idx" ON "tests_product"\nUSING bm25 (\n    "id",\n    "description"\n)\nWITH (key_field=\'id\')'
        )

    def test_fast_field_with_tokenizer(self):
        """Fast field can be combined with tokenizer config."""
        index = BM25Index(
            fields={
                "id": {},
                "description": {
                    "tokenizer": "simple",
                    "fast": True,
                },
            },
            key_field="id",
            name="product_search_idx",
        )
        schema_editor = DummySchemaEditor()
        sql = str(index.create_sql(model=Product, schema_editor=schema_editor))
        assert '("description"::pdb.simple)' in sql
        assert 'text_fields=\'{"description":{"fast":true}}\'' in sql

    def test_fast_field_decimal(self):
        """Index with fast=true on decimal field."""
        index = BM25Index(
            fields={
                "id": {},
                "price": {"fast": True},
            },
            key_field="id",
            name="product_search_idx",
        )
        schema_editor = DummySchemaEditor()
        sql = str(index.create_sql(model=Product, schema_editor=schema_editor))
        assert (
            sql
            == 'CREATE INDEX "product_search_idx" ON "tests_product"\nUSING bm25 (\n    "id",\n    "price"\n)\nWITH (key_field=\'id\', numeric_fields=\'{"price":{"fast":true}}\')'
        )

    def test_fast_field_mixed_true_false(self):
        """Index with mixed fast=true and fast=false on different fields."""
        index = BM25Index(
            fields={
                "id": {},
                "description": {"fast": True},
                "category": {"fast": False},
            },
            key_field="id",
            name="product_search_idx",
        )
        schema_editor = DummySchemaEditor()
        sql = str(index.create_sql(model=Product, schema_editor=schema_editor))
        # Both should appear in text_fields
        assert (
            'text_fields=\'{"description":{"fast":true},"category":{"fast":false}}\''
            in sql
        )
