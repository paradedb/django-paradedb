"""Unit tests for BM25Index configuration validation errors."""

from __future__ import annotations

from unittest.mock import Mock

import pytest
from django.db import models
from django.db.backends.base.schema import BaseDatabaseSchemaEditor
from django.db.migrations.autodetector import MigrationAutodetector
from django.db.migrations.graph import MigrationGraph
from django.db.migrations.questioner import NonInteractiveMigrationQuestioner
from django.db.migrations.state import ModelState, ProjectState

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


def test_bm25_index_with_equivalent_tokenizers_compares_equal() -> None:
    left = BM25Index(
        fields={
            "id": {},
            "description": {"tokenizer": Tokenizer.unicode_words()},
        },
        key_field="id",
        name="product_search_idx",
    )
    right = BM25Index(
        fields={
            "id": {},
            "description": {"tokenizer": Tokenizer.unicode_words()},
        },
        key_field="id",
        name="product_search_idx",
    )

    assert left == right


def test_repeated_makemigrations_does_not_recreate_tokenizer_indexes() -> None:
    # Use this function to create a fresh instance of the project state, importantly with
    # a fresh instance of Tokenizer.simple()
    def product_state() -> ProjectState:
        state = ProjectState()
        state.add_model(
            ModelState(
                "tests",
                "MigrationProduct",
                fields=[
                    ("id", models.AutoField(primary_key=True)),
                    ("description", models.TextField()),
                ],
                options={
                    "indexes": [
                        BM25Index(
                            fields={
                                "id": {},
                                "description": {"tokenizer": Tokenizer.simple()},
                            },
                            key_field="id",
                            name="migration_product_search_idx",
                        )
                    ],
                },
            )
        )
        return state

    questioner = NonInteractiveMigrationQuestioner(
        specified_apps={"tests"},
        dry_run=True,
    )
    graph = MigrationGraph()
    initial_changes = MigrationAutodetector(
        ProjectState(),
        product_state(),
        questioner,
    ).changes(graph=graph, trim_to_apps={"tests"})
    initial_migration = initial_changes["tests"][0]
    graph.add_node(("tests", initial_migration.name), initial_migration)

    migrated_state = ProjectState()
    for operation in initial_migration.operations:
        operation.state_forwards("tests", migrated_state)

    for _ in range(3):
        changes = MigrationAutodetector(
            migrated_state,
            product_state(),
            questioner,
        ).changes(graph=graph, trim_to_apps={"tests"})

        # Applying the same configuration repeatedly should yield no changes
        assert changes == {}
