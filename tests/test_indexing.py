"""Unit tests for ParadeDBIndex configuration validation errors."""

from __future__ import annotations

from unittest.mock import Mock

import pytest
from django.db import connection, models
from django.db.backends.base.schema import BaseDatabaseSchemaEditor
from django.db.migrations.autodetector import MigrationAutodetector
from django.db.migrations.graph import MigrationGraph
from django.db.migrations.questioner import NonInteractiveMigrationQuestioner
from django.db.migrations.state import ModelState, ProjectState
from django.db.migrations.writer import MigrationWriter
from django.db.models import F, Func, Q, Value
from django.db.models.functions import Length, Lower

from paradedb.indexes import IndexExpression, ParadeDBIndex
from paradedb.search import Tokenizer
from tests.models import MockItem


def _schema_editor():
    return connection.schema_editor(collect_sql=True)


class DummySchemaEditor(BaseDatabaseSchemaEditor):
    """Minimal schema editor for SQL generation in unit tests."""

    def __init__(self) -> None:
        connection = Mock()
        connection.features.uses_case_insensitive_names = False
        super().__init__(connection, collect_sql=False)

    def quote_name(self, name: str) -> str:
        return f'"{name}"'


def test_tokenizers_mixed_with_top_level_tokenizer_config_raises_value_error() -> None:
    index = ParadeDBIndex(
        fields={
            "id": {},
            "description": {
                "tokenizers": [{"tokenizer": Tokenizer.literal()}],
                "tokenizer": Tokenizer.simple(),
            },
        },
        name="mock_items_search_idx",
    )
    with pytest.raises(ValueError, match="cannot mix 'tokenizers'"):
        index.create_sql(model=MockItem, schema_editor=DummySchemaEditor())


def test_json_key_without_tokenizer_raises_value_error() -> None:
    index = ParadeDBIndex(
        fields={
            "id": {},
            "metadata": {
                "json_keys": {
                    "color": {},
                }
            },
        },
        name="mock_items_search_idx",
    )
    with pytest.raises(ValueError, match="requires an explicit"):
        index.create_sql(model=MockItem, schema_editor=DummySchemaEditor())


def test_json_key_with_invalid_tokenizer_type_raises_type_error() -> None:
    index = ParadeDBIndex(
        fields={
            "id": {},
            "metadata": {
                "json_keys": {
                    "color": {"tokenizer": "simple"},
                }
            },
        },
        name="mock_items_search_idx",
    )
    with pytest.raises(TypeError, match="tokenizer must be a Tokenizer"):
        index.create_sql(model=MockItem, schema_editor=DummySchemaEditor())


def test_native_json_fields_on_non_json_field_raises_value_error() -> None:
    index = ParadeDBIndex(
        fields={
            "id": {},
            "description": {
                "json_fields": {"fast": True},
            },
        },
        name="mock_items_search_idx",
    )
    with pytest.raises(ValueError, match="is not a JSONField"):
        index.create_sql(model=MockItem, schema_editor=DummySchemaEditor())


def test_index_with_equivalent_tokenizers_compares_equal() -> None:
    left = ParadeDBIndex(
        fields={
            "id": {},
            "description": {"tokenizer": Tokenizer.unicode_words()},
        },
        name="mock_items_search_idx",
    )
    right = ParadeDBIndex(
        fields={
            "id": {},
            "description": {"tokenizer": Tokenizer.unicode_words()},
        },
        name="mock_items_search_idx",
    )

    assert left == right


def test_repeated_makemigrations_does_not_recreate_tokenizer_indexes() -> None:
    # Use this function to create a fresh instance of the project state, importantly with
    # a fresh instance of Tokenizer.simple()
    def mock_item_state() -> ProjectState:
        state = ProjectState()
        state.add_model(
            ModelState(
                "tests",
                "MigrationMockItem",
                fields=[
                    ("id", models.AutoField(primary_key=True)),
                    ("description", models.TextField()),
                ],
                options={
                    "indexes": [
                        ParadeDBIndex(
                            fields={
                                "id": {},
                                "description": {"tokenizer": Tokenizer.simple()},
                            },
                            name="migration_mock_items_search_idx",
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
        mock_item_state(),
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
            mock_item_state(),
            questioner,
        ).changes(graph=graph, trim_to_apps={"tests"})

        # Applying the same configuration repeatedly should yield no changes
        assert changes == {}


class TestParadeDBIndex:
    """Test ParadeDB index SQL generation."""

    def test_basic_index_sql(self) -> None:
        """Basic ParadeDB index DDL generation."""
        index = ParadeDBIndex(
            fields={"id": {}, "description": {}},
            name="mock_items_search_idx",
        )
        schema_editor = _schema_editor()
        sql = str(index.create_sql(model=MockItem, schema_editor=schema_editor))
        assert (
            sql
            == 'CREATE INDEX "mock_items_search_idx" ON "mock_items"\nUSING paradedb (\n    "id",\n    "description"\n)'
        )

    def test_index_with_tokenizer(self) -> None:
        """Index with tokenizer configuration."""
        index = ParadeDBIndex(
            fields={
                "id": {},
                "description": {
                    "tokenizer": Tokenizer.simple(
                        options={"lowercase": True, "stemmer": "english"}
                    ),
                },
            },
            name="mock_items_search_idx",
        )
        schema_editor = _schema_editor()
        sql = str(index.create_sql(model=MockItem, schema_editor=schema_editor))
        assert (
            sql
            == 'CREATE INDEX "mock_items_search_idx" ON "mock_items"\nUSING paradedb (\n    "id",\n    ("description"::pdb.simple(\'lowercase=true\',\'stemmer=english\'))\n)'
        )

    def test_index_with_tokenizer_only(self) -> None:
        """Index with tokenizer only."""
        index = ParadeDBIndex(
            fields={
                "id": {},
                "description": {"tokenizer": Tokenizer.simple()},
            },
            name="mock_items_search_idx",
        )
        schema_editor = _schema_editor()
        sql = str(index.create_sql(model=MockItem, schema_editor=schema_editor))
        assert (
            sql
            == 'CREATE INDEX "mock_items_search_idx" ON "mock_items"\nUSING paradedb (\n    "id",\n    ("description"::pdb.simple)\n)'
        )

    def test_json_field_index(self) -> None:
        """JSON field with json_keys configuration."""
        index = ParadeDBIndex(
            fields={
                "id": {},
                "metadata": {
                    "json_keys": {
                        "title": {
                            "tokenizer": Tokenizer.simple(
                                options={
                                    "alias": "metadata_title",
                                    "lowercase": True,
                                }
                            )
                        },
                        "brand": {
                            "tokenizer": Tokenizer.simple(
                                options={"alias": "metadata_brand"}
                            )
                        },
                    }
                },
            },
            name="mock_items_search_idx",
        )
        schema_editor = _schema_editor()
        sql = str(index.create_sql(model=MockItem, schema_editor=schema_editor))
        assert (
            sql
            == "CREATE INDEX \"mock_items_search_idx\" ON \"mock_items\"\nUSING paradedb (\n    \"id\",\n    ((\"metadata\"->>'title')::pdb.simple('alias=metadata_title','lowercase=true')),\n    ((\"metadata\"->>'brand')::pdb.simple('alias=metadata_brand'))\n)"
        )

    def test_json_field_native_json_fields(self) -> None:
        """Native json_fields config is emitted via WITH (...) and indexes the column."""
        index = ParadeDBIndex(
            fields={
                "id": {},
                "metadata": {
                    "json_fields": {
                        "fast": True,
                        "expand_dots": False,
                    }
                },
            },
            name="mock_items_search_idx",
        )
        schema_editor = _schema_editor()
        sql = str(index.create_sql(model=MockItem, schema_editor=schema_editor))
        assert (
            sql
            == 'CREATE INDEX "mock_items_search_idx" ON "mock_items"\nUSING paradedb (\n    "id",\n    "metadata"\n)\nWITH (json_fields=\'{"metadata":{"expand_dots":false,"fast":true}}\')'
        )

    def test_json_key_without_tokenizer_raises(self) -> None:
        """JSON keys without an explicit tokenizer raise a descriptive ValueError."""
        index = ParadeDBIndex(
            fields={
                "id": {},
                "metadata": {
                    "json_keys": {
                        "color": {},
                    }
                },
            },
            name="mock_items_search_idx",
        )
        schema_editor = _schema_editor()
        with pytest.raises(ValueError, match="requires an explicit"):
            index.create_sql(model=MockItem, schema_editor=schema_editor)

    def test_json_field_literal_alias(self) -> None:
        """JSON subfields can be indexed with literal tokenizer aliases."""
        index = ParadeDBIndex(
            fields={
                "id": {},
                "description": {},
                "metadata": {
                    "json_keys": {
                        "color": {
                            "tokenizer": Tokenizer.literal(
                                options={"alias": "metadata_color"}
                            )
                        },
                        "location": {
                            "tokenizer": Tokenizer.literal(
                                options={"alias": "metadata_location"}
                            )
                        },
                    }
                },
            },
            name="mock_items_search_idx",
        )
        schema_editor = _schema_editor()
        sql = str(index.create_sql(model=MockItem, schema_editor=schema_editor))
        assert (
            sql
            == 'CREATE INDEX "mock_items_search_idx" ON "mock_items"\nUSING paradedb (\n    "id",\n    "description",\n    (("metadata"->>\'color\')::pdb.literal(\'alias=metadata_color\')),\n    (("metadata"->>\'location\')::pdb.literal(\'alias=metadata_location\'))\n)'
        )

    def test_field_with_multiple_tokenizers(self) -> None:
        """A field can include multiple tokenizer expressions."""
        index = ParadeDBIndex(
            fields={
                "id": {},
                "description": {
                    "tokenizers": [
                        {"tokenizer": Tokenizer.literal()},
                        {
                            "tokenizer": Tokenizer.simple(
                                options={
                                    "alias": "description_simple",
                                    "lowercase": True,
                                }
                            ),
                        },
                    ]
                },
            },
            name="mock_items_search_idx",
        )
        schema_editor = _schema_editor()
        sql = str(index.create_sql(model=MockItem, schema_editor=schema_editor))
        assert (
            sql
            == 'CREATE INDEX "mock_items_search_idx" ON "mock_items"\nUSING paradedb (\n    "id",\n    ("description"::pdb.literal),\n    ("description"::pdb.simple(\'alias=description_simple\',\'lowercase=true\'))\n)'
        )

    def test_multiple_tokenizers_allows_secondary_entries_without_alias(self) -> None:
        """Thin wrapper mode allows tokenizer entries without alias."""
        index = ParadeDBIndex(
            fields={
                "id": {},
                "description": {
                    "tokenizers": [
                        {"tokenizer": Tokenizer.literal()},
                        {"tokenizer": Tokenizer.simple()},
                    ]
                },
            },
            name="mock_items_search_idx",
        )
        schema_editor = _schema_editor()
        sql = str(index.create_sql(model=MockItem, schema_editor=schema_editor))
        assert (
            sql
            == 'CREATE INDEX "mock_items_search_idx" ON "mock_items"\nUSING paradedb (\n    "id",\n    ("description"::pdb.literal),\n    ("description"::pdb.simple)\n)'
        )

    def test_multiple_tokenizers_cannot_mix_with_single_tokenizer_keys(self) -> None:
        """The list syntax cannot be combined with top-level tokenizer keys."""
        index = ParadeDBIndex(
            fields={
                "id": {},
                "description": {
                    "tokenizers": [{"tokenizer": Tokenizer.literal()}],
                    "tokenizer": Tokenizer.simple(),
                },
            },
            name="mock_items_search_idx",
        )
        schema_editor = _schema_editor()
        with pytest.raises(ValueError, match="cannot mix 'tokenizers'"):
            index.create_sql(model=MockItem, schema_editor=schema_editor)

    def test_structured_ngram_args_and_named_args_in_multi_tokenizer_dsl(self) -> None:
        """Supports positional ngram args plus named args in DSL."""
        index = ParadeDBIndex(
            fields={
                "id": {},
                "description": {
                    "tokenizers": [
                        {"tokenizer": Tokenizer.literal()},
                        {
                            "tokenizer": Tokenizer.ngram(
                                3,
                                3,
                                options={
                                    "alias": "description_ngram",
                                    "prefix_only": True,
                                    "positions": True,
                                },
                            ),
                        },
                    ]
                },
            },
            name="mock_items_search_idx",
        )
        schema_editor = _schema_editor()
        sql = str(index.create_sql(model=MockItem, schema_editor=schema_editor))
        assert (
            sql
            == 'CREATE INDEX "mock_items_search_idx" ON "mock_items"\nUSING paradedb (\n    "id",\n    ("description"::pdb.literal),\n    ("description"::pdb.ngram(3,3,\'alias=description_ngram\',\'prefix_only=true\',\'positions=true\'))\n)'
        )

    def test_structured_regex_pattern_and_alias_in_multi_tokenizer_dsl(self) -> None:
        """Supports regex_pattern positional args with alias in DSL."""
        index = ParadeDBIndex(
            fields={
                "id": {},
                "description": {
                    "tokenizers": [
                        {"tokenizer": Tokenizer.literal()},
                        {
                            "tokenizer": Tokenizer.regex_pattern(
                                r"(?i)\bh\w*",
                                options={"alias": "description_regex"},
                            ),
                        },
                    ]
                },
            },
            name="mock_items_search_idx",
        )
        schema_editor = _schema_editor()
        sql = str(index.create_sql(model=MockItem, schema_editor=schema_editor))
        assert (
            sql
            == 'CREATE INDEX "mock_items_search_idx" ON "mock_items"\nUSING paradedb (\n    "id",\n    ("description"::pdb.literal),\n    ("description"::pdb.regex_pattern(\'(?i)\\bh\\w*\',\'alias=description_regex\'))\n)'
        )

    def test_structured_lindera_dictionary_argument_in_multi_tokenizer_dsl(
        self,
    ) -> None:
        """Supports lindera dictionary positional arg in DSL."""
        index = ParadeDBIndex(
            fields={
                "id": {},
                "description": {
                    "tokenizers": [
                        {"tokenizer": Tokenizer.literal()},
                        {
                            "tokenizer": Tokenizer.lindera(
                                "japanese",
                                options={"alias": "description_jp"},
                            ),
                        },
                    ]
                },
            },
            name="mock_items_search_idx",
        )
        schema_editor = _schema_editor()
        sql = str(index.create_sql(model=MockItem, schema_editor=schema_editor))
        assert (
            sql
            == 'CREATE INDEX "mock_items_search_idx" ON "mock_items"\nUSING paradedb (\n    "id",\n    ("description"::pdb.literal),\n    ("description"::pdb.lindera(\'japanese\',\'alias=description_jp\'))\n)'
        )

    def test_value_based_token_filter_named_args(self) -> None:
        """Supports non-boolean tokenizer options."""
        index = ParadeDBIndex(
            fields={
                "id": {},
                "description": {
                    "tokenizer": Tokenizer.simple(
                        options={
                            "lowercase": False,
                            "stopwords_language": "English,French",
                            "remove_long": 20,
                            "remove_short": 2,
                            "stemmer": "english",
                        }
                    ),
                },
            },
            name="mock_items_search_idx",
        )
        schema_editor = _schema_editor()
        sql = str(index.create_sql(model=MockItem, schema_editor=schema_editor))
        assert (
            sql
            == "CREATE INDEX \"mock_items_search_idx\" ON \"mock_items\"\nUSING paradedb (\n    \"id\",\n    (\"description\"::pdb.simple('lowercase=false','stopwords_language=English,French','remove_long=20','remove_short=2','stemmer=english'))\n)"
        )

    def test_indexed_expression_with_concat(self) -> None:
        """Supports non-boolean token filter named args in DSL."""
        index = ParadeDBIndex(
            fields={"id": {}},
            expressions=[
                IndexExpression(
                    Func(
                        F("description"),
                        Value(" "),
                        F("category"),
                        template="(%(expressions)s)",
                        arg_joiner=" || ",
                        output_field=models.TextField(),
                    ),
                    alias="description_concat",
                    tokenizer=Tokenizer.simple(options={"alias": "description_concat"}),
                )
            ],
            name="mock_items_search_idx",
        )
        schema_editor = _schema_editor()
        sql = str(index.create_sql(model=MockItem, schema_editor=schema_editor))
        assert (
            sql
            == 'CREATE INDEX "mock_items_search_idx" ON "mock_items"\nUSING paradedb (\n    "id",\n    ((("mock_items"."description" || \' \' || "mock_items"."category"))::pdb.simple(\'alias=description_concat\'))\n)'
        )

    def test_create_sql_concurrently(self) -> None:
        """create_sql with concurrently=True emits CREATE INDEX CONCURRENTLY."""
        index = ParadeDBIndex(
            fields={"id": {}, "description": {"tokenizer": Tokenizer.simple()}},
            name="mock_items_search_idx",
        )
        schema_editor = _schema_editor()
        sql = str(
            index.create_sql(
                model=MockItem, schema_editor=schema_editor, concurrently=True
            )
        )
        assert sql.startswith('CREATE INDEX CONCURRENTLY "mock_items_search_idx"')
        assert "USING paradedb" in sql

    def test_create_sql_without_concurrently(self) -> None:
        """create_sql without concurrently does not emit CONCURRENTLY."""
        index = ParadeDBIndex(
            fields={"id": {}, "description": {"tokenizer": Tokenizer.simple()}},
            name="mock_items_search_idx",
        )
        schema_editor = _schema_editor()
        sql = str(index.create_sql(model=MockItem, schema_editor=schema_editor))
        assert sql.startswith('CREATE INDEX "mock_items_search_idx"')
        assert "CONCURRENTLY" not in sql

    def test_create_sql_with_condition(self) -> None:
        """create_sql with condition appends a WHERE clause."""
        index = ParadeDBIndex(
            fields={"id": {}, "description": {"tokenizer": Tokenizer.simple()}},
            name="mock_items_search_idx",
            condition=Q(description__isnull=False),
        )
        schema_editor = _schema_editor()
        sql = str(index.create_sql(model=MockItem, schema_editor=schema_editor))
        assert sql.endswith('WHERE "description" IS NOT NULL')
        assert "USING paradedb" in sql

    def test_create_sql_with_condition_and_concurrently(self) -> None:
        """create_sql with both condition and concurrently emits both."""
        index = ParadeDBIndex(
            fields={"id": {}, "description": {"tokenizer": Tokenizer.simple()}},
            name="mock_items_search_idx",
            condition=Q(description__isnull=False),
        )
        schema_editor = _schema_editor()
        sql = str(
            index.create_sql(
                model=MockItem, schema_editor=schema_editor, concurrently=True
            )
        )
        assert sql.startswith('CREATE INDEX CONCURRENTLY "mock_items_search_idx"')
        assert sql.endswith('WHERE "description" IS NOT NULL')

    def test_create_sql_with_native_json_fields_and_condition(self) -> None:
        """json_fields and condition can both be emitted in the same CREATE INDEX."""
        index = ParadeDBIndex(
            fields={"id": {}, "metadata": {"json_fields": {"fast": True}}},
            name="mock_items_search_idx",
            condition=Q(description__isnull=False),
        )
        schema_editor = _schema_editor()
        sql = str(index.create_sql(model=MockItem, schema_editor=schema_editor))
        assert 'json_fields=\'{"metadata":{"fast":true}}\'' in sql
        assert sql.endswith('WHERE "description" IS NOT NULL')

    def test_create_sql_without_condition_no_where(self) -> None:
        """create_sql without condition does not append WHERE clause."""
        index = ParadeDBIndex(
            fields={"id": {}, "description": {"tokenizer": Tokenizer.simple()}},
            name="mock_items_search_idx",
        )
        schema_editor = _schema_editor()
        sql = str(index.create_sql(model=MockItem, schema_editor=schema_editor))
        assert "WHERE" not in sql

    def test_index_expression_with_lower_and_tokenizer(self) -> None:
        """IndexExpression with Lower() and tokenizer generates correct SQL."""
        index = ParadeDBIndex(
            fields={"id": {}, "description": {}},
            expressions=[
                IndexExpression(
                    Lower("description"),
                    alias="description_lower",
                    tokenizer=Tokenizer.simple(options={"alias": "description_lower"}),
                ),
            ],
            name="mock_items_search_idx",
        )
        schema_editor = _schema_editor()
        sql = str(index.create_sql(model=MockItem, schema_editor=schema_editor))
        assert (
            sql
            == 'CREATE INDEX "mock_items_search_idx" ON "mock_items"\nUSING paradedb (\n    "id",\n    "description",\n    ((LOWER("mock_items"."description"))::pdb.simple(\'alias=description_lower\'))\n)'
        )

    def test_index_expression_non_text_with_pdb_alias(self) -> None:
        """IndexExpression without tokenizer uses pdb.alias for non-text."""
        index = ParadeDBIndex(
            fields={"id": {}, "description": {}},
            expressions=[
                IndexExpression(
                    F("rating"),
                    alias="rating_indexed",
                ),
            ],
            name="mock_items_search_idx",
        )
        schema_editor = _schema_editor()
        sql = str(index.create_sql(model=MockItem, schema_editor=schema_editor))
        assert (
            sql
            == 'CREATE INDEX "mock_items_search_idx" ON "mock_items"\nUSING paradedb (\n    "id",\n    "description",\n    (("mock_items"."rating")::pdb.alias(\'rating_indexed\'))\n)'
        )

    def test_index_expression_with_tokenizer_and_filters(self) -> None:
        """IndexExpression with tokenizer, filters, and stemmer."""
        index = ParadeDBIndex(
            fields={"id": {}},
            expressions=[
                IndexExpression(
                    Lower("description"),
                    alias="desc_processed",
                    tokenizer=Tokenizer.simple(
                        options={
                            "alias": "desc_processed",
                            "lowercase": True,
                            "stemmer": "english",
                        }
                    ),
                ),
            ],
            name="mock_items_search_idx",
        )
        schema_editor = _schema_editor()
        sql = str(index.create_sql(model=MockItem, schema_editor=schema_editor))
        assert (
            sql
            == 'CREATE INDEX "mock_items_search_idx" ON "mock_items"\nUSING paradedb (\n    "id",\n    ((LOWER("mock_items"."description"))::pdb.simple(\'alias=desc_processed\',\'lowercase=true\',\'stemmer=english\'))\n)'
        )

    def test_index_expression_with_arithmetic(self) -> None:
        """IndexExpression with arithmetic constant is inlined into SQL."""
        index = ParadeDBIndex(
            fields={"id": {}},
            expressions=[
                IndexExpression(
                    F("rating") + 1,
                    alias="rating_plus_one",
                ),
            ],
            name="mock_items_search_idx",
        )
        schema_editor = _schema_editor()
        sql = str(index.create_sql(model=MockItem, schema_editor=schema_editor))
        assert (
            sql
            == 'CREATE INDEX "mock_items_search_idx" ON "mock_items"\nUSING paradedb (\n    "id",\n    ((("mock_items"."rating" + 1))::pdb.alias(\'rating_plus_one\'))\n)'
        )

    def test_index_expression_non_text_transform_from_text_source_uses_alias(
        self,
    ) -> None:
        """Non-text outputs from text fields should use pdb.alias without a tokenizer."""
        index = ParadeDBIndex(
            fields={"id": {}},
            expressions=[
                IndexExpression(
                    Length("description"),
                    alias="description_length",
                ),
            ],
            name="mock_items_search_idx",
        )
        schema_editor = _schema_editor()
        sql = str(index.create_sql(model=MockItem, schema_editor=schema_editor))
        assert (
            sql
            == 'CREATE INDEX "mock_items_search_idx" ON "mock_items"\nUSING paradedb (\n    "id",\n    ((LENGTH("mock_items"."description"))::pdb.alias(\'description_length\'))\n)'
        )

    def test_index_expression_with_json_path_reference(self) -> None:
        """JSON path expressions require a tokenizer on the source expression."""
        index = ParadeDBIndex(
            fields={"id": {}},
            expressions=[
                IndexExpression(
                    F("metadata__word_count"),
                    alias="word_count",
                ),
            ],
            name="mock_items_search_idx",
        )
        schema_editor = _schema_editor()
        with pytest.raises(ValueError, match="resolves to a text or JSON value"):
            index.create_sql(model=MockItem, schema_editor=schema_editor)

    def test_index_expression_with_string_field_reference(self) -> None:
        """IndexExpression with string field reference (converted to F())."""
        index = ParadeDBIndex(
            fields={"id": {}},
            expressions=[
                IndexExpression(
                    "rating",
                    alias="rating_alias",
                ),
            ],
            name="mock_items_search_idx",
        )
        schema_editor = _schema_editor()
        sql = str(index.create_sql(model=MockItem, schema_editor=schema_editor))
        assert (
            sql
            == 'CREATE INDEX "mock_items_search_idx" ON "mock_items"\nUSING paradedb (\n    "id",\n    (("mock_items"."rating")::pdb.alias(\'rating_alias\'))\n)'
        )

    def test_index_expression_with_ngram_tokenizer_and_args(self) -> None:
        """IndexExpression with ngram tokenizer and positional args."""
        index = ParadeDBIndex(
            fields={"id": {}},
            expressions=[
                IndexExpression(
                    Lower("description"),
                    alias="desc_ngram",
                    tokenizer=Tokenizer.ngram(
                        3,
                        3,
                        options={"alias": "desc_ngram", "prefix_only": True},
                    ),
                ),
            ],
            name="mock_items_search_idx",
        )
        schema_editor = _schema_editor()
        sql = str(index.create_sql(model=MockItem, schema_editor=schema_editor))
        assert "pdb.ngram(3,3,'alias=desc_ngram','prefix_only=true')" in sql

    def test_multiple_index_expressions(self) -> None:
        """Multiple IndexExpressions in a single index."""
        index = ParadeDBIndex(
            fields={"id": {}},
            expressions=[
                IndexExpression(
                    Lower("description"),
                    alias="desc_lower",
                    tokenizer=Tokenizer.simple(options={"alias": "desc_lower"}),
                ),
                IndexExpression(
                    F("rating"),
                    alias="rating_idx",
                ),
            ],
            name="mock_items_search_idx",
        )
        schema_editor = _schema_editor()
        sql = str(index.create_sql(model=MockItem, schema_editor=schema_editor))
        assert "pdb.simple('alias=desc_lower')" in sql
        assert "pdb.alias('rating_idx')" in sql

    def test_index_expression_deconstruct(self) -> None:
        """ParadeDBIndex with expressions deconstructs correctly for migrations."""
        expr = IndexExpression(
            Lower("description"),
            alias="desc_lower",
            tokenizer=Tokenizer.simple(options={"alias": "desc_lower"}),
        )
        index = ParadeDBIndex(
            fields={"id": {}},
            expressions=[expr],
            name="mock_items_search_idx",
        )
        _path, _args, kwargs = index.deconstruct()
        assert "expressions" in kwargs
        assert len(kwargs["expressions"]) == 1
        assert kwargs["expressions"][0].alias == "desc_lower"

    def test_index_expression_is_migration_serializable(self) -> None:
        """ParadeDBIndex with IndexExpression serializes through MigrationWriter."""
        index = ParadeDBIndex(
            fields={"id": {}},
            expressions=[
                IndexExpression(
                    Lower("description"),
                    alias="desc_lower",
                    tokenizer=Tokenizer.simple(options={"alias": "desc_lower"}),
                )
            ],
            name="mock_items_search_idx",
        )
        serialized, imports = MigrationWriter.serialize(index)
        assert "IndexExpression(" in serialized
        assert "Lower('description')" in serialized
        assert "import django.db.models.functions.text" in imports
        assert "import paradedb.indexes" in imports

    def test_index_expression_without_expressions_no_key_in_deconstruct(self) -> None:
        """ParadeDBIndex without expressions does not include key in deconstruct."""
        index = ParadeDBIndex(
            fields={"id": {}, "description": {}},
            name="mock_items_search_idx",
        )
        _path, _args, kwargs = index.deconstruct()
        assert "expressions" not in kwargs


class TestVectorIndex:
    """Test vector opclass DDL generation on ParadeDBIndex fields."""

    @pytest.mark.parametrize(
        ("metric", "opclass"),
        [
            ("l2", "vector_l2_ops"),
            ("cosine", "vector_cosine_ops"),
            ("ip", "vector_ip_ops"),
        ],
    )
    def test_index_with_metric_emits_opclass(self, metric: str, opclass: str) -> None:
        index = ParadeDBIndex(
            fields={"id": {}, "embedding": {"metric": metric}},
            name="mock_items_search_idx",
        )
        sql = str(index.create_sql(model=MockItem, schema_editor=_schema_editor()))
        assert (
            sql
            == f'CREATE INDEX "mock_items_search_idx" ON "mock_items"\nUSING paradedb (\n    "id",\n    "embedding" {opclass}\n)'
        )

    def test_index_without_metric_emits_plain_column(self) -> None:
        index = ParadeDBIndex(
            fields={"id": {}, "embedding": {}},
            name="mock_items_search_idx",
        )
        sql = str(index.create_sql(model=MockItem, schema_editor=_schema_editor()))
        assert (
            sql
            == 'CREATE INDEX "mock_items_search_idx" ON "mock_items"\nUSING paradedb (\n    "id",\n    "embedding"\n)'
        )

    def test_invalid_metric_raises(self) -> None:
        index = ParadeDBIndex(
            fields={"id": {}, "embedding": {"metric": "hamming"}},
            name="mock_items_search_idx",
        )
        with pytest.raises(ValueError, match="Vector metric must be one of"):
            index.create_sql(model=MockItem, schema_editor=_schema_editor())

    def test_metric_on_non_vector_field_raises(self) -> None:
        index = ParadeDBIndex(
            fields={"id": {}, "description": {"metric": "l2"}},
            name="mock_items_search_idx",
        )
        with pytest.raises(ValueError, match="is not a VectorField"):
            index.create_sql(model=MockItem, schema_editor=_schema_editor())

    def test_metric_mixed_with_tokenizer_raises(self) -> None:
        index = ParadeDBIndex(
            fields={
                "id": {},
                "embedding": {"metric": "l2", "tokenizer": Tokenizer.simple()},
            },
            name="mock_items_search_idx",
        )
        with pytest.raises(ValueError, match="cannot mix 'metric'"):
            index.create_sql(model=MockItem, schema_editor=_schema_editor())

    def test_index_with_metric_deconstructs(self) -> None:
        index = ParadeDBIndex(
            fields={"id": {}, "embedding": {"metric": "cosine"}},
            name="mock_items_search_idx",
        )
        _path, _args, kwargs = index.deconstruct()
        assert kwargs["fields"] == {"id": {}, "embedding": {"metric": "cosine"}}

        serialized, _imports = MigrationWriter.serialize(index)
        assert "'metric': 'cosine'" in serialized


class TestVectorIndexOptions:
    """Test vector build options in the WITH clause of ParadeDBIndex."""

    def test_index_with_all_options_emits_with_clause(self) -> None:
        index = ParadeDBIndex(
            fields={"id": {}, "embedding": {"metric": "cosine"}},
            name="mock_items_search_idx",
            training_sample_ratio=0.01,
            max_leaf_size=32,
            partition_by=["id"],
            target_segment_count=8,
            vector_fields={"embedding": {"quantization": False}},
        )
        sql = str(index.create_sql(model=MockItem, schema_editor=_schema_editor()))
        assert (
            sql
            == 'CREATE INDEX "mock_items_search_idx" ON "mock_items"\nUSING paradedb (\n    "id",\n    "embedding" vector_cosine_ops\n)\nWITH (training_sample_ratio=0.01, max_leaf_size=32, target_segment_count=8, partition_by=\'id\', vector_fields=\'{"embedding":{"quantization":false}}\')'
        )

    def test_index_with_single_option(self) -> None:
        index = ParadeDBIndex(
            fields={"id": {}, "embedding": {"metric": "l2"}},
            name="mock_items_search_idx",
        )
        sql = str(index.create_sql(model=MockItem, schema_editor=_schema_editor()))
        assert (
            sql
            == 'CREATE INDEX "mock_items_search_idx" ON "mock_items"\nUSING paradedb (\n    "id",\n    "embedding" vector_l2_ops\n)'
        )

    def test_options_allowed_without_vector_field(self) -> None:
        index = ParadeDBIndex(
            fields={"id": {}, "description": {}},
            name="mock_items_search_idx",
            training_sample_ratio=0.5,
        )
        sql = str(index.create_sql(model=MockItem, schema_editor=_schema_editor()))
        assert "training_sample_ratio=0.5" in sql

    @pytest.mark.parametrize(
        "training_sample_ratio",
        [0.0000001, 0.0, 1.5, -0.01],
    )
    def test_training_sample_ratio_out_of_range_raises(
        self, training_sample_ratio: float
    ) -> None:
        with pytest.raises(ValueError, match="training_sample_ratio must be between"):
            ParadeDBIndex(
                fields={"id": {}},
                name="mock_items_search_idx",
                training_sample_ratio=training_sample_ratio,
            )

    def test_training_sample_ratio_invalid_type_raises(self) -> None:
        with pytest.raises(TypeError, match="training_sample_ratio must be a number"):
            ParadeDBIndex(
                fields={"id": {}},
                name="mock_items_search_idx",
                training_sample_ratio="0.01",  # type: ignore[arg-type]
            )

    @pytest.mark.parametrize("max_leaf_size", [0, 2147483648, -1])
    def test_max_leaf_size_out_of_range_raises(self, max_leaf_size: int) -> None:
        with pytest.raises(ValueError, match="max_leaf_size must be between"):
            ParadeDBIndex(
                fields={"id": {}},
                name="mock_items_search_idx",
                max_leaf_size=max_leaf_size,
            )

    @pytest.mark.parametrize("max_leaf_size", [32.5, True, "32"])
    def test_max_leaf_size_invalid_type_raises(self, max_leaf_size: object) -> None:
        with pytest.raises(TypeError, match="max_leaf_size must be an integer"):
            ParadeDBIndex(
                fields={"id": {}},
                name="mock_items_search_idx",
                max_leaf_size=max_leaf_size,  # type: ignore[arg-type]
            )

    def test_options_deconstruct_and_serialize(self) -> None:
        index = ParadeDBIndex(
            fields={"id": {}, "embedding": {"metric": "cosine"}},
            name="mock_items_search_idx",
            training_sample_ratio=0.01,
            max_leaf_size=32,
            partition_by=["id"],
            target_segment_count=8,
            vector_fields={"embedding": {"quantization": False}},
        )
        _path, _args, kwargs = index.deconstruct()
        assert kwargs["training_sample_ratio"] == 0.01
        assert kwargs["max_leaf_size"] == 32
        assert kwargs["partition_by"] == ["id"]
        assert kwargs["target_segment_count"] == 8
        assert kwargs["vector_fields"] == {"embedding": {"quantization": False}}

        serialized, _imports = MigrationWriter.serialize(index)
        assert "training_sample_ratio=0.01" in serialized
        assert "max_leaf_size=32" in serialized

    def test_unset_options_omitted_from_deconstruct(self) -> None:
        index = ParadeDBIndex(
            fields={"id": {}},
            name="mock_items_search_idx",
        )
        _path, _args, kwargs = index.deconstruct()
        assert "training_sample_ratio" not in kwargs
        assert "max_leaf_size" not in kwargs

    def test_indexes_with_equal_options_compare_equal(self) -> None:
        def build(max_leaf_size: int) -> ParadeDBIndex:
            return ParadeDBIndex(
                fields={"id": {}, "embedding": {"metric": "cosine"}},
                name="mock_items_search_idx",
                training_sample_ratio=0.01,
                max_leaf_size=max_leaf_size,
            )

        assert build(1) == build(1)
        assert build(1) != build(2)


@pytest.mark.integration
@pytest.mark.django_db(transaction=True)
@pytest.mark.usefixtures("paradedb_ready")
def test_create_partial_index_with_nullable_nonunique_first_field() -> None:
    class KeylessItem(models.Model):  # noqa: DJ008
        description = models.TextField(null=True)  # noqa: DJ001
        rating = models.IntegerField()

        class Meta:
            app_label = "tests"
            db_table = "keyless_items"

    index = ParadeDBIndex(
        fields={"description": {"tokenizer": Tokenizer.simple()}, "rating": {}},
        name="keyless_partial_idx",
        condition=Q(rating__gte=3),
    )
    with connection.schema_editor() as editor:
        editor.create_model(KeylessItem)
    try:
        KeylessItem.objects.bulk_create(
            [
                KeylessItem(description="alpha", rating=3),
                KeylessItem(description="alpha", rating=4),
                KeylessItem(description=None, rating=3),
            ]
        )
        with connection.schema_editor() as editor:
            editor.add_index(KeylessItem, index)
        with connection.cursor() as cursor:
            cursor.execute("SELECT pg_get_indexdef('keyless_partial_idx'::regclass)")
            (definition,) = cursor.fetchone()
            assert "WHERE" in definition
    finally:
        with connection.schema_editor() as editor:
            editor.delete_model(KeylessItem)
