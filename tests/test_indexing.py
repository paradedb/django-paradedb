"""Unit tests for BM25Index configuration validation errors."""

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

from paradedb.indexes import BM25Index, IndexExpression
from paradedb.search import Tokenizer
from tests.models import Product


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


class TestBM25Index:
    """Test BM25 index SQL generation."""

    def test_basic_index_sql(self) -> None:
        """Basic BM25 index DDL generation."""
        index = BM25Index(
            fields={"id": {}, "description": {}},
            key_field="id",
            name="product_search_idx",
        )
        schema_editor = _schema_editor()
        sql = str(index.create_sql(model=Product, schema_editor=schema_editor))
        assert (
            sql
            == 'CREATE INDEX "product_search_idx" ON "tests_product"\nUSING bm25 (\n    "id",\n    "description"\n)\nWITH (key_field=\'id\')'
        )

    def test_index_with_tokenizer(self) -> None:
        """Index with tokenizer configuration."""
        index = BM25Index(
            fields={
                "id": {},
                "description": {
                    "tokenizer": Tokenizer.simple(
                        options={"lowercase": True, "stemmer": "english"}
                    ),
                },
            },
            key_field="id",
            name="product_search_idx",
        )
        schema_editor = _schema_editor()
        sql = str(index.create_sql(model=Product, schema_editor=schema_editor))
        assert (
            sql
            == 'CREATE INDEX "product_search_idx" ON "tests_product"\nUSING bm25 (\n    "id",\n    ("description"::pdb.simple(\'lowercase=true\',\'stemmer=english\'))\n)\nWITH (key_field=\'id\')'
        )

    def test_index_with_tokenizer_only(self) -> None:
        """Index with tokenizer only."""
        index = BM25Index(
            fields={
                "id": {},
                "description": {"tokenizer": Tokenizer.simple()},
            },
            key_field="id",
            name="product_search_idx",
        )
        schema_editor = _schema_editor()
        sql = str(index.create_sql(model=Product, schema_editor=schema_editor))
        assert (
            sql
            == 'CREATE INDEX "product_search_idx" ON "tests_product"\nUSING bm25 (\n    "id",\n    ("description"::pdb.simple)\n)\nWITH (key_field=\'id\')'
        )

    def test_json_field_index(self) -> None:
        """JSON field with json_keys configuration."""
        index = BM25Index(
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
            key_field="id",
            name="product_search_idx",
        )
        schema_editor = _schema_editor()
        sql = str(index.create_sql(model=Product, schema_editor=schema_editor))
        assert (
            sql
            == "CREATE INDEX \"product_search_idx\" ON \"tests_product\"\nUSING bm25 (\n    \"id\",\n    ((\"metadata\"->>'title')::pdb.simple('alias=metadata_title','lowercase=true')),\n    ((\"metadata\"->>'brand')::pdb.simple('alias=metadata_brand'))\n)\nWITH (key_field='id')"
        )

    def test_json_field_native_json_fields(self) -> None:
        """Native json_fields config is emitted via WITH (...) and indexes the column."""
        index = BM25Index(
            fields={
                "id": {},
                "metadata": {
                    "json_fields": {
                        "fast": True,
                        "expand_dots": False,
                    }
                },
            },
            key_field="id",
            name="product_search_idx",
        )
        schema_editor = _schema_editor()
        sql = str(index.create_sql(model=Product, schema_editor=schema_editor))
        assert (
            sql
            == 'CREATE INDEX "product_search_idx" ON "tests_product"\nUSING bm25 (\n    "id",\n    "metadata"\n)\nWITH (key_field=\'id\', json_fields=\'{"metadata":{"expand_dots":false,"fast":true}}\')'
        )

    def test_json_key_without_tokenizer_raises(self) -> None:
        """JSON keys without an explicit tokenizer raise TypeError."""
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
        schema_editor = _schema_editor()
        with pytest.raises(TypeError, match="tokenizer must be a Tokenizer"):
            index.create_sql(model=Product, schema_editor=schema_editor)

    def test_json_field_literal_alias(self) -> None:
        """JSON subfields can be indexed with literal tokenizer aliases."""
        index = BM25Index(
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
            key_field="id",
            name="product_search_idx",
        )
        schema_editor = _schema_editor()
        sql = str(index.create_sql(model=Product, schema_editor=schema_editor))
        assert (
            sql
            == 'CREATE INDEX "product_search_idx" ON "tests_product"\nUSING bm25 (\n    "id",\n    "description",\n    (("metadata"->>\'color\')::pdb.literal(\'alias=metadata_color\')),\n    (("metadata"->>\'location\')::pdb.literal(\'alias=metadata_location\'))\n)\nWITH (key_field=\'id\')'
        )

    def test_field_with_multiple_tokenizers(self) -> None:
        """A field can include multiple tokenizer expressions."""
        index = BM25Index(
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
            key_field="id",
            name="product_search_idx",
        )
        schema_editor = _schema_editor()
        sql = str(index.create_sql(model=Product, schema_editor=schema_editor))
        assert (
            sql
            == 'CREATE INDEX "product_search_idx" ON "tests_product"\nUSING bm25 (\n    "id",\n    ("description"::pdb.literal),\n    ("description"::pdb.simple(\'alias=description_simple\',\'lowercase=true\'))\n)\nWITH (key_field=\'id\')'
        )

    def test_multiple_tokenizers_allows_secondary_entries_without_alias(self) -> None:
        """Thin wrapper mode allows tokenizer entries without alias."""
        index = BM25Index(
            fields={
                "id": {},
                "description": {
                    "tokenizers": [
                        {"tokenizer": Tokenizer.literal()},
                        {"tokenizer": Tokenizer.simple()},
                    ]
                },
            },
            key_field="id",
            name="product_search_idx",
        )
        schema_editor = _schema_editor()
        sql = str(index.create_sql(model=Product, schema_editor=schema_editor))
        assert (
            sql
            == 'CREATE INDEX "product_search_idx" ON "tests_product"\nUSING bm25 (\n    "id",\n    ("description"::pdb.literal),\n    ("description"::pdb.simple)\n)\nWITH (key_field=\'id\')'
        )

    def test_multiple_tokenizers_cannot_mix_with_single_tokenizer_keys(self) -> None:
        """The list syntax cannot be combined with top-level tokenizer keys."""
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
        schema_editor = _schema_editor()
        with pytest.raises(ValueError, match="cannot mix 'tokenizers'"):
            index.create_sql(model=Product, schema_editor=schema_editor)

    def test_structured_ngram_args_and_named_args_in_multi_tokenizer_dsl(self) -> None:
        """Supports positional ngram args plus named args in DSL."""
        index = BM25Index(
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
            key_field="id",
            name="product_search_idx",
        )
        schema_editor = _schema_editor()
        sql = str(index.create_sql(model=Product, schema_editor=schema_editor))
        assert (
            sql
            == 'CREATE INDEX "product_search_idx" ON "tests_product"\nUSING bm25 (\n    "id",\n    ("description"::pdb.literal),\n    ("description"::pdb.ngram(3,3,\'alias=description_ngram\',\'prefix_only=true\',\'positions=true\'))\n)\nWITH (key_field=\'id\')'
        )

    def test_structured_regex_pattern_and_alias_in_multi_tokenizer_dsl(self) -> None:
        """Supports regex_pattern positional args with alias in DSL."""
        index = BM25Index(
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
            key_field="id",
            name="product_search_idx",
        )
        schema_editor = _schema_editor()
        sql = str(index.create_sql(model=Product, schema_editor=schema_editor))
        assert (
            sql
            == 'CREATE INDEX "product_search_idx" ON "tests_product"\nUSING bm25 (\n    "id",\n    ("description"::pdb.literal),\n    ("description"::pdb.regex_pattern(\'(?i)\\bh\\w*\',\'alias=description_regex\'))\n)\nWITH (key_field=\'id\')'
        )

    def test_structured_lindera_dictionary_argument_in_multi_tokenizer_dsl(
        self,
    ) -> None:
        """Supports lindera dictionary positional arg in DSL."""
        index = BM25Index(
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
            key_field="id",
            name="product_search_idx",
        )
        schema_editor = _schema_editor()
        sql = str(index.create_sql(model=Product, schema_editor=schema_editor))
        assert (
            sql
            == 'CREATE INDEX "product_search_idx" ON "tests_product"\nUSING bm25 (\n    "id",\n    ("description"::pdb.literal),\n    ("description"::pdb.lindera(\'japanese\',\'alias=description_jp\'))\n)\nWITH (key_field=\'id\')'
        )

    def test_value_based_token_filter_named_args(self) -> None:
        """Supports non-boolean tokenizer options."""
        index = BM25Index(
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
            key_field="id",
            name="product_search_idx",
        )
        schema_editor = _schema_editor()
        sql = str(index.create_sql(model=Product, schema_editor=schema_editor))
        assert (
            sql
            == "CREATE INDEX \"product_search_idx\" ON \"tests_product\"\nUSING bm25 (\n    \"id\",\n    (\"description\"::pdb.simple('lowercase=false','stopwords_language=English,French','remove_long=20','remove_short=2','stemmer=english'))\n)\nWITH (key_field='id')"
        )

    def test_indexed_expression_with_concat(self) -> None:
        """Supports non-boolean token filter named args in DSL."""
        index = BM25Index(
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
            key_field="id",
            name="product_search_idx",
        )
        schema_editor = _schema_editor()
        sql = str(index.create_sql(model=Product, schema_editor=schema_editor))
        assert (
            sql
            == 'CREATE INDEX "product_search_idx" ON "tests_product"\nUSING bm25 (\n    "id",\n    ((("tests_product"."description" || \' \' || "tests_product"."category"))::pdb.simple(\'alias=description_concat\'))\n)\nWITH (key_field=\'id\')'
        )

    def test_create_sql_concurrently(self) -> None:
        """create_sql with concurrently=True emits CREATE INDEX CONCURRENTLY."""
        index = BM25Index(
            fields={"id": {}, "description": {"tokenizer": Tokenizer.simple()}},
            key_field="id",
            name="product_search_idx",
        )
        schema_editor = _schema_editor()
        sql = str(
            index.create_sql(
                model=Product, schema_editor=schema_editor, concurrently=True
            )
        )
        assert sql.startswith('CREATE INDEX CONCURRENTLY "product_search_idx"')
        assert "USING bm25" in sql

    def test_create_sql_without_concurrently(self) -> None:
        """create_sql without concurrently does not emit CONCURRENTLY."""
        index = BM25Index(
            fields={"id": {}, "description": {"tokenizer": Tokenizer.simple()}},
            key_field="id",
            name="product_search_idx",
        )
        schema_editor = _schema_editor()
        sql = str(index.create_sql(model=Product, schema_editor=schema_editor))
        assert sql.startswith('CREATE INDEX "product_search_idx"')
        assert "CONCURRENTLY" not in sql

    def test_create_sql_with_condition(self) -> None:
        """create_sql with condition appends a WHERE clause."""
        index = BM25Index(
            fields={"id": {}, "description": {"tokenizer": Tokenizer.simple()}},
            key_field="id",
            name="product_search_idx",
            condition=Q(description__isnull=False),
        )
        schema_editor = _schema_editor()
        sql = str(index.create_sql(model=Product, schema_editor=schema_editor))
        assert sql.endswith('WHERE "description" IS NOT NULL')
        assert "USING bm25" in sql

    def test_create_sql_with_condition_and_concurrently(self) -> None:
        """create_sql with both condition and concurrently emits both."""
        index = BM25Index(
            fields={"id": {}, "description": {"tokenizer": Tokenizer.simple()}},
            key_field="id",
            name="product_search_idx",
            condition=Q(description__isnull=False),
        )
        schema_editor = _schema_editor()
        sql = str(
            index.create_sql(
                model=Product, schema_editor=schema_editor, concurrently=True
            )
        )
        assert sql.startswith('CREATE INDEX CONCURRENTLY "product_search_idx"')
        assert sql.endswith('WHERE "description" IS NOT NULL')

    def test_create_sql_with_native_json_fields_and_condition(self) -> None:
        """json_fields and condition can both be emitted in the same CREATE INDEX."""
        index = BM25Index(
            fields={"id": {}, "metadata": {"json_fields": {"fast": True}}},
            key_field="id",
            name="product_search_idx",
            condition=Q(description__isnull=False),
        )
        schema_editor = _schema_editor()
        sql = str(index.create_sql(model=Product, schema_editor=schema_editor))
        assert 'json_fields=\'{"metadata":{"fast":true}}\'' in sql
        assert sql.endswith('WHERE "description" IS NOT NULL')

    def test_create_sql_without_condition_no_where(self) -> None:
        """create_sql without condition does not append WHERE clause."""
        index = BM25Index(
            fields={"id": {}, "description": {"tokenizer": Tokenizer.simple()}},
            key_field="id",
            name="product_search_idx",
        )
        schema_editor = _schema_editor()
        sql = str(index.create_sql(model=Product, schema_editor=schema_editor))
        assert "WHERE" not in sql

    def test_index_expression_with_lower_and_tokenizer(self) -> None:
        """IndexExpression with Lower() and tokenizer generates correct SQL."""
        index = BM25Index(
            fields={"id": {}, "description": {}},
            expressions=[
                IndexExpression(
                    Lower("description"),
                    alias="description_lower",
                    tokenizer=Tokenizer.simple(options={"alias": "description_lower"}),
                ),
            ],
            key_field="id",
            name="product_search_idx",
        )
        schema_editor = _schema_editor()
        sql = str(index.create_sql(model=Product, schema_editor=schema_editor))
        assert (
            sql
            == 'CREATE INDEX "product_search_idx" ON "tests_product"\nUSING bm25 (\n    "id",\n    "description",\n    ((LOWER("tests_product"."description"))::pdb.simple(\'alias=description_lower\'))\n)\nWITH (key_field=\'id\')'
        )

    def test_index_expression_non_text_with_pdb_alias(self) -> None:
        """IndexExpression without tokenizer uses pdb.alias for non-text."""
        index = BM25Index(
            fields={"id": {}, "description": {}},
            expressions=[
                IndexExpression(
                    F("rating"),
                    alias="rating_indexed",
                ),
            ],
            key_field="id",
            name="product_search_idx",
        )
        schema_editor = _schema_editor()
        sql = str(index.create_sql(model=Product, schema_editor=schema_editor))
        assert (
            sql
            == 'CREATE INDEX "product_search_idx" ON "tests_product"\nUSING bm25 (\n    "id",\n    "description",\n    (("tests_product"."rating")::pdb.alias(\'rating_indexed\'))\n)\nWITH (key_field=\'id\')'
        )

    def test_index_expression_with_tokenizer_and_filters(self) -> None:
        """IndexExpression with tokenizer, filters, and stemmer."""
        index = BM25Index(
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
            key_field="id",
            name="product_search_idx",
        )
        schema_editor = _schema_editor()
        sql = str(index.create_sql(model=Product, schema_editor=schema_editor))
        assert (
            sql
            == 'CREATE INDEX "product_search_idx" ON "tests_product"\nUSING bm25 (\n    "id",\n    ((LOWER("tests_product"."description"))::pdb.simple(\'alias=desc_processed\',\'lowercase=true\',\'stemmer=english\'))\n)\nWITH (key_field=\'id\')'
        )

    def test_index_expression_with_arithmetic(self) -> None:
        """IndexExpression with arithmetic constant is inlined into SQL."""
        index = BM25Index(
            fields={"id": {}},
            expressions=[
                IndexExpression(
                    F("rating") + 1,
                    alias="rating_plus_one",
                ),
            ],
            key_field="id",
            name="product_search_idx",
        )
        schema_editor = _schema_editor()
        sql = str(index.create_sql(model=Product, schema_editor=schema_editor))
        assert (
            sql
            == 'CREATE INDEX "product_search_idx" ON "tests_product"\nUSING bm25 (\n    "id",\n    ((("tests_product"."rating" + 1))::pdb.alias(\'rating_plus_one\'))\n)\nWITH (key_field=\'id\')'
        )

    def test_index_expression_non_text_transform_from_text_source_uses_alias(
        self,
    ) -> None:
        """Non-text outputs from text fields should use pdb.alias without a tokenizer."""
        index = BM25Index(
            fields={"id": {}},
            expressions=[
                IndexExpression(
                    Length("description"),
                    alias="description_length",
                ),
            ],
            key_field="id",
            name="product_search_idx",
        )
        schema_editor = _schema_editor()
        sql = str(index.create_sql(model=Product, schema_editor=schema_editor))
        assert (
            sql
            == 'CREATE INDEX "product_search_idx" ON "tests_product"\nUSING bm25 (\n    "id",\n    ((LENGTH("tests_product"."description"))::pdb.alias(\'description_length\'))\n)\nWITH (key_field=\'id\')'
        )

    def test_index_expression_with_json_path_reference(self) -> None:
        """JSON path expressions require a tokenizer on the source expression."""
        index = BM25Index(
            fields={"id": {}},
            expressions=[
                IndexExpression(
                    F("metadata__word_count"),
                    alias="word_count",
                ),
            ],
            key_field="id",
            name="product_search_idx",
        )
        schema_editor = _schema_editor()
        with pytest.raises(ValueError, match="resolves to a text or JSON value"):
            index.create_sql(model=Product, schema_editor=schema_editor)

    def test_index_expression_with_string_field_reference(self) -> None:
        """IndexExpression with string field reference (converted to F())."""
        index = BM25Index(
            fields={"id": {}},
            expressions=[
                IndexExpression(
                    "rating",
                    alias="rating_alias",
                ),
            ],
            key_field="id",
            name="product_search_idx",
        )
        schema_editor = _schema_editor()
        sql = str(index.create_sql(model=Product, schema_editor=schema_editor))
        assert (
            sql
            == 'CREATE INDEX "product_search_idx" ON "tests_product"\nUSING bm25 (\n    "id",\n    (("tests_product"."rating")::pdb.alias(\'rating_alias\'))\n)\nWITH (key_field=\'id\')'
        )

    def test_index_expression_with_ngram_tokenizer_and_args(self) -> None:
        """IndexExpression with ngram tokenizer and positional args."""
        index = BM25Index(
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
            key_field="id",
            name="product_search_idx",
        )
        schema_editor = _schema_editor()
        sql = str(index.create_sql(model=Product, schema_editor=schema_editor))
        assert "pdb.ngram(3,3,'alias=desc_ngram','prefix_only=true')" in sql

    def test_multiple_index_expressions(self) -> None:
        """Multiple IndexExpressions in a single index."""
        index = BM25Index(
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
            key_field="id",
            name="product_search_idx",
        )
        schema_editor = _schema_editor()
        sql = str(index.create_sql(model=Product, schema_editor=schema_editor))
        assert "pdb.simple('alias=desc_lower')" in sql
        assert "pdb.alias('rating_idx')" in sql

    def test_index_expression_deconstruct(self) -> None:
        """BM25Index with expressions deconstructs correctly for migrations."""
        expr = IndexExpression(
            Lower("description"),
            alias="desc_lower",
            tokenizer=Tokenizer.simple(options={"alias": "desc_lower"}),
        )
        index = BM25Index(
            fields={"id": {}},
            expressions=[expr],
            key_field="id",
            name="product_search_idx",
        )
        _path, _args, kwargs = index.deconstruct()
        assert "expressions" in kwargs
        assert len(kwargs["expressions"]) == 1
        assert kwargs["expressions"][0].alias == "desc_lower"

    def test_index_expression_is_migration_serializable(self) -> None:
        """BM25Index with IndexExpression serializes through MigrationWriter."""
        index = BM25Index(
            fields={"id": {}},
            expressions=[
                IndexExpression(
                    Lower("description"),
                    alias="desc_lower",
                    tokenizer=Tokenizer.simple(options={"alias": "desc_lower"}),
                )
            ],
            key_field="id",
            name="product_search_idx",
        )
        serialized, imports = MigrationWriter.serialize(index)
        assert "IndexExpression(" in serialized
        assert "Lower('description')" in serialized
        assert "import django.db.models.functions.text" in imports
        assert "import paradedb.indexes" in imports

    def test_index_expression_without_expressions_no_key_in_deconstruct(self) -> None:
        """BM25Index without expressions does not include key in deconstruct."""
        index = BM25Index(
            fields={"id": {}, "description": {}},
            key_field="id",
            name="product_search_idx",
        )
        _path, _args, kwargs = index.deconstruct()
        assert "expressions" not in kwargs
