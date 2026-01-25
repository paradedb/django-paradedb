"""End-to-end migration flow tests (lightweight index presence check only)."""

from __future__ import annotations

import pytest
from django.db import connection, migrations, models
from django.db.migrations.state import ProjectState

from paradedb.indexes import BM25Index

pytestmark = [
    pytest.mark.integration,
    pytest.mark.django_db,
    pytest.mark.usefixtures("paradedb_ready"),
]


def _table_exists(table_name: str) -> bool:
    with connection.cursor() as cursor:
        cursor.execute("SELECT to_regclass(%s);", [table_name])
        (regclass,) = cursor.fetchone()
    return regclass == table_name


def _drop_table_if_exists(table_name: str) -> None:
    quoted = connection.ops.quote_name(table_name)
    with connection.cursor() as cursor:
        cursor.execute(f"DROP TABLE IF EXISTS {quoted} CASCADE;")


def test_apply_and_unapply_create_model_migration() -> None:
    """CreateModel with BM25Index migrates forwards/backwards and registers the index."""

    app_label = "migtests"
    table_name = "migtests_items"

    # Ensure a clean slate before applying the migration
    _drop_table_if_exists(table_name)

    create_model = migrations.CreateModel(
        name="MigratedItem",
        fields=[
            ("id", models.AutoField(primary_key=True)),
            ("title", models.TextField()),
            ("metadata", models.JSONField(default=dict)),
        ],
        options={
            "db_table": table_name,
            "indexes": [
                BM25Index(
                    fields={
                        "id": {},
                        "title": {"tokenizer": "simple"},
                        "metadata": {
                            "json_keys": {
                                "color": {"tokenizer": "literal"},
                            }
                        },
                    },
                    key_field="id",
                    name="migtests_items_bm25_idx",
                )
            ],
        },
    )

    from_state = ProjectState()
    to_state = from_state.clone()
    create_model.state_forwards(app_label, to_state)

    forward_applied = False
    with connection.schema_editor(atomic=True) as editor:
        create_model.database_forwards(app_label, editor, from_state, to_state)
        forward_applied = True

    try:
        assert _table_exists(table_name)
        with connection.cursor() as cursor:
            column_names = {
                column.name
                for column in connection.introspection.get_table_description(
                    cursor, table_name
                )
            }
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT indexname, indexdef
                FROM pg_indexes
                WHERE schemaname = current_schema()
                  AND tablename = %s
                  AND indexname = %s;
                """,
                [table_name, "migtests_items_bm25_idx"],
            )
            row = cursor.fetchone()
        assert row is not None
        index_name, index_def = row
        assert index_name == "migtests_items_bm25_idx"
        assert "USING bm25" in index_def
        assert {"id", "title", "metadata"}.issubset(column_names)

    finally:
        if forward_applied and _table_exists(table_name):
            with connection.schema_editor(atomic=True) as editor:
                create_model.database_backwards(app_label, editor, to_state, from_state)
        else:
            _drop_table_if_exists(table_name)
        assert not _table_exists(table_name)
