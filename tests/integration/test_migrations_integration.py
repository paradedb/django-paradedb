"""End-to-end migration flow tests with index usage verification via EXPLAIN ANALYZE."""

from __future__ import annotations

import pytest
from django.db import connection, migrations, models
from django.db.migrations.state import ProjectState

from paradedb.indexes import BM25Index

pytestmark = [
    pytest.mark.integration,
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


def _fetch_index_definition(table_name: str, index_name: str) -> str | None:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT indexdef
            FROM pg_indexes
            WHERE schemaname = current_schema()
              AND tablename = %s
              AND indexname = %s;
            """,
            [table_name, index_name],
        )
        row = cursor.fetchone()
    return row[0] if row else None


def _verify_index_usage(table_name: str, index_name: str) -> bool:
    """
    Verify BM25 index is actually used by PostgreSQL query planner.

    Returns True if ParadeDBScan is found in the execution plan, indicating
    the BM25 index is being utilized for query execution.
    """
    quoted_table = connection.ops.quote_name(table_name)
    with connection.cursor() as cursor:
        # Simple query that should trigger BM25 index usage via ParadeDB operators
        cursor.execute(
            f"""
            EXPLAIN (ANALYZE, BUFFERS)
            SELECT id, title, pdb.score(id) as relevance
            FROM {quoted_table}
            WHERE title &&& 'test'
            ORDER BY pdb.score(id) DESC;
            """
        )
        plan_rows = cursor.fetchall()

    # Join all plan rows into a single string for analysis
    plan_text = "\n".join(row[0] for row in plan_rows)

    # Check for indicators that BM25 index is being used
    # ParadeDB uses "Custom Scan" with "ParadeDB" or "ParadeDBScan" in the plan
    has_paradedb_scan = "ParadeDB" in plan_text or "Custom Scan" in plan_text
    has_index_reference = index_name in plan_text
    no_seq_scan = "Seq Scan" not in plan_text

    # Either explicit index reference OR (ParadeDB scan without sequential scan)
    return has_paradedb_scan and (has_index_reference or no_seq_scan)


@pytest.mark.django_db(transaction=True)
def test_apply_and_unapply_create_model_migration() -> None:
    """
    CreateModel with BM25Index migrates forwards/backwards, creates the index,
    and verifies the index is actually used by PostgreSQL query planner.

    Uses transaction=True to allow explicit transaction management and commits,
    simulating real migration behavior where indexes become visible after commit.
    """

    app_label = "migtests"
    table_name = "migtests_items"
    index_name = "migtests_items_bm25_idx"

    # Ensure a clean slate before applying the migration
    _drop_table_if_exists(table_name)
    connection.commit()

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
                    name=index_name,
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
    # Schema editor commits on exit

    try:
        # Verify table and index creation after commit
        assert _table_exists(table_name)
        with connection.cursor() as cursor:
            column_names = {
                column.name
                for column in connection.introspection.get_table_description(
                    cursor, table_name
                )
            }
        index_def = _fetch_index_definition(table_name, index_name)
        assert index_def is not None
        assert "USING bm25" in index_def
        assert {"id", "title", "metadata"}.issubset(column_names)

        # Insert test data in a separate transaction (simulates real usage)
        quoted_table = connection.ops.quote_name(table_name)
        with connection.cursor() as cursor:
            cursor.execute(
                f"INSERT INTO {quoted_table} (title, metadata) "
                f"VALUES ('test document', '{{}}'::jsonb), "
                f"('another test', '{{}}'::jsonb), "
                f"('sample text', '{{}}'::jsonb);"
            )
        connection.commit()

        # Verify BM25 index is used by query planner in a fresh transaction
        assert _verify_index_usage(table_name, index_name), (
            f"BM25 index {index_name} exists but is not being used by query planner. "
            f"Check EXPLAIN output for Custom Scan (ParadeDBScan)."
        )

    finally:
        if forward_applied and _table_exists(table_name):
            with connection.schema_editor(atomic=True) as editor:
                create_model.database_backwards(app_label, editor, to_state, from_state)
            # Schema editor commits on exit
        else:
            _drop_table_if_exists(table_name)
            connection.commit()
        assert not _table_exists(table_name)
