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


def _verify_index_usage(table_name: str, index_name: str) -> bool:  # noqa: ARG001
    """
    Verify BM25 index is actually used by PostgreSQL query planner.

    Returns True if a ParadeDB Custom Scan is found in the execution plan,
    indicating the BM25 index is being utilized for query execution.

    Args:
        table_name: Name of the table to check
        index_name: Name of the BM25 index (kept for API consistency)
    """
    quoted_table = connection.ops.quote_name(table_name)
    with connection.cursor() as cursor:
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

    plan_text = "\n".join(row[0] for row in plan_rows)

    # ParadeDB Custom Scan node names (from pg_search/src/postgres/customscan/)
    paradedb_scans = (
        "ParadeDB Scan",  # BaseScan - standard BM25 queries
        "ParadeDB Aggregate Scan",  # AggregateScan - GROUP BY/aggregates
        "ParadeDB Join Scan",  # JoinScan - join queries with LIMIT
    )
    return any(scan in plan_text for scan in paradedb_scans)


@pytest.mark.django_db(transaction=True)
@pytest.mark.parametrize(
    ("tokenizer", "table_name", "index_name"),
    [
        ("simple", "migtests_items_simple", "migtests_items_simple_bm25_idx"),
        (
            "unicode_words",
            "migtests_items_unicode_words",
            "migtests_items_unicode_words_bm25_idx",
        ),
        ("literal", "migtests_items_literal", "migtests_items_literal_bm25_idx"),
    ],
)
def test_apply_and_unapply_create_model_migration(
    tokenizer: str, table_name: str, index_name: str
) -> None:
    """
    CreateModel with BM25Index migrates forwards/backwards, creates the index,
    and verifies the index is actually used by PostgreSQL query planner.

    Uses transaction=True to allow explicit transaction management and commits,
    simulating real migration behavior where indexes become visible after commit.
    """

    app_label = "migtests"
    # Ensure a clean slate before applying the migration
    _drop_table_if_exists(table_name)
    connection.commit()

    create_model = migrations.CreateModel(
        name=f"MigratedItem_{tokenizer}",
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
                        "title": {"tokenizer": tokenizer},
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
        normalized_index_def = (
            index_def.replace('"', "")
            .replace("(", "")
            .replace(")", "")
            .replace(" ", "")
        )
        assert f"title::pdb.{tokenizer}" in normalized_index_def, index_def
        assert {"id", "title", "metadata"}.issubset(column_names)

        # Insert test data in a separate transaction (simulates real usage)
        quoted_table = connection.ops.quote_name(table_name)
        with connection.cursor() as cursor:
            cursor.execute(
                f"INSERT INTO {quoted_table} (title, metadata) "
                f"VALUES ('test document', '{{}}'::jsonb), "
                f"('another test', '{{}}'::jsonb), "
                f"('sample text', '{{}}'::jsonb), "
                f"('test', '{{}}'::jsonb);"
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
