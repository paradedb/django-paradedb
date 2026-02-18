"""Integration tests for ParadeDB diagnostics helpers and commands."""

from __future__ import annotations

import json
from io import StringIO

import pytest
from django.core.management import call_command
from django.db import connection

from paradedb.functions import (
    paradedb_index_segments,
    paradedb_indexes,
    paradedb_verify_all_indexes,
    paradedb_verify_index,
)

pytestmark = [
    pytest.mark.integration,
    pytest.mark.django_db,
    pytest.mark.usefixtures("mock_items"),
]


@pytest.fixture(autouse=True)
def require_diagnostic_functions(mock_items: None) -> None:
    _ = mock_items
    required = {"indexes", "index_segments", "verify_index", "verify_all_indexes"}
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT DISTINCT p.proname
            FROM pg_proc AS p
            JOIN pg_namespace AS n ON n.oid = p.pronamespace
            WHERE n.nspname = 'pdb'
              AND p.proname = ANY(%s)
            """,
            [list(required)],
        )
        available = {row[0] for row in cursor.fetchall()}
    missing = sorted(required - available)
    if missing:
        pytest.skip(
            "ParadeDB diagnostics not available in this pg_search version: "
            + ", ".join(missing)
        )


def test_paradedb_indexes_helper_returns_mock_items_index() -> None:
    rows = paradedb_indexes()
    assert any(row["indexname"] == "mock_items_bm25_idx" for row in rows)


def test_paradedb_index_segments_helper_returns_segments() -> None:
    rows = paradedb_index_segments("mock_items_bm25_idx")
    assert len(rows) > 0
    first = rows[0]
    assert "segment_idx" in first
    assert "segment_id" in first


def test_paradedb_verify_index_helper_returns_checks() -> None:
    rows = paradedb_verify_index("mock_items_bm25_idx", sample_rate=0.1)
    assert len(rows) > 0
    first = rows[0]
    assert "check_name" in first
    assert "passed" in first
    assert "details" in first


def test_paradedb_verify_all_indexes_helper_filters_by_pattern() -> None:
    rows = paradedb_verify_all_indexes(index_pattern="mock_items_bm25_idx")
    assert len(rows) > 0
    first = rows[0]
    assert "check_name" in first
    assert "passed" in first


def test_paradedb_indexes_command() -> None:
    stdout = StringIO()
    call_command("paradedb_indexes", stdout=stdout)
    payload = json.loads(stdout.getvalue())
    assert any(row["indexname"] == "mock_items_bm25_idx" for row in payload)


def test_paradedb_index_segments_command() -> None:
    stdout = StringIO()
    call_command("paradedb_index_segments", "mock_items_bm25_idx", stdout=stdout)
    payload = json.loads(stdout.getvalue())
    assert len(payload) > 0


def test_paradedb_verify_index_command() -> None:
    stdout = StringIO()
    call_command(
        "paradedb_verify_index",
        "mock_items_bm25_idx",
        sample_rate=0.1,
        stdout=stdout,
    )
    payload = json.loads(stdout.getvalue())
    assert len(payload) > 0
    assert "check_name" in payload[0]


def test_paradedb_verify_all_indexes_command() -> None:
    stdout = StringIO()
    call_command(
        "paradedb_verify_all_indexes",
        index_pattern="mock_items_bm25_idx",
        stdout=stdout,
    )
    payload = json.loads(stdout.getvalue())
    assert len(payload) > 0
    assert "check_name" in payload[0]
