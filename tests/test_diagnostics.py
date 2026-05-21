"""Unit tests for diagnostic command edge-case validation."""

from __future__ import annotations

import argparse
import json
from io import StringIO
from unittest.mock import patch

import pytest
from django.core.management import call_command
from django.core.management.base import CommandError

from paradedb.functions import (
    paradedb_index_segments,
    paradedb_indexes,
    paradedb_verify_all_indexes,
    paradedb_verify_index,
)
from paradedb.management.commands import (
    paradedb_index_segments as cmd_index_segments,
)
from paradedb.management.commands import (
    paradedb_indexes as cmd_indexes,
)
from paradedb.management.commands import (
    paradedb_verify_all_indexes as cmd_verify_all,
)
from paradedb.management.commands import (
    paradedb_verify_index as cmd_verify_index,
)
from paradedb.management.commands._paradedb_diag_utils import (
    validate_sample_rate,
)

pytestmark = [
    pytest.mark.integration,
    pytest.mark.django_db,
    pytest.mark.usefixtures("mock_items"),
]


@pytest.mark.parametrize("sample_rate", [-0.1, 1.1])
def test_validate_sample_rate_rejects_out_of_bounds(sample_rate: float) -> None:
    with pytest.raises(CommandError, match="sample-rate"):
        validate_sample_rate(sample_rate)


@pytest.mark.parametrize("sample_rate", [None, 0.0, 0.25, 1.0])
def test_validate_sample_rate_accepts_valid_values(sample_rate: float | None) -> None:
    validate_sample_rate(sample_rate)


def test_paradedb_indexes_command_parser_and_handle() -> None:
    parser = argparse.ArgumentParser()
    command = cmd_indexes.Command()
    command.add_arguments(parser)
    parsed = parser.parse_args(["--database", "search"])
    assert parsed.database == "search"

    with (
        patch.object(
            cmd_indexes, "paradedb_indexes", return_value=[{"indexname": "x"}]
        ) as helper,
        patch.object(cmd_indexes, "write_json") as write,
    ):
        command.handle(database="search")
    helper.assert_called_once_with(using="search")
    write.assert_called_once()


def test_paradedb_index_segments_command_parser_and_handle() -> None:
    parser = argparse.ArgumentParser()
    command = cmd_index_segments.Command()
    command.add_arguments(parser)
    parsed = parser.parse_args(["my_idx", "--database", "search"])
    assert parsed.index == "my_idx"
    assert parsed.database == "search"

    with (
        patch.object(
            cmd_index_segments, "paradedb_index_segments", return_value=[{}]
        ) as helper,
        patch.object(cmd_index_segments, "write_json") as write,
    ):
        command.handle(index="my_idx", database="search")
    helper.assert_called_once_with(index="my_idx", using="search")
    write.assert_called_once()


def test_paradedb_verify_index_command_parser_and_handle() -> None:
    parser = argparse.ArgumentParser()
    command = cmd_verify_index.Command()
    command.add_arguments(parser)
    parsed = parser.parse_args(
        [
            "my_idx",
            "--heapallindexed",
            "--sample-rate",
            "0.25",
            "--report-progress",
            "--verbose",
            "--on-error-stop",
            "--segment-id",
            "1",
            "--segment-id",
            "3",
            "--database",
            "search",
        ]
    )
    assert parsed.index == "my_idx"
    assert parsed.segment_ids == [1, 3]
    assert parsed.database == "search"

    with (
        patch.object(cmd_verify_index, "validate_sample_rate") as validate,
        patch.object(
            cmd_verify_index, "paradedb_verify_index", return_value=[{}]
        ) as helper,
        patch.object(cmd_verify_index, "write_json") as write,
    ):
        command.handle(
            index="my_idx",
            heapallindexed=True,
            sample_rate=0.25,
            report_progress=True,
            verbose=True,
            on_error_stop=True,
            segment_ids=[1, 3],
            database="search",
        )
    validate.assert_called_once_with(0.25)
    helper.assert_called_once_with(
        index="my_idx",
        heapallindexed=True,
        sample_rate=0.25,
        report_progress=True,
        verbose=True,
        on_error_stop=True,
        segment_ids=[1, 3],
        using="search",
    )
    write.assert_called_once()


def test_paradedb_verify_index_command_handle_without_sample_rate_skips_validation() -> (
    None
):
    command = cmd_verify_index.Command()
    with (
        patch.object(cmd_verify_index, "validate_sample_rate") as validate,
        patch.object(
            cmd_verify_index, "paradedb_verify_index", return_value=[{}]
        ) as helper,
        patch.object(cmd_verify_index, "write_json"),
    ):
        command.handle(
            index="my_idx",
            heapallindexed=False,
            sample_rate=None,
            report_progress=False,
            verbose=False,
            on_error_stop=False,
            segment_ids=None,
            database="default",
        )
    validate.assert_not_called()
    helper.assert_called_once()


def test_paradedb_verify_all_indexes_command_parser_and_handle() -> None:
    parser = argparse.ArgumentParser()
    command = cmd_verify_all.Command()
    command.add_arguments(parser)
    parsed = parser.parse_args(
        [
            "--schema-pattern",
            "public",
            "--index-pattern",
            "%bm25%",
            "--heapallindexed",
            "--sample-rate",
            "0.1",
            "--report-progress",
            "--on-error-stop",
            "--database",
            "search",
        ]
    )
    assert parsed.schema_pattern == "public"
    assert parsed.index_pattern == "%bm25%"
    assert parsed.database == "search"

    with (
        patch.object(cmd_verify_all, "validate_sample_rate") as validate,
        patch.object(
            cmd_verify_all, "paradedb_verify_all_indexes", return_value=[{}]
        ) as helper,
        patch.object(cmd_verify_all, "write_json") as write,
    ):
        command.handle(
            schema_pattern="public",
            index_pattern="%bm25%",
            heapallindexed=True,
            sample_rate=0.1,
            report_progress=True,
            on_error_stop=True,
            database="search",
        )
    validate.assert_called_once_with(0.1)
    helper.assert_called_once_with(
        schema_pattern="public",
        index_pattern="%bm25%",
        heapallindexed=True,
        sample_rate=0.1,
        report_progress=True,
        on_error_stop=True,
        using="search",
    )
    write.assert_called_once()


def test_paradedb_indexes_helper_returns_mock_items_index() -> None:
    rows = [row | {"indexrelid": 0} for row in paradedb_indexes()]
    assert (
        json.dumps(rows, indent=2, sort_keys=True, default=str)
        == """[
  {
    "indexname": "mock_items_bm25_idx",
    "indexrelid": 0,
    "num_segments": 1,
    "schemaname": "public",
    "tablename": "mock_items",
    "total_docs": 41
  }
]"""
    )


def test_paradedb_indexes_all_arguments() -> None:
    rows = [row | {"indexrelid": 0} for row in paradedb_indexes(using="default")]
    assert (
        json.dumps(rows, indent=2, sort_keys=True, default=str)
        == """[
  {
    "indexname": "mock_items_bm25_idx",
    "indexrelid": 0,
    "num_segments": 1,
    "schemaname": "public",
    "tablename": "mock_items",
    "total_docs": 41
  }
]"""
    )


def test_paradedb_index_segments_helper_returns_segments() -> None:
    rows = [
        row | {"segment_id": "<segment_id>"}
        for row in paradedb_index_segments("mock_items_bm25_idx")
    ]
    assert (
        json.dumps(rows, indent=2, sort_keys=True, default=str)
        == """[
  {
    "max_doc": 41,
    "num_deleted": 0,
    "num_docs": 41,
    "partition_name": "mock_items_bm25_idx",
    "segment_id": "<segment_id>",
    "segment_idx": 0
  }
]"""
    )


def test_paradedb_index_segments_all_arguments() -> None:
    rows = [
        row | {"segment_id": "<segment_id>"}
        for row in paradedb_index_segments("mock_items_bm25_idx", using="default")
    ]
    assert (
        json.dumps(rows, indent=2, sort_keys=True, default=str)
        == """[
  {
    "max_doc": 41,
    "num_deleted": 0,
    "num_docs": 41,
    "partition_name": "mock_items_bm25_idx",
    "segment_id": "<segment_id>",
    "segment_idx": 0
  }
]"""
    )


def test_paradedb_verify_index_helper_returns_checks() -> None:
    rows = paradedb_verify_index("mock_items_bm25_idx")
    assert (
        json.dumps(rows, indent=2, sort_keys=True, default=str)
        == """[
  {
    "check_name": "mock_items_bm25_idx: schema_valid",
    "details": "Index schema loaded successfully",
    "passed": true
  },
  {
    "check_name": "mock_items_bm25_idx: index_readable",
    "details": "Index reader opened successfully",
    "passed": true
  },
  {
    "check_name": "mock_items_bm25_idx: checksums_valid",
    "details": "All segment checksums validated successfully",
    "passed": true
  },
  {
    "check_name": "mock_items_bm25_idx: segment_metadata_valid",
    "details": "1 segments validated successfully",
    "passed": true
  }
]"""
    )


def test_paradedb_verify_index_all_arguments() -> None:
    rows = paradedb_verify_index(
        "mock_items_bm25_idx",
        heapallindexed=True,
        sample_rate=0.7,
        report_progress=True,
        verbose=True,
        on_error_stop=True,
        segment_ids=[0],
        using="default",
    )
    assert (
        json.dumps(rows, indent=2, sort_keys=True, default=str)
        == """[
  {
    "check_name": "mock_items_bm25_idx: schema_valid",
    "details": "Index schema loaded successfully",
    "passed": true
  },
  {
    "check_name": "mock_items_bm25_idx: index_readable",
    "details": "Index reader opened successfully",
    "passed": true
  },
  {
    "check_name": "mock_items_bm25_idx: checksums_valid",
    "details": "All segment checksums validated successfully",
    "passed": true
  },
  {
    "check_name": "mock_items_bm25_idx: segment_metadata_valid",
    "details": "1 of 1 segments validated successfully",
    "passed": true
  },
  {
    "check_name": "mock_items_bm25_idx: ctid_field_valid",
    "details": "All 29 documents have valid ctid (sampled 29 of 29 docs)",
    "passed": true
  },
  {
    "check_name": "mock_items_bm25_idx: heap_references_valid",
    "details": "All 29 indexed ctids exist in heap (sampled 29 of 29 docs)",
    "passed": true
  }
]"""
    )


def test_paradedb_verify_all_indexes_basic() -> None:
    rows = paradedb_verify_all_indexes()
    assert (
        json.dumps(rows, indent=2, sort_keys=True, default=str)
        == """[
  {
    "check_name": "mock_items_bm25_idx: schema_valid",
    "details": "Index schema loaded successfully",
    "indexname": "mock_items_bm25_idx",
    "passed": true,
    "schemaname": "public"
  },
  {
    "check_name": "mock_items_bm25_idx: index_readable",
    "details": "Index reader opened successfully",
    "indexname": "mock_items_bm25_idx",
    "passed": true,
    "schemaname": "public"
  },
  {
    "check_name": "mock_items_bm25_idx: checksums_valid",
    "details": "All segment checksums validated successfully",
    "indexname": "mock_items_bm25_idx",
    "passed": true,
    "schemaname": "public"
  },
  {
    "check_name": "mock_items_bm25_idx: segment_metadata_valid",
    "details": "1 segments validated successfully",
    "indexname": "mock_items_bm25_idx",
    "passed": true,
    "schemaname": "public"
  }
]"""
    )


def test_paradedb_verify_all_indexes_all_arguments() -> None:
    rows = paradedb_verify_all_indexes(
        index_pattern="mock_items_bm25_idx",
        schema_pattern="public",
        heapallindexed=True,
        sample_rate=0.7,
        report_progress=True,
        on_error_stop=True,
    )
    assert (
        json.dumps(rows, indent=2, sort_keys=True, default=str)
        == """[
  {
    "check_name": "mock_items_bm25_idx: schema_valid",
    "details": "Index schema loaded successfully",
    "indexname": "mock_items_bm25_idx",
    "passed": true,
    "schemaname": "public"
  },
  {
    "check_name": "mock_items_bm25_idx: index_readable",
    "details": "Index reader opened successfully",
    "indexname": "mock_items_bm25_idx",
    "passed": true,
    "schemaname": "public"
  },
  {
    "check_name": "mock_items_bm25_idx: checksums_valid",
    "details": "All segment checksums validated successfully",
    "indexname": "mock_items_bm25_idx",
    "passed": true,
    "schemaname": "public"
  },
  {
    "check_name": "mock_items_bm25_idx: segment_metadata_valid",
    "details": "1 segments validated successfully",
    "indexname": "mock_items_bm25_idx",
    "passed": true,
    "schemaname": "public"
  },
  {
    "check_name": "mock_items_bm25_idx: ctid_field_valid",
    "details": "All 29 documents have valid ctid (sampled 29 of 29 docs)",
    "indexname": "mock_items_bm25_idx",
    "passed": true,
    "schemaname": "public"
  },
  {
    "check_name": "mock_items_bm25_idx: heap_references_valid",
    "details": "All 29 indexed ctids exist in heap (sampled 29 of 29 docs)",
    "indexname": "mock_items_bm25_idx",
    "passed": true,
    "schemaname": "public"
  }
]"""
    )


def test_paradedb_indexes_command() -> None:
    stdout = StringIO()
    call_command("paradedb_indexes", stdout=stdout)
    payload = json.loads(stdout.getvalue())
    assert any(row["indexname"] == "mock_items_bm25_idx" for row in payload)


def test_paradedb_index_segments_command() -> None:
    stdout = StringIO()
    call_command("paradedb_index_segments", "mock_items_bm25_idx", stdout=stdout)
    payload = json.loads(stdout.getvalue())
    assert payload


def test_paradedb_verify_index_command() -> None:
    stdout = StringIO()
    call_command(
        "paradedb_verify_index",
        "mock_items_bm25_idx",
        sample_rate=0.1,
        stdout=stdout,
    )
    payload = json.loads(stdout.getvalue())
    assert payload
    assert "check_name" in payload[0]


def test_paradedb_verify_all_indexes_command() -> None:
    stdout = StringIO()
    call_command(
        "paradedb_verify_all_indexes",
        index_pattern="mock_items_bm25_idx",
        stdout=stdout,
    )
    payload = json.loads(stdout.getvalue())
    assert payload
    assert "check_name" in payload[0]
