"""Unit tests for diagnostic command edge-case validation."""

from __future__ import annotations

import argparse
import json
from datetime import date
from io import StringIO
from unittest.mock import patch

import pytest
from django.core.management.base import CommandError

from paradedb import functions as paradedb_functions
from paradedb.functions import (
    Agg,
    _execute_table_function,
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
    write_json,
)


@pytest.mark.parametrize("sample_rate", [-0.1, 1.1])
def test_validate_sample_rate_rejects_out_of_bounds(sample_rate: float) -> None:
    with pytest.raises(CommandError, match="sample-rate"):
        validate_sample_rate(sample_rate)


@pytest.mark.parametrize("sample_rate", [None, 0.0, 0.25, 1.0])
def test_validate_sample_rate_accepts_valid_values(sample_rate: float | None) -> None:
    validate_sample_rate(sample_rate)


def test_write_json_uses_default_str() -> None:
    stdout = StringIO()
    write_json(stdout, {"when": date(2026, 2, 23)})
    payload = json.loads(stdout.getvalue())
    assert payload == {"when": "2026-02-23"}


class _FakeCursor:
    def __init__(
        self,
        *,
        description: list[tuple[str]] | None,
        rows: list[tuple[object, ...]] | None = None,
    ) -> None:
        self.description = description
        self._rows = rows or []
        self.executed: list[tuple[str, tuple[object, ...]]] = []

    def execute(self, sql: str, params: object) -> None:
        self.executed.append((sql, tuple(params)))

    def fetchall(self) -> list[tuple[object, ...]]:
        return self._rows

    def __enter__(self) -> _FakeCursor:
        return self

    def __exit__(self, *_exc: object) -> None:
        return None


class _FakeConnection:
    def __init__(self, cursor: _FakeCursor) -> None:
        self._cursor = cursor

    def cursor(self) -> _FakeCursor:
        return self._cursor


def test_execute_table_function_returns_empty_when_no_result_set() -> None:
    cursor = _FakeCursor(description=None)
    with patch.object(
        paradedb_functions, "connections", {"default": _FakeConnection(cursor)}
    ):
        rows = _execute_table_function("SELECT 1", (), using="default")
    assert rows == []
    assert cursor.executed == [("SELECT 1", ())]


def test_execute_table_function_maps_rows_to_dicts() -> None:
    cursor = _FakeCursor(
        description=[("indexname",), ("schema",)],
        rows=[("mock_items_bm25_idx", "public")],
    )
    with patch.object(
        paradedb_functions, "connections", {"search": _FakeConnection(cursor)}
    ):
        rows = _execute_table_function(
            "SELECT * FROM pdb.indexes()", (), using="search"
        )
    assert rows == [{"indexname": "mock_items_bm25_idx", "schema": "public"}]
    assert cursor.executed == [("SELECT * FROM pdb.indexes()", ())]


def test_agg_exact_type_validation() -> None:
    with pytest.raises(TypeError, match="Agg exact must be a boolean"):
        Agg("{}", exact="nope")  # type: ignore[arg-type]


def test_paradedb_indexes_wrapper_calls_execute_table_function() -> None:
    with patch.object(
        paradedb_functions, "_execute_table_function", return_value=[]
    ) as mocked:
        paradedb_indexes(using="search")
    mocked.assert_called_once_with("SELECT * FROM pdb.indexes()", (), using="search")


def test_paradedb_index_segments_wrapper_calls_execute_table_function() -> None:
    with patch.object(
        paradedb_functions, "_execute_table_function", return_value=[]
    ) as mocked:
        paradedb_index_segments("search_idx", using="search")
    mocked.assert_called_once_with(
        "SELECT * FROM pdb.index_segments(%s::regclass)",
        ("search_idx",),
        using="search",
    )


def test_paradedb_verify_index_minimal_sql() -> None:
    with patch.object(
        paradedb_functions, "_execute_table_function", return_value=[]
    ) as mocked:
        paradedb_verify_index("search_idx")
    mocked.assert_called_once_with(
        "SELECT * FROM pdb.verify_index(%s::regclass)",
        ["search_idx"],
        using="default",
    )


def test_paradedb_verify_index_all_options_sql() -> None:
    with patch.object(
        paradedb_functions, "_execute_table_function", return_value=[]
    ) as mocked:
        paradedb_verify_index(
            "search_idx",
            heapallindexed=True,
            sample_rate=0.25,
            report_progress=True,
            verbose=True,
            on_error_stop=True,
            segment_ids=(1, 3),
            using="search",
        )
    mocked.assert_called_once()
    sql, params = mocked.call_args.args
    kwargs = mocked.call_args.kwargs
    assert sql == (
        "SELECT * FROM pdb.verify_index(%s::regclass"
        ", heapallindexed => %s::boolean"
        ", sample_rate => %s::double precision"
        ", report_progress => %s::boolean"
        ", verbose => %s::boolean"
        ", on_error_stop => %s::boolean"
        ", segment_ids => %s::int[]"
        ")"
    )
    assert params == ["search_idx", True, 0.25, True, True, True, [1, 3]]
    assert kwargs == {"using": "search"}


def test_paradedb_verify_all_indexes_minimal_sql() -> None:
    with patch.object(
        paradedb_functions, "_execute_table_function", return_value=[]
    ) as mocked:
        paradedb_verify_all_indexes()
    mocked.assert_called_once_with(
        "SELECT * FROM pdb.verify_all_indexes()",
        [],
        using="default",
    )


def test_paradedb_verify_all_indexes_all_options_sql() -> None:
    with patch.object(
        paradedb_functions, "_execute_table_function", return_value=[]
    ) as mocked:
        paradedb_verify_all_indexes(
            schema_pattern="public",
            index_pattern="%bm25%",
            heapallindexed=True,
            sample_rate=0.1,
            report_progress=True,
            on_error_stop=True,
            using="search",
        )
    mocked.assert_called_once()
    sql, params = mocked.call_args.args
    kwargs = mocked.call_args.kwargs
    assert sql == (
        "SELECT * FROM pdb.verify_all_indexes("
        "schema_pattern => %s::text, "
        "index_pattern => %s::text, "
        "heapallindexed => %s::boolean, "
        "sample_rate => %s::double precision, "
        "report_progress => %s::boolean, "
        "on_error_stop => %s::boolean"
        ")"
    )
    assert params == ["public", "%bm25%", True, 0.1, True, True]
    assert kwargs == {"using": "search"}


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


def test_paradedb_verify_all_indexes_command_handle_without_sample_rate_skips_validation() -> (
    None
):
    command = cmd_verify_all.Command()
    with (
        patch.object(cmd_verify_all, "validate_sample_rate") as validate,
        patch.object(
            cmd_verify_all, "paradedb_verify_all_indexes", return_value=[{}]
        ) as helper,
        patch.object(cmd_verify_all, "write_json"),
    ):
        command.handle(
            schema_pattern=None,
            index_pattern=None,
            heapallindexed=False,
            sample_rate=None,
            report_progress=False,
            on_error_stop=False,
            database="default",
        )
    validate.assert_not_called()
    helper.assert_called_once_with(
        schema_pattern=None,
        index_pattern=None,
        heapallindexed=False,
        sample_rate=None,
        report_progress=False,
        on_error_stop=False,
        using="default",
    )
