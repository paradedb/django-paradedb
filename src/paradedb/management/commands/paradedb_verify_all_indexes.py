"""Verify BM25 indexes via ``pdb.verify_all_indexes()``."""

from __future__ import annotations

import argparse
from typing import cast

from django.core.management.base import BaseCommand
from django.db import DEFAULT_DB_ALIAS

from paradedb.functions import paradedb_verify_all_indexes
from paradedb.management.commands._paradedb_diag_utils import (
    validate_sample_rate,
    write_json,
)


class Command(BaseCommand):
    help = "Verify ParadeDB BM25 indexes in the database (pdb.verify_all_indexes)."

    def add_arguments(self, parser: argparse.ArgumentParser) -> None:
        parser.add_argument(
            "--schema-pattern",
            default=None,
            help="SQL LIKE pattern to filter schema names.",
        )
        parser.add_argument(
            "--index-pattern",
            default=None,
            help="SQL LIKE pattern to filter index names.",
        )
        parser.add_argument(
            "--heapallindexed",
            action="store_true",
            help="Check that all indexed ctids exist in the heap.",
        )
        parser.add_argument(
            "--sample-rate",
            type=float,
            default=None,
            help="Fraction of documents to check (0.0-1.0).",
        )
        parser.add_argument(
            "--report-progress",
            action="store_true",
            help="Emit progress messages while verification runs.",
        )
        parser.add_argument(
            "--on-error-stop",
            action="store_true",
            help="Stop on the first error found.",
        )
        parser.add_argument(
            "--database",
            default=DEFAULT_DB_ALIAS,
            help="Database connection alias to use.",
        )

    def handle(self, *_args: object, **options: object) -> None:
        sample_rate = cast(float | None, options["sample_rate"])
        if sample_rate is not None:
            validate_sample_rate(sample_rate)

        rows = paradedb_verify_all_indexes(
            schema_pattern=(
                str(options["schema_pattern"])
                if options["schema_pattern"] is not None
                else None
            ),
            index_pattern=(
                str(options["index_pattern"])
                if options["index_pattern"] is not None
                else None
            ),
            heapallindexed=bool(options["heapallindexed"]),
            sample_rate=sample_rate,
            report_progress=bool(options["report_progress"]),
            on_error_stop=bool(options["on_error_stop"]),
            using=str(options["database"]),
        )
        write_json(self.stdout, rows)
