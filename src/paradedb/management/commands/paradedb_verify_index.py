"""Verify one BM25 index via ``pdb.verify_index()``."""

from __future__ import annotations

import argparse
from typing import cast

from django.core.management.base import BaseCommand
from django.db import DEFAULT_DB_ALIAS

from paradedb.functions import paradedb_verify_index
from paradedb.management.commands._paradedb_diag_utils import (
    validate_sample_rate,
    write_json,
)


class Command(BaseCommand):
    help = "Verify one ParadeDB BM25 index (pdb.verify_index)."

    def add_arguments(self, parser: argparse.ArgumentParser) -> None:
        parser.add_argument("index", help="Index name (optionally schema-qualified).")
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
            "--verbose",
            action="store_true",
            help="Emit detailed segment-level progress and resume hints.",
        )
        parser.add_argument(
            "--on-error-stop",
            action="store_true",
            help="Stop on the first error found.",
        )
        parser.add_argument(
            "--segment-id",
            dest="segment_ids",
            type=int,
            action="append",
            default=None,
            help="Segment index to verify; pass multiple times to target more than one.",
        )
        parser.add_argument(
            "--database",
            default=DEFAULT_DB_ALIAS,
            help="Database connection alias to use.",
        )

    def handle(self, *_args: object, **options: object) -> None:
        sample_rate = cast(float | None, options["sample_rate"])
        segment_ids = cast(list[int] | None, options["segment_ids"])
        if sample_rate is not None:
            validate_sample_rate(sample_rate)

        rows = paradedb_verify_index(
            index=str(options["index"]),
            heapallindexed=bool(options["heapallindexed"]),
            sample_rate=sample_rate,
            report_progress=bool(options["report_progress"]),
            verbose=bool(options["verbose"]),
            on_error_stop=bool(options["on_error_stop"]),
            segment_ids=segment_ids,
            using=str(options["database"]),
        )
        write_json(self.stdout, rows)
