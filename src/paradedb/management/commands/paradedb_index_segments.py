"""Inspect BM25 index segments via ``pdb.index_segments()``."""

from __future__ import annotations

import argparse

from django.core.management.base import BaseCommand
from django.db import DEFAULT_DB_ALIAS

from paradedb.functions import paradedb_index_segments
from paradedb.management.commands._paradedb_diag_utils import write_json


class Command(BaseCommand):
    help = "List segments for a ParadeDB BM25 index (pdb.index_segments)."

    def add_arguments(self, parser: argparse.ArgumentParser) -> None:
        parser.add_argument("index", help="Index name (optionally schema-qualified).")
        parser.add_argument(
            "--database",
            default=DEFAULT_DB_ALIAS,
            help="Database connection alias to use.",
        )

    def handle(self, *_args: object, **options: object) -> None:
        rows = paradedb_index_segments(
            index=str(options["index"]),
            using=str(options["database"]),
        )
        write_json(self.stdout, rows)
