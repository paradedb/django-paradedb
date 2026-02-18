"""List BM25 indexes via ``pdb.indexes()``."""

from __future__ import annotations

import argparse

from django.core.management.base import BaseCommand
from django.db import DEFAULT_DB_ALIAS

from paradedb.functions import paradedb_indexes
from paradedb.management.commands._paradedb_diag_utils import write_json


class Command(BaseCommand):
    help = "List ParadeDB BM25 indexes (pdb.indexes)."

    def add_arguments(self, parser: argparse.ArgumentParser) -> None:
        parser.add_argument(
            "--database",
            default=DEFAULT_DB_ALIAS,
            help="Database connection alias to use.",
        )

    def handle(self, *_args: object, **options: object) -> None:
        rows = paradedb_indexes(using=str(options["database"]))
        write_json(self.stdout, rows)
