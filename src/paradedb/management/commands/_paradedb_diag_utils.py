"""Shared helpers for ParadeDB diagnostic management commands."""

from __future__ import annotations

import json
from typing import Any

from django.core.management.base import CommandError


def write_json(stdout: Any, payload: object) -> None:
    stdout.write(json.dumps(payload, indent=2, default=str))


def validate_sample_rate(sample_rate: float | None) -> None:
    if sample_rate is None:
        return
    if sample_rate < 0 or sample_rate > 1:
        raise CommandError("--sample-rate must be between 0.0 and 1.0")
