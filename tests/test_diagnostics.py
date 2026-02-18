"""Unit tests for diagnostic command edge-case validation."""

from __future__ import annotations

import pytest
from django.core.management.base import CommandError

from paradedb.management.commands._paradedb_diag_utils import validate_sample_rate


@pytest.mark.parametrize("sample_rate", [-0.1, 1.1])
def test_validate_sample_rate_rejects_out_of_bounds(sample_rate: float) -> None:
    with pytest.raises(CommandError, match="sample-rate"):
        validate_sample_rate(sample_rate)


@pytest.mark.parametrize("sample_rate", [None, 0.0, 0.25, 1.0])
def test_validate_sample_rate_accepts_valid_values(sample_rate: float | None) -> None:
    validate_sample_rate(sample_rate)
