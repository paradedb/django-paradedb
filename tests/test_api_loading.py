"""Unit tests for api.json loading and validation helpers."""

from __future__ import annotations

import pytest

from paradedb.api import _validate_api_payload


def test_validate_api_payload_accepts_expected_shape() -> None:
    payload = {
        "operators": {"OP_SEARCH": "@@@"},
        "functions": {"FN_ALL": "pdb.all"},
        "types": {"PDB_TYPE_QUERY": "pdb.query"},
    }
    validated = _validate_api_payload(payload)
    assert validated == payload


def test_validate_api_payload_rejects_missing_sections() -> None:
    payload = {
        "operators": {"OP_SEARCH": "@@@"},
        "functions": {"FN_ALL": "pdb.all"},
    }
    with pytest.raises(ValueError, match="section 'types' must be an object"):
        _validate_api_payload(payload)


def test_validate_api_payload_rejects_non_string_values() -> None:
    payload = {
        "operators": {"OP_SEARCH": "@@@"},
        "functions": {"FN_ALL": 1},
        "types": {"PDB_TYPE_QUERY": "pdb.query"},
    }
    with pytest.raises(ValueError, match="must map to a string"):
        _validate_api_payload(payload)
