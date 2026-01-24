"""Smoke tests against a real ParadeDB instance."""

from __future__ import annotations

import pytest
from tests.models import MockItem

from paradedb.functions import Score, Snippet
from paradedb.search import ParadeDB

pytestmark = [
    pytest.mark.integration,
    pytest.mark.django_db,
    pytest.mark.usefixtures("mock_items"),
]


def test_mock_items_seeded() -> None:
    """Ensure the ParadeDB helper seeded mock_items with data."""
    assert MockItem.objects.count() > 0


def test_paradedb_lookup_returns_rows() -> None:
    """Basic ParadeDB search should return at least one result."""
    assert MockItem.objects.filter(description=ParadeDB("shoes")).exists()


def test_paradedb_score_annotation() -> None:
    """Score annotation executes against ParadeDB."""
    queryset = (
        MockItem.objects.filter(description=ParadeDB("shoes"))
        .annotate(search_score=Score())
        .order_by("-search_score")
    )
    assert queryset.count() > 0


def test_paradedb_score_filter() -> None:
    """Score annotations can be filtered."""
    assert (
        MockItem.objects.filter(description=ParadeDB("shoes"))
        .annotate(search_score=Score())
        .filter(search_score__gt=0)
        .exists()
    )


def test_snippet_custom_formatting() -> None:
    """Snippet custom formatting works against ParadeDB."""
    snippet = (
        MockItem.objects.filter(description=ParadeDB("running shoes"))
        .annotate(
            snippet=Snippet(
                "description",
                start_sel="<mark>",
                stop_sel="</mark>",
                max_num_chars=100,
            )
        )
        .values_list("snippet", flat=True)
        .first()
    )
    assert snippet is not None
    assert "<mark>" in snippet and "</mark>" in snippet
