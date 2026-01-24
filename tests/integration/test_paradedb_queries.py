"""Integration coverage for ParadeDB query operators and annotations."""

from __future__ import annotations

import pytest
from tests.models import MockItem

from paradedb.functions import Snippet
from paradedb.search import (
    PQ,
    Fuzzy,
    MoreLikeThis,
    ParadeDB,
    Parse,
    Phrase,
    Regex,
    Term,
)

pytestmark = [
    pytest.mark.integration,
    pytest.mark.django_db,
    pytest.mark.usefixtures("mock_items"),
]


def _ids(queryset) -> set[int]:
    return set(queryset.values_list("id", flat=True))


def test_pq_or_semantics() -> None:
    ids = _ids(
        MockItem.objects.filter(description=ParadeDB(PQ("running") | PQ("wireless")))
    )
    assert {3, 12}.issubset(ids)
    assert len(ids) >= 2


def test_pq_and_semantics() -> None:
    ids = _ids(
        MockItem.objects.filter(description=ParadeDB(PQ("running") & PQ("shoes")))
    )
    assert ids == {3}


def test_multi_term_and() -> None:
    ids = _ids(MockItem.objects.filter(description=ParadeDB("running", "shoes")))
    assert ids == {3}


def test_phrase_with_slop() -> None:
    ids = _ids(
        MockItem.objects.filter(description=ParadeDB(Phrase("running shoes", slop=1)))
    )
    assert 3 in ids


def test_fuzzy_distance() -> None:
    ids = _ids(
        MockItem.objects.filter(description=ParadeDB(Fuzzy("runnning", distance=1)))
    )
    assert 3 in ids


def test_regex_query() -> None:
    ids = _ids(MockItem.objects.filter(description=ParadeDB(Regex(".*running.*"))))
    assert ids == {3}


def test_parse_lenient() -> None:
    ids = _ids(
        MockItem.objects.filter(
            description=ParadeDB(Parse("running AND shoes", lenient=True))
        )
    )
    assert ids == {3}


def test_term_query() -> None:
    ids = _ids(MockItem.objects.filter(description=ParadeDB(Term("shoes"))))
    assert {3, 4, 5}.issubset(ids)


def test_snippet_rendering() -> None:
    snippet = (
        MockItem.objects.filter(description=ParadeDB("running shoes"))
        .annotate(snippet=Snippet("description"))
        .order_by("id")
        .values_list("snippet", flat=True)
        .first()
    )
    assert snippet is not None
    assert "<b>running</b>" in snippet and "<b>shoes</b>" in snippet


def test_more_like_this_by_id() -> None:
    ids = _ids(MockItem.objects.filter(MoreLikeThis(product_id=3)).order_by("id"))
    assert ids == {3, 4, 5}


def test_more_like_this_multiple_ids() -> None:
    ids = _ids(
        MockItem.objects.filter(MoreLikeThis(product_ids=[3, 12])).order_by("id")
    )
    assert {3, 12}.issubset(ids)


def test_more_like_this_by_text() -> None:
    ids = _ids(
        MockItem.objects.filter(
            MoreLikeThis(text='{"description": "wireless earbuds"}')
        )
    )
    assert 12 in ids
    assert len(ids) >= 1
