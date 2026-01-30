"""Integration coverage for ParadeDB query operators and annotations."""

from __future__ import annotations

import pytest
from django.db import connection
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
    assert ids == {3, 12}


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
    assert ids == {3}


def test_fuzzy_distance() -> None:
    ids = _ids(
        MockItem.objects.filter(description=ParadeDB(Fuzzy("runnning", distance=1)))
    )
    assert ids == {3}


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
    assert ids == {3, 4, 5}


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
    """MLT by ID with fields=['description'] returns similar items."""
    ids = _ids(
        MockItem.objects.filter(
            MoreLikeThis(product_id=3, fields=["description"])
        ).order_by("id")
    )
    assert ids == {3, 4, 5}


def test_more_like_this_multiple_ids() -> None:
    """MLT with multiple IDs and fields=['description'] returns union."""
    ids = _ids(
        MockItem.objects.filter(
            MoreLikeThis(product_ids=[3, 12], fields=["description"])
        ).order_by("id")
    )
    assert ids == {3, 4, 5, 12}


def test_more_like_this_by_text_with_fields() -> None:
    """MLT with text + fields auto-converts to JSON document."""
    ids = _ids(
        MockItem.objects.filter(
            MoreLikeThis(text="comfortable running shoes", fields=["description"])
        )
    )
    # Should find documents similar to the text in the description field
    assert 3 in ids  # "Sleek running shoes"


def test_more_like_this_by_document() -> None:
    """MLT with document dict generates correct JSON."""
    ids = _ids(
        MockItem.objects.filter(
            MoreLikeThis(document={"description": "wireless earbuds"})
        )
    )
    assert ids == {12}


def test_more_like_this_with_stopwords() -> None:
    """MLT with stopwords excludes terms from matching - verified by comparing results."""
    # Get baseline results without stopwords
    baseline_ids = _ids(
        MockItem.objects.filter(
            MoreLikeThis(
                product_id=3,  # "Sleek running shoes"
                fields=["description"],
            )
        )
    )

    # Get results with "shoes" as stopword
    with_stopword_ids = _ids(
        MockItem.objects.filter(
            MoreLikeThis(
                product_id=3,
                fields=["description"],
                stopwords=["shoes"],
            )
        )
    )

    # Source item always included in both
    assert 3 in baseline_ids
    assert 3 in with_stopword_ids

    # With "shoes" as stopword, results should be different
    # (fewer matches since "shoes" term is excluded from matching)
    assert (
        with_stopword_ids != baseline_ids
    ), "Stopwords should change the results - excluding 'shoes' should remove shoe-related matches"

    # Typically, stopwords should reduce the number of matches
    # (unless all matches are from other terms like "running")
    assert len(with_stopword_ids) <= len(
        baseline_ids
    ), "Stopwords should not increase match count"


def test_more_like_this_with_word_length() -> None:
    """MLT with min/max word length filters terms - verified by comparing results."""
    # Get baseline without word length filter
    baseline_ids = _ids(
        MockItem.objects.filter(
            MoreLikeThis(
                product_id=3,  # "Sleek running shoes"
                fields=["description"],
            )
        )
    )

    # Get results with min_word_length=6 (filters out "shoes" which is 5 chars)
    with_min_length_ids = _ids(
        MockItem.objects.filter(
            MoreLikeThis(
                product_id=3,
                fields=["description"],
                min_word_length=6,
            )
        )
    )

    # Source item always included
    assert 3 in baseline_ids
    assert 3 in with_min_length_ids

    # Results should differ when short words are filtered
    assert (
        with_min_length_ids != baseline_ids
    ), "min_word_length=6 should filter out 'shoes' (5 chars) and change results"


def test_more_like_this_stopwords_reversible() -> None:
    """Verify stopwords effect is consistent and reversible."""
    # First query with stopwords
    ids_with = _ids(
        MockItem.objects.filter(
            MoreLikeThis(
                product_id=3,
                fields=["description"],
                stopwords=["shoes"],  # The main matching term
            )
        )
    )

    # Second query without stopwords (baseline)
    ids_without = _ids(
        MockItem.objects.filter(
            MoreLikeThis(
                product_id=3,
                fields=["description"],
            )
        )
    )

    # Third query with same stopwords again - should be consistent
    ids_with_again = _ids(
        MockItem.objects.filter(
            MoreLikeThis(
                product_id=3,
                fields=["description"],
                stopwords=["shoes"],
            )
        )
    )

    # Stopwords should produce consistent results (deterministic)
    assert ids_with == ids_with_again, "Same stopwords should produce same results"

    # With "shoes" as stopword, we should have fewer or equal matches
    # (excluding the main matching term reduces similarity matches)
    assert len(ids_with) <= len(
        ids_without
    ), "Stopwords should not increase match count"

    # Source item always included regardless of stopwords
    assert 3 in ids_with
    assert 3 in ids_without


def test_metadata_color_literal_search() -> None:
    """Search over JSON color subfield should return the silver items."""
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT id FROM mock_items WHERE (metadata->>'color') &&& 'Silver' ORDER BY id;"
        )
        rows = cursor.fetchall()
    ids = {row[0] for row in rows}
    assert ids == {1, 9}


def test_metadata_location_standard_filter() -> None:
    """Filter by JSON location subfield using standard SQL."""
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT id FROM mock_items WHERE (metadata->>'location') = 'Canada' ORDER BY id;"
        )
        rows = cursor.fetchall()
    ids = {row[0] for row in rows}
    assert ids == {2, 5, 8, 11, 14, 17, 20, 23, 26, 29, 32, 35, 38, 41}
