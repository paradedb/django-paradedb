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


def test_exact_literal_disjunction_single() -> None:
    ids = _ids(
        MockItem.objects.filter(description=ParadeDB("running shoes", operator="OR"))
    )
    assert 3 in ids
    assert {3, 4, 5}.issubset(ids)


def test_exact_literal_disjunction_multi() -> None:
    ids = _ids(
        MockItem.objects.filter(
            description=ParadeDB("running", "wireless", operator="OR")
        )
    )
    assert ids == {3, 12}


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


def test_fuzzy_conjunction() -> None:
    ids = _ids(
        MockItem.objects.filter(
            description=ParadeDB(Fuzzy("runnning shose", distance=2, operator="AND"))
        )
    )
    assert 3 in ids


def test_fuzzy_term_form() -> None:
    ids = _ids(
        MockItem.objects.filter(
            description=ParadeDB(Fuzzy("shose", distance=2, operator="TERM"))
        )
    )
    assert len(ids) > 0


def test_fuzzy_prefix() -> None:
    ids = _ids(
        MockItem.objects.filter(
            description=ParadeDB(
                Fuzzy("runn", distance=0, prefix=True, operator="TERM")
            )
        )
    )
    assert 3 in ids


def test_fuzzy_transposition_cost_one() -> None:
    ids = _ids(
        MockItem.objects.filter(
            description=ParadeDB(
                Fuzzy(
                    "shose",
                    distance=1,
                    transposition_cost_one=True,
                    operator="TERM",
                )
            )
        )
    )
    assert len(ids) > 0


def test_tokenizer_override_match() -> None:
    ids = _ids(
        MockItem.objects.filter(
            description=ParadeDB("running shoes", tokenizer="whitespace")
        )
    )
    assert 3 in ids


def test_tokenizer_override_phrase() -> None:
    ids = _ids(
        MockItem.objects.filter(
            description=ParadeDB(Phrase("running shoes", tokenizer="whitespace"))
        )
    )
    assert 3 in ids


def test_boost_does_not_change_result_set() -> None:
    baseline_ids = _ids(MockItem.objects.filter(description=ParadeDB("shoes")))
    boosted_ids = _ids(
        MockItem.objects.filter(description=ParadeDB("shoes", boost=2.0))
    )
    assert boosted_ids == baseline_ids


def test_boost_with_fuzzy_integration() -> None:
    ids = _ids(
        MockItem.objects.filter(
            description=ParadeDB(Fuzzy("runnning", distance=1, boost=2.0))
        )
    )
    assert ids == {3}


def test_const_does_not_change_result_set() -> None:
    baseline_ids = _ids(MockItem.objects.filter(description=ParadeDB("shoes")))
    const_ids = _ids(MockItem.objects.filter(description=ParadeDB("shoes", const=1.0)))
    assert const_ids == baseline_ids


def test_const_with_fuzzy_rejected() -> None:
    with pytest.raises(
        ValueError, match="Fuzzy queries do not support constant scoring"
    ):
        Fuzzy("runnning", distance=1, const=1.0)


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


def test_more_like_this_by_document() -> None:
    """MLT with document finds similar documents."""
    ids = _ids(
        MockItem.objects.filter(
            MoreLikeThis(document={"description": "wireless earbuds"})
        )
    )
    # Should find documents similar to the text
    assert 12 in ids  # "Innovative wireless earbuds"


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
    assert with_stopword_ids != baseline_ids, (
        "Stopwords should change the results - excluding 'shoes' should remove shoe-related matches"
    )

    # Typically, stopwords should reduce the number of matches
    # (unless all matches are from other terms like "running")
    assert len(with_stopword_ids) <= len(baseline_ids), (
        "Stopwords should not increase match count"
    )


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
    assert with_min_length_ids != baseline_ids, (
        "min_word_length=6 should filter out 'shoes' (5 chars) and change results"
    )


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
    assert len(ids_with) <= len(ids_without), (
        "Stopwords should not increase match count"
    )

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


def test_more_like_this_document_input_generates_correct_sql() -> None:
    """MLT with document should generate pdb.more_like_this(%s) with JSON param."""
    # This test validates that document input uses parameterized JSON format:
    # pdb.more_like_this(%s) with params containing the JSON string

    # Create a simple query with a document
    query = MockItem.objects.filter(
        MoreLikeThis(document={"description": "wireless earbuds"})
    )

    sql, params = query.query.sql_with_params()

    # With parameterized SQL, the SQL should contain placeholder
    assert "pdb.more_like_this(%s)" in sql, (
        f"Expected parameterized format: pdb.more_like_this(%s)\nGot SQL: {sql}"
    )

    # Check that the parameter contains the JSON string
    assert len(params) > 0, "Expected at least one parameter"
    # Find the JSON parameter (it should be a string containing the JSON)
    json_params = [p for p in params if isinstance(p, str) and "description" in p]
    assert len(json_params) > 0, f"Expected JSON parameter in {params}"
    assert '"wireless earbuds"' in json_params[0], (
        f"Expected JSON content in {json_params[0]}"
    )

    # Ensure it does NOT contain the array form
    assert "ARRAY[" not in sql or "description" not in sql, (
        f"Should not use array form for document input\nGot SQL: {sql}"
    )


def test_more_like_this_empty_stopwords_generates_correct_sql() -> None:
    """MLT with empty stopwords array should omit the option."""
    # This test validates that empty stopwords don't generate stopwords option
    # Expected: stopwords option omitted entirely

    query = MockItem.objects.filter(
        MoreLikeThis(
            product_id=3,
            fields=["description"],
            stopwords=[],  # Empty array
        )
    )

    sql, _params = query.query.sql_with_params()

    # Check that stopwords is not present at all
    assert "stopwords" not in sql, (
        f"Empty stopwords should be omitted entirely\nGot SQL: {sql}"
    )


def test_more_like_this_with_key_field() -> None:
    """MLT with custom key_field uses that column in SQL."""
    query = MockItem.objects.filter(
        MoreLikeThis(
            product_id=3,
            fields=["description"],
            key_field="id",
        )
    )

    sql, _ = query.query.sql_with_params()

    # Should use the specified key_field column
    assert '"mock_items"."id"' in sql, f"Expected key_field column in SQL: {sql}"
    assert "pdb.more_like_this" in sql
    # Verify the query executes without error
    ids = _ids(query)
    assert 3 in ids


def test_more_like_this_document_as_json_string() -> None:
    """MLT with document as pre-serialized JSON string works correctly."""
    import json

    # Pre-serialize the JSON
    json_doc = json.dumps({"description": "wireless earbuds"})

    query = MockItem.objects.filter(MoreLikeThis(document=json_doc))

    sql, params = query.query.sql_with_params()

    # Should use parameterized JSON
    assert "pdb.more_like_this(%s)" in sql, f"Expected parameterized SQL: {sql}"

    # Verify the JSON string is in params
    json_params = [p for p in params if isinstance(p, str) and "wireless" in p]
    assert len(json_params) > 0, f"Expected JSON in params: {params}"

    # Verify it executes and finds similar items
    ids = _ids(query)
    assert 12 in ids  # "Innovative wireless earbuds"


def test_more_like_this_word_length_min_greater_than_max() -> None:
    """MLT accepts min_word_length > max_word_length (ParadeDB handles validation)."""
    # ParadeDB may handle this at runtime, but Django should accept it
    # This test documents the current behavior
    mlt = MoreLikeThis(
        product_id=3,
        fields=["description"],
        min_word_length=10,
        max_word_length=5,
    )

    # Should create the MLT object without error
    assert mlt.min_word_length == 10
    assert mlt.max_word_length == 5

    # Generate SQL to verify it compiles
    query = MockItem.objects.filter(mlt)
    sql, _params = query.query.sql_with_params()

    assert "min_word_length => 10" in sql
    assert "max_word_length => 5" in sql
