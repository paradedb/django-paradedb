"""Integration coverage for ParadeDB query operators and annotations."""

from __future__ import annotations

import pytest
from django.db import connection

from paradedb.search import (
    Boost,
    Fuzzy,
    MatchAll,
    MatchAny,
    MoreLikeThis,
    ParadeDB,
    Term,
    Tokenized,
    Tokenizer,
)
from tests.models import MockItem

pytestmark = [
    pytest.mark.integration,
    pytest.mark.django_db,
    pytest.mark.usefixtures("mock_items"),
]


def _ids(queryset) -> set[int]:
    return set(queryset.values_list("id", flat=True))


def _raw_ids(sql: str) -> set[int]:
    with connection.cursor() as cursor:
        cursor.execute(sql)
        return {int(row[0]) for row in cursor.fetchall()}


def _where_sql(lhs_sql: str, expr: ParadeDB) -> str:
    sql, _ = expr.as_sql(None, connection, lhs_sql)  # type: ignore[arg-type]
    return sql


def _assert_sql(sql: str, expected: str) -> None:
    assert " ".join(sql.split()) == " ".join(expected.split())


def test_fuzzy_transposition_cost_one() -> None:
    queryset = MockItem.objects.filter(
        description=ParadeDB(Fuzzy(Term("shose"), 1, False, True))
    )
    _assert_sql(
        str(queryset.query),
        """
        SELECT "mock_items"."id", "mock_items"."description", "mock_items"."category", "mock_items"."rating", "mock_items"."in_stock", "mock_items"."created_at", "mock_items"."metadata"
        FROM "mock_items"
        WHERE "mock_items"."description" @@@ pdb.term('shose')::pdb.fuzzy(1, f, t)
        """,
    )
    ids = _ids(queryset)
    assert 3 in ids


def test_boost_multi_term_or_query() -> None:
    # Verifies ARRAY['shoes', 'boots']::pdb.boost(2.0) is valid SQL and executes.
    queryset = MockItem.objects.filter(
        description=ParadeDB(MatchAny(Boost(("shoes", "boots"), 2.0)))
    )
    _assert_sql(
        str(queryset.query),
        """
        SELECT "mock_items"."id", "mock_items"."description", "mock_items"."category", "mock_items"."rating", "mock_items"."in_stock", "mock_items"."created_at", "mock_items"."metadata"
        FROM "mock_items"
        WHERE "mock_items"."description" ||| ARRAY['shoes', 'boots']::pdb.boost(2.0)
        """,
    )
    ids = _ids(queryset)
    assert ids == {3, 4, 5, 13}


def test_paradedb_operators_over_expression_lhs() -> None:
    with connection.cursor() as cursor:
        cursor.execute("DROP TABLE IF EXISTS tmp_expr_ops;")
        cursor.execute(
            "CREATE TABLE tmp_expr_ops (id integer primary key, description text, category text);"
        )
        cursor.execute(
            "INSERT INTO tmp_expr_ops (id, description, category) VALUES "
            "(1, 'sleek running shoes', 'Sportswear'), "
            "(2, 'wireless earbuds', 'Electronics'), "
            "(3, 'formal shoes', 'Fashion');"
        )
        cursor.execute(
            "CREATE INDEX tmp_expr_ops_bm25_idx ON tmp_expr_ops USING bm25 "
            "(id, (((description || ' ' || category)::pdb.simple('alias=combined')))) "
            "WITH (key_field='id');"
        )

    expr_where = _where_sql(
        "(description || ' ' || category)",
        ParadeDB(MatchAll(Tokenized("running Sportswear", Tokenizer.simple()))),
    )
    ids = _raw_ids(f"SELECT id FROM tmp_expr_ops WHERE {expr_where} ORDER BY id;")
    assert ids == {1}

    with connection.cursor() as cursor:
        cursor.execute("DROP TABLE IF EXISTS tmp_expr_ops;")


def test_more_like_this_document_input_generates_correct_sql() -> None:
    """MLT with document should generate pdb.more_like_this(%s) with JSON param."""
    query = MockItem.objects.filter(
        id=ParadeDB(MoreLikeThis(document={"description": "wireless earbuds"}))
    )

    sql, params = query.query.sql_with_params()

    _assert_sql(
        sql,
        """
        SELECT "mock_items"."id", "mock_items"."description", "mock_items"."category", "mock_items"."rating", "mock_items"."in_stock", "mock_items"."created_at", "mock_items"."metadata"
        FROM "mock_items"
        WHERE "mock_items"."id" @@@ pdb.more_like_this(%s)
        """,
    )

    assert len(params) > 0, "Expected at least one parameter"
    json_params = [p for p in params if isinstance(p, str) and "description" in p]
    assert len(json_params) > 0, f"Expected JSON parameter in {params}"
    assert '"wireless earbuds"' in json_params[0], (
        f"Expected JSON content in {json_params[0]}"
    )
    assert "ARRAY[" not in sql or "description" not in sql, (
        f"Should not use array form for document input\nGot SQL: {sql}"
    )


def test_more_like_this_document_as_json_string() -> None:
    """MLT with document as pre-serialized JSON string works correctly."""
    import json

    # Pre-serialize the JSON
    json_doc = json.dumps({"description": "wireless earbuds"})

    query = MockItem.objects.filter(id=ParadeDB(MoreLikeThis(document=json_doc)))

    sql, params = query.query.sql_with_params()

    _assert_sql(
        sql,
        """
        SELECT "mock_items"."id", "mock_items"."description", "mock_items"."category", "mock_items"."rating", "mock_items"."in_stock", "mock_items"."created_at", "mock_items"."metadata"
        FROM "mock_items"
        WHERE "mock_items"."id" @@@ pdb.more_like_this(%s)
        """,
    )

    # Verify the JSON string is in params
    json_params = [p for p in params if isinstance(p, str) and "wireless" in p]
    assert len(json_params) > 0, f"Expected JSON in params: {params}"

    # Verify it executes and finds similar items
    ids = _ids(query)
    assert 12 in ids  # "Innovative wireless earbuds"


def test_multi_term_fuzzy_match_and_prefix() -> None:
    """FUZZY-5: Match with multiple terms, AND operator, distance=1, and prefix."""
    queryset = MockItem.objects.filter(
        description=ParadeDB(MatchAll(Fuzzy(("slee", "rann"), 1, True)))
    )
    _assert_sql(
        str(queryset.query),
        """
        SELECT "mock_items"."id", "mock_items"."description", "mock_items"."category", "mock_items"."rating", "mock_items"."in_stock", "mock_items"."created_at", "mock_items"."metadata"
        FROM "mock_items"
        WHERE "mock_items"."description" &&& ARRAY['slee', 'rann']::pdb.fuzzy(1, t)
        """,
    )
    ids = _ids(queryset)
    assert 3 in ids


@pytest.mark.parametrize(
    ("expected", "tokenizer"),
    [
        ("pdb.whitespace", Tokenizer.whitespace()),
        (
            "pdb.whitespace('alias=my_column')",
            Tokenizer.whitespace(options={"alias": "my_column"}),
        ),
        ("pdb.unicode_words", Tokenizer.unicode_words()),
        ("pdb.literal", Tokenizer.literal()),
        ("pdb.literal_normalized", Tokenizer.literal_normalized()),
        ("pdb.ngram(3,3)", Tokenizer.ngram(3, 3)),
        (
            "pdb.ngram(3,3,'positions=true')",
            Tokenizer.ngram(3, 3, options={"positions": True}),
        ),
        ("pdb.edge_ngram(2,5)", Tokenizer.edge_ngram(2, 5)),
        ("pdb.simple", Tokenizer.simple()),
        ("pdb.regex_pattern('.*')", Tokenizer.regex_pattern(".*")),
        ("pdb.chinese_compatible", Tokenizer.chinese_compatible()),
        ("pdb.lindera('chinese')", Tokenizer.lindera("chinese")),
        ("pdb.icu", Tokenizer.icu()),
        ("pdb.jieba", Tokenizer.jieba()),
        ("pdb.source_code", Tokenizer.source_code()),
    ],
)
def test_all_tokenizers(expected: str, tokenizer: Tokenizer) -> None:
    queryset = MockItem.objects.filter(
        description=ParadeDB(MatchAll(Tokenized("running shoes", tokenizer)))
    )

    _ = _ids(queryset)
    _assert_sql(
        str(queryset.query),
        f"""
        SELECT "mock_items"."id", "mock_items"."description", "mock_items"."category", "mock_items"."rating", "mock_items"."in_stock", "mock_items"."created_at", "mock_items"."metadata"
        FROM "mock_items"
        WHERE "mock_items"."description" &&& 'running shoes'::{expected}
        """,
    )
