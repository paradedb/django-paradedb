"""Integration coverage for ParadeDB query operators and annotations."""

from __future__ import annotations

import pytest
from django.db import DatabaseError, connection

from paradedb.functions import Snippet
from paradedb.search import (
    All,
    Match,
    MoreLikeThis,
    ParadeDB,
    Parse,
    Phrase,
    PhrasePrefix,
    Proximity,
    ProxRegex,
    RangeTerm,
    Regex,
    RegexPhrase,
    Term,
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


def test_multi_term_and() -> None:
    queryset = MockItem.objects.filter(
        description=ParadeDB(Match("running", "shoes", operator="AND"))
    )
    _assert_sql(
        str(queryset.query),
        """
        SELECT "mock_items"."id", "mock_items"."description", "mock_items"."category", "mock_items"."rating", "mock_items"."in_stock", "mock_items"."created_at", "mock_items"."metadata"
        FROM "mock_items"
        WHERE "mock_items"."description" &&& ARRAY['running', 'shoes']
        """,
    )
    ids = _ids(queryset)
    assert ids == {3}


def test_exact_literal_disjunction_single() -> None:
    queryset = MockItem.objects.filter(
        description=ParadeDB(Match("running shoes", operator="OR"))
    )
    _assert_sql(
        str(queryset.query),
        """
        SELECT "mock_items"."id", "mock_items"."description", "mock_items"."category", "mock_items"."rating", "mock_items"."in_stock", "mock_items"."created_at", "mock_items"."metadata"
        FROM "mock_items"
        WHERE "mock_items"."description" ||| 'running shoes'
        """,
    )
    ids = _ids(queryset)
    assert 3 in ids
    assert {3, 4, 5}.issubset(ids)


def test_exact_literal_disjunction_multi() -> None:
    queryset = MockItem.objects.filter(
        description=ParadeDB(Match("running", "wireless", operator="OR"))
    )
    _assert_sql(
        str(queryset.query),
        """
        SELECT "mock_items"."id", "mock_items"."description", "mock_items"."category", "mock_items"."rating", "mock_items"."in_stock", "mock_items"."created_at", "mock_items"."metadata"
        FROM "mock_items"
        WHERE "mock_items"."description" ||| ARRAY['running', 'wireless']
        """,
    )
    ids = _ids(queryset)
    assert ids == {3, 12}


def test_term_operator_for_plain_strings() -> None:
    queryset = MockItem.objects.filter(description=ParadeDB(Term("shoes")))
    _assert_sql(
        str(queryset.query),
        """
        SELECT "mock_items"."id", "mock_items"."description", "mock_items"."category", "mock_items"."rating", "mock_items"."in_stock", "mock_items"."created_at", "mock_items"."metadata"
        FROM "mock_items"
        WHERE "mock_items"."description" @@@ pdb.term('shoes')
        """,
    )
    ids = _ids(queryset)
    assert ids == {3, 4, 5}


def test_phrase_with_slop() -> None:
    queryset = MockItem.objects.filter(
        description=ParadeDB(Phrase("running shoes", slop=1))
    )
    _assert_sql(
        str(queryset.query),
        """
        SELECT "mock_items"."id", "mock_items"."description", "mock_items"."category", "mock_items"."rating", "mock_items"."in_stock", "mock_items"."created_at", "mock_items"."metadata"
        FROM "mock_items"
        WHERE "mock_items"."description" ### 'running shoes'::pdb.slop(1)
        """,
    )
    ids = _ids(queryset)
    assert ids == {3}


def test_proximity_unordered() -> None:
    queryset = MockItem.objects.filter(
        description=ParadeDB(Proximity("keyboard").within(1, "metal"))
    )
    _assert_sql(
        str(queryset.query),
        """
        SELECT "mock_items"."id", "mock_items"."description", "mock_items"."category", "mock_items"."rating", "mock_items"."in_stock", "mock_items"."created_at", "mock_items"."metadata"
        FROM "mock_items"
        WHERE "mock_items"."description" @@@ ('keyboard' ## 1 ## 'metal')
        """,
    )
    ids = _ids(queryset)
    assert 1 in ids


def test_proximity_ordered() -> None:
    queryset = MockItem.objects.filter(
        description=ParadeDB(Proximity("sleek").within(1, "running", ordered=True))
    )
    _assert_sql(
        str(queryset.query),
        """
        SELECT "mock_items"."id", "mock_items"."description", "mock_items"."category", "mock_items"."rating", "mock_items"."in_stock", "mock_items"."created_at", "mock_items"."metadata"
        FROM "mock_items"
        WHERE "mock_items"."description" @@@ ('sleek' ##> 1 ##> 'running')
        """,
    )
    ids = _ids(queryset)
    assert 3 in ids


def test_proximity_with_boost() -> None:
    queryset = MockItem.objects.filter(
        description=ParadeDB(
            Proximity("sleek").within(2, "running", ordered=True).boost(2.0)
        )
    )
    _assert_sql(
        str(queryset.query),
        """
        SELECT "mock_items"."id", "mock_items"."description", "mock_items"."category", "mock_items"."rating", "mock_items"."in_stock", "mock_items"."created_at", "mock_items"."metadata"
        FROM "mock_items"
        WHERE "mock_items"."description" @@@ ('sleek' ##> 2 ##> 'running')::pdb.boost(2.0)
        """,
    )
    ids = _ids(queryset)
    assert ids == {3}


def test_proximity_with_const() -> None:
    queryset = MockItem.objects.filter(
        description=ParadeDB(
            Proximity("sleek").within(2, "running", ordered=True).const(1.0)
        )
    )
    _assert_sql(
        str(queryset.query),
        """
        SELECT "mock_items"."id", "mock_items"."description", "mock_items"."category", "mock_items"."rating", "mock_items"."in_stock", "mock_items"."created_at", "mock_items"."metadata"
        FROM "mock_items"
        WHERE "mock_items"."description" @@@ ('sleek' ##> 2 ##> 'running')::pdb.const(1.0)
        """,
    )
    ids = _ids(queryset)
    assert ids == {3}


def test_proximity_regex_query() -> None:
    queryset = MockItem.objects.filter(
        description=ParadeDB(Proximity("running").within(1, ProxRegex("sho.*")))
    )
    _assert_sql(
        str(queryset.query),
        """
        SELECT "mock_items"."id", "mock_items"."description", "mock_items"."category", "mock_items"."rating", "mock_items"."in_stock", "mock_items"."created_at", "mock_items"."metadata"
        FROM "mock_items"
        WHERE "mock_items"."description" @@@ ('running' ## 1 ## pdb.prox_regex('sho.*'))
        """,
    )
    ids = _ids(queryset)
    assert ids == {3}


def test_proximity_array_query() -> None:
    queryset = MockItem.objects.filter(
        description=ParadeDB(Proximity(["sleek", "running"]).within(1, "shoes"))
    )
    _assert_sql(
        str(queryset.query),
        """
        SELECT "mock_items"."id", "mock_items"."description", "mock_items"."category", "mock_items"."rating", "mock_items"."in_stock", "mock_items"."created_at", "mock_items"."metadata"
        FROM "mock_items"
        WHERE "mock_items"."description" @@@ (pdb.prox_array('sleek', 'running') ## 1 ## 'shoes')
        """,
    )
    ids = _ids(queryset)
    assert ids == {3}


def test_proximity_array_with_mixed_prox_regex_items() -> None:
    queryset = MockItem.objects.filter(
        description=ParadeDB(
            Proximity(["sleek", ProxRegex("run.*")]).within(1, "shoes")
        )
    )
    _assert_sql(
        str(queryset.query),
        """
        SELECT "mock_items"."id", "mock_items"."description", "mock_items"."category", "mock_items"."rating", "mock_items"."in_stock", "mock_items"."created_at", "mock_items"."metadata"
        FROM "mock_items"
        WHERE "mock_items"."description" @@@ (pdb.prox_array('sleek', pdb.prox_regex('run.*')) ## 1 ## 'shoes')
        """,
    )
    ids = _ids(queryset)
    assert ids == {3}


def test_proximity_array_with_mixed_prox_regex_items_ordered() -> None:
    queryset = MockItem.objects.filter(
        description=ParadeDB(
            Proximity(["sleek", ProxRegex("run.*")]).within(
                1,
                "shoes",
                ordered=True,
            )
        )
    )
    _assert_sql(
        str(queryset.query),
        """
        SELECT "mock_items"."id", "mock_items"."description", "mock_items"."category", "mock_items"."rating", "mock_items"."in_stock", "mock_items"."created_at", "mock_items"."metadata"
        FROM "mock_items"
        WHERE "mock_items"."description" @@@ (pdb.prox_array('sleek', pdb.prox_regex('run.*')) ##> 1 ##> 'shoes')
        """,
    )
    ids = _ids(queryset)
    assert ids == {3}


def test_proximity_array_with_only_prox_regex_left_term() -> None:
    queryset = MockItem.objects.filter(
        description=ParadeDB(Proximity(ProxRegex("run.*")).within(1, "shoes"))
    )
    _assert_sql(
        str(queryset.query),
        """
        SELECT "mock_items"."id", "mock_items"."description", "mock_items"."category", "mock_items"."rating", "mock_items"."in_stock", "mock_items"."created_at", "mock_items"."metadata"
        FROM "mock_items"
        WHERE "mock_items"."description" @@@ (pdb.prox_regex('run.*') ## 1 ## 'shoes')
        """,
    )
    ids = _ids(queryset)
    assert ids == {3}


def test_proximity_array_with_prox_regex_custom_max_expansions() -> None:
    queryset = MockItem.objects.filter(
        description=ParadeDB(
            Proximity(ProxRegex("run.*", max_expansions=100)).within(
                1,
                "shoes",
            )
        )
    )
    _assert_sql(
        str(queryset.query),
        """
        SELECT "mock_items"."id", "mock_items"."description", "mock_items"."category", "mock_items"."rating", "mock_items"."in_stock", "mock_items"."created_at", "mock_items"."metadata"
        FROM "mock_items"
        WHERE "mock_items"."description" @@@ (pdb.prox_regex('run.*', 100) ## 1 ## 'shoes')
        """,
    )
    ids = _ids(queryset)
    assert ids == {3}


def test_proximity_array_with_right_term_list() -> None:
    queryset = MockItem.objects.filter(
        description=ParadeDB(
            Proximity("running").within(
                1,
                ["shoes", ProxRegex("boot.*")],
            )
        )
    )
    _assert_sql(
        str(queryset.query),
        """
        SELECT "mock_items"."id", "mock_items"."description", "mock_items"."category", "mock_items"."rating", "mock_items"."in_stock", "mock_items"."created_at", "mock_items"."metadata"
        FROM "mock_items"
        WHERE "mock_items"."description" @@@ ('running' ## 1 ## pdb.prox_array('shoes', pdb.prox_regex('boot.*')))
        """,
    )
    ids = _ids(queryset)
    assert ids == {3}


def test_proximity_with_nested_proximity_arrays() -> None:
    queryset = MockItem.objects.filter(
        description=ParadeDB(
            Proximity("running").within(
                1,
                ["shoes", ["shoes", [ProxRegex("boot.*")]]],
            )
        )
    )
    _assert_sql(
        str(queryset.query),
        """
        SELECT "mock_items"."id", "mock_items"."description", "mock_items"."category", "mock_items"."rating", "mock_items"."in_stock", "mock_items"."created_at", "mock_items"."metadata"
        FROM "mock_items"
        WHERE "mock_items"."description" @@@ ('running' ## 1 ## pdb.prox_array('shoes', pdb.prox_array('shoes', pdb.prox_array(pdb.prox_regex('boot.*')))))
        """,
    )
    ids = _ids(queryset)
    assert ids == {3}


def test_proximity_array_with_prox_regex_bool_max_expansions() -> None:
    with pytest.raises(TypeError, match="ProxRegex max_expansions must be an integer"):
        ProxRegex("run.*", max_expansions=True)


def test_proximity_array_with_prox_regex_float_max_expansions() -> None:
    with pytest.raises(TypeError, match="ProxRegex max_expansions must be an integer"):
        ProxRegex("run.*", max_expansions=1.5)  # type: ignore[arg-type]


def test_proximity_array_with_prox_regex_non_string_pattern() -> None:
    with pytest.raises(TypeError, match="ProxRegex pattern must be a string"):
        ProxRegex(123)  # type: ignore[arg-type]


def test_proximity_array_with_non_string_left_term() -> None:
    with pytest.raises(
        TypeError,
        match="Proximity term must be strings or ProxRegex instances",
    ):
        Proximity(123)  # type: ignore[arg-type]


def test_proximity_array_with_invalid_prox_regex_pattern_raises() -> None:
    with pytest.raises(DatabaseError, match="regex parse error"):
        MockItem.objects.filter(
            description=ParadeDB(Proximity(ProxRegex("[invalid")).within(1, "shoes"))
        ).exists()


def test_fuzzy_distance() -> None:
    queryset = MockItem.objects.filter(
        description=ParadeDB(Match("runnning", operator="OR", distance=1))
    )
    _assert_sql(
        str(queryset.query),
        """
        SELECT "mock_items"."id", "mock_items"."description", "mock_items"."category", "mock_items"."rating", "mock_items"."in_stock", "mock_items"."created_at", "mock_items"."metadata"
        FROM "mock_items"
        WHERE "mock_items"."description" ||| 'runnning'::pdb.fuzzy(1)
        """,
    )
    ids = _ids(queryset)
    assert ids == {3}


def test_fuzzy_conjunction() -> None:
    queryset = MockItem.objects.filter(
        description=ParadeDB(Match("runnning shose", operator="AND", distance=2))
    )
    _assert_sql(
        str(queryset.query),
        """
        SELECT "mock_items"."id", "mock_items"."description", "mock_items"."category", "mock_items"."rating", "mock_items"."in_stock", "mock_items"."created_at", "mock_items"."metadata"
        FROM "mock_items"
        WHERE "mock_items"."description" &&& 'runnning shose'::pdb.fuzzy(2)
        """,
    )
    ids = _ids(queryset)
    assert 3 in ids


def test_fuzzy_multi_term_or() -> None:
    """Multi-term Match with distance uses ARRAY['t1', 't2']::pdb.fuzzy(N) — docs snippet."""
    qs = MockItem.objects.filter(
        description=ParadeDB(Match("runing", "shose", operator="OR", distance=2))
    )
    _assert_sql(
        str(qs.query),
        """
        SELECT "mock_items"."id", "mock_items"."description", "mock_items"."category", "mock_items"."rating", "mock_items"."in_stock", "mock_items"."created_at", "mock_items"."metadata"
        FROM "mock_items"
        WHERE "mock_items"."description" ||| ARRAY['runing', 'shose']::pdb.fuzzy(2)
        """,
    )
    ids = _ids(qs)
    assert 3 in ids


def test_fuzzy_multi_term_and() -> None:
    """Multi-term Match AND with distance uses ARRAY['t1', 't2']::pdb.fuzzy(N)."""
    queryset = MockItem.objects.filter(
        description=ParadeDB(Match("runing", "shose", operator="AND", distance=2))
    )
    _assert_sql(
        str(queryset.query),
        """
        SELECT "mock_items"."id", "mock_items"."description", "mock_items"."category", "mock_items"."rating", "mock_items"."in_stock", "mock_items"."created_at", "mock_items"."metadata"
        FROM "mock_items"
        WHERE "mock_items"."description" &&& ARRAY['runing', 'shose']::pdb.fuzzy(2)
        """,
    )
    ids = _ids(queryset)
    assert 3 in ids


def test_fuzzy_term_form() -> None:
    queryset = MockItem.objects.filter(description=ParadeDB(Term("shose", distance=2)))
    _assert_sql(
        str(queryset.query),
        """
        SELECT "mock_items"."id", "mock_items"."description", "mock_items"."category", "mock_items"."rating", "mock_items"."in_stock", "mock_items"."created_at", "mock_items"."metadata"
        FROM "mock_items"
        WHERE "mock_items"."description" @@@ pdb.term('shose')::pdb.fuzzy(2)
        """,
    )
    ids = _ids(queryset)
    assert 3 in ids


def test_fuzzy_prefix() -> None:
    queryset = MockItem.objects.filter(
        description=ParadeDB(Term("runn", distance=0, prefix=True))
    )
    _assert_sql(
        str(queryset.query),
        """
        SELECT "mock_items"."id", "mock_items"."description", "mock_items"."category", "mock_items"."rating", "mock_items"."in_stock", "mock_items"."created_at", "mock_items"."metadata"
        FROM "mock_items"
        WHERE "mock_items"."description" @@@ pdb.term('runn')::pdb.fuzzy(0, t)
        """,
    )
    ids = _ids(queryset)
    assert 3 in ids


def test_fuzzy_transposition_cost_one() -> None:
    queryset = MockItem.objects.filter(
        description=ParadeDB(Term("shose", distance=1, transposition_cost_one=True))
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


def test_fuzzy_multi_term_prefix_and() -> None:
    """Multi-term Match AND with distance + prefix — docs/full-text/fuzzy.mdx snippet 3."""
    queryset = MockItem.objects.filter(
        description=ParadeDB(
            Match("slee", "rann", operator="AND", distance=1, prefix=True)
        )
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
    assert len(ids) > 0


def test_all_query() -> None:
    """All() matches every indexed document — docs/aggregates/overview.mdx snippet 4."""
    queryset = MockItem.objects.filter(id=ParadeDB(All()))
    _assert_sql(
        str(queryset.query),
        """
        SELECT "mock_items"."id", "mock_items"."description", "mock_items"."category", "mock_items"."rating", "mock_items"."in_stock", "mock_items"."created_at", "mock_items"."metadata"
        FROM "mock_items"
        WHERE "mock_items"."id" @@@ pdb.all()
        """,
    )
    count = queryset.count()
    assert count > 0


def test_filter_category_in_with_paradedb() -> None:
    """Term search with category__in filter — docs/filtering.mdx snippet 3."""
    queryset = MockItem.objects.filter(
        description=ParadeDB(Term("shoes")),
        category__in=["Footwear", "Apparel"],
    ).values("description", "rating", "category")
    rows = list(queryset)
    for row in rows:
        assert row["category"] in ("Footwear", "Apparel")


def test_tokenizer_override_match() -> None:
    queryset = MockItem.objects.filter(
        description=ParadeDB(
            Match("running shoes", operator="AND", tokenizer=Tokenizer.whitespace())
        )
    )
    _assert_sql(
        str(queryset.query),
        """
        SELECT "mock_items"."id", "mock_items"."description", "mock_items"."category", "mock_items"."rating", "mock_items"."in_stock", "mock_items"."created_at", "mock_items"."metadata"
        FROM "mock_items"
        WHERE "mock_items"."description" &&& 'running shoes'::pdb.whitespace
        """,
    )
    ids = _ids(queryset)
    assert 3 in ids


def test_tokenizer_override_phrase() -> None:
    queryset = MockItem.objects.filter(
        description=ParadeDB(Phrase("running shoes", tokenizer=Tokenizer.whitespace()))
    )
    _assert_sql(
        str(queryset.query),
        """
        SELECT "mock_items"."id", "mock_items"."description", "mock_items"."category", "mock_items"."rating", "mock_items"."in_stock", "mock_items"."created_at", "mock_items"."metadata"
        FROM "mock_items"
        WHERE "mock_items"."description" ### 'running shoes'::pdb.whitespace
        """,
    )
    ids = _ids(queryset)
    assert 3 in ids


def test_tokenizer_override_match_with_args_sql() -> None:
    queryset = MockItem.objects.filter(
        description=ParadeDB(
            Match(
                "running shoes",
                operator="AND",
                tokenizer=Tokenizer.whitespace(options={"lowercase": False}),
            )
        )
    )
    _assert_sql(
        str(queryset.query),
        """
        SELECT "mock_items"."id", "mock_items"."description", "mock_items"."category", "mock_items"."rating", "mock_items"."in_stock", "mock_items"."created_at", "mock_items"."metadata"
        FROM "mock_items"
        WHERE "mock_items"."description" &&& 'running shoes'::pdb.whitespace('lowercase=false')
        """,
    )


def test_tokenizer_override_or_with_args_sql() -> None:
    queryset = MockItem.objects.filter(
        description=ParadeDB(
            Match(
                "wireless keyboard",
                operator="OR",
                tokenizer=Tokenizer.simple(options={"lowercase": False}),
            )
        )
    )
    _assert_sql(
        str(queryset.query),
        """
        SELECT "mock_items"."id", "mock_items"."description", "mock_items"."category", "mock_items"."rating", "mock_items"."in_stock", "mock_items"."created_at", "mock_items"."metadata"
        FROM "mock_items"
        WHERE "mock_items"."description" ||| 'wireless keyboard'::pdb.simple('lowercase=false')
        """,
    )


def test_tokenizer_override_phrase_with_multi_args_sql() -> None:
    queryset = MockItem.objects.filter(
        description=ParadeDB(
            Phrase(
                "wireless mouse",
                slop=2,
                tokenizer=Tokenizer.ngram(3, 8),
            )
        )
    )
    _assert_sql(
        str(queryset.query),
        """
        SELECT "mock_items"."id", "mock_items"."description", "mock_items"."category", "mock_items"."rating", "mock_items"."in_stock", "mock_items"."created_at", "mock_items"."metadata"
        FROM "mock_items"
        WHERE "mock_items"."description" ### 'wireless mouse'::pdb.slop(2)::pdb.ngram(3,8)
        """,
    )


def test_boost_does_not_change_result_set() -> None:
    queryset = MockItem.objects.filter(
        description=ParadeDB(Match("shoes", operator="AND", boost=2.0))
    )
    _assert_sql(
        str(queryset.query),
        """
        SELECT "mock_items"."id", "mock_items"."description", "mock_items"."category", "mock_items"."rating", "mock_items"."in_stock", "mock_items"."created_at", "mock_items"."metadata"
        FROM "mock_items"
        WHERE "mock_items"."description" &&& 'shoes'::pdb.boost(2.0)
        """,
    )
    ids = _ids(queryset)
    assert ids == {3, 4, 5}


def test_boost_with_fuzzy_integration() -> None:
    queryset = MockItem.objects.filter(
        description=ParadeDB(Match("runnning", operator="OR", distance=1, boost=2.0))
    )
    _assert_sql(
        str(queryset.query),
        """
        SELECT "mock_items"."id", "mock_items"."description", "mock_items"."category", "mock_items"."rating", "mock_items"."in_stock", "mock_items"."created_at", "mock_items"."metadata"
        FROM "mock_items"
        WHERE "mock_items"."description" ||| 'runnning'::pdb.fuzzy(1)::pdb.boost(2.0)
        """,
    )
    ids = _ids(queryset)
    assert ids == {3}


def test_const_with_fuzzy_integration() -> None:
    queryset = MockItem.objects.filter(
        description=ParadeDB(Match("shose", operator="OR", distance=2, const=1.0))
    )
    _assert_sql(
        str(queryset.query),
        """
        SELECT "mock_items"."id", "mock_items"."description", "mock_items"."category", "mock_items"."rating", "mock_items"."in_stock", "mock_items"."created_at", "mock_items"."metadata"
        FROM "mock_items"
        WHERE "mock_items"."description" ||| 'shose'::pdb.fuzzy(2)::pdb.query::pdb.const(1.0)
        """,
    )
    ids = _ids(queryset)
    assert ids == {3, 4, 5}


def test_const_with_phrase_slop_integration() -> None:
    # Verifies pdb.slop::pdb.query::pdb.const executes (previously failed with
    # "cannot cast type pdb.slop to pdb.const").
    queryset = MockItem.objects.filter(
        description=ParadeDB(Phrase("running shoes", slop=2, const=1.0))
    )
    _assert_sql(
        str(queryset.query),
        """
        SELECT "mock_items"."id", "mock_items"."description", "mock_items"."category", "mock_items"."rating", "mock_items"."in_stock", "mock_items"."created_at", "mock_items"."metadata"
        FROM "mock_items"
        WHERE "mock_items"."description" ### 'running shoes'::pdb.slop(2)::pdb.query::pdb.const(1.0)
        """,
    )
    ids = _ids(queryset)
    assert ids == {3}


def test_const_does_not_change_result_set() -> None:
    queryset = MockItem.objects.filter(
        description=ParadeDB(Match("shoes", operator="AND", const=1.0))
    )
    _assert_sql(
        str(queryset.query),
        """
        SELECT "mock_items"."id", "mock_items"."description", "mock_items"."category", "mock_items"."rating", "mock_items"."in_stock", "mock_items"."created_at", "mock_items"."metadata"
        FROM "mock_items"
        WHERE "mock_items"."description" &&& 'shoes'::pdb.const(1.0)
        """,
    )
    ids = _ids(queryset)
    assert ids == {3, 4, 5}


def test_boost_multi_term_or_query() -> None:
    # Verifies ARRAY['shoes', 'boots']::pdb.boost(2.0) is valid SQL and executes.
    queryset = MockItem.objects.filter(
        description=ParadeDB(Match("shoes", "boots", operator="OR", boost=2.0))
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


def test_const_multi_term_or_query() -> None:
    queryset = MockItem.objects.filter(
        description=ParadeDB(Match("shoes", "boots", operator="OR", const=1.5))
    )
    _assert_sql(
        str(queryset.query),
        """
        SELECT "mock_items"."id", "mock_items"."description", "mock_items"."category", "mock_items"."rating", "mock_items"."in_stock", "mock_items"."created_at", "mock_items"."metadata"
        FROM "mock_items"
        WHERE "mock_items"."description" ||| ARRAY['shoes', 'boots']::pdb.const(1.5)
        """,
    )
    ids = _ids(queryset)
    assert ids == {3, 4, 5, 13}


def test_match_tokenizer_and_distance_rejected() -> None:
    with pytest.raises(
        ValueError, match="Match tokenizer cannot be combined with fuzzy options"
    ):
        Match(
            "running shoes",
            operator="AND",
            tokenizer=Tokenizer.whitespace(),
            distance=1,
        )


def test_multi_term_fuzzy_tokenizer_rejected() -> None:
    with pytest.raises(
        ValueError, match="Match tokenizer cannot be combined with fuzzy options"
    ):
        Match("a", "b", operator="OR", tokenizer=Tokenizer.whitespace(), distance=1)


def test_multi_term_fuzzy_boost_rejected() -> None:
    with pytest.raises(
        ValueError, match="Multi-term fuzzy Match does not support boost or const"
    ):
        Match("a", "b", operator="OR", distance=1, boost=2.0)


def test_multi_term_fuzzy_const_rejected() -> None:
    with pytest.raises(
        ValueError, match="Multi-term fuzzy Match does not support boost or const"
    ):
        Match("a", "b", operator="OR", distance=1, const=1.0)


def test_boost_and_const_error_deferred_to_database() -> None:
    queryset = MockItem.objects.filter(
        description=ParadeDB(Match("shoes", operator="AND", boost=2.0, const=1.0))
    )
    _assert_sql(
        str(queryset.query),
        """
        SELECT "mock_items"."id", "mock_items"."description", "mock_items"."category", "mock_items"."rating", "mock_items"."in_stock", "mock_items"."created_at", "mock_items"."metadata"
        FROM "mock_items"
        WHERE "mock_items"."description" &&& 'shoes'::pdb.boost(2.0)::pdb.const(1.0)
        """,
    )
    with pytest.raises(DatabaseError, match="cannot cast type"):
        list(queryset)


def test_regex_query() -> None:
    queryset = MockItem.objects.filter(description=ParadeDB(Regex(".*running.*")))
    _assert_sql(
        str(queryset.query),
        """
        SELECT "mock_items"."id", "mock_items"."description", "mock_items"."category", "mock_items"."rating", "mock_items"."in_stock", "mock_items"."created_at", "mock_items"."metadata"
        FROM "mock_items"
        WHERE "mock_items"."description" @@@ pdb.regex('.*running.*')
        """,
    )
    ids = _ids(queryset)
    assert ids == {3}


def test_parse_lenient() -> None:
    queryset = MockItem.objects.filter(
        description=ParadeDB(Parse("running AND shoes", lenient=True))
    )
    _assert_sql(
        str(queryset.query),
        """
        SELECT "mock_items"."id", "mock_items"."description", "mock_items"."category", "mock_items"."rating", "mock_items"."in_stock", "mock_items"."created_at", "mock_items"."metadata"
        FROM "mock_items"
        WHERE "mock_items"."description" @@@ pdb.parse('running AND shoes', lenient => true)
        """,
    )
    ids = _ids(queryset)
    assert ids == {3}


def test_parse_conjunction_mode() -> None:
    default_queryset = MockItem.objects.filter(
        description=ParadeDB(Parse("running shoes"))
    )
    conjunction_queryset = MockItem.objects.filter(
        description=ParadeDB(Parse("running shoes", conjunction_mode=True))
    )
    _assert_sql(
        str(default_queryset.query),
        """
        SELECT "mock_items"."id", "mock_items"."description", "mock_items"."category", "mock_items"."rating", "mock_items"."in_stock", "mock_items"."created_at", "mock_items"."metadata"
        FROM "mock_items"
        WHERE "mock_items"."description" @@@ pdb.parse('running shoes')
        """,
    )
    _assert_sql(
        str(conjunction_queryset.query),
        """
        SELECT "mock_items"."id", "mock_items"."description", "mock_items"."category", "mock_items"."rating", "mock_items"."in_stock", "mock_items"."created_at", "mock_items"."metadata"
        FROM "mock_items"
        WHERE "mock_items"."description" @@@ pdb.parse('running shoes', conjunction_mode => true)
        """,
    )
    default_ids = _ids(default_queryset)
    conjunction_ids = _ids(conjunction_queryset)
    assert conjunction_ids == {3}
    assert conjunction_ids.issubset(default_ids)
    assert len(default_ids) > len(conjunction_ids)


def test_phrase_prefix_query() -> None:
    queryset = MockItem.objects.filter(
        description=ParadeDB(PhrasePrefix("running", "sh"))
    )
    _assert_sql(
        str(queryset.query),
        """
        SELECT "mock_items"."id", "mock_items"."description", "mock_items"."category", "mock_items"."rating", "mock_items"."in_stock", "mock_items"."created_at", "mock_items"."metadata"
        FROM "mock_items"
        WHERE "mock_items"."description" @@@ pdb.phrase_prefix(ARRAY['running', 'sh'])
        """,
    )
    ids = _ids(queryset)
    assert ids == {3}


def test_phrase_prefix_with_max_expansion() -> None:
    queryset = MockItem.objects.filter(
        description=ParadeDB(PhrasePrefix("running", "sh", max_expansion=100))
    )
    _assert_sql(
        str(queryset.query),
        """
        SELECT "mock_items"."id", "mock_items"."description", "mock_items"."category", "mock_items"."rating", "mock_items"."in_stock", "mock_items"."created_at", "mock_items"."metadata"
        FROM "mock_items"
        WHERE "mock_items"."description" @@@ pdb.phrase_prefix(ARRAY['running', 'sh'], max_expansion => 100)
        """,
    )
    ids = _ids(queryset)
    assert ids == {3}


def test_phrase_prefix_with_boost() -> None:
    queryset = MockItem.objects.filter(
        description=ParadeDB(PhrasePrefix("running", "sh", boost=2.0))
    )
    _assert_sql(
        str(queryset.query),
        """
        SELECT "mock_items"."id", "mock_items"."description", "mock_items"."category", "mock_items"."rating", "mock_items"."in_stock", "mock_items"."created_at", "mock_items"."metadata"
        FROM "mock_items"
        WHERE "mock_items"."description" @@@ pdb.phrase_prefix(ARRAY['running', 'sh'])::pdb.boost(2.0)
        """,
    )
    ids = _ids(queryset)
    assert ids == {3}


def test_phrase_prefix_with_const() -> None:
    queryset = MockItem.objects.filter(
        description=ParadeDB(PhrasePrefix("running", "sh", const=1.0))
    )
    _assert_sql(
        str(queryset.query),
        """
        SELECT "mock_items"."id", "mock_items"."description", "mock_items"."category", "mock_items"."rating", "mock_items"."in_stock", "mock_items"."created_at", "mock_items"."metadata"
        FROM "mock_items"
        WHERE "mock_items"."description" @@@ pdb.phrase_prefix(ARRAY['running', 'sh'])::pdb.const(1.0)
        """,
    )
    ids = _ids(queryset)
    assert ids == {3}


def test_regex_phrase_query() -> None:
    queryset = MockItem.objects.filter(
        description=ParadeDB(RegexPhrase("run.*", "sho.*"))
    )
    _assert_sql(
        str(queryset.query),
        """
        SELECT "mock_items"."id", "mock_items"."description", "mock_items"."category", "mock_items"."rating", "mock_items"."in_stock", "mock_items"."created_at", "mock_items"."metadata"
        FROM "mock_items"
        WHERE "mock_items"."description" @@@ pdb.regex_phrase(ARRAY['run.*', 'sho.*'])
        """,
    )
    ids = _ids(queryset)
    assert ids == {3}


def test_regex_phrase_with_options() -> None:
    queryset = MockItem.objects.filter(
        description=ParadeDB(RegexPhrase("run.*", "sho.*", slop=2, max_expansions=100))
    )
    _assert_sql(
        str(queryset.query),
        """
        SELECT "mock_items"."id", "mock_items"."description", "mock_items"."category", "mock_items"."rating", "mock_items"."in_stock", "mock_items"."created_at", "mock_items"."metadata"
        FROM "mock_items"
        WHERE "mock_items"."description" @@@ pdb.regex_phrase(ARRAY['run.*', 'sho.*'], slop => 2, max_expansions => 100)
        """,
    )
    ids = _ids(queryset)
    assert ids == {3}


def test_term_query() -> None:
    queryset = MockItem.objects.filter(description=ParadeDB(Term("shoes")))
    _assert_sql(
        str(queryset.query),
        """
        SELECT "mock_items"."id", "mock_items"."description", "mock_items"."category", "mock_items"."rating", "mock_items"."in_stock", "mock_items"."created_at", "mock_items"."metadata"
        FROM "mock_items"
        WHERE "mock_items"."description" @@@ pdb.term('shoes')
        """,
    )
    ids = _ids(queryset)
    assert ids == {3, 4, 5}


def test_array_element_search_operator_with_text_array() -> None:
    with connection.cursor() as cursor:
        cursor.execute("DROP TABLE IF EXISTS tmp_array_ops;")
        cursor.execute(
            "CREATE TABLE tmp_array_ops (id integer primary key, tags text[], description text);"
        )
        cursor.execute(
            "INSERT INTO tmp_array_ops (id, tags, description) VALUES "
            "(1, ARRAY['red','shoe'], 'red shoe'), "
            "(2, ARRAY['blue','hat'], 'blue hat'), "
            "(3, ARRAY['red','hat'], 'red hat');"
        )
        cursor.execute(
            "CREATE INDEX tmp_array_ops_bm25_idx ON tmp_array_ops USING bm25 "
            "(id, tags, description) WITH (key_field='id');"
        )

    where_sql = _where_sql("tags", ParadeDB(Term("red")))
    ids = _raw_ids(f"SELECT id FROM tmp_array_ops WHERE {where_sql} ORDER BY id;")
    assert ids == {1, 3}

    with connection.cursor() as cursor:
        cursor.execute("DROP TABLE IF EXISTS tmp_array_ops;")


def test_range_term_query_with_range_field() -> None:
    with connection.cursor() as cursor:
        cursor.execute("DROP TABLE IF EXISTS tmp_range_ops;")
        cursor.execute(
            "CREATE TABLE tmp_range_ops (id integer primary key, weight_range int4range, description text);"
        )
        cursor.execute(
            "INSERT INTO tmp_range_ops (id, weight_range, description) VALUES "
            "(1, '[1,4]'::int4range, 'low'), "
            "(2, '[3,9]'::int4range, 'mid'), "
            "(3, '[10,12]'::int4range, 'high');"
        )
        cursor.execute(
            "CREATE INDEX tmp_range_ops_bm25_idx ON tmp_range_ops USING bm25 "
            "(id, weight_range, description) WITH (key_field='id');"
        )

    scalar_where = _where_sql("weight_range", ParadeDB(RangeTerm(1)))
    scalar_ids = _raw_ids(
        f"SELECT id FROM tmp_range_ops WHERE {scalar_where} ORDER BY id;"
    )
    assert scalar_ids == {1}

    relation_where = _where_sql(
        "weight_range",
        ParadeDB(RangeTerm("(10, 12]", relation="Intersects", range_type="int4range")),
    )
    relation_ids = _raw_ids(
        f"SELECT id FROM tmp_range_ops WHERE {relation_where} ORDER BY id;"
    )
    assert relation_ids == {3}

    with pytest.raises(ValueError, match="Range type must be one of"):
        _where_sql(
            "weight_range",
            ParadeDB(
                RangeTerm("(10, 12]", relation="Intersects", range_type="badtype")
            ),
        )

    with connection.cursor() as cursor:
        cursor.execute("DROP TABLE IF EXISTS tmp_range_ops;")


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
        ParadeDB(
            Match("running Sportswear", operator="AND", tokenizer=Tokenizer.simple())
        ),
    )
    ids = _raw_ids(f"SELECT id FROM tmp_expr_ops WHERE {expr_where} ORDER BY id;")
    assert ids == {1}

    with connection.cursor() as cursor:
        cursor.execute("DROP TABLE IF EXISTS tmp_expr_ops;")


def test_snippet_rendering() -> None:
    queryset = (
        MockItem.objects.filter(
            description=ParadeDB(Match("running shoes", operator="AND"))
        )
        .annotate(snippet=Snippet("description"))
        .order_by("id")
        .values_list("snippet", flat=True)
    )
    _assert_sql(
        str(queryset.query),
        """
        SELECT
            pdb.snippet("mock_items"."description") AS "snippet"
        FROM "mock_items"
        WHERE "mock_items"."description" &&& 'running shoes'
        ORDER BY "mock_items"."id" ASC
        """,
    )
    snippet = queryset.first()
    assert snippet is not None
    assert "<b>running</b>" in snippet and "<b>shoes</b>" in snippet


def test_more_like_this_by_id() -> None:
    """MLT by ID with fields=['description'] returns similar items."""
    queryset = MockItem.objects.filter(
        id=ParadeDB(MoreLikeThis(id=3, fields=["description"]))
    ).order_by("id")
    _assert_sql(
        str(queryset.query),
        """
        SELECT "mock_items"."id", "mock_items"."description", "mock_items"."category", "mock_items"."rating", "mock_items"."in_stock", "mock_items"."created_at", "mock_items"."metadata"
        FROM "mock_items"
        WHERE "mock_items"."id" @@@ pdb.more_like_this(3, ARRAY[description]::text[])
        ORDER BY "mock_items"."id" ASC
        """,
    )
    ids = _ids(queryset)
    assert ids == {3, 4, 5}


def test_more_like_this_multiple_ids() -> None:
    """MLT with multiple IDs and fields=['description'] returns union."""
    queryset = MockItem.objects.filter(
        id=ParadeDB(MoreLikeThis(ids=[3, 12], fields=["description"]))
    ).order_by("id")
    _assert_sql(
        str(queryset.query),
        """
        SELECT "mock_items"."id", "mock_items"."description", "mock_items"."category", "mock_items"."rating", "mock_items"."in_stock", "mock_items"."created_at", "mock_items"."metadata"
        FROM "mock_items"
        WHERE ("mock_items"."id" @@@ pdb.more_like_this(3, ARRAY[description]::text[]) OR "mock_items"."id" @@@ pdb.more_like_this(12, ARRAY[description]::text[]))
        ORDER BY "mock_items"."id" ASC
        """,
    )
    ids = _ids(queryset)
    assert ids == {3, 4, 5, 12}


def test_more_like_this_by_document() -> None:
    """MLT with document finds similar documents."""
    query = MockItem.objects.filter(
        id=ParadeDB(MoreLikeThis(document={"description": "wireless earbuds"}))
    )
    sql, _ = query.query.sql_with_params()
    _assert_sql(
        sql,
        """
        SELECT "mock_items"."id", "mock_items"."description", "mock_items"."category", "mock_items"."rating", "mock_items"."in_stock", "mock_items"."created_at", "mock_items"."metadata"
        FROM "mock_items"
        WHERE "mock_items"."id" @@@ pdb.more_like_this(%s)
        """,
    )
    ids = _ids(query)
    # Should find documents similar to the text
    assert 12 in ids  # "Innovative wireless earbuds"


def test_more_like_this_with_stopwords() -> None:
    """MLT with stopwords excludes terms from matching - verified by comparing results."""
    # Get baseline results without stopwords
    baseline_query = MockItem.objects.filter(
        id=ParadeDB(
            MoreLikeThis(
                id=3,  # "Sleek running shoes"
                fields=["description"],
            )
        )
    )
    _assert_sql(
        str(baseline_query.query),
        """
        SELECT "mock_items"."id", "mock_items"."description", "mock_items"."category", "mock_items"."rating", "mock_items"."in_stock", "mock_items"."created_at", "mock_items"."metadata"
        FROM "mock_items"
        WHERE "mock_items"."id" @@@ pdb.more_like_this(3, ARRAY[description]::text[])
        """,
    )
    baseline_ids = _ids(baseline_query)

    # Get results with "shoes" as stopword
    with_stopword_query = MockItem.objects.filter(
        id=ParadeDB(
            MoreLikeThis(
                id=3,
                fields=["description"],
                stopwords=["shoes"],
            )
        )
    )
    _assert_sql(
        str(with_stopword_query.query),
        """
        SELECT "mock_items"."id", "mock_items"."description", "mock_items"."category", "mock_items"."rating", "mock_items"."in_stock", "mock_items"."created_at", "mock_items"."metadata"
        FROM "mock_items"
        WHERE "mock_items"."id" @@@ pdb.more_like_this(3, ARRAY[description]::text[], stopwords => ARRAY[shoes])
        """,
    )
    with_stopword_ids = _ids(with_stopword_query)

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
    baseline_query = MockItem.objects.filter(
        id=ParadeDB(
            MoreLikeThis(
                id=3,  # "Sleek running shoes"
                fields=["description"],
            )
        )
    )
    _assert_sql(
        str(baseline_query.query),
        """
        SELECT "mock_items"."id", "mock_items"."description", "mock_items"."category", "mock_items"."rating", "mock_items"."in_stock", "mock_items"."created_at", "mock_items"."metadata"
        FROM "mock_items"
        WHERE "mock_items"."id" @@@ pdb.more_like_this(3, ARRAY[description]::text[])
        """,
    )
    baseline_ids = _ids(baseline_query)

    # Get results with min_word_length=6 (filters out "shoes" which is 5 chars)
    with_min_length_query = MockItem.objects.filter(
        id=ParadeDB(
            MoreLikeThis(
                id=3,
                fields=["description"],
                min_word_length=6,
            )
        )
    )
    _assert_sql(
        str(with_min_length_query.query),
        """
        SELECT "mock_items"."id", "mock_items"."description", "mock_items"."category", "mock_items"."rating", "mock_items"."in_stock", "mock_items"."created_at", "mock_items"."metadata"
        FROM "mock_items"
        WHERE "mock_items"."id" @@@ pdb.more_like_this(3, ARRAY[description]::text[], min_word_length => 6)
        """,
    )
    with_min_length_ids = _ids(with_min_length_query)

    # Source item always included
    assert 3 in baseline_ids
    assert 3 in with_min_length_ids

    # Results should differ when short words are filtered
    assert with_min_length_ids != baseline_ids, (
        "min_word_length=6 should filter out 'shoes' (5 chars) and change results"
    )


def test_more_like_this_stopwords_reversible() -> None:
    """Verify stopwords effect is consistent and reversible."""
    query_with = MockItem.objects.filter(
        id=ParadeDB(
            MoreLikeThis(
                id=3,
                fields=["description"],
                stopwords=["shoes"],  # The main matching term
            )
        )
    )
    _assert_sql(
        str(query_with.query),
        """
        SELECT "mock_items"."id", "mock_items"."description", "mock_items"."category", "mock_items"."rating", "mock_items"."in_stock", "mock_items"."created_at", "mock_items"."metadata"
        FROM "mock_items"
        WHERE "mock_items"."id" @@@ pdb.more_like_this(3, ARRAY[description]::text[], stopwords => ARRAY[shoes])
        """,
    )
    ids_with = _ids(query_with)

    query_without = MockItem.objects.filter(
        id=ParadeDB(
            MoreLikeThis(
                id=3,
                fields=["description"],
            )
        )
    )
    _assert_sql(
        str(query_without.query),
        """
        SELECT "mock_items"."id", "mock_items"."description", "mock_items"."category", "mock_items"."rating", "mock_items"."in_stock", "mock_items"."created_at", "mock_items"."metadata"
        FROM "mock_items"
        WHERE "mock_items"."id" @@@ pdb.more_like_this(3, ARRAY[description]::text[])
        """,
    )
    ids_without = _ids(query_without)

    assert ids_with == {3}
    assert ids_without == {3, 4, 5}


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
        id=ParadeDB(MoreLikeThis(document={"description": "wireless earbuds"}))
    )

    sql, params = query.query.sql_with_params()

    # With parameterized SQL, the SQL should contain placeholder
    _assert_sql(
        sql,
        """
        SELECT "mock_items"."id", "mock_items"."description", "mock_items"."category", "mock_items"."rating", "mock_items"."in_stock", "mock_items"."created_at", "mock_items"."metadata"
        FROM "mock_items"
        WHERE "mock_items"."id" @@@ pdb.more_like_this(%s)
        """,
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
        id=ParadeDB(
            MoreLikeThis(
                id=3,
                fields=["description"],
                stopwords=[],  # Empty array
            )
        )
    )

    sql, _params = query.query.sql_with_params()

    _assert_sql(
        sql,
        """
        SELECT "mock_items"."id", "mock_items"."description", "mock_items"."category", "mock_items"."rating", "mock_items"."in_stock", "mock_items"."created_at", "mock_items"."metadata"
        FROM "mock_items"
        WHERE "mock_items"."id" @@@ pdb.more_like_this(%s, ARRAY[%s]::text[])
        """,
    )


def test_more_like_this_with_key_field() -> None:
    """MLT with custom key_field uses that column in SQL."""
    query = MockItem.objects.filter(
        id=ParadeDB(
            MoreLikeThis(
                id=3,
                fields=["description"],
                key_field="id",
            )
        )
    )

    sql, _ = query.query.sql_with_params()

    _assert_sql(
        sql,
        """
        SELECT "mock_items"."id", "mock_items"."description", "mock_items"."category", "mock_items"."rating", "mock_items"."in_stock", "mock_items"."created_at", "mock_items"."metadata"
        FROM "mock_items"
        WHERE "mock_items"."id" @@@ pdb.more_like_this(%s, ARRAY[%s]::text[])
        """,
    )
    # Verify the query executes without error
    ids = _ids(query)
    assert 3 in ids


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


def test_more_like_this_word_length_min_greater_than_max() -> None:
    """MLT accepts min_word_length > max_word_length (ParadeDB handles validation)."""
    # ParadeDB may handle this at runtime, but Django should accept it
    # This test documents the current behavior
    mlt = MoreLikeThis(
        id=3,
        fields=["description"],
        min_word_length=10,
        max_word_length=5,
    )

    # Should create the MLT object without error
    assert mlt.min_word_length == 10
    assert mlt.max_word_length == 5

    # Generate SQL to verify it compiles
    query = MockItem.objects.filter(id=ParadeDB(mlt))
    sql, _params = query.query.sql_with_params()

    _assert_sql(
        sql,
        """
        SELECT "mock_items"."id", "mock_items"."description", "mock_items"."category", "mock_items"."rating", "mock_items"."in_stock", "mock_items"."created_at", "mock_items"."metadata"
        FROM "mock_items"
        WHERE "mock_items"."id" @@@ pdb.more_like_this(%s, ARRAY[%s]::text[], min_word_length => 10, max_word_length => 5)
        """,
    )


def test_multi_term_fuzzy_match_or() -> None:
    """FUZZY-1: Match with multiple terms and OR operator uses fuzzy on array."""
    # This should generate: ARRAY['runing', 'shose']::pdb.fuzzy(2)
    # NOT: ARRAY['runing'::pdb.fuzzy(2), 'shose'::pdb.fuzzy(2)]
    queryset = MockItem.objects.filter(
        description=ParadeDB(Match("runing", "shose", operator="OR", distance=2))
    )
    _assert_sql(
        str(queryset.query),
        """
        SELECT "mock_items"."id", "mock_items"."description", "mock_items"."category", "mock_items"."rating", "mock_items"."in_stock", "mock_items"."created_at", "mock_items"."metadata"
        FROM "mock_items"
        WHERE "mock_items"."description" ||| ARRAY['runing', 'shose']::pdb.fuzzy(2)
        """,
    )
    ids = _ids(queryset)
    # Should match item 3 (running shoes) with fuzzy distance 2
    assert 3 in ids


def test_multi_term_fuzzy_match_and() -> None:
    """FUZZY-2: Match with multiple terms and AND operator uses fuzzy on array."""
    queryset = MockItem.objects.filter(
        description=ParadeDB(Match("runing", "shose", operator="AND", distance=2))
    )
    _assert_sql(
        str(queryset.query),
        """
        SELECT "mock_items"."id", "mock_items"."description", "mock_items"."category", "mock_items"."rating", "mock_items"."in_stock", "mock_items"."created_at", "mock_items"."metadata"
        FROM "mock_items"
        WHERE "mock_items"."description" &&& ARRAY['runing', 'shose']::pdb.fuzzy(2)
        """,
    )
    ids = _ids(queryset)
    # Should match item 3 (running shoes) with fuzzy distance 2
    assert 3 in ids


def test_multi_term_fuzzy_match_and_prefix() -> None:
    """FUZZY-5: Match with multiple terms, AND operator, distance=1, and prefix."""
    queryset = MockItem.objects.filter(
        description=ParadeDB(
            Match("slee", "rann", operator="AND", distance=1, prefix=True)
        )
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
    # Should match item 3 (sleek running) with prefix fuzzy
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
        description=ParadeDB(
            Match("running shoes", operator="AND", tokenizer=tokenizer)
        )
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
