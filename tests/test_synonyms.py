"""Integration tests for synonym expansion with `Expand`."""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

import pytest
from django.db import DEFAULT_DB_ALIAS, connection, connections

from paradedb.search import (
    Boost,
    Expand,
    Fuzzy,
    MatchAll,
    MatchAny,
    ParadeDB,
    Parse,
    Phrase,
)
from tests.models import MockItem, ShopSynonym, Synonym

pytestmark = [
    pytest.mark.integration,
    pytest.mark.django_db(transaction=True),
]

_SELECT_MOCK_ITEMS = (
    'SELECT "mock_items"."id", "mock_items"."description", "mock_items"."category", '
    '"mock_items"."rating", "mock_items"."in_stock", "mock_items"."created_at", '
    '"mock_items"."metadata", "mock_items"."embedding" FROM "mock_items"'
)

_SHOP_RULES = '(SELECT pdb.expand(\'%s\', U0."rule") AS "paradedb_expansion" FROM "shop_synonyms" U0)'

# Each case is a query, the SQL of its right side, its rows and its synonym index.
_CASES = {
    "whole_table": (
        MatchAny(Expand("couch", ShopSynonym, "rule")),
        f"||| {_SHOP_RULES % 'couch'}",
        {24},
        "shop_synonyms_idx",
    ),
    # The other set also expands `sneakers` and `white`, so its rules would add row 2.
    "set": (
        MatchAll(
            Expand("white sneakers", Synonym.objects.filter(set_name="shop"), "rule")
        ),
        '&&& (SELECT pdb.expand(\'white sneakers\', U0."rule") AS "paradedb_expansion" '
        'FROM "synonyms" U0 WHERE U0."set_name" = \'shop\')',
        {4},
        "synonyms_shop_idx",
    ),
    "phrase": (
        Phrase(Expand("leather couch", ShopSynonym, "rule")),
        f"### {_SHOP_RULES % 'leather couch'}",
        {24},
        "shop_synonyms_idx",
    ),
    "parse": (
        Parse(Expand("leather AND couch", ShopSynonym, "rule")),
        f"@@@ {_SHOP_RULES % 'leather AND couch'}",
        {24},
        "shop_synonyms_idx",
    ),
    "fuzzy_and_boost": (
        MatchAny(Boost(Fuzzy(Expand("couch keyboards", ShopSynonym, "rule"), 1), 2.0)),
        f"||| {_SHOP_RULES % 'couch keyboards'}::pdb.fuzzy(1)::pdb.boost(2.0)",
        {1, 2, 24},
        "shop_synonyms_idx",
    ),
}


@pytest.fixture(scope="module")
def synonym_tables(paradedb_ready: None, django_db_blocker: Any) -> Iterator[None]:
    _ = paradedb_ready
    with django_db_blocker.unblock(), connection.cursor() as cursor:
        for statement in (
            "DROP TABLE IF EXISTS shop_synonyms, synonyms",
            "CREATE TABLE shop_synonyms (id bigserial PRIMARY KEY, rule pdb.synonym_rule NOT NULL)",
            "INSERT INTO shop_synonyms (rule) VALUES ('couch, sofa, divan'), ('sneakers, shoes')",
            "CREATE INDEX shop_synonyms_idx ON shop_synonyms USING paradedb (rule)",
            "CREATE TABLE synonyms (id bigserial PRIMARY KEY, set_name text NOT NULL, rule pdb.synonym_rule NOT NULL)",
            "INSERT INTO synonyms (set_name, rule) VALUES "
            "('shop', 'sneakers, shoes'), ('other', 'sneakers, keyboard'), ('other', 'white, plastic')",
            "CREATE INDEX synonyms_shop_idx ON synonyms USING paradedb (rule) WHERE set_name = 'shop'",
        ):
            cursor.execute(statement)
    yield
    with django_db_blocker.unblock(), connection.cursor() as cursor:
        cursor.execute("DROP TABLE IF EXISTS shop_synonyms, synonyms")


def _search(term: Any) -> Any:
    return MockItem.objects.filter(description=ParadeDB(term))


def _assert_where(queryset: Any, expected: str) -> None:
    sql, params = queryset.query.sql_with_params()
    assert params == ()
    assert " ".join(sql.split()) == (
        f'{_SELECT_MOCK_ITEMS} WHERE "mock_items"."description" {expected}'
    )


def _ids(queryset: Any) -> set[int]:
    return set(queryset.values_list("id", flat=True))


@pytest.mark.parametrize("case", _CASES)
def test_expand_renders_the_subquery_with_literals(case: str) -> None:
    term, expected, _, _ = _CASES[case]
    _assert_where(_search(term), expected)


def test_expand_with_an_empty_queryset_renders_the_text() -> None:
    _assert_where(
        _search(MatchAny(Expand("sofa", Synonym.objects.none(), "rule"))), "||| 'sofa'"
    )


@pytest.mark.usefixtures("synonym_tables")
@pytest.mark.parametrize("case", _CASES)
def test_expand_reads_the_synonym_index(case: str) -> None:
    term, _, ids, index = _CASES[case]
    queryset = _search(term)
    assert _ids(queryset) == ids
    assert f"Synonyms: {index}" in queryset.explain()


@pytest.mark.usefixtures("synonym_tables")
def test_expand_with_no_rule_searches_the_text_as_typed() -> None:
    for rules in (Synonym.objects.none(), Synonym.objects.filter(set_name="none")):
        assert _ids(_search(MatchAny(Expand("sofa", rules, "rule")))) == {24}


@pytest.mark.usefixtures("synonym_tables")
def test_expand_set_filter_picks_the_partial_index_in_a_generic_plan() -> None:
    settings = {
        **connection.settings_dict,
        "OPTIONS": {**connection.settings_dict["OPTIONS"], "server_side_binding": True},
    }
    server_binding = type(connections[DEFAULT_DB_ALIAS])(
        settings, alias="server_side_binding"
    )
    # psycopg prepares only a statement that has a parameter, so the rating
    # filter gives it one.
    queryset = MockItem.objects.filter(
        rating__gte=1,
        description=ParadeDB(
            MatchAny(
                Expand("sneakers", Synonym.objects.filter(set_name="shop"), "rule")
            )
        ),
    ).values_list("id", flat=True)
    sql, params = queryset.query.get_compiler(connection=server_binding).as_sql()
    assert params == (1,)
    try:
        with server_binding.cursor() as cursor:
            # A generic plan has no values for bind parameters, and the error
            # setting fails the query if no synonym index fits.
            cursor.execute("SET plan_cache_mode = force_generic_plan")
            cursor.execute("SET paradedb.planner_warnings = 'error'")
            cursor.execute(sql, params)
            ids = {row[0] for row in cursor.fetchall()}
    finally:
        server_binding.close()
    assert ids == {3, 4, 5}
