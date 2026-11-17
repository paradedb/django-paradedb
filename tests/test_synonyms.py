"""Integration tests for synonym expansion with `Expand`."""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

import pytest
from django.db import DEFAULT_DB_ALIAS, connection, connections

from paradedb.search import Boost, Expand, Fuzzy, MatchAll, MatchAny, ParadeDB
from tests.models import MockItem, ShopSynonym, Synonym

pytestmark = [
    pytest.mark.integration,
    pytest.mark.django_db(transaction=True),
    pytest.mark.usefixtures("synonym_tables"),
]

_SELECT_MOCK_ITEMS = (
    'SELECT "mock_items"."id", "mock_items"."description", "mock_items"."category", '
    '"mock_items"."rating", "mock_items"."in_stock", "mock_items"."created_at", '
    '"mock_items"."metadata", "mock_items"."embedding" FROM "mock_items"'
)


@pytest.fixture(scope="module")
def synonym_tables(paradedb_ready: None, django_db_blocker: Any) -> Iterator[None]:
    _ = paradedb_ready
    with django_db_blocker.unblock(), connection.cursor() as cursor:
        cursor.execute("SELECT to_regprocedure('pdb.expand(text, text)') IS NOT NULL")
        (has_expand,) = cursor.fetchone()
        if not has_expand:
            pytest.skip("this ParadeDB has no pdb.expand")
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


def _assert_sql(sql: str, expected: str) -> None:
    assert " ".join(sql.split()) == " ".join(expected.split())


def _ids(queryset: Any) -> set[int]:
    return set(queryset.values_list("id", flat=True))


def test_expand_with_a_synonym_index_on_the_whole_table() -> None:
    queryset = MockItem.objects.filter(
        description=ParadeDB(MatchAny(Expand("couch", ShopSynonym, "rule")))
    )
    _assert_sql(
        str(queryset.query),
        f"""
        {_SELECT_MOCK_ITEMS}
        WHERE "mock_items"."description" ||| (SELECT pdb.expand('couch', U0."rule") AS "query" FROM "shop_synonyms" U0)
        """,
    )
    assert _ids(queryset) == {24}
    assert "Synonyms: shop_synonyms_idx" in queryset.explain()


def test_expand_with_a_partial_synonym_index_per_set() -> None:
    # The other set also expands `sneakers` and `white`, so its rules would add row 2.
    queryset = MockItem.objects.filter(
        description=ParadeDB(
            MatchAll(
                Expand(
                    "white sneakers", Synonym.objects.filter(set_name="shop"), "rule"
                )
            )
        )
    )
    sql, params = queryset.query.sql_with_params()
    _assert_sql(
        sql,
        f"""
        {_SELECT_MOCK_ITEMS}
        WHERE "mock_items"."description" &&& (SELECT pdb.expand('white sneakers', U0."rule") AS "query" FROM "synonyms" U0 WHERE U0."set_name" = 'shop')
        """,
    )
    assert params == ()
    assert _ids(queryset) == {4}
    assert "Synonyms: synonyms_shop_idx" in queryset.explain()


def test_expand_set_filter_picks_the_partial_index_with_server_side_binding() -> None:
    settings = {
        **connection.settings_dict,
        "OPTIONS": {**connection.settings_dict["OPTIONS"], "server_side_binding": True},
    }
    server_binding = type(connections[DEFAULT_DB_ALIAS])(
        settings, alias="server_side_binding"
    )
    queryset = MockItem.objects.filter(
        description=ParadeDB(
            MatchAny(
                Expand("sneakers", Synonym.objects.filter(set_name="shop"), "rule")
            )
        )
    ).values_list("id", flat=True)
    sql, params = queryset.query.get_compiler(connection=server_binding).as_sql()
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


def test_expand_composes_with_fuzzy_and_boost() -> None:
    queryset = MockItem.objects.filter(
        description=ParadeDB(
            MatchAny(
                Boost(Fuzzy(Expand("couch keyboards", ShopSynonym, "rule"), 1), 2.0)
            )
        )
    )
    _assert_sql(
        str(queryset.query),
        f"""
        {_SELECT_MOCK_ITEMS}
        WHERE "mock_items"."description" ||| (SELECT pdb.expand('couch keyboards', U0."rule") AS "query" FROM "shop_synonyms" U0)::pdb.fuzzy(1)::pdb.boost(2.0)
        """,
    )
    assert _ids(queryset) == {1, 2, 24}
    assert "Synonyms: shop_synonyms_idx" in queryset.explain()
