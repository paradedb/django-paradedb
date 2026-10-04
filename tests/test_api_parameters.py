import pytest
from django.db import DatabaseError, connection

from paradedb import (
    BooleanQuery,
    DisjunctionMax,
    MatchAll,
    ParadeDB,
    ParadeDBIndex,
    SearchQuery,
    Snippet,
    SnippetPositions,
    Tokenizer,
    paradedb_aggregate,
)
from tests.models import MockItem

pytestmark = [
    pytest.mark.integration,
    pytest.mark.django_db(transaction=True),
    pytest.mark.usefixtures("mock_items"),
]


def test_nested_query_inputs_and_direct_aggregate():
    conjunction = BooleanQuery(
        should=["description:running", "description:shoes"], minimum_should_match=2
    )
    expected = MockItem.objects.filter(
        description=ParadeDB(MatchAll("running shoes"))
    ).count()
    assert expected > 0
    assert MockItem.objects.filter(id=ParadeDB(conjunction)).count() == expected
    query = BooleanQuery(
        must=[DisjunctionMax([conjunction, "description:boots"], tie_breaker=0.5)],
        must_not=["description:sandals"],
    )
    count = MockItem.objects.filter(id=ParadeDB(query)).count()
    assert count > 0
    result = paradedb_aggregate(
        "mock_items_search_idx",
        query,
        {"count": {"value_count": {"field": "id"}}},
        memory_limit=10000000,
        bucket_limit=100,
        visibility="transaction",
    )
    assert result["count"]["value"] == count
    assert (
        MockItem.objects.filter(
            id=ParadeDB(SearchQuery.parse('description:"O\'Reilly"'))
        ).count()
        == 0
    )


@pytest.mark.parametrize(
    "options",
    [
        {"max_num_chars": 20},
        {"stop_sel": "</mark>"},
        {"start_sel": "<mark>"},
        {"limit": 1, "offset": 1},
    ],
)
def test_snippet_named_options_preserve_server_defaults(options):
    rows = list(
        MockItem.objects.filter(id=ParadeDB(SearchQuery.parse("description:shoes")))
        .annotate(snippet=Snippet("description", **options))
        .values_list("id", "snippet")
        .order_by("id")
    )
    names = {"start_sel": "start_tag", "stop_sel": "end_tag"}
    args = ", ".join(f'"{names.get(name, name)}" => %s' for name in options)
    with connection.cursor() as cursor:
        cursor.execute(
            f"SELECT id, pdb.snippet(description, {args}) FROM mock_items WHERE id @@@ paradedb.parse(%s) ORDER BY id",
            [*options.values(), "description:shoes"],
        )
        assert rows == cursor.fetchall()


def test_snippet_position_pagination():
    query = MockItem.objects.filter(
        id=ParadeDB(SearchQuery.parse("description:shoes"))
    ).annotate(positions=SnippetPositions("description", limit=0, offset=1))
    assert query.exists()
    actual = list(query.values_list("id", "positions").order_by("id"))
    with connection.cursor() as cursor:
        cursor.execute(
            'SELECT id, pdb.snippet_positions(description, "limit" => 0, "offset" => 1) FROM mock_items WHERE id @@@ paradedb.parse(%s) ORDER BY id',
            ["description:shoes"],
        )
        assert actual == cursor.fetchall()


def test_index_tuning_round_trip():
    options = {
        "search_tokenizer": Tokenizer.simple(options={"lowercase": False}),
        "layer_sizes": "0",
        "background_layer_sizes": "100MB, 1GB",
        "mutable_segment_rows": 1000,
    }
    index = ParadeDBIndex(
        fields={"id": {}, "description": {}}, name="api_options_idx", **options
    )
    _, _, kwargs = index.deconstruct()
    assert ParadeDBIndex(**kwargs) == index
    with connection.cursor() as cursor:
        cursor.execute("CREATE TABLE api_options_items (id int, description text)")
        ddl = str(index.create_sql(MockItem, connection.schema_editor())).replace(
            '"mock_items"', '"api_options_items"'
        )
        cursor.execute(ddl)
        try:
            cursor.execute(
                "SELECT reloptions FROM pg_class WHERE oid = 'api_options_idx'::regclass"
            )
            actual = dict(value.split("=", 1) for value in cursor.fetchone()[0])
            assert actual["search_tokenizer"] == "simple(lowercase=false)"
            assert actual["layer_sizes"] == "0"
            assert actual["background_layer_sizes"] == "100MB, 1GB"
            assert actual["mutable_segment_rows"] == "1000"
        finally:
            cursor.execute("DROP TABLE api_options_items")


def test_invalid_api_parameters():
    with pytest.raises(ValueError, match="minimum_should_match"):
        BooleanQuery(minimum_should_match=-1)
    with pytest.raises(ValueError, match="tie_breaker"):
        DisjunctionMax(["description:shoes"], tie_breaker=float("nan"))
    with pytest.raises(ValueError, match="must not be empty"):
        DisjunctionMax([])
    with pytest.raises(ValueError, match="offset"):
        SnippetPositions("description", offset=-1)
    with pytest.raises(ValueError, match="mutable_segment_rows"):
        ParadeDBIndex(fields={"id": {}}, name="invalid_idx", mutable_segment_rows=10001)
    with pytest.raises(ValueError, match="not both"):
        paradedb_aggregate("idx", "*", {}, solve_mvcc=True, visibility="raw")


def test_direct_aggregate_enforces_bucket_limit():
    with pytest.raises(DatabaseError, match="bucket limit was exceeded"):
        paradedb_aggregate(
            "mock_items_search_idx",
            "description:shoes",
            {"ids": {"terms": {"field": "id", "size": 10}}},
            memory_limit=10000000,
            bucket_limit=1,
        )
