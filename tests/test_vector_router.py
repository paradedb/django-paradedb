import pytest
from django.db import connection

from paradedb import ParadeDBIndex
from tests.models import MockItem


@pytest.mark.parametrize("router", ["graph", "ivf"])
@pytest.mark.django_db(transaction=True)
@pytest.mark.integration
@pytest.mark.usefixtures("paradedb_ready")
def test_vector_router_index_and_migration_round_trip(router):
    index = ParadeDBIndex(
        fields={"id": {}, "embedding": {"metric": "l2"}},
        name="router_idx",
        vector_router=router,
    )
    _, _, kwargs = index.deconstruct()
    assert kwargs["vector_router"] == router
    assert ParadeDBIndex(**kwargs) == index
    ddl = str(index.create_sql(MockItem, connection.schema_editor())).replace(
        '"mock_items"', '"router_items"'
    )
    with connection.cursor() as cursor:
        cursor.execute("CREATE TABLE router_items (id int, embedding vector(64))")
        try:
            cursor.execute(ddl)
            cursor.execute(
                "SELECT reloptions FROM pg_class WHERE oid = 'router_idx'::regclass"
            )
            assert f"vector_router={router}" in cursor.fetchone()[0]
        finally:
            cursor.execute("DROP TABLE router_items CASCADE")


def test_vector_router_default_and_validation():
    index = ParadeDBIndex(fields={"id": {}}, name="default_idx")
    assert "vector_router" not in index.deconstruct()[2]
    assert "vector_router" not in str(
        index.create_sql(MockItem, connection.schema_editor())
    )
    with pytest.raises(ValueError, match="vector_router"):
        ParadeDBIndex(fields={"id": {}}, name="bad_idx", vector_router="invalid")
