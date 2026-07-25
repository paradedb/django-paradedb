"""Vector search support: field serialization, distance SQL, and index DDL.

The SQL-generation tests are unit tests that don't require a database.
Integration tests are gated on bm25 vector opclass support, which is not
yet in a released pg_search (it lives on branch ``mvp/vector-search``).
"""

from __future__ import annotations

from collections.abc import Iterator

import pytest
from django.db import connection
from django.db.migrations.writer import MigrationWriter
from django.db.models import F

from paradedb.indexes import ParadeDBIndex
from paradedb.search import All, ParadeDB
from paradedb.vector import (
    CosineDistance,
    InnerProduct,
    L2Distance,
    VectorField,
    parse_vector,
    serialize_vector,
)
from tests.models import VectorItem


def _schema_editor():
    return connection.schema_editor(collect_sql=True)


class TestVectorField:
    def test_db_type_with_dimensions(self) -> None:
        assert VectorField(dimensions=384).db_type(connection) == "vector(384)"

    def test_db_type_without_dimensions(self) -> None:
        assert VectorField().db_type(connection) == "vector"

    @pytest.mark.parametrize("dimensions", [0, -1, True, 1.5, "3"])
    def test_invalid_dimensions_raise(self, dimensions: object) -> None:
        with pytest.raises(ValueError, match="positive integer"):
            VectorField(dimensions=dimensions)  # type: ignore[arg-type]

    def test_get_prep_value_serializes_sequence(self) -> None:
        assert VectorField().get_prep_value([1, 2.5, -3]) == "[1.0,2.5,-3.0]"

    def test_get_prep_value_passes_through_none_and_str(self) -> None:
        field = VectorField()
        assert field.get_prep_value(None) is None
        assert field.get_prep_value("[1,2,3]") == "[1,2,3]"

    def test_from_db_value_parses_text(self) -> None:
        field = VectorField()
        assert field.from_db_value("[1,2.5,-3]", None, connection) == [1.0, 2.5, -3.0]  # type: ignore[arg-type]
        assert field.from_db_value(None, None, connection) is None  # type: ignore[arg-type]

    def test_to_python(self) -> None:
        field = VectorField()
        assert field.to_python("[1,2]") == [1.0, 2.0]
        assert field.to_python([1.0, 2.0]) == [1.0, 2.0]
        assert field.to_python(None) is None

    def test_serialize_parse_roundtrip(self) -> None:
        assert parse_vector(serialize_vector([0.25, -1.5, 3])) == [0.25, -1.5, 3.0]
        assert parse_vector("[]") == []

    def test_deconstruct_includes_dimensions(self) -> None:
        field = VectorField(dimensions=3, null=True)
        field.set_attributes_from_name("embedding")
        name, path, args, kwargs = field.deconstruct()
        assert name == "embedding"
        assert path == "paradedb.vector.VectorField"
        assert args == []
        assert kwargs == {"dimensions": 3, "null": True}

    def test_migration_writer_serializes_field(self) -> None:
        field = VectorField(dimensions=3)
        field.set_attributes_from_name("embedding")
        serialized, imports = MigrationWriter.serialize(field)
        assert serialized == "paradedb.vector.VectorField(dimensions=3)"
        assert "import paradedb.vector" in imports


class TestVectorDistanceSQL:
    def test_l2_distance_order_by(self) -> None:
        queryset = VectorItem.objects.order_by(L2Distance("embedding", [1, 0, 0]))[:5]
        sql, params = queryset.query.sql_with_params()
        assert 'ORDER BY ("vector_items"."embedding" <-> %s::vector) ASC' in sql
        assert "LIMIT 5" in sql
        assert params[-1] == "[1.0,0.0,0.0]"

    def test_cosine_distance_order_by(self) -> None:
        queryset = VectorItem.objects.order_by(CosineDistance("embedding", [1, 0, 0]))[
            :5
        ]
        sql, params = queryset.query.sql_with_params()
        assert 'ORDER BY ("vector_items"."embedding" <=> %s::vector) ASC' in sql
        assert params[-1] == "[1.0,0.0,0.0]"

    def test_inner_product_order_by(self) -> None:
        queryset = VectorItem.objects.order_by(InnerProduct("embedding", [1, 0, 0]))[:5]
        sql, params = queryset.query.sql_with_params()
        assert 'ORDER BY ("vector_items"."embedding" <#> %s::vector) ASC' in sql
        assert params[-1] == "[1.0,0.0,0.0]"

    def test_distance_annotation(self) -> None:
        queryset = VectorItem.objects.annotate(
            distance=L2Distance(F("embedding"), [0.5, 0.5, 0.5])
        ).order_by("distance")[:3]
        sql, params = queryset.query.sql_with_params()
        assert '("vector_items"."embedding" <-> %s::vector) AS "distance"' in sql
        assert "ORDER BY 4 ASC" in sql
        assert params[-1] == "[0.5,0.5,0.5]"

    def test_topk_query_with_match_all_predicate(self) -> None:
        queryset = VectorItem.objects.filter(id=ParadeDB(All())).order_by(
            L2Distance("embedding", [1, 0, 0])
        )[:2]
        sql, params = queryset.query.sql_with_params()
        assert '"vector_items"."id" @@@ pdb.all()' in sql
        assert 'ORDER BY ("vector_items"."embedding" <-> %s::vector) ASC' in sql
        assert "LIMIT 2" in sql
        assert params[-1] == "[1.0,0.0,0.0]"


class TestVectorIndexSQL:
    @pytest.mark.parametrize(
        ("metric", "opclass"),
        [
            ("l2", "vector_l2_ops"),
            ("cosine", "vector_cosine_ops"),
            ("ip", "vector_ip_ops"),
        ],
    )
    def test_index_with_metric_emits_opclass(self, metric: str, opclass: str) -> None:
        index = ParadeDBIndex(
            fields={"id": {}, "embedding": {"metric": metric}},
            key_field="id",
            name="vector_items_search_idx",
        )
        sql = str(index.create_sql(model=VectorItem, schema_editor=_schema_editor()))
        assert (
            sql
            == f'CREATE INDEX "vector_items_search_idx" ON "vector_items"\nUSING paradedb (\n    "id",\n    "embedding" {opclass}\n)\nWITH (key_field=\'id\')'
        )

    def test_index_without_metric_emits_plain_column(self) -> None:
        index = ParadeDBIndex(
            fields={"id": {}, "embedding": {}},
            key_field="id",
            name="vector_items_search_idx",
        )
        sql = str(index.create_sql(model=VectorItem, schema_editor=_schema_editor()))
        assert '"embedding"\n' in sql
        assert "vector_l2_ops" not in sql

    def test_invalid_metric_raises(self) -> None:
        index = ParadeDBIndex(
            fields={"id": {}, "embedding": {"metric": "hamming"}},
            key_field="id",
            name="vector_items_search_idx",
        )
        with pytest.raises(ValueError, match="Vector metric must be one of"):
            index.create_sql(model=VectorItem, schema_editor=_schema_editor())

    def test_metric_on_non_vector_field_raises(self) -> None:
        index = ParadeDBIndex(
            fields={"id": {}, "description": {"metric": "l2"}},
            key_field="id",
            name="vector_items_search_idx",
        )
        with pytest.raises(ValueError, match="is not a VectorField"):
            index.create_sql(model=VectorItem, schema_editor=_schema_editor())

    def test_metric_mixed_with_tokenizer_raises(self) -> None:
        from paradedb.search import Tokenizer

        index = ParadeDBIndex(
            fields={
                "id": {},
                "embedding": {"metric": "l2", "tokenizer": Tokenizer.simple()},
            },
            key_field="id",
            name="vector_items_search_idx",
        )
        with pytest.raises(ValueError, match="cannot mix 'metric'"):
            index.create_sql(model=VectorItem, schema_editor=_schema_editor())

    def test_index_with_metric_deconstructs(self) -> None:
        index = ParadeDBIndex(
            fields={"id": {}, "embedding": {"metric": "cosine"}},
            key_field="id",
            name="vector_items_search_idx",
        )
        _path, _args, kwargs = index.deconstruct()
        assert kwargs["fields"] == {"id": {}, "embedding": {"metric": "cosine"}}

        serialized, _imports = MigrationWriter.serialize(index)
        assert "'metric': 'cosine'" in serialized


def _paradedb_supports_vector_opclasses() -> bool:
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT 1 FROM pg_opclass o JOIN pg_am a ON o.opcmethod = a.oid "
            "WHERE a.amname = 'paradedb' AND o.opcname = 'vector_l2_ops';"
        )
        return cursor.fetchone() is not None


@pytest.mark.integration
@pytest.mark.django_db(transaction=True)
class TestVectorSearchIntegration:
    @pytest.fixture(autouse=True)
    def vector_items(self, paradedb_ready: None) -> Iterator[None]:
        _ = paradedb_ready
        if not _paradedb_supports_vector_opclasses():
            pytest.skip(
                "bm25 vector opclasses are not available in this pg_search build "
                "(vector-in-bm25 support is unreleased; branch mvp/vector-search)"
            )
        with connection.cursor() as cursor:
            cursor.execute("CREATE EXTENSION IF NOT EXISTS vector;")
            cursor.execute("DROP TABLE IF EXISTS vector_items;")
            cursor.execute(
                "CREATE TABLE vector_items ("
                "id integer PRIMARY KEY, description text, embedding vector(3));"
            )
        index = ParadeDBIndex(
            fields={"id": {}, "description": {}, "embedding": {"metric": "l2"}},
            key_field="id",
            name="vector_items_bm25_idx",
        )
        with connection.schema_editor(atomic=False) as editor:
            editor.execute(index.create_sql(model=VectorItem, schema_editor=editor))
        VectorItem.objects.bulk_create(
            [
                VectorItem(id=1, description="red", embedding=[1, 0, 0]),
                VectorItem(id=2, description="green", embedding=[0, 1, 0]),
                VectorItem(id=3, description="blue", embedding=[0, 0, 1]),
            ]
        )
        connection.commit()
        yield
        with connection.cursor() as cursor:
            cursor.execute("DROP TABLE IF EXISTS vector_items;")
        connection.commit()

    def test_embedding_roundtrip(self) -> None:
        item = VectorItem.objects.get(id=1)
        assert item.embedding == [1.0, 0.0, 0.0]

    def test_topk_l2_query(self) -> None:
        results = list(
            VectorItem.objects.filter(id=ParadeDB(All())).order_by(
                L2Distance("embedding", [0.9, 0.1, 0.0])
            )[:2]
        )
        assert [item.id for item in results] == [1, 2]

    def test_topk_uses_paradedb_scan(self) -> None:
        queryset = VectorItem.objects.filter(id=ParadeDB(All())).order_by(
            L2Distance("embedding", [1, 0, 0])
        )[:2]
        sql, params = queryset.query.sql_with_params()
        with connection.cursor() as cursor:
            cursor.execute(f"EXPLAIN {sql}", params)
            plan = "\n".join(row[0] for row in cursor.fetchall())
        assert "ParadeDB" in plan, plan
