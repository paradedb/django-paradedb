import json

import pytest
from django.db import connection

from paradedb import (
    Agg,
    ParadeDBIndex,
    Tokenizer,
    paradedb_vector_config,
    paradedb_vector_estimator_info,
    paradedb_vector_info,
)
from tests.models import MockItem


@pytest.mark.django_db(transaction=True)
@pytest.mark.integration
@pytest.mark.usefixtures("paradedb_ready")
def test_partitioning_quantization_and_diagnostics():
    index = ParadeDBIndex(
        fields={
            "id": {},
            "rating": {},
            "description": {
                "tokenizers": [
                    {"tokenizer": Tokenizer.simple(options={"pnorms": True})},
                    {
                        "tokenizer": Tokenizer.jieba(
                            options={"alias": "description_jieba", "search_mode": False}
                        )
                    },
                    {
                        "tokenizer": Tokenizer.chinese_compatible(
                            options={
                                "alias": "description_chinese",
                                "chinese_convert": "t2s",
                            }
                        )
                    },
                ]
            },
            "embedding": {"metric": "l2"},
        },
        name="pg26_idx",
        vector_router="ivf",
        partition_by="rating,id",
        target_segment_count=8,
        vector_fields={"embedding": {"quantization": False}},
    )
    _, _, kwargs = index.deconstruct()
    assert kwargs["partition_by"] == "rating,id"
    assert ParadeDBIndex(**kwargs) == index
    ddl = str(index.create_sql(MockItem, connection.schema_editor())).replace(
        '"mock_items"', '"pg26_items"'
    )
    with connection.cursor() as cursor:
        cursor.execute(
            "CREATE TABLE pg26_items (id int, rating int, description text, embedding vector(64))"
        )
        cursor.execute(
            "INSERT INTO pg26_items SELECT i, i % 3, 'partitioned shoes', ARRAY(SELECT sin(i*j)::real FROM generate_series(1,64) j)::vector FROM generate_series(1, 2048) i"
        )
        cursor.execute(ddl)
        cursor.execute(
            "SELECT reloptions FROM pg_class WHERE oid = 'pg26_idx'::regclass"
        )
        options = dict(option.split("=", 1) for option in cursor.fetchone()[0])
        assert options["partition_by"] == "rating,id"
        assert options["target_segment_count"] == "8"
        assert (
            json.loads(options["vector_fields"])["embedding"]["quantization"] is False
        )
    assert paradedb_vector_config("pg26_idx", "embedding")[0]["quantized"] is False
    assert paradedb_vector_info("pg26_idx", "embedding")
    with connection.cursor() as cursor:
        cursor.execute(
            'ALTER INDEX pg26_idx SET (target_segment_count = 1, max_leaf_size = 16, vector_fields = \'{"embedding":{"quantization":true}}\')'
        )
        cursor.execute("REINDEX INDEX pg26_idx")
    assert paradedb_vector_config("pg26_idx", "embedding")[0]["quantized"] is True
    assert isinstance(paradedb_vector_estimator_info("pg26_idx", "embedding"), list)
    assert isinstance(
        paradedb_vector_estimator_info("pg26_idx", "embedding", [[0.1] * 64]), list
    )
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT COUNT(*) FROM pg26_items WHERE description @@@ 'shoes' AND rating = 1"
        )
        assert cursor.fetchone()[0] == 683
        cursor.execute("DROP TABLE pg26_items")


@pytest.mark.parametrize("visibility", ["transaction", "raw", "threshold"])
@pytest.mark.django_db
@pytest.mark.integration
@pytest.mark.usefixtures("mock_items")
def test_aggregate_visibility(visibility):
    value = MockItem.objects.aggregate(
        result=Agg('{"value_count":{"field":"id"}}', visibility=visibility)
    )
    assert value["result"]["value"] == MockItem.objects.count()


def test_invalid_partition_and_visibility():
    with pytest.raises(ValueError, match="partition_by"):
        ParadeDBIndex(fields={"id": {}}, name="bad", partition_by="id,")
    with pytest.raises(ValueError, match="visibility"):
        Agg("{}", visibility="invalid")
    with pytest.raises(ValueError, match="not both"):
        Agg("{}", visibility="raw", exact=False)
