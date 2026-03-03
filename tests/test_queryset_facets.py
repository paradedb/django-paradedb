"""Unit tests for ParadeDBQuerySet facet row extraction helpers."""

from __future__ import annotations

from types import SimpleNamespace

from paradedb.queryset import ParadeDBQuerySet


def test_extract_facets_multi_single_alias_dict_rows() -> None:
    rows = [
        {"id": 1, "_paradedb_facets": {"buckets": [{"key": "a"}]}},
        {"id": 2, "_paradedb_facets": {"buckets": [{"key": "a"}]}},
    ]
    facets = ParadeDBQuerySet._extract_facets_multi(rows, ["_paradedb_facets"])
    assert facets == {"buckets": [{"key": "a"}]}
    assert rows == [{"id": 1}, {"id": 2}]


def test_extract_facets_multi_multi_alias_dict_rows() -> None:
    rows = [
        {
            "id": 1,
            "rating_terms": {"buckets": [{"key": 5}]},
            "category_terms": {"buckets": [{"key": "Footwear"}]},
        },
        {
            "id": 2,
            "rating_terms": {"buckets": [{"key": 4}]},
            "category_terms": {"buckets": [{"key": "Electronics"}]},
        },
    ]
    facets = ParadeDBQuerySet._extract_facets_multi(
        rows, ["rating_terms", "category_terms"]
    )
    assert facets == {
        "rating_terms": {"buckets": [{"key": 5}]},
        "category_terms": {"buckets": [{"key": "Footwear"}]},
    }
    assert rows == [{"id": 1}, {"id": 2}]


def test_extract_facets_multi_single_alias_tuple_rows() -> None:
    rows = [(1, {"buckets": [{"key": "a"}]}), (2, {"buckets": [{"key": "a"}]})]
    facets = ParadeDBQuerySet._extract_facets_multi(rows, ["_paradedb_facets"])
    assert facets == {"buckets": [{"key": "a"}]}
    assert rows == [(1,), (2,)]


def test_extract_facets_multi_multi_alias_tuple_rows() -> None:
    rows = [
        (1, "A", {"buckets": [{"key": 5}]}, {"buckets": [{"key": "Footwear"}]}),
        (2, "B", {"buckets": [{"key": 4}]}, {"buckets": [{"key": "Electronics"}]}),
    ]
    facets = ParadeDBQuerySet._extract_facets_multi(
        rows, ["rating_terms", "category_terms"]
    )
    assert facets == {
        "rating_terms": {"buckets": [{"key": 5}]},
        "category_terms": {"buckets": [{"key": "Footwear"}]},
    }
    assert rows == [(1, "A"), (2, "B")]


def test_extract_facets_multi_single_alias_object_rows() -> None:
    rows = [
        SimpleNamespace(id=1, _paradedb_facets={"buckets": [{"key": "x"}]}),
        SimpleNamespace(id=2, _paradedb_facets={"buckets": [{"key": "x"}]}),
    ]
    facets = ParadeDBQuerySet._extract_facets_multi(rows, ["_paradedb_facets"])
    assert facets == {"buckets": [{"key": "x"}]}
    assert not hasattr(rows[0], "_paradedb_facets")
    assert not hasattr(rows[1], "_paradedb_facets")
