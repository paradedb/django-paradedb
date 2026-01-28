"""Integration tests for ParadeDB JSON faceted queries."""

from __future__ import annotations

import pytest
from tests.models import JsonItem, JsonItemNoExpand

from paradedb.search import All, ParadeDB

pytestmark = [
    pytest.mark.integration,
    pytest.mark.django_db(transaction=True),
    pytest.mark.usefixtures("json_items"),
]


def _keys(facets: dict[str, object]) -> set[object]:
    buckets = facets.get("buckets", [])
    if not isinstance(buckets, list):
        return set()
    return {bucket.get("key") for bucket in buckets if isinstance(bucket, dict)}


def _null_markers(keys: set[object]) -> set[object]:
    return {key for key in keys if str(key).endswith("__PDB_NULL__")}


def _facet(field: str) -> dict[str, object]:
    return JsonItem.objects.filter(id=ParadeDB(All())).facets(field, include_rows=False)


def _facet_no_expand(field: str) -> dict[str, object]:
    return JsonItemNoExpand.objects.filter(id=ParadeDB(All())).facets(
        field, include_rows=False
    )


class TestJsonFacetIntegration:
    """Validate JSON field facet behavior."""

    def test_facets_json_dotted_fields(self) -> None:
        facets = _facet("metadata.color")
        keys = _keys(facets)
        assert {"red", "blue"} <= keys

    def test_facets_json_nested_fields(self) -> None:
        facets = _facet("metadata.deep.nested.value")
        keys = _keys(facets)
        assert {"x", "y"} <= keys
        assert _null_markers(keys)

    def test_facets_json_boolean_field(self) -> None:
        facets = _facet("metadata.flags.featured")
        keys = _keys(facets)
        assert {0, 1} <= keys
        assert _null_markers(keys)

    def test_facets_json_numeric_field(self) -> None:
        facets = _facet("metadata.size")
        keys = _keys(facets)
        assert {3, 5} <= keys

    def test_facets_json_array_field(self) -> None:
        facets = _facet("metadata.tags")
        keys = _keys(facets)
        assert {"a", "b", "c"} <= keys
        assert _null_markers(keys)

    def test_facets_json_numeric_key(self) -> None:
        facets = _facet("metadata.metrics.1")
        keys = _keys(facets)
        assert {"one", "uno"} <= keys
        assert _null_markers(keys)

    def test_facets_json_dot_key_expand(self) -> None:
        facets = _facet("metadata.dot.key")
        keys = _keys(facets)
        assert {"dk", "dk2"} <= keys
        assert _null_markers(keys)

    def test_facets_json_dash_key(self) -> None:
        facets = _facet("metadata.dash-key")
        keys = _keys(facets)
        assert {"d"} <= keys
        assert _null_markers(keys)

    def test_facets_json_space_key(self) -> None:
        facets = _facet("metadata.weird key")
        keys = _keys(facets)
        assert {"space"} <= keys
        assert _null_markers(keys)

    def test_facets_json_array_numbers(self) -> None:
        facets = _facet("metadata.arrnums")
        keys = _keys(facets)
        assert {1, 2, 3} <= keys
        assert _null_markers(keys)

    def test_facets_json_bools_array(self) -> None:
        facets = _facet("metadata.bools")
        keys = _keys(facets)
        assert {0, 1} <= keys
        assert _null_markers(keys)

    def test_facets_json_mixed_types(self) -> None:
        facets = _facet("metadata.mixed")
        keys = _keys(facets)
        assert {"1", 1} <= keys
        assert _null_markers(keys)

    def test_facets_json_null_value(self) -> None:
        facets = _facet("metadata.nullval")
        keys = _keys(facets)
        assert _null_markers(keys)

    def test_facets_json_empty_string(self) -> None:
        facets = _facet("metadata.emptystr")
        keys = _keys(facets)
        assert "" in keys
        assert _null_markers(keys)

    def test_facets_json_float_value(self) -> None:
        facets = _facet("metadata.numfloat")
        keys = _keys(facets)
        assert 2.5 in keys
        assert _null_markers(keys)

    def test_facets_json_object_field(self) -> None:
        facets = _facet("metadata.obj.inner")
        keys = _keys(facets)
        assert {1, 2} <= keys
        assert _null_markers(keys)

    def test_facets_json_object_array_field(self) -> None:
        facets = _facet("metadata.objarray.k")
        keys = _keys(facets)
        assert {"v1", "v2"} <= keys
        assert _null_markers(keys)

    def test_facets_json_object_array_deep_field(self) -> None:
        facets = _facet("metadata.objarray_deep.meta.code")
        keys = _keys(facets)
        assert {"c1", "c2"} <= keys
        assert _null_markers(keys)

    def test_facets_json_nested_array_field(self) -> None:
        facets = _facet("metadata.nested_array")
        keys = _keys(facets)
        assert {"x", "y"} <= keys
        assert _null_markers(keys)

    def test_facets_json_missing_field(self) -> None:
        facets = _facet("metadata.missing")
        keys = _keys(facets)
        assert _null_markers(keys)


@pytest.mark.usefixtures("json_items_no_expand")
class TestJsonFacetNoExpandIntegration:
    """Validate JSON dot-key behavior with expand_dots disabled."""

    def test_facets_json_dot_key_no_expand(self) -> None:
        facets = _facet_no_expand("metadata.dot.key")
        keys = _keys(facets)
        assert _null_markers(keys)
