from unittest.mock import Mock

import pytest
from django.db.models import Value
from tests.models import MockItem

from paradedb.functions import Snippet, SnippetPositions, Snippets
from paradedb.search import ParadeDB

pytestmark = [
    pytest.mark.integration,
    pytest.mark.django_db,
    pytest.mark.usefixtures("mock_items"),
]


class TestSnippetsIntegration:
    """Integration tests for pdb.snippets() function."""

    def test_snippets_basic(self) -> None:
        """Verify pdb.snippets returns an array of matching fragments."""
        qs = MockItem.objects.filter(description=ParadeDB("wireless")).annotate(
            fragments=Snippets("description")
        )
        item = qs.first()
        assert item is not None
        assert isinstance(item.fragments, list)
        assert len(item.fragments) > 0
        assert any("<b>wireless</b>" in f for f in item.fragments)

    def test_snippets_max_num_chars(self) -> None:
        """Verify snippets max_num_chars parameter."""
        qs = MockItem.objects.filter(description=ParadeDB("wireless")).annotate(
            fragments=Snippets("description", max_num_chars=10)
        )
        item = qs.first()
        assert item is not None
        # Fragments should be short
        assert all(len(f) <= 40 for f in item.fragments)  # 10 chars + tags + context

    def test_snippets_with_limit_offset(self) -> None:
        """Verify snippets limit and offset parameters."""
        qs = MockItem.objects.filter(description=ParadeDB("wireless")).annotate(
            fragments=Snippets("description", limit=1, offset=0)
        )
        item = qs.first()
        assert item is not None
        assert len(item.fragments) == 1

    def test_snippets_custom_tags(self) -> None:
        """Verify custom start/end tags."""
        qs = MockItem.objects.filter(description=ParadeDB("wireless")).annotate(
            fragments=Snippets("description", start_tag="<mark>", end_tag="</mark>")
        )
        item = qs.first()
        assert item is not None
        assert any("<mark>wireless</mark>" in f for f in item.fragments)

    def test_snippets_sort_by_score(self) -> None:
        """Verify sort_by='score' executes without error."""
        qs = MockItem.objects.filter(description=ParadeDB("wireless")).annotate(
            fragments=Snippets("description", sort_by="score")
        )
        item = qs.first()
        assert item is not None
        assert isinstance(item.fragments, list)

    def test_snippets_sort_by_position(self) -> None:
        """Verify sort_by='position' executes without error."""
        qs = MockItem.objects.filter(description=ParadeDB("wireless")).annotate(
            fragments=Snippets("description", sort_by="position")
        )
        item = qs.first()
        assert item is not None
        assert isinstance(item.fragments, list)

    def test_snippets_invalid_sort_by_raises(self) -> None:
        """Verify sort_by validation in integration suite."""
        with pytest.raises(ValueError, match="sort_by must be one of"):
            Snippets("description", sort_by="invalid")  # type: ignore[arg-type]

    def test_snippets_parameterized_field_raises(self) -> None:
        """Verify parameterized field check in Snippets.as_sql."""
        snippets = Snippets("description")
        snippets.source_expressions = [Value("parameterized")]
        compiler = Mock()
        compiler.compile.return_value = ("'parameterized'", ["parameterized"])
        with pytest.raises(ValueError, match="does not support parameterized fields"):
            snippets.as_sql(compiler, Mock())


class TestSnippetIntegration:
    """Integration tests for pdb.snippet() function."""

    def test_snippet_basic(self) -> None:
        """Verify pdb.snippet returns a highlighted string."""
        qs = MockItem.objects.filter(description=ParadeDB("wireless")).annotate(
            highlight=Snippet("description")
        )
        item = qs.first()
        assert item is not None
        assert "<b>wireless</b>" in item.highlight

    def test_snippet_parameterized_field_raises(self) -> None:
        """Verify parameterized field check in Snippet.as_sql."""
        snippet = Snippet("description")
        snippet.source_expressions = [Value("parameterized")]
        compiler = Mock()
        compiler.compile.return_value = ("'parameterized'", ["parameterized"])
        with pytest.raises(ValueError, match="does not support parameterized fields"):
            snippet.as_sql(compiler, Mock())


class TestSnippetPositionsIntegration:
    """Integration tests for pdb.snippet_positions() function."""

    def test_snippet_positions_basic(self) -> None:
        """Verify pdb.snippet_positions returns byte offset pairs."""
        qs = MockItem.objects.filter(description=ParadeDB("wireless")).annotate(
            positions=SnippetPositions("description")
        )
        item = qs.first()
        assert item is not None
        assert isinstance(item.positions, list)
        # ParadeDB returns offsets as strings in some versions, or items in array
        # Just verify we get results
        assert len(item.positions) > 0

    def test_snippet_positions_parameterized_field_raises(self) -> None:
        """Verify parameterized field check in SnippetPositions.as_sql."""
        positions = SnippetPositions("description")
        positions.source_expressions = [Value("parameterized")]
        compiler = Mock()
        compiler.compile.return_value = ("'parameterized'", ["parameterized"])
        with pytest.raises(ValueError, match="does not support parameterized fields"):
            positions.as_sql(compiler, Mock())
