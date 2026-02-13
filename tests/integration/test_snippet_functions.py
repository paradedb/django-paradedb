import pytest
from tests.models import MockItem

from paradedb.functions import SnippetPositions, Snippets
from paradedb.search import ParadeDB

pytestmark = [
    pytest.mark.integration,
    pytest.mark.django_db,
    pytest.mark.usefixtures("mock_items"),
]


class TestSnippetsIntegration:
    """Integration tests for pdb.snippets() function."""

    def test_snippets_basic(self) -> None:
        """Verify pdb.snippets returns fragments for multiple rows."""
        qs = (
            MockItem.objects.filter(description=ParadeDB("shoes"))
            .annotate(fragments=Snippets("description"))
            .order_by("id")
        )

        results = list(qs)
        assert len(results) == 3  # IDs 3, 4, 5

        # Check specific highlighting for ID 3: "Sleek running shoes"
        item3 = next(i for i in results if i.id == 3)
        assert item3.fragments == ["Sleek running <b>shoes</b>"]

    def test_snippets_max_num_chars(self) -> None:
        """Verify snippets max_num_chars parameter constrains fragment length."""
        # Using ID 4: "White jogging shoes"
        # max_num_chars=10 produces an exact short fragment
        qs = MockItem.objects.filter(id=4, description=ParadeDB("shoes")).annotate(
            fragments=Snippets("description", max_num_chars=10)
        )
        item = qs.get()
        assert item.fragments == ["<b>shoes</b>"]

    def test_snippets_with_limit_offset(self) -> None:
        """Verify snippets limit and offset parameters using fragmentation."""
        # Searching for "White shoes" in ID 4: "White jogging shoes"
        # max_num_chars=10 forces fragmentation into: ['<b>White</b>', '<b>shoes</b>']

        # 1. Limit=1, Offset=0 -> Should get just the first fragment
        qs0 = MockItem.objects.filter(
            id=4, description=ParadeDB("White shoes")
        ).annotate(
            fragments=Snippets("description", max_num_chars=10, limit=1, offset=0)
        )
        assert qs0.get().fragments == ["<b>White</b>"]

        # 2. Limit=1, Offset=1 -> Should get just the second fragment
        qs1 = MockItem.objects.filter(
            id=4, description=ParadeDB("White shoes")
        ).annotate(
            fragments=Snippets("description", max_num_chars=10, limit=1, offset=1)
        )
        assert qs1.get().fragments == ["<b>shoes</b>"]

    def test_snippets_custom_tags(self) -> None:
        """Verify custom start/end tags work on multi-row results."""
        qs = MockItem.objects.filter(description=ParadeDB("Generic")).annotate(
            fragments=Snippets("description", start_tag="<mark>", end_tag="</mark>")
        )
        item = qs.get(id=5)  # ID 5: "Generic shoes"
        assert item.fragments == ["<mark>Generic</mark> shoes"]

    def test_snippets_sort_by_score(self) -> None:
        """Verify sort_by returns expected fragment arrays."""
        # For a single short description, score/position usually return the same.
        # This test ensures the parameter is accepted and doesn't crash.
        qs = MockItem.objects.filter(description=ParadeDB("shoes")).annotate(
            fragments=Snippets("description", sort_by="score")
        )
        item = qs.first()
        assert len(item.fragments) > 0
        assert "<b>" in item.fragments[0]


class TestSnippetPositionsIntegration:
    """Integration tests for pdb.snippet_positions() function."""

    def test_snippet_positions_basic(self) -> None:
        """Verify pdb.snippet_positions returns byte offset pairs."""
        qs = MockItem.objects.filter(description=ParadeDB("wireless")).annotate(
            positions=SnippetPositions("description")
        )
        # "Innovative wireless earbuds" -> "wireless" starts at 11, ends at 19
        item = qs.get(id=12)
        assert item.positions == [[11, 19]]

    def test_snippet_positions_multiple_matches(self) -> None:
        """Verify multiple matches return multiple position pairs."""
        # Searching for "white shoes" to get multiple matches in a single record
        qs = MockItem.objects.filter(description=ParadeDB("white shoes")).annotate(
            positions=SnippetPositions("description")
        )
        item = qs.get(id=4)
        # "White jogging shoes" -> "White" at [0, 5], "shoes" at [14, 19]
        assert item.positions == [[0, 5], [14, 19]]

    def test_snippet_positions_no_match(self) -> None:
        """Verify no match returns empty list."""
        # Ensure row is returned but no snippet is matched
        qs = MockItem.objects.filter(description=ParadeDB("wireless")).annotate(
            positions=SnippetPositions("category")
        )
        item = qs.get(id=12)
        assert item.positions is None or item.positions == []
