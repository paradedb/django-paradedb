"""Tests for ParadeDB search terms inside subqueries and other expression walks.

Django relabels the inner query's aliases when a queryset is inlined via
``pk__in`` / ``Exists``, and walks a lookup's source expressions for
``get_refs`` / ``get_group_by_cols``; the term must behave as a leaf.
"""

from __future__ import annotations

import pytest
from django.db.models import BooleanField, Count, Exists, ExpressionWrapper, OuterRef, Q

from paradedb.search import MatchAll, ParadeDB, Term
from tests.models import MockItem

pytestmark = pytest.mark.django_db


class TestSearchTermInSubquery:
    def test_pk_in_subquery_compiles_and_runs(self, mock_items: None) -> None:
        _ = mock_items
        inner = MockItem.objects.filter(description=ParadeDB(MatchAll("shoes"))).values(
            "pk"
        )
        queryset = MockItem.objects.filter(pk__in=inner)
        sql = str(queryset.query)
        assert 'IN (SELECT U0."id"' in sql
        assert "&&& 'shoes'" in sql
        assert list(queryset.order_by("pk").values_list("pk", flat=True)) == list(
            MockItem.objects.filter(description=ParadeDB(MatchAll("shoes")))
            .order_by("pk")
            .values_list("pk", flat=True)
        )

    def test_exclude_pk_in_subquery(self, mock_items: None) -> None:
        _ = mock_items
        inner = MockItem.objects.filter(category=ParadeDB(Term("electronics"))).values(
            "pk"
        )
        excluded = MockItem.objects.exclude(pk__in=inner).count()
        matched = MockItem.objects.filter(
            category=ParadeDB(Term("electronics"))
        ).count()
        assert excluded + matched == MockItem.objects.count()

    def test_exists_with_outerref(self, mock_items: None) -> None:
        _ = mock_items
        inner = MockItem.objects.filter(
            pk=OuterRef("pk"), description=ParadeDB(MatchAll("shoes"))
        )
        queryset = MockItem.objects.filter(Exists(inner))
        assert "EXISTS(SELECT" in str(queryset.query)
        assert (
            queryset.count()
            == MockItem.objects.filter(description=ParadeDB(MatchAll("shoes"))).count()
        )

    def test_group_by_expression_containing_term(self, mock_items: None) -> None:
        _ = mock_items
        hit = ExpressionWrapper(
            Q(description=ParadeDB(MatchAll("shoes"))), output_field=BooleanField()
        )
        rows = {
            row["hit"]: row["n"]
            for row in MockItem.objects.annotate(hit=hit)
            .values("hit")
            .annotate(n=Count("id"))
        }
        assert (
            rows[True]
            == MockItem.objects.filter(description=ParadeDB(MatchAll("shoes"))).count()
        )

    def test_term_is_a_leaf_expression(self) -> None:
        term = ParadeDB(MatchAll("shoes"))
        other = ParadeDB(MatchAll("boots"))
        assert term.relabeled_clone({"T1": "U0"}) is term
        assert term.get_source_expressions() == []
        term.set_source_expressions([])
        assert term.get_source_expressions() == []
        assert term.get_refs() == set()
        assert term.get_group_by_cols() == []
        assert term.replace_expressions({}) is term
        assert term.replace_expressions({term: other}) is other

    def test_aggregate_with_filtered_count(self, mock_items: None) -> None:
        _ = mock_items
        result = MockItem.objects.aggregate(
            n=Count("id", filter=Q(description=ParadeDB(MatchAll("shoes"))))
        )
        assert (
            result["n"]
            == MockItem.objects.filter(description=ParadeDB(MatchAll("shoes"))).count()
        )

    def test_grouped_filtered_count(self, mock_items: None) -> None:
        _ = mock_items
        rows = list(
            MockItem.objects.values("category").annotate(
                n=Count("id", filter=Q(description=ParadeDB(MatchAll("shoes"))))
            )
        )
        assert rows
