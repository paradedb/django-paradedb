from __future__ import annotations

from django.http import HttpRequest, HttpResponse
from django.shortcuts import render
from django.urls import path

from paradedb.functions import Score, Snippet
from paradedb.search import Match, ParadeDB
from tests.models import MockItem


def search(request: HttpRequest) -> HttpResponse:
    """
    Simple full-text search view over ParadeDB's mock_items test table.
    """
    query = request.GET.get("q", "").strip()
    error = None

    try:
        if len(query):
            results = (
                MockItem.objects.filter(
                    description=ParadeDB(Match(query, operator="OR"))
                )
                .annotate(
                    score=Score(),
                    snippet=Snippet(
                        "description", start_sel="<mark>", stop_sel="</mark>"
                    ),
                )
                .order_by("-score")[:20]
            )
        else:
            results = MockItem.objects.all().order_by("id")[:20]
    except Exception as exc:
        error = str(exc)

    return render(
        request,
        "tests/search.html",
        {"query": query, "results": results, "error": error},
    )


urlpatterns = [
    path("", search, name="search"),
]
