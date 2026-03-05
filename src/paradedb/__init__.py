"""ParadeDB integration for Django ORM."""

from __future__ import annotations

from importlib import import_module
from importlib.metadata import PackageNotFoundError, version
from typing import Any

try:
    __version__ = version("django-paradedb")
except PackageNotFoundError:
    __version__ = "0+unknown"

__all__ = [
    "Agg",
    "All",
    "BM25Index",
    "Empty",
    "Exists",
    "FuzzyTerm",
    "IndexExpression",
    "Match",
    "MoreLikeThis",
    "ParadeDB",
    "ParadeDBManager",
    "ParadeDBQuerySet",
    "ParadeOperator",
    "Parse",
    "ParseWithField",
    "Phrase",
    "PhrasePrefix",
    "ProxRegex",
    "Proximity",
    "ProximityArray",
    "ProximityRegex",
    "Range",
    "RangeRelation",
    "RangeTerm",
    "RangeType",
    "Regex",
    "RegexPhrase",
    "Score",
    "Snippet",
    "SnippetPositions",
    "Snippets",
    "Term",
    "TermSet",
    "paradedb_index_segments",
    "paradedb_indexes",
    "paradedb_verify_all_indexes",
    "paradedb_verify_index",
]

_EXPORTS: dict[str, tuple[str, str]] = {
    "Agg": ("paradedb.functions", "Agg"),
    "Score": ("paradedb.functions", "Score"),
    "Snippet": ("paradedb.functions", "Snippet"),
    "SnippetPositions": ("paradedb.functions", "SnippetPositions"),
    "Snippets": ("paradedb.functions", "Snippets"),
    "paradedb_index_segments": ("paradedb.functions", "paradedb_index_segments"),
    "paradedb_indexes": ("paradedb.functions", "paradedb_indexes"),
    "paradedb_verify_all_indexes": (
        "paradedb.functions",
        "paradedb_verify_all_indexes",
    ),
    "paradedb_verify_index": ("paradedb.functions", "paradedb_verify_index"),
    "BM25Index": ("paradedb.indexes", "BM25Index"),
    "IndexExpression": ("paradedb.indexes", "IndexExpression"),
    "ParadeDBManager": ("paradedb.queryset", "ParadeDBManager"),
    "ParadeDBQuerySet": ("paradedb.queryset", "ParadeDBQuerySet"),
    "All": ("paradedb.search", "All"),
    "Empty": ("paradedb.search", "Empty"),
    "Exists": ("paradedb.search", "Exists"),
    "FuzzyTerm": ("paradedb.search", "FuzzyTerm"),
    "Match": ("paradedb.search", "Match"),
    "MoreLikeThis": ("paradedb.search", "MoreLikeThis"),
    "ParadeDB": ("paradedb.search", "ParadeDB"),
    "ParadeOperator": ("paradedb.search", "ParadeOperator"),
    "Parse": ("paradedb.search", "Parse"),
    "ParseWithField": ("paradedb.search", "ParseWithField"),
    "Phrase": ("paradedb.search", "Phrase"),
    "PhrasePrefix": ("paradedb.search", "PhrasePrefix"),
    "Proximity": ("paradedb.search", "Proximity"),
    "ProximityArray": ("paradedb.search", "ProximityArray"),
    "ProximityRegex": ("paradedb.search", "ProximityRegex"),
    "ProxRegex": ("paradedb.search", "ProxRegex"),
    "Range": ("paradedb.search", "Range"),
    "RangeRelation": ("paradedb.search", "RangeRelation"),
    "RangeTerm": ("paradedb.search", "RangeTerm"),
    "RangeType": ("paradedb.search", "RangeType"),
    "Regex": ("paradedb.search", "Regex"),
    "RegexPhrase": ("paradedb.search", "RegexPhrase"),
    "Term": ("paradedb.search", "Term"),
    "TermSet": ("paradedb.search", "TermSet"),
}


def __getattr__(name: str) -> Any:
    export = _EXPORTS.get(name)
    if export is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module_name, attr_name = export
    value = getattr(import_module(module_name), attr_name)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted(set(globals()) | set(__all__))
