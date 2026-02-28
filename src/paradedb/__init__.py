"""ParadeDB integration for Django ORM."""

__version__ = "0.3.0"

from paradedb.functions import (
    Agg,
    Score,
    Snippet,
    SnippetPositions,
    Snippets,
    paradedb_index_segments,
    paradedb_indexes,
    paradedb_verify_all_indexes,
    paradedb_verify_index,
)
from paradedb.indexes import BM25Index
from paradedb.search import (
    All,
    Match,
    MoreLikeThis,
    ParadeDB,
    Parse,
    Phrase,
    PhrasePrefix,
    Proximity,
    ProximityArray,
    ProximityRegex,
    ProxRegex,
    RangeTerm,
    Regex,
    RegexPhrase,
    Term,
)

__all__ = [
    "Agg",
    "All",
    "BM25Index",
    "Match",
    "MoreLikeThis",
    "ParadeDB",
    "Parse",
    "Phrase",
    "PhrasePrefix",
    "ProxRegex",
    "Proximity",
    "ProximityArray",
    "ProximityRegex",
    "RangeTerm",
    "Regex",
    "RegexPhrase",
    "Score",
    "Snippet",
    "SnippetPositions",
    "Snippets",
    "Term",
    "paradedb_index_segments",
    "paradedb_indexes",
    "paradedb_verify_all_indexes",
    "paradedb_verify_index",
]
