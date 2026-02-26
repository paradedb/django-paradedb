"""Load paradedb SQL API constants from api.json at the repo root.

To add or modify a symbol, edit api.json.  This file just exposes each
entry as a typed module-level name so the rest of the library can import
them normally::

    from paradedb.api import FN_ALL, OP_SEARCH, PDB_TYPE_BOOST
"""

import json
from pathlib import Path

_api = json.loads((Path(__file__).parent.parent.parent / "api.json").read_text())
_ops = _api["operators"]
_fns = _api["functions"]
_types = _api["types"]

# ---------------------------------------------------------------------------
# Operators
# ---------------------------------------------------------------------------

OP_SEARCH: str = _ops["OP_SEARCH"]
OP_AND: str = _ops["OP_AND"]
OP_OR: str = _ops["OP_OR"]
OP_PHRASE: str = _ops["OP_PHRASE"]
OP_PROXIMITY: str = _ops["OP_PROXIMITY"]
OP_PROXIMITY_ORD: str = _ops["OP_PROXIMITY_ORD"]
OP_TERM: str = _ops["OP_TERM"]

# ---------------------------------------------------------------------------
# Query builder functions
# ---------------------------------------------------------------------------

FN_ALL: str = _fns["FN_ALL"]
FN_TERM: str = _fns["FN_TERM"]
FN_PARSE: str = _fns["FN_PARSE"]
FN_PHRASE_PREFIX: str = _fns["FN_PHRASE_PREFIX"]
FN_REGEX_PHRASE: str = _fns["FN_REGEX_PHRASE"]
FN_RANGE_TERM: str = _fns["FN_RANGE_TERM"]
FN_REGEX: str = _fns["FN_REGEX"]
FN_PROXIMITY: str = _fns["FN_PROXIMITY"]
FN_PROX_REGEX: str = _fns["FN_PROX_REGEX"]
FN_PROX_ARRAY: str = _fns["FN_PROX_ARRAY"]
FN_MORE_LIKE_THIS: str = _fns["FN_MORE_LIKE_THIS"]
FN_EMPTY: str = _fns["FN_EMPTY"]
FN_EXISTS: str = _fns["FN_EXISTS"]
FN_FUZZY_TERM: str = _fns["FN_FUZZY_TERM"]
FN_PARSE_WITH_FIELD: str = _fns["FN_PARSE_WITH_FIELD"]
FN_RANGE: str = _fns["FN_RANGE"]
FN_TERM_SET: str = _fns["FN_TERM_SET"]

# ---------------------------------------------------------------------------
# Annotation functions
# ---------------------------------------------------------------------------

FN_SCORE: str = _fns["FN_SCORE"]
FN_SNIPPET: str = _fns["FN_SNIPPET"]
FN_SNIPPETS: str = _fns["FN_SNIPPETS"]
FN_SNIPPET_POSITIONS: str = _fns["FN_SNIPPET_POSITIONS"]
FN_AGG: str = _fns["FN_AGG"]

# ---------------------------------------------------------------------------
# Diagnostic functions
# ---------------------------------------------------------------------------

FN_INDEXES: str = _fns["FN_INDEXES"]
FN_INDEX_SEGMENTS: str = _fns["FN_INDEX_SEGMENTS"]
FN_VERIFY_INDEX: str = _fns["FN_VERIFY_INDEX"]
FN_VERIFY_ALL_INDEXES: str = _fns["FN_VERIFY_ALL_INDEXES"]

# ---------------------------------------------------------------------------
# Type casts — scoring and matching modifiers
# ---------------------------------------------------------------------------

PDB_TYPE_BOOST: str = _types["PDB_TYPE_BOOST"]
PDB_TYPE_CONST: str = _types["PDB_TYPE_CONST"]
PDB_TYPE_FUZZY: str = _types["PDB_TYPE_FUZZY"]
PDB_TYPE_SLOP: str = _types["PDB_TYPE_SLOP"]
PDB_TYPE_QUERY: str = _types["PDB_TYPE_QUERY"]

# ---------------------------------------------------------------------------
# Type casts — tokenizers (used in field::pdb.<tokenizer> syntax)
# ---------------------------------------------------------------------------

PDB_TYPE_TOKENIZER_ALIAS: str = _types["PDB_TYPE_TOKENIZER_ALIAS"]
PDB_TYPE_TOKENIZER_CHINESE_COMPATIBLE: str = _types["PDB_TYPE_TOKENIZER_CHINESE_COMPATIBLE"]
PDB_TYPE_TOKENIZER_ICU: str = _types["PDB_TYPE_TOKENIZER_ICU"]
PDB_TYPE_TOKENIZER_JIEBA: str = _types["PDB_TYPE_TOKENIZER_JIEBA"]
PDB_TYPE_TOKENIZER_LINDERA: str = _types["PDB_TYPE_TOKENIZER_LINDERA"]
PDB_TYPE_TOKENIZER_LITERAL: str = _types["PDB_TYPE_TOKENIZER_LITERAL"]
PDB_TYPE_TOKENIZER_LITERAL_NORMALIZED: str = _types["PDB_TYPE_TOKENIZER_LITERAL_NORMALIZED"]
PDB_TYPE_TOKENIZER_NGRAM: str = _types["PDB_TYPE_TOKENIZER_NGRAM"]
PDB_TYPE_TOKENIZER_REGEX: str = _types["PDB_TYPE_TOKENIZER_REGEX"]
PDB_TYPE_TOKENIZER_SIMPLE: str = _types["PDB_TYPE_TOKENIZER_SIMPLE"]
PDB_TYPE_TOKENIZER_SOURCE_CODE: str = _types["PDB_TYPE_TOKENIZER_SOURCE_CODE"]
PDB_TYPE_TOKENIZER_UNICODE_WORDS: str = _types["PDB_TYPE_TOKENIZER_UNICODE_WORDS"]
PDB_TYPE_TOKENIZER_WHITESPACE: str = _types["PDB_TYPE_TOKENIZER_WHITESPACE"]
