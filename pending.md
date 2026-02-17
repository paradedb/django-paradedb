# Pending Features

## Unsupported Features

| # | Feature | Docs link |
|---|---------|-----------|
| 1 | Exact literal disjunction form (`\|\|\|` with multiple terms) | <https://docs.paradedb.com/documentation/full-text/match#match-disjunction> |
| 2 | Array element search operator over `text[]` (`=== '...'`) | <https://docs.paradedb.com/documentation/indexing/indexing-arrays> |
| 3 | TERM (`===` operator) | <https://docs.paradedb.com/documentation/full-text/term> |
| 4 | ParadeDB operators over ad-hoc SQL expressions (e.g. `(description \|\| ' ' \|\| category)`) | <https://docs.paradedb.com/documentation/indexing/indexing-expressions> |
| 5 | Custom query tokenizer override for `match` (`::pdb.whitespace`, etc.) | <https://docs.paradedb.com/documentation/full-text/match#using-a-custom-tokenizer> |
| 6 | Custom query tokenizer override for `phrase` (`::pdb.whitespace`) | <https://docs.paradedb.com/documentation/full-text/phrase#using-a-custom-tokenizer> |
| 7 | Proximity operators (`##`, `##>`) | <https://docs.paradedb.com/documentation/full-text/proximity#overview> |
| 8 | Proximity regex (`pdb.prox_regex`) | <https://docs.paradedb.com/documentation/full-text/proximity#proximity-regex> |
| 9 | Proximity arrays (`pdb.prox_array`) | <https://docs.paradedb.com/documentation/full-text/proximity#proximity-array> |
| 10 | Fuzzy conjunction mode (`&&& '...'::pdb.fuzzy(...)`) | <https://docs.paradedb.com/documentation/full-text/fuzzy#overview> |
| 11 | Fuzzy term form (`=== '...'::pdb.fuzzy(...)`) | <https://docs.paradedb.com/documentation/full-text/fuzzy#overview> |
| 12 | Fuzzy prefix option (`pdb.fuzzy(..., t)` / `prefix=true`) | <https://docs.paradedb.com/documentation/full-text/fuzzy#fuzzy-prefix> |
| 13 | Fuzzy transposition cost option (`transposition_cost_one`) | <https://docs.paradedb.com/documentation/full-text/fuzzy#transposition-cost> |
| 14 | `pdb.parse(..., conjunction_mode => true)` option | <https://docs.paradedb.com/documentation/query-builder/compound/query-parser#conjunction-mode> |
| 15 | `pdb.phrase_prefix(...)` | <https://docs.paradedb.com/documentation/query-builder/phrase/phrase-prefix> |
| 16 | `pdb.regex_phrase(...)` | <https://docs.paradedb.com/documentation/query-builder/phrase/regex-phrase> |
| 17 | Boosting (`::pdb.boost(factor)`) | <https://docs.paradedb.com/documentation/sorting/boost#boosting> |
| 18 | Constant scoring (`::pdb.const(score)`) | <https://docs.paradedb.com/documentation/sorting/boost#constant-scoring> |
| 19 | `pdb.range_term()` | <https://docs.paradedb.com/documentation/query-builder/term/range-term> |
