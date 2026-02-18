# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added (Unreleased)

- `Proximity` query expression with unordered (`##`) and ordered (`##>`) forms
- Plain string `ParadeDB(..., operator='OR' | 'AND' | 'TERM')` support
- `tokenizer=` override support for plain-string `ParadeDB(...)` queries
- `tokenizer` support on `Phrase(...)`
- Scoring modifiers across query expressions via `boost` and `const`
- Extended `Fuzzy(...)` options: `operator`, `prefix`, `transposition_cost_one`
- `Parse(..., conjunction_mode=...)` passthrough support
- `PhrasePrefix(...)` support via `pdb.phrase_prefix(...)`
- `RegexPhrase(...)` support via `pdb.regex_phrase(...)`
- `ProximityRegex(...)` support via `pdb.prox_regex(...)` within proximity queries
- `ProximityArray(...)` support via `pdb.prox_array(...)` within proximity queries
- `RangeTerm(...)` support via `pdb.range_term(...)`
- ParadeDB operators over ad-hoc SQL expressions

### Changed

- Added validation for incompatible argument combinations:
  - `operator` on non-string `ParadeDB(...)` terms is rejected
  - mixed fuzzy operators in one query are rejected
  - multiple `Proximity(...)` terms in one query are rejected (use `ProximityArray(...)`)
- Removed wrapper-side scoring validation for `boost`/`const`; scoring options are
  forwarded to ParadeDB/PostgreSQL.

## [0.1.0] - 2025-01-30

### Added

- `BM25Index` for declarative BM25 index creation in Django models
- `ParadeDB` lookup wrapper with `&&&`, `|||`, `###` operators
- `PQ` query object for boolean composition (AND/OR)
- Search expressions: `Phrase`, `Fuzzy`, `Parse`, `Term`, `Regex`, `All`
- `MoreLikeThis` query filter for similarity search
- `Score` annotation for BM25 relevance scores
- `Snippet` annotation for highlighted text excerpts
- `ParadeDBQuerySet` with `.facets()` method for aggregations
- `ParadeDBManager` for easy model integration
- JSON field key indexing support
- Full Django ORM integration with `Q` objects and standard filters

[0.1.0]: https://github.com/paradedb/django-paradedb/releases/tag/v0.1.0
