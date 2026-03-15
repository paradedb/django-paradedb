# Changelog

<!-- markdownlint-disable MD024 -->

All notable changes to this project will be documented in this file. The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

## [0.4.0] - 2026-02-28

### Added

- `Match` query expression requiring explicit `operator='AND'` or `operator='OR'` for
  text-match queries, replacing implicit bare-string matching. (#37)
- 6 new query expression types: `Empty` (match-nothing), `Exists` (field existence),
  `FuzzyTerm` (dedicated fuzzy term via `pdb.fuzzy_term()`), `ParseWithField`
  (field-scoped query parsing via `pdb.parse_with_field()`), `Range` (range queries via
  `pdb.range()` with typed PostgreSQL ranges), and `TermSet` (set membership via
  `pdb.term_set()` with typed arrays). (#44)
- Diagnostics helpers: `paradedb_indexes()`, `paradedb_index_segments()`,
  `paradedb_verify_index()`, `paradedb_verify_all_indexes()` wrapping ParadeDB `pdb.*`
  diagnostic table functions. (#39)
- Django management commands: `paradedb_indexes`, `paradedb_index_segments`,
  `paradedb_verify_index`, `paradedb_verify_all_indexes` (requires `"paradedb"` in
  `INSTALLED_APPS`). (#39)
- Non-exact facets: `exact` parameter on `Agg` and `facets()` — pass `exact=False` for
  approximate facet aggregations. (#39)
- Partial index support: `BM25Index` now accepts a `condition` parameter (Django `Q`
  object) to create partial BM25 indexes with a `WHERE` clause. (#41)
- Concurrent index creation: `BM25Index.create_sql()` now correctly forwards the
  `concurrently` kwarg so `AddIndexConcurrently` works. (#41)
- Top-level package re-exports: all public API symbols are now importable directly from
  `paradedb` (e.g., `from paradedb import ParadeDB, Match, Score, BM25Index`). (#43)
- Centralised API constants: `api.json`, `api.py`, and `api.pyi` providing a single
  source of truth for all ParadeDB SQL operators, functions, and types. (#44)
- Schema compatibility checking: `scripts/check_schema_compat.py` and `apiignore.json`
  for bidirectional validation against ParadeDB's SQL schema. (#44)
- Schema compatibility CI workflow triggered on each ParadeDB release with failure
  notifications via GitHub Issues and Slack. (#46)
- Inline fuzzy parameters (`distance`, `prefix`, `transposition_cost_one`) on `Match`
  and `Term` query expressions. (#40)
- `ProxRegex` items (`pdb.prox_regex(...)`) can now be mixed with plain strings inside
  `ProximityArray`. (#48)

### Changed

- **BREAKING**: `ParadeDB('shoes')` (bare strings) no longer works for text matching —
  use `ParadeDB(Match('shoes', operator='AND'))` instead. (#37)
- **BREAKING**: Fuzzy search is no longer a standalone `Fuzzy(...)` expression — use
  `Match('shoez', operator='OR', distance=1)` or `Term('shoez', distance=1)`. (#37, #40)
- **BREAKING**: `operator` kwarg removed from `ParadeDB(...)` — pass it via `Match(...)`
  instead. (#51)
- JSON field aggregations now use native `json_fields` index configuration with dot
  notation (`metadata.color`) instead of column aliases. (#42)
- Tokenizer quoting improved to support invocation syntax like
  `whitespace('lowercase=false')` without over-quoting. (#40)
- Tokenizer resolution now uses a validated lookup backed by `api.json` constants. (#44)
- All hardcoded ParadeDB SQL strings replaced with constants from `paradedb.api`. (#44)
- `Match` now validates invalid combinations (tokenizer + fuzzy, multi-term fuzzy +
  boost/const) eagerly at construction time. (#57)
- `TermSet` enforces homogeneous element types. (#50)
- `MoreLikeThis` validation tightened for key field, fields, stopwords, and numeric
  options. (#50, #58)
- README quickstart rewritten to use `MockItemDjango` model with `mock_items` dataset. (#36)

### Fixed

- `BM25Index.create_sql()` now properly passes `concurrently` kwarg, fixing silent
  failures with Django's `AddIndexConcurrently`. (#41)
- Multi-term fuzzy queries generate correct SQL (`ARRAY[...]::pdb.fuzzy(N)` instead of
  invalid per-element casts). (#42, #57)
- Tokenizer + fuzzy ordering for single-term queries corrected. (#42)
- `api.json` loaded from package data when installed (source-tree fallback for dev). (#49)
- `boost`/`const` validated as finite numbers; snippet numeric options validated as
  non-negative integers. (#49)

### Removed

- **BREAKING**: `PQ` class removed — use Django `Q` objects with `Match` clauses. (#37, #40)
- **BREAKING**: `Fuzzy` standalone expression removed — use inline fuzzy params on
  `Match` and `Term`. (#37, #40)
- `TERM` removed from `ParadeOperator` literal type (only `AND` and `OR` remain). (#37)

## [0.3.0] - 2026-02-19

### Added

- `Proximity` query expression with unordered (`##`) and ordered (`##>`) forms
- `Match(...)` query expression for explicit `AND`/`OR` literal matching
- `tokenizer=` override support on `Match(...)` queries
- `tokenizer` support on `Phrase(...)`
- Scoring modifiers across query expressions via `boost` and `const`
- Extended fuzzy options on `Match(...)`/`Term(...)`: `distance`, `prefix`, `transposition_cost_one`
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
- Compatibility and packaging metadata were updated for broader support:
  Django `4.2+`, Python `3.10+`, and refreshed project metadata/dependencies.

## [0.2.0] - 2026-02-13

### Added

- `Snippets` annotation wrapper for `pdb.snippets(...)`, including support for
  custom tags, `max_num_chars`, `limit`/`offset`, and `sort_by` (`score` or
  `position`).
- `SnippetPositions` annotation wrapper for `pdb.snippet_positions(...)` to
  return byte-offset ranges for matched terms.
- Expanded `BM25Index` tokenizer DSL with:
  `tokenizers` (multiple tokenizers per field), positional `args`, `named_args`,
  and alias-aware configuration.

### Changed

- `BM25Index` now enforces explicit tokenizer declarations when tokenizer config
  keys are provided (`filters`, `stemmer`, `args`, `named_args`, `alias`), and
  requires explicit tokenizers for `json_keys`.
- Deprecated tokenizer `options` key was removed in favor of `named_args`.
- Hybrid RRF example was refactored to a single ORM query approach (no Python
  post-join).
- Project quality tooling moved from `pre-commit` to `prek`, with CI updates for
  linting/test matrix and Codecov reporting.

### Fixed

- Validation and error reporting for invalid tokenizer config shapes (mixed
  single/multi-tokenizer configuration and malformed `tokenizers` lists).
- SQL generation coverage for tokenizer configuration and snippet functions.

## [0.1.1] - 2026-02-04

### Fixed

- Release automation was corrected to publish to PyPI from the release workflow
  path instead of relying on tag-push behavior.

## [0.1.0] - 2025-01-30

### Added

- `BM25Index` for declarative BM25 index creation in Django models
- `ParadeDB` lookup wrapper with `&&&`, `|||`, `###` operators
- Boolean composition support via Django `Q` objects
- Search expressions: `Phrase`, `FuzzyTerm`, `Parse`, `Term`, `Regex`, `All`
- `MoreLikeThis` query filter for similarity search
- `Score` annotation for BM25 relevance scores
- `Snippet` annotation for highlighted text excerpts
- `ParadeDBQuerySet` with `.facets()` method for aggregations
- `ParadeDBManager` for easy model integration
- JSON field key indexing support
- Full Django ORM integration with `Q` objects and standard filters

[0.4.0]: https://github.com/paradedb/django-paradedb/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/paradedb/django-paradedb/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/paradedb/django-paradedb/compare/v0.1.1...v0.2.0
[0.1.1]: https://github.com/paradedb/django-paradedb/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/paradedb/django-paradedb/releases/tag/v0.1.0
