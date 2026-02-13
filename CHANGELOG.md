# Changelog
<!-- markdownlint-disable MD024 -->

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
- `PQ` query object for boolean composition (AND/OR)
- Search expressions: `Phrase`, `Fuzzy`, `Parse`, `Term`, `Regex`, `All`
- `MoreLikeThis` query filter for similarity search
- `Score` annotation for BM25 relevance scores
- `Snippet` annotation for highlighted text excerpts
- `ParadeDBQuerySet` with `.facets()` method for aggregations
- `ParadeDBManager` for easy model integration
- JSON field key indexing support
- Full Django ORM integration with `Q` objects and standard filters

[0.2.0]: https://github.com/paradedb/django-paradedb/compare/v0.1.1...e8734548d5e823efd7bac4bef0d56a2a6d18c865
[0.1.1]: https://github.com/paradedb/django-paradedb/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/paradedb/django-paradedb/releases/tag/v0.1.0
