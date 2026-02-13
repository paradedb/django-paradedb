# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.2] - 2026-02-13

### Changed

- Prepare next patch release and align package version metadata

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

[0.1.2]: https://github.com/paradedb/django-paradedb/releases/tag/v0.1.2
[0.1.0]: https://github.com/paradedb/django-paradedb/releases/tag/v0.1.0
