# django-paradedb

[![CI](https://github.com/paradedb/django-paradedb/actions/workflows/ci.yml/badge.svg)](https://github.com/paradedb/django-paradedb/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/django-paradedb)](https://pypi.org/project/django-paradedb/)
[![Python](https://img.shields.io/pypi/pyversions/django-paradedb)](https://pypi.org/project/django-paradedb/)
[![License](https://img.shields.io/github/license/paradedb/django-paradedb?color=blue)](https://github.com/paradedb/django-paradedb?tab=MIT-1-ov-file#readme)
[![Slack URL](https://img.shields.io/badge/Join%20Slack-purple?logo=slack&link=https%3A%2F%2Fjoin.slack.com%2Ft%2Fparadedbcommunity%2Fshared_invite%2Fzt-32abtyjg4-yoYoi~RPh9MSW8tDbl0BQw)](https://join.slack.com/t/paradedbcommunity/shared_invite/zt-32abtyjg4-yoYoi~RPh9MSW8tDbl0BQw)
[![X URL](https://img.shields.io/twitter/url?url=https%3A%2F%2Ftwitter.com%2Fparadedb&label=Follow%20%40paradedb)](https://x.com/paradedb)

ParadeDB full-text search integration for Django ORM.

## Requirements & Compatibility

| Component  | Version            |
|-----------|--------------------|
| Python    | 3.13+              |
| Django    | 6.0+               |
| ParadeDB  | 0.21.* (tested on 0.21.4) |
| PostgreSQL| 17, 18 (with ParadeDB extension) |

## Installation

```bash
pip install django-paradedb
```

## Quick Start

```python
from django.db import models
from paradedb.indexes import BM25Index
from paradedb.queryset import ParadeDBManager, ParadeDBQuerySet
from paradedb.search import ParadeDB

class Product(models.Model):
    description = models.TextField()
    category = models.CharField(max_length=100)

    objects = ParadeDBManager()

    class Meta:
        indexes = [
            BM25Index(
                fields={'id': {}, 'description': {'tokenizer': 'simple'}},
                key_field='id',
                name='product_search_idx',
            ),
        ]

# Search
Product.objects.filter(description=ParadeDB('shoes'))
```

If you already have a custom manager, compose it with the ParadeDB QuerySet:

```python
class CustomManager(models.Manager):
    ...

CustomManagerWithFacets = CustomManager.from_queryset(ParadeDBQuerySet)

class Product(models.Model):
    ...
    objects = CustomManagerWithFacets()
```

## Faceted search

Faceted queries return both rows and an aggregate in one query. ParadeDB requires an `ORDER BY ... LIMIT` Top-N query and a ParadeDB operator in the `WHERE` clause. For text fields, use a literal tokenizer for facet fields.

```python
rows, facets = (
    Product.objects.filter(description=ParadeDB("running shoes"))
    .order_by("id")[:5]
    .facets("category")
)
```

To return only the aggregate (no rows), set `include_rows=False`:

```python
facets = Product.objects.filter(description=ParadeDB("running shoes")).facets(
    "category",
    include_rows=False,
)

Note: `facets()` currently supports a single field. For multiple aggregations,
pass raw JSON via `agg=...`.
```

## Feature Support

Supported:

- ParadeDB lookup with AND/OR operators (`&&&`, `|||`)
- `PQ` boolean composition
- Search expressions: `Phrase`, `Fuzzy`, `Parse`, `Term`, `Regex`
- Annotations: `Score`, `Snippet`
- Faceted search (`.facets()` and `pdb.agg(...)` integration)
- `BM25Index` DDL generation (basic + JSON field keys)
- `MoreLikeThis` query filter
- Django ORM integration with `Q`, standard filters, and window functions

Unsupported / pending:

## Documentation

- **Package Documentation**: <https://paradedb.github.io/django-paradedb>
- **ParadeDB Official Docs**: <https://docs.paradedb.com>
- **ParadeDB Website**: <https://paradedb.com>

## Development

```bash
# Install dev dependencies
pip install -e ".[dev]"

# Setup pre-commit hooks
pre-commit install

# Run tests
pytest

# Run linting
ruff check .
ruff format .
```

### Integration tests

```bash
# Start ParadeDB locally (uses Docker) and run the integration suite
bash scripts/run_integration_tests.sh
```

## Support

If you're missing a feature or have found a bug, please open a
[GitHub Issue](https://github.com/paradedb/django-paradedb/issues/new/choose).

To get community support, you can:

- Post a question in the [ParadeDB Slack Community](https://join.slack.com/t/paradedbcommunity/shared_invite/zt-32abtyjg4-yoYoi~RPh9MSW8tDbl0BQw)
- Ask for help on our [GitHub Discussions](https://github.com/paradedb/paradedb/discussions)

If you need commercial support, please [contact the ParadeDB team](mailto:sales@paradedb.com).

## Acknowledgments

We would like to thank the following members of the Django community for their valuable feedback and reviews during the development of this package:

- [Timothy Allen](https://github.com/FlipperPA) - Principal Engineer at The Wharton School, PSF and DSF member
- [Frank Wiles](https://github.com/frankwiles) - President & Founder of REVSYS

## License

django-paradedb is licensed under the [MIT License](LICENSE).
