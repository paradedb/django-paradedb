# django-paradedb

[![PyPI](https://img.shields.io/pypi/v/django-paradedb)](https://pypi.org/project/django-paradedb/)
[![Codecov](https://codecov.io/gh/paradedb/django-paradedb/graph/badge.svg)](https://codecov.io/gh/paradedb/django-paradedb)
[![CI](https://github.com/paradedb/django-paradedb/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/paradedb/django-paradedb/actions/workflows/ci.yml)
[![License](https://img.shields.io/github/license/paradedb/django-paradedb?color=blue)](https://github.com/paradedb/django-paradedb?tab=MIT-1-ov-file#readme)
[![Slack URL](https://img.shields.io/badge/Join%20Slack-purple?logo=slack&link=https%3A%2F%2Fjoin.slack.com%2Ft%2Fparadedbcommunity%2Fshared_invite%2Fzt-32abtyjg4-yoYoi~RPh9MSW8tDbl0BQw)](https://join.slack.com/t/paradedbcommunity/shared_invite/zt-32abtyjg4-yoYoi~RPh9MSW8tDbl0BQw)
[![X URL](https://img.shields.io/twitter/url?url=https%3A%2F%2Ftwitter.com%2Fparadedb&label=Follow%20%40paradedb)](https://x.com/paradedb)

[ParadeDB](https://paradedb.com) — simple, Elastic-quality search for Postgres — integration for Django ORM.

## Requirements & Compatibility

| Component  | Supported                        |
| ---------- | -------------------------------- |
| Python     | 3.10+                            |
| Django     | 4.2+                             |
| ParadeDB   | 0.21.0+                          |
| PostgreSQL | 17+    (with ParadeDB extension) |

## Installation

```bash
pip install django-paradedb
```

## Quick Start

Add a BM25 index to your model and use `ParadeDBManager`:

```python
from django.db import models
from paradedb.indexes import BM25Index
from paradedb.queryset import ParadeDBManager

class Product(models.Model):
    description = models.TextField()
    category = models.CharField(max_length=100)
    rating = models.IntegerField(default=0)

    objects = ParadeDBManager()

    class Meta:
        indexes = [
            BM25Index(
                fields={
                    'id': {},
                    'description': {'tokenizer': 'unicode_words'},
                    'category': {'tokenizer': 'literal'},
                    'rating': {},
                },
                key_field='id',
                name='product_search_idx',
            ),
        ]
```

Run migrations to create the index:

```bash
python manage.py makemigrations
python manage.py migrate
```

Search with a simple query:

```python
from paradedb.search import ParadeDB, Fuzzy

# Single term
Product.objects.filter(description=ParadeDB('shoes'))

# Multiple terms (AND by default)
Product.objects.filter(description=ParadeDB('running', 'shoes'))

# OR across terms
Product.objects.filter(description=ParadeDB('shoes', 'boots', operator='OR'))

# Fuzzy search (typo tolerance)
Product.objects.filter(description=ParadeDB(Fuzzy('shoez')))
```

Annotate with BM25 relevance score and sort by it:

```python
from paradedb.functions import Score

Product.objects.filter(
    description=ParadeDB('shoes')
).annotate(
    score=Score()
).order_by('-score')
```

## Django ORM Integration

django-paradedb works seamlessly with Django's ORM features:

```python
from django.db.models import Q
from paradedb.search import ParadeDB

# Combine with Q objects
Product.objects.filter(
    Q(description=ParadeDB('shoes')) & Q(rating__gte=4)
)

# Chain with standard filters
Product.objects.filter(
    description=ParadeDB('shoes')
).filter(
    category='footwear'
).exclude(
    rating__lt=3
)

# Select related
Product.objects.filter(
    description=ParadeDB('shoes')
).select_related('brand')

# Prefetch related
Product.objects.filter(
    description=ParadeDB('shoes')
).prefetch_related('reviews')
```

## Custom Manager

If you have a custom manager, compose it with `ParadeDBQuerySet`:

```python
from paradedb.queryset import ParadeDBQuerySet

class CustomManager(models.Manager):
    def active(self):
        return self.filter(is_active=True)

CustomManagerWithParadeDB = CustomManager.from_queryset(ParadeDBQuerySet)

class Product(models.Model):
    objects = CustomManagerWithParadeDB()
```

## Common Errors

### "facets() requires a ParadeDB operator in the WHERE clause"

```python
# ❌ Missing ParadeDB filter
Product.objects.filter(price__lt=100).order_by('id')[:10].facets('category')

# ✅ Add a ParadeDB search filter
Product.objects.filter(
    price__lt=100,
    description=ParadeDB('shoes')
).order_by('id')[:10].facets('category')
```

### "facets(include_rows=True) requires order_by() and a LIMIT"

```python
# ❌ Missing ordering or limit
Product.objects.filter(description=ParadeDB('shoes'))[:10].facets('category')
Product.objects.filter(description=ParadeDB('shoes')).order_by('id').facets('category')

# ✅ Both ordering and limit
Product.objects.filter(description=ParadeDB('shoes')).order_by('id')[:10].facets('category')

# ✅ Or skip rows entirely
Product.objects.filter(description=ParadeDB('shoes')).facets('category', include_rows=False)
```

## Security

django-paradedb uses SQL literal escaping (rather than parameterized queries) for search terms. This is intentional: ParadeDB's full-text operators (`&&&`, `|||`, `===`, `@@@`, etc.) require string literals that the query planner can inspect at parse time — parameterized placeholders are incompatible with this design. All user input is escaped via PostgreSQL's standard single-quote escaping (`'` → `''`) before being embedded in the query. The implementation is covered by 300+ tests including special-character and injection cases. `MoreLikeThis` and standard Django filters continue to use normal parameterization.

## Examples

- [Quick Start](examples/quickstart/quickstart.py)
- [Faceted Search](examples/faceted_search/faceted_search.py)
- [Autocomplete](examples/autocomplete/autocomplete.py)
- [More Like This](examples/more_like_this/more_like_this.py)
- [Hybrid Search (RRF)](examples/hybrid_rrf/hybrid_rrf.py)
- [RAG](examples/rag/rag.py)

## Documentation

- **Package Documentation**: <https://paradedb.github.io/django-paradedb>
- **ParadeDB Official Docs**: <https://docs.paradedb.com>
- **ParadeDB Website**: <https://paradedb.com>

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for development setup, running tests, linting, and the PR workflow.

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
