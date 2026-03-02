# django-paradedb

[![PyPI](https://img.shields.io/pypi/v/django-paradedb)](https://pypi.org/project/django-paradedb/)
[![Codecov](https://codecov.io/gh/paradedb/django-paradedb/graph/badge.svg)](https://codecov.io/gh/paradedb/django-paradedb)
[![CI](https://github.com/paradedb/django-paradedb/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/paradedb/django-paradedb/actions/workflows/ci.yml)
[![License](https://img.shields.io/github/license/paradedb/django-paradedb?color=blue)](https://github.com/paradedb/django-paradedb?tab=MIT-1-ov-file#readme)
[![Slack URL](https://img.shields.io/badge/Join%20Slack-purple?logo=slack&link=https%3A%2F%2Fjoin.slack.com%2Ft%2Fparadedbcommunity%2Fshared_invite%2Fzt-32abtyjg4-yoYoi~RPh9MSW8tDbl0BQw)](https://join.slack.com/t/paradedbcommunity/shared_invite/zt-32abtyjg4-yoYoi~RPh9MSW8tDbl0BQw)
[![X URL](https://img.shields.io/twitter/url?url=https%3A%2F%2Ftwitter.com%2Fparadedb&label=Follow%20%40paradedb)](https://x.com/paradedb)

[ParadeDB](https://paradedb.com) — simple, Elastic-quality search for Postgres — integration for Django ORM.

## Requirements & Compatibility

| Component  | Supported                     |
| ---------- | ----------------------------- |
| Python     | 3.10+                         |
| Django     | 4.2+                          |
| ParadeDB   | 0.21.0+                       |
| PostgreSQL | 17+ (with ParadeDB extension) |

## Installation

```bash
uv add django-paradedb
```

## Quick Start

### Prerequisites

This guide assumes you have installed `pg_search`, and have configured your Django project with
the Postgres database where `pg_search` is installed.

### Create an Index

Add a BM25 index to your model and use `ParadeDBManager`:

```python
from django.db import models
from django.contrib.postgres.fields import IntegerRangeField
from paradedb.indexes import BM25Index
from paradedb.queryset import ParadeDBManager

class MockItemDjango(models.Model):
    description = models.TextField(null=True, blank=True)
    rating = models.IntegerField(null=True, blank=True)
    category = models.CharField(max_length=255, null=True, blank=True)
    in_stock = models.BooleanField(null=True, blank=True)
    metadata = models.JSONField(null=True, blank=True)
    created_at = models.DateTimeField(null=True, blank=True)
    last_updated_date = models.DateField(null=True, blank=True)
    latest_available_time = models.TimeField(null=True, blank=True)
    weight_range = IntegerRangeField(null=True, blank=True)

    objects = ParadeDBManager()

    class Meta:
        db_table = "mock_items_django"
        indexes = [
            BM25Index(
                fields={
                    "id": {},
                    "description": {"tokenizer": "unicode_words"},
                    "category": {"tokenizer": "literal"},
                    "rating": {},
                    "in_stock": {},
                    "metadata": {},
                    "created_at": {},
                    "last_updated_date": {},
                    "latest_available_time": {},
                    "weight_range": {},
                },
                key_field="id",
                name="search_idx",
            ),
        ]
```

Run migrations to create the index:

```bash
python manage.py makemigrations
python manage.py migrate
```

### Index Computed Expressions

You can index computed expressions using `IndexExpression`. This allows indexing
transformed values or combinations of fields:

```python
from django.db.models import F
from django.db.models.functions import Lower
from paradedb.indexes import BM25Index, IndexExpression

class Article(models.Model):
    title = models.CharField(max_length=255)
    body = models.TextField()
    views = models.IntegerField(default=0)

    class Meta:
        indexes = [
            BM25Index(
                fields={"id": {}, "title": {}, "body": {}},
                expressions=[
                    # Text expression with tokenizer
                    IndexExpression(
                        Lower("title"),
                        alias="title_lower",
                        tokenizer="simple",
                    ),
                    # Non-text expression with pdb.alias
                    IndexExpression(
                        F("views"),
                        alias="views_indexed",
                    ),
                ],
                key_field="id",
                name="article_search_idx",
            ),
        ]
```

For text expressions, specify a tokenizer. For non-text expressions (integers,
timestamps, etc.), omit the tokenizer to use `pdb.alias`.

### Generate Test Data

To demonstrate search, we need to populate the table we just created.
First, open a Python shell:

```bash
python manage.py shell
```

And paste the following commands:

```python
from django.db import connection

cursor = connection.cursor()

cursor.execute("""
    CALL paradedb.create_bm25_test_table(
      schema_name => 'public',
      table_name  => 'mock_items'
    );
""")

cursor.execute("""
    INSERT INTO public.mock_items_django
    SELECT * FROM public.mock_items;
""")

cursor.close()
```

### Text Search

Search with a simple query:

```python
from paradedb.search import ParadeDB, Match, Term

# Single term
MockItemDjango.objects.filter(description=ParadeDB(Match('shoes', operator='AND')))

# Multiple terms (explicit AND)
MockItemDjango.objects.filter(description=ParadeDB(Match('running', 'shoes', operator='AND')))

# OR across terms
MockItemDjango.objects.filter(description=ParadeDB(Match('shoes', 'boots', operator='OR')))

# Fuzzy search (typo tolerance via distance)
MockItemDjango.objects.filter(description=ParadeDB(Match('shoez', operator='OR', distance=1)))

# Fuzzy prefix (distance + prefix matching)
MockItemDjango.objects.filter(description=ParadeDB(Term('runn', distance=1, prefix=True)))

# Fuzzy transposition-cost-one
MockItemDjango.objects.filter(description=ParadeDB(Term('shose', distance=1, transposition_cost_one=True)))
```

Annotate with BM25 relevance score and sort by it:

```python
from paradedb.functions import Score

MockItemDjango.objects.filter(
    description=ParadeDB(Match('shoes', operator='AND'))
).annotate(
    score=Score()
).order_by('-score')
```

## Django ORM Compatibility

`django-paradedb` works seamlessly with Django's ORM features:

```python
from django.db.models import Q
from paradedb.search import ParadeDB, Match

# Combine with Q objects
MockItemDjango.objects.filter(
    Q(description=ParadeDB(Match('shoes', operator='AND'))) & Q(rating__gte=4)
)

# Chain with standard filters
MockItemDjango.objects.filter(
    description=ParadeDB(Match('shoes', operator='AND'))
).filter(
    category='footwear'
).exclude(
    rating__lt=4
)
```

## Custom Manager

If you have a custom manager, compose it with `ParadeDBQuerySet`:

```python
from paradedb.queryset import ParadeDBQuerySet

class CustomManager(models.Manager):
    def active(self):
        return self.filter(is_active=True)

CustomManagerWithParadeDB = CustomManager.from_queryset(ParadeDBQuerySet)

class MockItemDjango(models.Model):
    objects = CustomManagerWithParadeDB()
```

## Diagnostics Helpers and Commands

`django-paradedb` includes helper functions for ParadeDB diagnostic table functions and
optional Django management commands:

- `paradedb_indexes()`
- `paradedb_index_segments()`
- `paradedb_verify_index()`
- `paradedb_verify_all_indexes()`

Python helper example:

```python
from paradedb.functions import paradedb_indexes, paradedb_verify_index

# Uses Django's default DB alias ("default")
rows = paradedb_indexes()

# Multi-DB: run against a specific database alias
checks = paradedb_verify_index("search_idx", using="search")
```

Management command examples:

```bash
# Uses Django's default DB alias ("default")
python manage.py paradedb_indexes

# Multi-DB: target a specific database alias
python manage.py paradedb_verify_index search_idx --database search
```

Notes:

- Management commands are discovered by Django only when `"paradedb"` is in `INSTALLED_APPS`.
- The selected database must have ParadeDB (`pg_search`) installed, and the target BM25 index must exist there.
- Some diagnostics functions may not be available on older `pg_search` versions.

## Common Errors

### "facets() requires a ParadeDB search condition in the WHERE clause"

```python
# ❌ Missing ParadeDB filter
MockItemDjango.objects.filter(rating__lt=4).order_by('id')[:10].facets('category')

# ✅ Add a ParadeDB search filter
MockItemDjango.objects.filter(
    rating__gte=4,
    description=ParadeDB(Match('shoes', operator='AND'))
).order_by('id')[:10].facets('category')
```

### "facets(include_rows=True) requires order_by() and a LIMIT"

```python
# ❌ Missing ordering or limit
MockItemDjango.objects.filter(description=ParadeDB(Match('shoes', operator='AND')))[:10].facets('category')
MockItemDjango.objects.filter(description=ParadeDB(Match('shoes', operator='AND'))).order_by('id').facets('category')

# ✅ Both ordering and limit
MockItemDjango.objects.filter(description=ParadeDB(Match('shoes', operator='AND'))).order_by('id')[:10].facets('category')

# ✅ Or skip rows entirely
MockItemDjango.objects.filter(description=ParadeDB(Match('shoes', operator='AND'))).facets('category', include_rows=False)
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
