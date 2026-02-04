# django-paradedb

[![CI](https://github.com/paradedb/django-paradedb/actions/workflows/ci.yml/badge.svg)](https://github.com/paradedb/django-paradedb/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/django-paradedb)](https://pypi.org/project/django-paradedb/)
[![License](https://img.shields.io/github/license/paradedb/django-paradedb?color=blue)](https://github.com/paradedb/django-paradedb?tab=MIT-1-ov-file#readme)
[![Slack URL](https://img.shields.io/badge/Join%20Slack-purple?logo=slack&link=https%3A%2F%2Fjoin.slack.com%2Ft%2Fparadedbcommunity%2Fshared_invite%2Fzt-32abtyjg4-yoYoi~RPh9MSW8tDbl0BQw)](https://join.slack.com/t/paradedbcommunity/shared_invite/zt-32abtyjg4-yoYoi~RPh9MSW8tDbl0BQw)
[![X URL](https://img.shields.io/twitter/url?url=https%3A%2F%2Ftwitter.com%2Fparadedb&label=Follow%20%40paradedb)](https://x.com/paradedb)

[ParadeDB](https://paradedb.com) — simple, Elastic-quality search for Postgres — integration for Django ORM.

## Requirements & Compatibility

| Component  | Version                          |
|------------|----------------------------------|
| Python     | 3.13+                            |
| Django     | 6.0+                             |
| ParadeDB   | 0.21.* (tested on 0.21.4)        |
| PostgreSQL | 17, 18 (with ParadeDB extension) |

## Installation

```bash
pip install django-paradedb
```

## Quick Start

Add a BM25 index to your model

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
                    'description': {'tokenizer': 'default'},
                    'category': {'tokenizer': 'keyword'},
                    'rating': {},
                },
                key_field='id',
                name='product_search_idx',
            ),
        ]
```

Search with a simple query

```python
from paradedb.search import ParadeDB

Product.objects.filter(description=ParadeDB('shoes'))
```

Check out some examples:

- [Quick Start](examples/quickstart/quickstart.py)
- [Faceted Search](examples/faceted_search/faceted_search.py)
- [Autocomplete](examples/autocomplete/autocomplete.py)
- [More Like This](examples/more_like_this/more_like_this.py)
- [Hybrid Search (RRF)](examples/hybrid_rrf/hybrid_rrf.py)
- [RAG](examples/rag/rag.py)

## BM25 Index

Define a BM25 index on your model fields. For more advanced indexing options like JSON indexing or indexing expressions, see the [ParadeDB Indexing Documentation](https://docs.paradedb.com/documentation/indexing/create-index).

```python
from paradedb.indexes import BM25Index

class Meta:
    indexes = [
        BM25Index(
            fields={
                'id': {},
                'title': {'tokenizer': 'default'},
                'body': {'tokenizer': 'default', 'stemmer': 'English'},
                'category': {'tokenizer': 'keyword'},
            },
            key_field='id',
            name='article_idx',
        ),
    ]
```

For a full list of supported tokenizers and their configurations, please refer to the [ParadeDB Tokenizer Documentation](https://docs.paradedb.com/documentation/tokenizers/overview).

```python
'body': {
    'tokenizer': 'default',
    'stemmer': 'English',        # Stemming language
    'filters': ['lowercase'],    # Token filters
}
```

### JSON Field Keys

Index specific keys within a JSONField

```python
'metadata': {
    'json_keys': {
        'author': {'tokenizer': 'keyword'},
        'tags': {'tokenizer': 'default'},
    }
}
```

## Query Types

For a full list of supported query types and advanced options, please refer to the [ParadeDB Query Builder Documentation](https://docs.paradedb.com/documentation/query-builder/overview).

### Basic Search

Simple full-text search with `&&&` (AND) operator

```python
from paradedb.search import ParadeDB

# Single term
Product.objects.filter(description=ParadeDB('shoes'))

# Multiple terms (AND)
Product.objects.filter(description=ParadeDB('running', 'shoes'))
```

### Boolean Composition with PQ

Use `PQ` for explicit boolean logic

```python
from paradedb.search import ParadeDB, PQ

# OR query
Product.objects.filter(description=ParadeDB(PQ('shoes') | PQ('boots')))

# AND query
Product.objects.filter(description=ParadeDB(PQ('running') & PQ('shoes')))

# Combine multiple terms
Product.objects.filter(
    description=ParadeDB(PQ('shoes') | PQ('boots') | PQ('sandals'))
)
```

### Phrase Search

Match exact phrases with optional slop (word distance)

```python
from paradedb.search import ParadeDB, Phrase

# Exact phrase
Product.objects.filter(description=ParadeDB(Phrase('running shoes')))

# Phrase with slop (allow up to 2 words between)
Product.objects.filter(description=ParadeDB(Phrase('running shoes', slop=2)))
```

### Fuzzy Search

Match terms with typo tolerance (Levenshtein distance)

```python
from paradedb.search import ParadeDB, Fuzzy

# Fuzzy match with distance 1 (default)
Product.objects.filter(description=ParadeDB(Fuzzy('shoez')))

# Fuzzy match with distance 2 (max)
Product.objects.filter(description=ParadeDB(Fuzzy('runing', distance=2)))
```

### Term Query

Match exact terms without tokenization

```python
from paradedb.search import ParadeDB, Term

Product.objects.filter(category=ParadeDB(Term('electronics')))
```

### Regex Query

Match terms using a regular expression

```python
from paradedb.search import ParadeDB, Regex

Product.objects.filter(description=ParadeDB(Regex('run.*')))
```

### Match All

Return all documents (useful with facets)

```python
from paradedb.search import ParadeDB, All

Product.objects.filter(id=ParadeDB(All()))
```

### More Like This

Find similar documents

```python
from paradedb.search import MoreLikeThis

# Similar to a specific document by ID
Product.objects.filter(MoreLikeThis(product_id=42))

# Similar to multiple documents
Product.objects.filter(MoreLikeThis(product_ids=[1, 2, 3]))

# Similar to a custom document
Product.objects.filter(
    MoreLikeThis(document={"description": "comfortable running shoes"})
)

# With tuning parameters
Product.objects.filter(
    MoreLikeThis(
        product_id=42,
        min_term_freq=2,
        max_query_terms=25,
        min_doc_freq=5,
    )
)
```

## Annotations

### BM25 Score

Get the relevance score for each result. For more information on how scores are calculated, see [BM25 Scoring](https://docs.paradedb.com/documentation/sorting/score).

```python
from paradedb.functions import Score

Product.objects.filter(
    description=ParadeDB('shoes')
).annotate(
    score=Score()
).order_by('-score')
```

### Snippet

Get highlighted text snippets. For more details on snippet configuration, see [Highlighting](https://docs.paradedb.com/documentation/full-text/highlight).

```python
from paradedb.functions import Snippet

Product.objects.filter(
    description=ParadeDB('shoes')
).annotate(
    highlight=Snippet('description', start_sel='<b>', stop_sel='</b>')
)
```

Snippet options:

| Option         | Description                    |
|----------------|--------------------------------|
| `start_sel`    | Opening highlight tag          |
| `stop_sel`     | Closing highlight tag          |
| `max_num_chars`| Maximum snippet length         |

## Faceted Search

For a full list of supported aggregations and advanced options, please refer to the [ParadeDB Aggregations Documentation](https://docs.paradedb.com/documentation/aggregates/overview).

Get aggregated counts alongside results

```python
from paradedb.search import ParadeDB

# Basic faceted search
rows, facets = (
    Product.objects.filter(description=ParadeDB('shoes'))
    .order_by('id')[:10]
    .facets('category')
)
# facets = {'buckets': [{'key': 'footwear', 'doc_count': 5}, ...]}
```

Facets-only (no rows)

```python
facets = (
    Product.objects.filter(description=ParadeDB('shoes'))
    .facets('category', include_rows=False)
)
```

Multiple facet fields

```python
rows, facets = (
    Product.objects.filter(description=ParadeDB('shoes'))
    .order_by('id')[:10]
    .facets('category', 'rating')
)
# facets = {'category_terms': {...}, 'rating_terms': {...}}
```

Facet options

```python
.facets(
    'category',
    size=20,           # Number of buckets (default: 10)
    order='-count',    # Sort order: count, -count, key, -key
    missing='Unknown', # Value for documents without the field
)
```

Custom aggregation JSON

```python
.facets(agg={'value_count': {'field': 'id'}})
```

## Custom Manager

If you have a custom manager, compose it with `ParadeDBQuerySet`

```python
from paradedb.queryset import ParadeDBQuerySet

class CustomManager(models.Manager):
    def active(self):
        return self.filter(is_active=True)

CustomManagerWithFacets = CustomManager.from_queryset(ParadeDBQuerySet)

class Product(models.Model):
    objects = CustomManagerWithFacets()
```

## Django ORM Integration

Works seamlessly with Django's ORM features

```python
from django.db.models import Q

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

## Documentation

- **Package Documentation**: <https://paradedb.github.io/django-paradedb>
- **ParadeDB Official Docs**: <https://docs.paradedb.com>
- **ParadeDB Website**: <https://paradedb.com>

## Development

### Setup

```bash
# Install dev dependencies
pip install -e ".[dev]"

# Setup pre-commit hooks
pre-commit install
```

### Testing

**Unit tests** verify individual components and logic without requiring a database connection.

**Integration tests** validate the full workflow against a real ParadeDB instance to ensure everything works end-to-end.

```bash
# Run unit tests only
pytest

# Run integration tests (requires Docker)
# This script automatically starts ParadeDB in Docker and runs the integration suite
bash scripts/run_integration_tests.sh

# Or manually start ParadeDB and run integration tests
bash scripts/run_paradedb.sh  # Starts ParadeDB container
export PARADEDB_INTEGRATION=1
export PARADEDB_TEST_DSN="postgresql://postgres:postgres@localhost:5432/postgres"
pytest -m integration
```

### Linting & Type Checking

```bash
# Run linting
ruff check .
ruff format .

# Run type checking
mypy src/paradedb
```

For more details on contributing, development workflow, and PR conventions, see our [Contributing Guide](CONTRIBUTING.md).

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
