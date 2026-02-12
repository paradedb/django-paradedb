# django-paradedb

[![PyPI](https://img.shields.io/pypi/v/django-paradedb)](https://pypi.org/project/django-paradedb/)
[![Codecov](https://codecov.io/gh/paradedb/django-paradedb/graph/badge.svg)](https://codecov.io/gh/paradedb/django-paradedb)
[![CI](https://github.com/paradedb/django-paradedb/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/paradedb/django-paradedb/actions/workflows/ci.yml)
[![License](https://img.shields.io/github/license/paradedb/django-paradedb?color=blue)](https://github.com/paradedb/django-paradedb?tab=MIT-1-ov-file#readme)
[![Slack URL](https://img.shields.io/badge/Join%20Slack-purple?logo=slack&link=https%3A%2F%2Fjoin.slack.com%2Ft%2Fparadedbcommunity%2Fshared_invite%2Fzt-32abtyjg4-yoYoi~RPh9MSW8tDbl0BQw)](https://join.slack.com/t/paradedbcommunity/shared_invite/zt-32abtyjg4-yoYoi~RPh9MSW8tDbl0BQw)
[![X URL](https://img.shields.io/twitter/url?url=https%3A%2F%2Ftwitter.com%2Fparadedb&label=Follow%20%40paradedb)](https://x.com/paradedb)

[ParadeDB](https://paradedb.com) — simple, Elastic-quality search for Postgres — integration for Django ORM.

## Requirements & Compatibility

| Component  | Supported                        | Tested in CI         |
| ---------- | -------------------------------- | -------------------- |
| Python     | 3.13+                            | 3.10, 3.11, 3.12, 3.13 |
| Django     | 6.0+                             | 5.2, 6.0             |
| ParadeDB   | 0.21.\*                          |                      |
| PostgreSQL | 17, 18 (with ParadeDB extension) |                      |

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

### Migrations

BM25Index works seamlessly with Django's migration system. You can add indexes to existing models or new models - Django will automatically generate and apply the necessary migrations.

**Adding an index to an existing model:**

```python
# Simply add BM25Index to your existing model's Meta.indexes
class Article(models.Model):
    title = models.TextField()
    body = models.TextField()

    class Meta:
        indexes = [
            BM25Index(
                fields={'id': {}, 'title': {}, 'body': {}},
                key_field='id',
                name='article_idx',
            ),
        ]
```

Then run Django's standard migration commands:

```bash
python manage.py makemigrations
python manage.py migrate
```

**Modifying an existing index:**

To change index configuration (e.g., tokenizer settings), remove the old index and add a new one with a different name. Django will drop and recreate the index during migration.

**Important notes:**

- The table can contain existing data when adding a BM25Index - the index will be built from the existing rows
- Index creation may take time on large tables (millions of rows)
- Django automatically handles index cleanup when reverting migrations

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

ParadeDB provides two ways to perform AND operations:

#### Simple AND - Multiple Terms (Recommended for most cases)

```python
from paradedb.search import ParadeDB

# Simple syntax - terms are automatically combined with AND
Product.objects.filter(description=ParadeDB('running', 'shoes'))
# SQL: WHERE description &&& ARRAY['running', 'shoes']
```

**Use this when:** You have a simple list of terms that must all match.

#### Explicit PQ Objects - Complex Boolean Logic

```python
from paradedb.search import ParadeDB, PQ

# OR query - find documents matching ANY term
Product.objects.filter(description=ParadeDB(PQ('shoes') | PQ('boots')))
# SQL: WHERE description ||| ARRAY['shoes', 'boots']

# AND query - explicit boolean combination
Product.objects.filter(description=ParadeDB(PQ('running') & PQ('shoes')))
# SQL: WHERE description &&& ARRAY['running', 'shoes']

# Combine multiple terms with OR
Product.objects.filter(
    description=ParadeDB(PQ('shoes') | PQ('boots') | PQ('sandals'))
)
```

**Use PQ when:**

- You need **OR logic** (must use PQ with `|`)
- You're building dynamic queries where the operator might vary
- You want explicit control over boolean operators

#### Combining with Django Q Objects

Mix ParadeDB search with Django's Q objects for complex filtering:

```python
from django.db.models import Q
from paradedb.search import ParadeDB, PQ

# (ParadeDB search AND standard filter) OR (different search AND filter)
Product.objects.filter(
    Q(description=ParadeDB('running', 'shoes'), rating__gte=4) |
    Q(description=ParadeDB(PQ('boots') | PQ('sandals')), in_stock=True)
)
```

**Note:** The simple comma-separated syntax `ParadeDB('a', 'b')` is equivalent to `ParadeDB(PQ('a') & PQ('b'))` but more concise. Use the simple syntax unless you need OR operations or explicit boolean control.

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

Find similar documents based on term frequency analysis.

**Note:** Unlike other search expressions, `MoreLikeThis` is a filter `Expression` (not a lookup), so it's used directly in `.filter()` without wrapping in `ParadeDB()`. This is because MLT operates on the entire indexed document (typically multiple fields) rather than a single field.

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

**Combining with other filters:**

Since `MoreLikeThis` is an `Expression`, it composes naturally with Django's ORM:

```python
from django.db.models import Q

# Combine with standard filters
Product.objects.filter(
    MoreLikeThis(product_id=42),
    in_stock=True,
    rating__gte=4
)

# Use with Q objects for complex logic
Product.objects.filter(
    Q(MoreLikeThis(product_id=42)) | Q(category='featured')
)

# Chain with other querysets
Product.objects.filter(
    MoreLikeThis(product_id=42)
).exclude(
    id=42  # Exclude the source document itself
).order_by('-rating')[:10]
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

| Option          | Description            |
| --------------- | ---------------------- |
| `start_sel`     | Opening highlight tag  |
| `stop_sel`      | Closing highlight tag  |
| `max_num_chars` | Maximum snippet length |

## Faceted Search

For a full list of supported aggregations and advanced options, please refer to the [ParadeDB Aggregations Documentation](https://docs.paradedb.com/documentation/aggregates/overview).

### Requirements

The `.facets()` method has specific requirements based on how you use it:

**When using `include_rows=True` (default):**

- ✅ **MUST** have a ParadeDB search filter (e.g., `ParadeDB()` or `MoreLikeThis()`)
- ✅ **MUST** call `.order_by()` on the queryset
- ✅ **MUST** slice the queryset (e.g., `[:10]`)

**When using `include_rows=False`:**

- ✅ **MUST** have a ParadeDB search filter
- ❌ No ordering or slicing required

**Why these requirements?**

ParadeDB's aggregation uses window functions (`pdb.agg() OVER ()`) which require ordered, limited result sets when combined with row data. Without ordering and limits, PostgreSQL cannot efficiently compute the aggregations.

### Basic Usage

Get aggregated counts alongside results

```python
from paradedb.search import ParadeDB

# ✅ Correct: Has filter, ordering, and limit
rows, facets = (
    Product.objects.filter(description=ParadeDB('shoes'))
    .order_by('id')[:10]  # REQUIRED when include_rows=True
    .facets('category')
)
# facets = {'buckets': [{'key': 'footwear', 'doc_count': 5}, ...]}
```

```python
# ❌ This will raise ValueError
rows, facets = (
    Product.objects.filter(description=ParadeDB('shoes'))
    .facets('category')  # Missing order_by() and slice!
)
# ValueError: facets(include_rows=True) requires order_by() and a LIMIT.
```

Facets-only (no rows)

```python
# ✅ No ordering/limit needed when include_rows=False
facets = (
    Product.objects.filter(description=ParadeDB('shoes'))
    .facets('category', include_rows=False)
)
```

### Multiple Facet Fields

```python
rows, facets = (
    Product.objects.filter(description=ParadeDB('shoes'))
    .order_by('id')[:10]
    .facets('category', 'rating')
)
# facets = {'category_terms': {...}, 'rating_terms': {...}}
```

### Facet Options

```python
rows, facets = (
    Product.objects.filter(description=ParadeDB('shoes'))
    .order_by('rating')[:20]
    .facets(
        'category',
        size=20,           # Number of buckets (default: 10)
        order='-count',    # Sort order: count, -count, key, -key
        missing='Unknown', # Value for documents without the field
    )
)
```

### Custom Aggregation JSON

```python
rows, facets = (
    Product.objects.filter(description=ParadeDB('shoes'))
    .order_by('id')[:10]
    .facets(agg={'value_count': {'field': 'id'}})
)
```

### Combining with Other QuerySet Methods

```python
# Filter, annotate, order, limit, then facet
from paradedb.search import ParadeDB, Score

rows, facets = (
    Product.objects
    .filter(description=ParadeDB('running', 'shoes'), price__lt=100)
    .annotate(score=Score())
    .order_by('-score')[:20]
    .facets('category', 'brand')
)

# Works with prefetch_related
rows, facets = (
    Product.objects.filter(description=ParadeDB('shoes'))
    .prefetch_related('reviews')
    .order_by('id')[:10]
    .facets('category')
)
```

### Common Errors and Solutions

#### Error: "facets() requires a ParadeDB operator in the WHERE clause"

```python
# ❌ Missing ParadeDB filter
Product.objects.filter(price__lt=100).order_by('id')[:10].facets('category')

# ✅ Add a ParadeDB search filter
Product.objects.filter(
    price__lt=100,
    description=ParadeDB('shoes')  # Add this!
).order_by('id')[:10].facets('category')
```

#### Error: "facets(include_rows=True) requires order_by() and a LIMIT"

```python
# ❌ Missing ordering
Product.objects.filter(description=ParadeDB('shoes'))[:10].facets('category')

# ❌ Missing limit
Product.objects.filter(description=ParadeDB('shoes')).order_by('id').facets('category')

# ✅ Both ordering and limit
Product.objects.filter(description=ParadeDB('shoes')).order_by('id')[:10].facets('category')

# ✅ Or use include_rows=False
Product.objects.filter(description=ParadeDB('shoes')).facets('category', include_rows=False)
```

#### Error: "Facet field names must be unique"

```python
# ❌ Duplicate fields
.facets('category', 'category')

# ✅ Each field only once
.facets('category', 'brand')
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

## Security

### SQL Injection Protection

django-paradedb uses **SQL literal escaping** for search terms rather than parameterized queries. This design choice is intentional and safe:

**Escaping Strategy:**

- All user input is escaped using PostgreSQL's single-quote escaping (`'` → `''`)
- Search terms are wrapped in SQL string literals: `'user input'`
- This prevents SQL injection while maintaining compatibility with ParadeDB's full-text operators

**Implementation Details:**

```python
# All search terms are escaped via _quote_term()
def _quote_term(term: str) -> str:
    escaped = term.replace("'", "''")  # PostgreSQL standard escaping
    return f"'{escaped}'"
```

**Which features use escaping:**

- `ParadeDB()` - All search terms (strings, Phrase, Fuzzy, Parse, Term, Regex)
- `Snippet()` - HTML tag markers (start_sel, stop_sel)
- `Agg()` - JSON aggregation specs

**Which features use parameterization:**

- `MoreLikeThis()` - Uses `%s` placeholders for IDs, documents, and options
- Standard Django filters - Use Django's native parameterization

**Why literals instead of parameters?**

ParadeDB's full-text operators (`&&&`, `|||`, `###`, `@@@`) work with:

1. Single string literals: `description &&& 'shoes'`
2. Array literals: `description &&& ARRAY['running', 'shoes']`
3. Function calls with type casts: `description ### 'exact phrase'::pdb.slop(2)`

Parameterized queries would require PostgreSQL to parse the search syntax at execution time, which is incompatible with ParadeDB's operator design. The literal approach allows the query planner to optimize full-text searches effectively.

**Safety Guarantee:**

All escaping follows PostgreSQL's standard string literal rules. The implementation has been reviewed by Django Security Framework members and is protected by:

- Comprehensive test coverage (103 tests including special character escaping)
- Input validation at the ORM layer
- PostgreSQL's built-in literal escaping semantics

**Example - User Input is Safe:**

```python
# Even malicious input is safely escaped
user_query = "'; DROP TABLE products; --"
Product.objects.filter(description=ParadeDB(user_query))
# Generates: WHERE description &&& '''; DROP TABLE products; --'
# The query is escaped and treated as a literal search term
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

# Setup prek hooks
prek install
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
