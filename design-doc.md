
# ParadeDB's Plugin for Django ORM

## Overview

Overall Interface rests on following abstractions

1. ParadeDB - custom lookup. Wraps search terms and generates ParadeDB SQL operators like &&&, |||, ###
2. PQ  - Query object like Django's Q objects. Enables OR and complex boolean logic for ParadeDB’s operators.
3. Phrase, Fuzzy, Parse, Term, Regex, MoreLikeThis : Search Expression Classes. They map to pdb.functions for one on one. They are used inside ParadeDb custom lookup.
4. Snippet, Score : Custom Annotation functions. Can be combined with F objects for further computations on these generated columns.
5. BM25Index : Index creation function used in Models

## Interface Details, Examples & Generated SQL

## Setup & Indexing

```python
from django.db import models
from paradedb.indexes import BM25Index

class Product(models.Model):
    description = models.TextField()
    category = models.CharField(max_length=100)
    rating = models.IntegerField()
    in_stock = models.BooleanField()
    price = models.DecimalField(max_digits=10, decimal_places=2)
    created_at = models.DateTimeField(auto_now_add=True)
    metadata = models.JSONField(default=dict)

    objects = models.Manager()

    class Meta:
        indexes = [
            BM25Index(
                fields={
                    'id': {},
                    'description': {
                        'tokenizer': 'simple',
                         'filters': ['lowercase', 'stemmer'],
                         'stemmer': 'english',
                     },
                     'category': {
                         'tokenizer': 'simple',
                         'filters': ['lowercase']
                     },
                },
             key_field='id',
             name='product_search_idx',
        ),
    ]
```

Generated SQL:

```sql
-- CREATE INDEX product_search_idx ON products
--  USING bm25 (
--      id,
--      (description::pdb.simple('stemmer=english,lowercase=true')),
--      (category::pdb.simple('lowercase=true'))
--  )
--  WITH (key_field='id');

```

## BM25 Index Options

```python
# Advanced configuration
BM25Index(
        fields={
            # Standard columns
            'id': {},
            'description': {
                'tokenizer': 'simple',
                'filters': ['lowercase', 'stemmer']
            },
            'category': {
                'tokenizer': 'simple',
                'filters': ['lowercase', 'stemmer']
            },

           # Definition for the JSON field, assuming the column is named 'metadata'.
           # 'json_keys' is now an attribute of the field it applies to.
           'metadata': {
               'json_keys': {
                   # Each key from the JSON becomes a virtual field in the index.
                   # A tokenizer can be applied to them as well.
                   'title': {'tokenizer': 'simple', 'filters': ['lowercase']},
                   'brand': {'tokenizer': 'simple', 'filters': ['lowercase']},
                   'tags': {'tokenizer': 'simple', 'filters': ['lowercase']},
               }
           }
       },
       key_field='id',
       name='product_search_idx',
   )

```

Generated SQL:

```sql
CREATE INDEX product_search_idx ON products
  USING bm25 (
     id,

     (description::pdb.simple('lowercase=true,stemmer=english')),
     (category::pdb.simple('lowercase=true,stemmer=english')),
        -- JSON keys extracted into separate indexed expressions
     ((metadata->>'title')::pdb.simple('alias=metadata_title,lowercase=true')),
     ((metadata->>'brand')::pdb.simple('alias=metadata_brand,lowercase=true')),
     ((metadata->>'tags')::pdb.simple('alias=metadata_tags,lowercase=true'))
  )
 WITH (
    key_field='id'
 );
```

## Basic Search Operators

### Simple Search vs Complex Logic

ParadeDB provides three patterns for search:

1. **Simple Search** - Use strings for single terms
2. **Simple AND** - Use comma-separated terms within ParadeDB()
3. **Complex Logic** - Use PQ objects for OR operations, negation, and nesting

```python
from paradedb.search import ParadeDB

# Simple single term search
Product.objects.filter(description=ParadeDB('shoes'))

# Simple AND search (comma-separated)
Product.objects.filter(description=ParadeDB('running', 'shoes'))

# Complex logic requires PQ objects for OR conditions. This matches Django's Q object for an OR.
Product.objects.filter(description=ParadeDB(PQ('shoes') | PQ('sandals')))

# Mix simple AND with PQ for complex logic
Product.objects.filter(
    Q(description=ParadeDB('running', 'shoes')) | Q(category=ParadeDB('footwear'))
)
```

### Conjunction (AND)

Search for products containing multiple terms:

```python
Product.objects.filter(
    description=ParadeDB('running', 'shoes')
)
<QuerySet [
    <Product: Sleek running shoes>,
    <Product: Comfortable running shoes for athletes>,
    <Product: Lightweight running shoes with cushion>
]>

```

Generated SQL:

```sql
-- SELECT * FROM products
-- WHERE description &&& ARRAY['running', 'shoes'];

```

Single term search:

```python
Product.objects.filter(
    description=ParadeDB('shoes')
)
<QuerySet [
    <Product: Sleek running shoes>,
    <Product: Comfortable running shoes for athletes>,
    <Product: Casual shoes for everyday wear>,
    <Product: Lightweight running shoes with cushion>
]>

```

Generated SQL:

```sql
-- SELECT * FROM products
-- WHERE description &&& 'shoes';

```

### Disjunction (OR)

Search for products containing any of the specified terms. PQ object is used for paradedb OR, similar to Q object for default OR.

```python
from paradedb.search import ParadeDB, PQ

Product.objects.filter(
    description=ParadeDB(PQ('wireless') | PQ('bluetooth'))
)
<QuerySet [
    <Product: Compact wireless bluetooth speaker>,
    <Product: Wireless noise-cancelling headphones>,
    <Product: Bluetooth gaming mouse with RGB lighting>,
    <Product: Wireless charging pad for smartphones>
]>

```

Generated SQL:

```sql
-- SELECT * FROM products
-- WHERE description ||| ARRAY['wireless', 'bluetooth'];

```

### Phrase Queries

Search for exact phrases:

```python
from paradedb.search import Phrase

Product.objects.filter(
    description=ParadeDB(Phrase('wireless bluetooth'))
)
<QuerySet [
    <Product: Compact wireless bluetooth speaker>,
    <Product: Premium wireless bluetooth headphones>
]>

```

Generated SQL:

```sql
-- SELECT * FROM products
-- WHERE description ### 'wireless bluetooth';

```

Phrase with slop (allows terms to be within certain distance):

```python
Product.objects.filter(
    description=ParadeDB(Phrase('running shoes', slop=1))
)
<QuerySet [
    <Product: Sleek running shoes>,
    <Product: Comfortable running shoes for athletes>,
    <Product: Running shoes with advanced cushioning>,
    <Product: Lightweight running shoes with cushion>
]>

```

Generated SQL:

```sql
-- SELECT * FROM products
-- WHERE description ### 'running shoes'::pdb.slop(1);

```

### Fuzzy Matching

Search for terms with typos or variations:

```python
from paradedb.search import Fuzzy

Product.objects.filter(
    description=ParadeDB(Fuzzy('sheos', distance=1))
)
<QuerySet [
    <Product: Sleek running shoes>,
    <Product: Comfortable running shoes for athletes>,
    <Product: Casual shoes for everyday wear>
]>

```

Generated SQL:

```sql
-- SELECT * FROM products
-- WHERE description ||| 'runnning'::pdb.fuzzy(1);

```

Multiple fuzzy terms:

```python
Product.objects.filter(
    description=ParadeDB(
        Fuzzy('runnning', distance=1),
        Fuzzy('shoez', distance=1)
    )
)
<QuerySet [
    <Product: Sleek running shoes>,
    <Product: Lightweight running shoes with cushion>
]>

```

Generated SQL:

```sql
-- SELECT * FROM products
-- WHERE description ||| 'runnning'::pdb.fuzzy(1) OR description ||| 'shoez'::pdb.fuzzy(1);

```

## Complex Boolean Logic

### Operator Precedence

Operators follow this precedence (highest to lowest): Phrase > AND > OR > NOT

```python
from django.db.models import Q
from paradedb.search import Phrase, ParadeDB

# Find products with phrase "running shoes" AND rating >= 4
# OR products in Electronics category AND containing "wireless"
Product.objects.filter(
    Q(description=ParadeDB(Phrase('running shoes')), rating__gte=4) |
    Q(category=ParadeDB('Electronics'), description=ParadeDB('wireless'))
)
<QuerySet [
    <Product: Sleek running shoes>,  # rating: 5
    <Product: Compact wireless bluetooth speaker>,  # category: Electronics
    <Product: Wireless noise-cancelling headphones>  # category: Electronics
]>

```

Generated SQL:

```sql
-- SELECT * FROM products
-- WHERE (
--     (description ### 'running shoes' AND rating >= 4)
--     OR (category &&& 'Electronics' AND description &&& 'wireless')
-- );

```

### Nested Expressions

```python
from django.db.models import Q
from paradedb.search import Phrase, ParadeDB

# Find athletic running shoes that aren't cheap, OR electronics under $100
Product.objects.filter(
    Q(description=ParadeDB('running', 'athletic'), ~Q(description=ParadeDB('cheap'))) |
    Q(category=ParadeDB(Phrase('Electronics')), price__lt=100)
)
<QuerySet [
    <Product: Premium athletic running shoes>,
    <Product: Compact wireless bluetooth speaker>,
    <Product: Bluetooth gaming mouse with RGB lighting>
]>

```

Generated SQL:

```sql
-- SELECT * FROM products
-- WHERE (
--     (description &&& ARRAY['running', 'athletic'] AND NOT (description &&& 'cheap'))
--     OR (category ### 'Electronics' AND price < 100)
-- );

```

### More Complex Combinations

```python
from django.db.models import Q
from paradedb.search import Phrase, ParadeDB

# Multiple conditions with different priorities:
# (running shoes AND in stock) OR (electronics AND wireless AND price < 200)
Product.objects.filter(
    Q(description=ParadeDB('running', 'shoes'), in_stock=True) |
    Q(category=ParadeDB('Electronics'), description=Paradedb('wireless'), price__lt=200)
)
<QuerySet [
    <Product: Sleek running shoes>,
    <Product: Compact wireless bluetooth speaker>,
    <Product: Wireless charging pad for smartphones>
]>

```

Generated SQL:

```sql
-- SELECT * FROM products
-- WHERE (
--     (description &&& ARRAY['running', 'shoes'] AND in_stock = true)
--     OR (category &&& 'Electronics' AND description &&& 'wireless' AND price < 200)
-- );

```

## Query Builder Functions

### Parse Queries

Parse complex query strings:

```python
from paradedb.search import Parse, ParadeDB

Product.objects.filter(
    description=ParadeDB(Parse('running AND shoes', lenient=True))
)
<QuerySet [
    <Product: Sleek running shoes>,
    <Product: Comfortable running shoes for athletes>,
    <Product: Lightweight running shoes with cushion>
]>

```

Generated SQL:

```sql
-- SELECT * FROM products
-- WHERE description @@@ pdb.parse('running AND shoes', lenient => true);

```

### Term Queries

Exact term matching:

```python
from paradedb.search import Term, ParadeDB

Product.objects.filter(
    description=ParadeDB(Term('shoes'))
)
<QuerySet [
    <Product: Sleek running shoes>,
    <Product: Comfortable running shoes for athletes>,
    <Product: Casual shoes for everyday wear>,
    <Product: Lightweight running shoes with cushion>
]>

```

Generated SQL:

```sql
-- SELECT * FROM products
-- WHERE description @@@ pdb.term('shoes');

```

### Regex Queries

Search with regular expressions:

```python
from paradedb.search import Regex, ParadeDB

Product.objects.filter(
    description=ParadeDB(Regex('run.*shoes'))
)
<QuerySet [
    <Product: Sleek running shoes>,
    <Product: Comfortable running shoes for athletes>,
    <Product: Running shoes with advanced cushioning>
]>

```

Generated SQL:

```sql
-- SELECT * FROM products
-- WHERE description @@@ pdb.regex('run.*');
```

## More Like This

> Note: MoreLikeThis is used here as a top-level filter (e.g., Product.objects.filter(MoreLikeThis(...))), which is a different pattern from other search expressions that are wrapped within a ParadeDB() lookup (.filter(field=ParadeDB(...))). This is done as a "More Like This" query often implicitly operates on multiple fields as defined in the index, rather than being applied to just a single field.
>

```python
from paradedb.search import MoreLikeThis

# Find similar products to a specific product
Product.objects.filter(
    MoreLikeThis(product_id=5)  # Find products similar to product with id=5
)
```

Generated SQL:

```sql
-- The MLT query is used with the @@@ operator against the key_field.
SELECT * FROM products
WHERE id @@@ pdb.more_like_this(5);
```

```python
# Find similar to multiple reference products
Product.objects.filter(
    MoreLikeThis(product_ids=[5, 12, 23])
)
```

Generated SQL:

```sql
-- The ORM would likely translate a list of IDs into an OR query.
-- The underlying pdb.more_like_this function may also support arrays.
SELECT * FROM products
WHERE (id @@@ pdb.more_like_this(5))
   OR (id @@@ pdb.more_like_this(12))
   OR (id @@@ pdb.more_like_this(23));
```

```python
# Control similarity parameters
Product.objects.filter(
    MoreLikeThis(
        product_id=5,
        min_term_freq=2,        # Minimum term frequency in source doc
        max_query_terms=10,     # Maximum terms to use in query
        min_doc_freq=1          # Minimum document frequency for terms
    )
)
```

Generated SQL:

```sql
-- Tuning parameters are passed as named arguments to the function.
SELECT * FROM products
WHERE id @@@ pdb.more_like_this(
    5,
    min_term_frequency => 2,
    max_query_terms => 10,
    min_doc_frequency => 1
);
```

```python
# Similar to arbitrary text content (not just existing records)
Product.objects.filter(
    MoreLikeThis(
        text="comfortable running shoes for athletes",
        fields=['description', 'category']  # Fields to analyze
    )
)
```

Generated SQL:

```sql
-- Arbitrary text and a list of fields to consider can be passed.
SELECT * FROM products
WHERE id @@@ pdb.more_like_this(
    'comfortable running shoes for athletes',
    fields => ARRAY['description', 'category']
);
```

```python
# Combine with Django filters
Product.objects.filter(
    in_stock=True,
    rating__gte=4
).filter(
    MoreLikeThis(product_id=15)  # Find similar high-quality, in-stock products
)
```

Generated SQL:

```sql
-- The MLT query is combined with other filters using AND.
SELECT * FROM products
WHERE in_stock = true
  AND rating >= 4
  AND id @@@ pdb.more_like_this(15);
```

## Scoring and Ordering

Annotate results with search scores:

```python
from paradedb.search import Score, ParadeDB

Product.objects.filter(description=ParadeDB('running', 'shoes')).annotate(
    search_score=Score()
)[:]
<QuerySet [
    <Product: Sleek running shoes>,
    <Product: Comfortable running shoes for athletes>,
    <Product: Running shoes with advanced cushioning>
]>

```

Generated SQL:

```sql
-- SELECT *, pdb.score(id) AS search_score
-- FROM products
-- WHERE description &&& ARRAY['running', 'shoes']
```

### Weighted multi-field search

To apply different weights to different search terms, you should use boosting within the query itself. The `^` operator can be used to increase or decrease the importance of different parts of a query. The entire weighted query is defined in the `.filter()` call, and we then use the simple `Score()` annotation to retrieve the final calculated score.

```python
```python
from paradedb.search import Score, ParadeDB, Parse

Product.objects.filter(
    description=ParadeDB(Parse("(running shoes)^0.6 | (category:Footwear OR category:Athletic)^0.4"))
).annotate(
    search_score=Score(),
).filter(search_score__gt=0).order_by('-search_score')[:]
<QuerySet [
    <Product: Sleek running shoes>,  # search_score: 4.0
    <Product: Premium athletic running shoes>,  # search_score: 3.5
    <Product: Lightweight running shoes with cushion>  # search_score: 3.0
]
```

Generated SQL:

```sql
-- SELECT *, pdb.score(id) AS search_score
-- FROM products
-- WHERE
--    description @@@ pdb.parse('(running shoes)^0.6 | (category:Footwear OR category:Athletic)^0.4')
-- AND
--    pdb.score(id) > 0
-- ORDER BY
--    search_score DESC;

```

## Snippets and Highlighting

Extract highlighted snippets from a field.

```python
from paradedb.search import Snippet, ParadeDB, PQ

Product.objects.filter(
    description=ParadeDB(PQ('wireless') | PQ('bluetooth'))
).annotate(
    # Snippet infers the query from the filter context
    snippet=Snippet('description')
).values('id', 'description', 'snippet')
<QuerySet [
    {
        'id': 1,
        'description': 'Compact wireless bluetooth speaker',
        'snippet': 'Compact <b>wireless</b> <b>bluetooth</b> speaker'
    },
    {
        'id': 2,
        'description': 'Wireless noise-cancelling headphones',
        'snippet': '<b>Wireless</b> noise-cancelling headphones'
    },
    {
        'id': 5,
        'description': 'Bluetooth gaming mouse with RGB lighting',
        'snippet': '<b>Bluetooth</b> gaming mouse with RGB lighting'
    }
]>

```

Generated SQL:

```sql
-- SELECT id, description, pdb.snippet(description) AS snippet
-- FROM products
-- WHERE description ||| ARRAY['wireless', 'bluetooth'];
```

### Custom snippet formatting

```python
Product.objects.filter(
    description=ParadeDB('running', 'shoes')
).annotate(
    snippet=Snippet(
        'description',
        start_sel='<mark>',
        stop_sel='</mark>',
        max_num_chars=100
    )
).values('description', 'snippet')
<QuerySet [
    {
        'description': 'Sleek running shoes with advanced cushioning',
        'snippet': 'Sleek <mark>running</mark> <mark>shoes</mark> with advanced...'
    },
    {
        'description': 'Comfortable running shoes for athletes',
        'snippet': 'Comfortable <mark>running</mark> <mark>shoes</mark> for athletes'
    }
]>

```

Generated SQL:

```sql
-- SELECT description,
-- pdb.snippet(description, '<mark>', '</mark>', 100)
-- FROM products
-- WHERE description &&& ARRAY['running', 'shoes'];

```

## Combining with Django Filters

Mix ParadeDB search with standard Django ORM filters:

```python
# Search with price and stock filters
Product.objects.filter(
    description=ParadeDB('shoes')
).filter(
    price__lt=100,
    in_stock=True,
    rating__gte=4
)[:]
<QuerySet [
    <Product: Sleek running shoes>,  # price: 89.99, in_stock: True, rating: 5
    <Product: Comfortable running shoes for athletes>  # price: 79.99, in_stock: True, rating: 4
]>

```

Generated SQL:

```sql
-- SELECT * FROM products
-- WHERE description &&& 'shoes'
-- AND price < 100
-- AND in_stock = true
-- AND rating >= 4;

```

Conditional search based on user input:

```python
query = Product.objects.all()
if search_terms:
    query = query.filter(description=ParadeDB(*search_terms))  # Simple AND with comma separation
if category:
    query = query.filter(category=ParadeDB(Phrase(category)))
if min_rating:
    query = query.filter(rating__gte=min_rating)
results = query[:] # force query execution
<QuerySet [
    <Product: Sleek running shoes>,
    <Product: Compact wireless bluetooth speaker>,
    <Product: Premium wireless bluetooth headphones>
]>

```

Generated SQL (example with all conditions):

```sql
-- SELECT * FROM products
-- WHERE (
--     description &&& ARRAY['running', 'shoes']
--     AND category ### 'Footwear'
--     AND rating >= 4
-- );

```

## Combine CTE with ParadeDB

You can combine ParadeDB searches with advanced database features like Common Table Expressions (CTEs). The following example uses  `with_cte()` method to first find a set of categories using a ParadeDB search, and then uses that result to filter products.

```python
from django.db.models import F
from paradedb.search import ParadeDB, PQ

footwear_categories_cte = Category.objects.filter(
    name=ParadeDB(PQ('shoes') | PQ('sandals') | PQ('boots'))
).values('id')

# The .with_cte() method makes the CTE available to be referenced.
in_stock_footwear = Product.objects.with_cte(footwear_categories_cte).filter(
    in_stock=True,
    category_id__in=footwear_categories_cte.values('id')
)

```

Generated SQL:

```sql
-- The Django ORM translates this into a WITH clause
-- WITH footwear_categories_cte AS (
--    SELECT id FROM categories
--    WHERE name ||| ARRAY['shoes', 'sandals', 'boots']
--)
-- The main query can then reference the CTE
-- SELECT * FROM products
-- WHERE in_stock = true
-- AND category_id IN (SELECT id FROM footwear_categories_cte);
```

## Faceted Search (Aggregations)

The proposed API uses a `.facets()` method that can be chained after a `.filter()` call. You pass the names of the fields you want to get term counts for. This method executes the query and returns a dictionary of the aggregation results, not a QuerySet.

```python
# After searching for "shoes", get the counts for each "category"
# and each "brand" within the search results.
rows, facets = Product.objects.filter(
    description=ParadeDB('shoes')
).facets('category', 'brand')

# The method returns rows and the facet counts.
print(facets)
# Expected output:
# {
#     "category": {
#         "buckets": [
#             {"key": "Running", "doc_count": 50},
#             {"key": "Casual", "doc_count": 35},
#             {"key": "Hiking", "doc_count": 20},
#         ]
#     },
#     "brand": {
#         "buckets": [
#             {"key": "Nike", "doc_count": 45},
#             {"key": "Adidas", "doc_count": 30},
#             {"key": "New Balance", "doc_count": 30},
#         ]
#     }
# }
```

Generated SQL:

The `.facets('category', 'brand')` call would be responsible for generating the correct JSON aggregation request for the `paradedb.aggregate` function.

```sql
  SELECT description, pdb.agg('{"value_count": {"field": "id"}}') OVER ()
  FROM mock_items
  WHERE description ||| 'shoes'
  ORDER BY rating
  LIMIT 5;

```

### Detailed API Proposal

The goal is a Django-idiomatic API that mirrors QuerySet chaining and keeps ParadeDB-specific syntax encapsulated in `.facets(...)`.

```python
rows, facets = Product.objects.filter(
    description=ParadeDB("shoes")
).facets(
    "category",
    "brand",
    size=10,
    order="-count",
    missing="(missing)",
)

# Expected shape (mirrors Elasticsearch-style aggregation output):
# {
#   "category": {
#     "buckets": [{"key": "Running", "doc_count": 50}, ...]
#   },
#   "brand": {
#     "buckets": [{"key": "Nike", "doc_count": 45}, ...]
#   }
# }
```

#### Signature (proposed)

```python
QuerySet.facets(
    *fields: str,
    size: int | None = 10,
    order: str | None = "-count",
    missing: str | None = None,
    agg: dict[str, object] | None = None,
    include_rows: bool = True,
) -> tuple[list[Model], dict[str, object]] | dict[str, object]
```

Notes:

- `fields`: list of model field names to facet on (text, keyword, or numeric). Currently limited to one field unless `agg` is provided. Text facet fields must use a literal tokenizer in the BM25 index.
- `size`: max buckets per field; `None` emits no size clause.
- `order`: `"-count"` or `"count"` or `"key"`/`"-key"` to align with Django’s ordering style.
- `missing`: value for missing bucket (optional).
- `agg`: advanced escape hatch to pass raw Elasticsearch-style JSON for power users; when provided, `fields`/`size`/`order`/`missing` are ignored.
- `include_rows`: when `True`, return `(rows, facets)` via a window aggregate; when `False`, return facets only.

#### SQL shape

Faceted search uses `pdb.agg(...) OVER ()` and returns both rows and an aggregation payload. ParadeDB requires an `ORDER BY ... LIMIT` Top-N query and a ParadeDB operator in the WHERE clause for this to work.

```sql
SELECT
  *,
  pdb.agg('{"terms": {"field": "category", "size": 10}}') OVER () AS facets
FROM products
WHERE description ||| 'shoes'
ORDER BY rating DESC
LIMIT 20;
```

To return only aggregation results (no rows), `.facets(..., include_rows=False)` can execute a separate aggregate-only query:

```sql
SELECT pdb.agg('{"terms": {"field": "category", "size": 10}}')
FROM products
WHERE description &&& ARRAY['shoes'];
```

#### ORM integration points

- Implement `facets()` on a custom `ParadeDBQuerySet` subclass used by the manager.
- Use `QuerySet.query` to extract existing WHERE clauses (including `ParadeDB(...)`) and attach `pdb.agg(...)`.
- For windowed facets, annotate with a custom `Func` class:

```python
class Agg(Func):
    function = "pdb.agg"
    output_field = JSONField()

    def __init__(self, json_spec: str) -> None:
        super().__init__(Value(json_spec))
```

This allows `.annotate(facets=Agg(json_spec))` and `Window` with `OVER ()` when needed.

#### Django-like behavior

- `.facets(...)` should execute immediately and return a dict (similar to `.aggregate()`).
- When chained after `.values()` or `.values_list()`, `facets()` ignores projection and only uses filters/order/limits for the result set.
- Support `queryset = queryset.order_by(...)` and `[:limit]` to control which rows are returned alongside facet results.
- Facets are computed against the full filtered set (window semantics). `LIMIT/OFFSET` affect rows, not facet buckets.

#### ParadeDB operator detection

ParadeDB aggregate pushdown requires a ParadeDB operator in the WHERE clause. If no ParadeDB operator is found, `facets()` should raise a helpful error and ask the caller to add a ParadeDB search filter.

## Using Window Functions

You can combine ParadeDB search with Django's powerful window functions to perform complex calculations and ranking on your search results.

The following example finds all products matching "shoes" and then, for each product, calculates its price rank within its own category.

```python
from django.db.models import Window, F
from django.db.models.functions import RowNumber
from paradedb.search import ParadeDB

# Find all "shoes" products, and then rank them by price within their own category.
ranked_shoes = Product.objects.filter(
    description=ParadeDB('shoes')
).annotate(
    rank_in_category=Window(
        expression=RowNumber(),
        partition_by=[F('category')],
        order_by=F('price').desc()
    )
)

```

Generated SQL:

```sql
-- SELECT *,
--     ROW_NUMBER() OVER (PARTITION BY category ORDER BY price DESC) as rank_in_category
-- FROM products
-- WHERE description &&& 'shoes';

```

## Pending and other concerns

### Pagination

### Other Concerns

- Add BM25Index to existing tables with data?
- Handle index recreation after schema changes?
- Deal with Django's migration system?
- Errors and notices from postgres communicated
- Validation errors that can be generated by plugin/orm layer
- Support AND with PQ object as well.. `ParadeDB(PQ('shoes') & PQ('sandals'))`)
