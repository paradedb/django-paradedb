# MoreLikeThis

Find similar documents based on term frequency analysis (TF-IDF) - no vector embeddings required.

## What is MoreLikeThis?

MoreLikeThis analyzes the terms in a source document and finds other documents with similar term patterns. It's a pure BM25-based similarity measure built into ParadeDB.

**Use cases:**
- "Related products" on product pages
- "Similar articles" recommendations
- "Customers who viewed this also viewed..."
- Content discovery without ML infrastructure

**Advantages over vector similarity:**
- No embedding model needed
- No vector storage overhead
- Works immediately with any text field
- Fully explainable (based on shared terms)

## Run

```bash
bash scripts/run_paradedb.sh
source scripts/paradedb_env.sh
python examples/more_like_this/example.py
```

## API

```python
from paradedb.search import MoreLikeThis

# Similar to a single product
Product.objects.filter(
    MoreLikeThis(product_id=5, fields=["description"])
)

# Similar to multiple products (browsing history)
Product.objects.filter(
    MoreLikeThis(product_ids=[5, 12, 23], fields=["description"])
)

# Similar to text (user describes what they want)
Product.objects.filter(
    MoreLikeThis(text='{"description": "comfortable running shoes"}')
)
```

## Tuning Parameters

| Parameter | Description | Default |
|-----------|-------------|---------|
| `min_term_freq` | Minimum times term must appear in source | 1 |
| `max_query_terms` | Maximum terms to use in query | 25 |
| `min_doc_freq` | Minimum docs term must appear in | 1 |
| `max_doc_freq` | Maximum docs term can appear in | unlimited |

```python
# Tuned for precision: use only significant terms
Product.objects.filter(
    MoreLikeThis(
        product_id=5,
        fields=["description"],
        min_doc_freq=2,      # Ignore super-rare terms
        max_query_terms=10,  # Use only top 10 terms
    )
)
```

## Demos in this example

1. **Similar to single product** - Basic "related items" pattern
2. **Similar to multiple products** - "Based on your history" pattern
3. **Similar to text** - User describes what they want
4. **Tuning parameters** - Control precision vs. recall
5. **Combined with ORM filters** - Add in_stock, rating constraints
6. **Multi-field similarity** - Analyze across multiple indexed fields
