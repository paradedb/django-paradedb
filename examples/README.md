# Examples

This folder contains self-contained examples demonstrating various ParadeDB features.

## Quick Start

```bash
# Install dependencies
pip install -r requirements.txt

# Start ParadeDB and export DATABASE_URL
source scripts/run_paradedb.sh

# Run an example
python examples/quickstart/quickstart.py
```

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `DATABASE_URL` | postgresql://postgres:postgres@localhost:5432/postgres | Database connection URL |
| `OPENROUTER_API_KEY` | - | Required for RAG example |
| `RAG_MODEL` | anthropic/claude-3-haiku | LLM model for RAG example |

The `scripts/run_paradedb.sh` script sets `DATABASE_URL` automatically when sourced.

---

## quickstart.py

Basic full-text search with BM25: keyword search, scoring, phrase search, snippets, and ORM filters.

```bash
python examples/quickstart/quickstart.py
```

**Features demonstrated:**

- **Basic search**: Simple keyword matching
- **Scored search**: BM25 relevance scoring
- **Phrase search**: Exact phrase matching with `Phrase()`
- **Snippet highlighting**: Highlight matched terms with `Snippet()`
- **Filtered search**: Combine ParadeDB search with Django ORM filters

**API used:**

```python
from paradedb.search import ParadeDB, Phrase
from paradedb.functions import Score, Snippet

# Keyword search
Product.objects.filter(description=ParadeDB("shoes"))

# Phrase search (exact sequence)
Product.objects.filter(description=ParadeDB(Phrase("running shoes")))

# With BM25 scoring
Product.objects.filter(description=ParadeDB("shoes")).annotate(score=Score())

# With snippet highlighting
Product.objects.filter(description=ParadeDB("shoes")).annotate(
    snippet=Snippet("description", start_sel="<b>", stop_sel="</b>")
)
```

---

## autocomplete/

As-you-type autocomplete using ngram tokenizers for substring matching.

```bash
cd examples/autocomplete
python setup.py          # Creates autocomplete_items table with ngram index
python autocomplete.py   # Run the demo
```

**How it works:**

- **ngram(3,8)**: Indexes 3- to 8-character substrings
- Queries of 1-2 characters won't match (intentional - prevents too many results)
- Queries of 3+ characters match against indexed ngrams

**API used:**

```python
from paradedb.search import ParadeDB, Parse
from paradedb.functions import Score

Product.objects.filter(description=ParadeDB(Parse("description_ngram:wirel")))
    .annotate(score=Score())
    .order_by("-score")[:5]
```

---

## hybrid_rrf/

Hybrid search combining BM25 (keyword) + vector (semantic) with Reciprocal Rank Fusion.

**Prerequisites:** `pip install pgvector`

```bash
cd examples/hybrid_rrf
python setup.py         # Loads pre-computed embeddings from CSV
python hybrid_rrf.py    # Run the demo
```

**How it works:**

1. **BM25 Search**: Keyword-based search for exact matches
2. **Vector Search**: Semantic similarity using pre-computed embeddings
3. **RRF Fusion**: Combines both result sets using Reciprocal Rank Fusion

**RRF Formula:** `score = sum(1 / (k + rank_i))` where k=60 and rank_i is the item's rank in each list.

**Files:**

- `setup.py` - Loads pre-computed embeddings from CSV
- `hybrid_rrf.py` - Runs the hybrid search demo
- `mock_items_embeddings.csv` - Pre-computed embeddings (384-dim, all-MiniLM-L6-v2)

---

## rag.py

RAG (Retrieval-Augmented Generation) using BM25 search + OpenRouter LLM.

**Prerequisites:** Add `OPENROUTER_API_KEY` to your `.env` file

```bash
cd examples/rag
# Add OPENROUTER_API_KEY to .env first
python rag.py

# Use different model
RAG_MODEL=openai/gpt-4o-mini python rag.py
```

**How it works:**

1. **Retrieve**: Uses BM25 search to find relevant products
2. **Generate**: Sends retrieved products as context to an LLM via OpenRouter

**API used:**

```python
from paradedb.search import ParadeDB, Parse

# Parse allows natural language queries with lenient parsing
Product.objects.filter(description=ParadeDB(Parse(query, lenient=True)))
```

| Parameter | Description |
|-----------|-------------|
| `query` | Natural language search query |
| `lenient` | If True, ignores syntax errors in the query |

---

## more_like_this.py

Find similar documents based on term frequency analysis (TF-IDF) - no vector embeddings required.

```bash
python examples/more_like_this/more_like_this.py
```

**Use cases:**

- "Related products" on product pages
- "Similar articles" recommendations
- "Customers who viewed this also viewed..."

**API used:**

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

> **Note:** `MoreLikeThis` is used directly in `.filter()`, not wrapped in `ParadeDB()`.
> This is because it's a table-level similarity query, not a field-targeted search.

**Tuning Parameters:**

| Parameter | Description | Default |
|-----------|-------------|---------|
| `min_term_freq` | Minimum times term must appear in source | 1 |
| `max_query_terms` | Maximum terms to use in query | 25 |
| `min_doc_freq` | Minimum docs term must appear in | 1 |
| `max_doc_freq` | Maximum docs term can appear in | unlimited |

```python
# Tuned for precision
Product.objects.filter(
    MoreLikeThis(
        product_id=5,
        fields=["description"],
        min_doc_freq=2,      # Ignore rare terms
        max_query_terms=10,  # Use only top 10 terms
    )
)
```

---

## faceted_search.py

Faceted search for e-commerce style filters (categories, ratings, in_stock, colors).

```bash
python examples/faceted_search/faceted_search.py
```

**Features demonstrated:**

- **Facet buckets**: Count of results per category, rating, etc.
- **Top-N rows**: Get actual search results alongside facets
- **Window aggregation**: Efficient single-query approach

**API used:**

```python
from paradedb.search import ParadeDB

# Facets only (no rows)
MockItem.objects.filter(description=ParadeDB("shoes")).facets(
    "category",
    "rating",
    "in_stock",
    include_rows=False,
)

# Facets + rows (requires order_by + limit)
MockItem.objects.filter(description=ParadeDB("shoes")).order_by("-rating")[:5].facets(
    "category",
    "metadata_color",
)
```

> **Note:** `facets(include_rows=True)` requires `order_by(...)` and a slice (LIMIT).

---

## Common Module

The `common.py` file at the examples root provides shared utilities:

- `configure_django()` - Django settings setup for standalone scripts
- `setup_mock_items()` - Creates the mock_items table with BM25 index
- `MockItem` - Django model for the mock_items table
- `MockItemWithEmbedding` - Model with vector field (for hybrid search)

Each example imports this module via `sys.path` manipulation to stay self-contained.
