# Examples

## Quick Start

```bash
# Install dependencies
pip install -r examples/requirements.txt

# Start ParadeDB and export DATABASE_URL
source scripts/run_paradedb.sh

# Run an example
python examples/quickstart.py
```

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `DATABASE_URL` | postgresql://postgres:postgres@localhost:5432/postgres | Database connection URL |
| `OPENROUTER_API_KEY` | - | Required for RAG and hybrid search examples |
| `RAG_MODEL` | anthropic/claude-3-haiku | LLM model for RAG example |

The `scripts/run_paradedb.sh` script sets `DATABASE_URL` automatically when sourced.

## Available Examples

### quickstart.py

Basic full-text search with BM25: keyword search, scoring, phrase search, snippets, and ORM filters.

```bash
python examples/quickstart.py
```

**Example output:**

```text
--- Basic Search: 'shoes' ---
  • Sleek running shoes...
  • Generic shoes...

--- Scored Search: 'running' ---
  • Sleek running shoes... (score: 5.82)

--- Phrase Search: 'running shoes' ---
  • Sleek running shoes... (score: 8.41)

--- Snippet Highlighting: 'shoes' ---
  • Sleek running <b>shoes</b>

--- Filtered Search: 'shoes' + in_stock + rating >= 4 ---
  • Sleek running shoes... (rating: 5)
```

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

### autocomplete.py

Simple typo-tolerant autocomplete using fuzzy matching.

```bash
python examples/autocomplete_setup.py
python examples/autocomplete.py
```

**API used:**

```python
from paradedb.search import ParadeDB, Fuzzy
from paradedb.functions import Score

Product.objects.filter(description=ParadeDB(Fuzzy("sheos", distance=1)))
    .annotate(score=Score())
    .order_by("-score")[:5]
```

---

### rag.py

RAG (Retrieval-Augmented Generation) using BM25 search + OpenRouter LLM.

**Prerequisites:** Add `OPENROUTER_API_KEY` to `.env` file

```bash
python examples/rag.py

# Use different model
RAG_MODEL=openai/gpt-4o-mini python examples/rag.py
```

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

### hybrid_rrf.py

Hybrid search combining BM25 (keyword) + vector (semantic) with Reciprocal Rank Fusion.

**Prerequisites:** Run setup first to generate embeddings.

```bash
# Add OPENROUTER_API_KEY to .env
python examples/hybrid_rrf_setup.py
python examples/hybrid_rrf.py
```

**Example output:**

```text
Query: 'running shoes'

BM25 Results (keyword):
  1. Sleek running shoes                               (score: 5.96)

Vector Results (semantic):
  1. Sleek running shoes                               (dist: 0.076)
  2. White jogging shoes                               (dist: 0.109)

Hybrid RRF Results (combined):
  1. Sleek running shoes                               (RRF: 0.0328)
  2. White jogging shoes                               (RRF: 0.0161)
```

**RRF Formula:** `score = sum(1 / (k + rank_i))` where k=60 and rank_i is the item's rank in each list.

---

### more_like_this.py

Find similar documents based on term frequency analysis (TF-IDF) - no vector embeddings required.

```bash
python examples/more_like_this.py
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

### faceted_search.py

Faceted search for e-commerce style filters (categories, ratings, in_stock, colors).

```bash
python examples/faceted_search.py
```

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
