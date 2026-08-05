# Examples

Self-contained scripts that show how to use ParadeDB from Django. Run them all with `scripts/run_examples.sh`, or follow the setup below and run them one at a time.

## Getting Started

### 1. Install dependencies

```bash
# Install uv: https://docs.astral.sh/uv/getting-started/installation/
uv sync --extra examples
```

### 2. Start ParadeDB

```bash
source scripts/run_paradedb.sh
```

This starts a ParadeDB container via Docker and exports `DATABASE_URL`. If you already have a Postgres instance with ParadeDB installed, set `DATABASE_URL` yourself instead:

```bash
export DATABASE_URL=postgresql://user:password@localhost:5432/dbname
```

## Quickstart (`quickstart/quickstart.py`)

The "Hello World" of ParadeDB. Covers defining a `ParadeDBIndex` on a model, running keyword queries, sorting by BM25 relevance, and highlighting matched terms in snippets.

```bash
uv run python examples/quickstart/quickstart.py
```

## Vector Search (`vector_search/vector_search.py`)

Top-K nearest-neighbor retrieval over pgvector `vector` columns. ParadeDB indexes the vector column inside its search index, so one index serves both keyword and vector queries. Declares a `VectorField`, includes it in the shared `ParadeDBIndex` with a `cosine` metric opclass, and queries with the required `@@@` predicate and `LIMIT`.

Requires the `pgvector` extension, which is included in the ParadeDB Docker image.

```bash
uv run python examples/vector_search/vector_search.py
```

## Faceted Search (`faceted_search/faceted_search.py`)

Builds an e-commerce-style filter sidebar. Computes search results and facet counts (by category, rating, and so on) together in a single query.

```bash
uv run python examples/faceted_search/faceted_search.py
```

## Hybrid Search (RRF) (`hybrid_rrf/hybrid_rrf.py`)

Combines BM25 keyword search (good for exact matches like part numbers) with vector similarity (good for meaning) using Reciprocal Rank Fusion, which ranks better than either method alone.

Requires the `pgvector` extension, which is included in the ParadeDB Docker image.

```bash
uv run python examples/hybrid_rrf/hybrid_rrf.py
```

## RAG (`rag/rag.py`)

A small question-answering flow. Retrieves relevant context with ParadeDB, then sends it to an LLM so answers are grounded in your own data.

Retrieval runs without any configuration. Set an [OpenRouter](https://openrouter.ai/) API key to enable the generation step:

```bash
export OPENROUTER_API_KEY=sk-...
uv run python examples/rag/rag.py
```

## Autocomplete (`autocomplete/autocomplete.py`)

As-you-type suggestions using n-gram tokenization, which matches substrings in the middle of words — typing `wir` matches `wireless`.

```bash
uv run python examples/autocomplete/autocomplete.py
```

The script bootstraps its own data. Run `examples/autocomplete/setup.py` only if you want to inspect or prepare the demo data separately.

## More Like This (`more_like_this/more_like_this.py`)

"Related content" recommendations. Finds documents with similar keywords using TF-IDF logic, without requiring vector embeddings.

```bash
uv run python examples/more_like_this/more_like_this.py
```

## Shared Helpers (`common.py`)

Most examples import from `examples/common.py`, which keeps the boilerplate out of the example scripts:

- `configure_django()` sets up a minimal standalone Django configuration.
- `MockItem` is the Django model used across examples to simulate products.
- `setup_mock_items()` populates the database with demo data.
