# django-paradedb Examples

Runnable examples demonstrating django-paradedb features.

## Prerequisites

- Python 3.13+
- Docker (for running ParadeDB)

## Dataset (mock_items)

All examples use ParadeDB's built-in `mock_items` table (41 rows). Each example calls
`paradedb.create_bm25_test_table()` at startup, so you do not need to run migrations
or load data manually. The table includes product descriptions, ratings, stock status,
and JSON metadata (e.g., color and location).

## ParadeDB ORM helpers used

- `ParadeDB()` for BM25 queries
- `Score()` for ranking
- `Snippet()` for highlighting
- `Phrase()` and `Parse()` for structured queries (example dependent)

## Quick Start

```bash
# Start ParadeDB (from project root)
bash scripts/run_paradedb.sh

# Set environment variables
source scripts/paradedb_env.sh

# Run an example
python examples/quickstart/example.py
```

## Examples

| Example | Description |
|---------|-------------|
| [quickstart](quickstart/) | Basic full-text search with BM25 scoring and snippets |
| [rag_ollama](rag_ollama/) | RAG using BM25 retrieval + Ollama for local LLM generation |
