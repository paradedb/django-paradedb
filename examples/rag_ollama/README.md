# RAG with Ollama

Retrieval-Augmented Generation using django-paradedb for BM25 retrieval and Ollama for local LLM generation.

## How it works

1. **Retrieve**: User query → BM25 search via `ParadeDB(Parse(...))` → top-k products
2. **Generate**: Products formatted as context → Ollama LLM → answer

## Prerequisites

- ParadeDB running (see project root `scripts/run_paradedb.sh`)
- [Ollama](https://ollama.ai) installed and running
- A model pulled: `ollama pull llama3.2`

## Run

```bash
# From project root, start ParadeDB and set env vars
bash scripts/run_paradedb.sh
source scripts/paradedb_env.sh

# Start Ollama and pull a model
ollama serve &
ollama pull llama3.2

# Run the example
python examples/rag_ollama/example.py

# Or use a different model
OLLAMA_MODEL=mistral python examples/rag_ollama/example.py
```

## What it does

1. Creates `pg_search` extension and `mock_items` table (41 sample products)
2. Uses `Parse(query, lenient=True)` for natural language queries
3. Retrieves relevant products via BM25, formats as context, sends to Ollama

## Example Output

> Note: BM25 scores may vary slightly across ParadeDB versions. LLM responses will differ.

```text
RAG with django-paradedb + Ollama
Using model: llama3.2
Loaded 41 products

============================================================
Question: What running shoes do you have?
============================================================

Retrieved 3 products:
  • Sleek running shoes (score: 5.82)
  • Generic shoes (score: 2.88)
  • White jogging shoes (score: 2.48)

Answer:
----------------------------------------
Based on our catalog, we have several shoe options:
- Sleek running shoes (5/5 rating, in stock, blue)
- White jogging shoes (3/5 rating, out of stock)
...
```
