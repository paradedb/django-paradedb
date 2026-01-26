# Quickstart Example

A minimal example demonstrating django-paradedb full-text search with:

- Basic keyword search
- BM25 score annotations
- Phrase search
- Snippet highlighting
- Combining search with Django ORM filters

## Run

```bash
# From project root, start ParadeDB and set env vars
bash scripts/run_paradedb.sh
source scripts/paradedb_env.sh

# Run the example
python examples/quickstart/example.py
```

## What it does

1. Creates the `pg_search` extension (if not exists)
2. Creates `mock_items` table with 41 sample products via `paradedb.create_bm25_test_table()`
3. Runs several search demos using django-paradedb

## Example Output

> Note: BM25 scores may vary slightly across ParadeDB versions.

```text
Loaded 41 mock items

--- Basic Search: 'shoes' ---
  • Sleek running shoes...
  • White jogging shoes...
  • Generic shoes...

--- Scored Search: 'running' ---
  • Sleek running shoes... (score: 3.33)

--- Phrase Search: 'running shoes' ---
  • Sleek running shoes... (score: 5.82)

--- Snippet Highlighting: 'shoes' ---
  • Generic <b>shoes</b>
  • Sleek running <b>shoes</b>

--- Filtered Search: 'shoes' + in_stock + rating >= 4 ---
  • Generic shoes... (rating: 4)
  • Sleek running shoes... (rating: 5)
```
