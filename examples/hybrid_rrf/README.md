# Hybrid Search with RRF

Combine BM25 keyword search + vector semantic search using Reciprocal Rank Fusion.

## Setup

First, generate embeddings (one-time, requires OpenRouter API key in `.env`):

```bash
bash scripts/run_paradedb.sh
source scripts/paradedb_env.sh
python examples/hybrid_rrf/setup.py
```

## Run

```bash
python examples/hybrid_rrf/example.py
```

## What is RRF?

Reciprocal Rank Fusion (RRF) combines rankings from multiple search methods:

```
RRF_score = sum(1 / (k + rank_i))
```

Where `k=60` (constant) and `rank_i` is the item's rank in each result list.

## Example Output

```text
================================================================================
Query: 'running shoes'
================================================================================

BM25 Results (keyword):
  1. Sleek running shoes                                          (score: 5.96)

Vector Results (semantic):
  1. Sleek running shoes                                          (dist: 0.076)
  2. White jogging shoes                                          (dist: 0.109)
  3. Generic shoes                                                (dist: 0.258)

Hybrid RRF Results (combined):
  1. Sleek running shoes                                          (RRF: 0.0328)  ← Boosted!
  2. White jogging shoes                                          (RRF: 0.0161)
  3. Generic shoes                                                (RRF: 0.0159)

================================================================================
Query: 'footwear for exercise'
================================================================================

BM25 Results (keyword):
  (no exact keyword matches)

Vector Results (semantic):
  1. White jogging shoes                                          (dist: 0.239)
  2. Sleek running shoes                                          (dist: 0.312)

Hybrid RRF Results (combined):
  1. White jogging shoes                                          (RRF: 0.0164)
  2. Sleek running shoes                                          (RRF: 0.0161)

================================================================================
Query: 'wireless earbuds'
================================================================================

BM25 Results (keyword):
  1. Innovative wireless earbuds                                  (score: 6.92)

Vector Results (semantic):
  1. Innovative wireless earbuds                                  (dist: 0.036)
  2. Bluetooth-enabled speaker                                    (dist: 0.451)

Hybrid RRF Results (combined):
  1. Innovative wireless earbuds                                  (RRF: 0.0328)  ← Best of both!
  2. Bluetooth-enabled speaker                                    (RRF: 0.0161)
```

**Key insight:** RRF gives higher scores to items that rank well in *multiple* search methods, providing more robust results than either method alone.
