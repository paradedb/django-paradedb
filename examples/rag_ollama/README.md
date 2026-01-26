# RAG with Ollama

Retrieval-Augmented Generation using BM25 search + local LLM.

## Prerequisites

- [Ollama](https://ollama.ai) installed: `ollama pull llama3.2`

## Run

```bash
bash scripts/run_paradedb.sh
source scripts/paradedb_env.sh
python examples/rag_ollama/example.py

# Use different model
OLLAMA_MODEL=mistral python examples/rag_ollama/example.py
```

## Example Output

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
We have three shoe options that might interest you:

1. **Sleek running shoes** (5/5 stars, in stock) - Our top-rated running shoe,
   available in blue. Perfect for serious runners.

2. **Generic shoes** (4/5 stars, in stock) - A versatile everyday shoe option.

3. **White jogging shoes** (3/5 stars, currently out of stock) - A lighter
   option for casual jogging.

I'd recommend the Sleek running shoes for the best performance and quality.

============================================================
Question: I need comfortable shoes for everyday use
============================================================

Retrieved 4 products:
  • Comfortable slippers (score: 3.86)
  • Generic shoes (score: 2.88)
  • Sleek running shoes (score: 2.48)
  • White jogging shoes (score: 2.48)

Answer:
----------------------------------------
For everyday comfort, I'd suggest:

**Comfortable slippers** - These are designed specifically for comfort and
daily wear. While they're categorized as slippers, they're perfect for
relaxed everyday use around the house.

**Generic shoes** - Also a good option if you need something more versatile
for going out. They have a 4/5 rating and are currently in stock.

Both options prioritize comfort for daily activities.

============================================================
Question: Do you have any wireless audio products?
============================================================

Retrieved 1 products:
  • Innovative wireless earbuds (score: 3.33)

Answer:
----------------------------------------
Yes! We have **Innovative wireless earbuds** available. These are categorized
under Electronics and offer wireless audio connectivity. They're currently in
our catalog and would be perfect for your audio needs.
```
