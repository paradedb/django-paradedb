#!/usr/bin/env python
"""RAG example using django-paradedb for retrieval and Ollama for generation."""

import os

import ollama
from _common import MockItem, setup_mock_items

from paradedb.functions import Score
from paradedb.search import ParadeDB, Parse

MODEL = os.environ.get("OLLAMA_MODEL", "llama3.2")


def retrieve(query: str, top_k: int = 5) -> list[MockItem]:
    """Retrieve relevant products using BM25 search."""
    qs = (
        MockItem.objects.filter(description=ParadeDB(Parse(query, lenient=True)))
        .annotate(score=Score())
        .order_by("-score")[:top_k]
    )
    return list(qs)


def format_context(items: list[MockItem]) -> str:
    """Format retrieved items as context for the LLM."""
    if not items:
        return "No products found."

    lines = []
    for item in items:
        stock = "In Stock" if item.in_stock else "Out of Stock"
        color = item.metadata.get("color", "N/A") if item.metadata else "N/A"
        lines.append(
            f"- {item.description} | Category: {item.category} | "
            f"Rating: {item.rating}/5 | {stock} | Color: {color}"
        )
    return "\n".join(lines)


def generate(query: str, context: str) -> str:
    """Generate answer using Ollama."""
    prompt = f"""You are a helpful product assistant. Answer the customer's question based only on the product information provided below.

Product Catalog:
{context}

Customer Question: {query}

Provide a helpful, concise answer. If the products don't match what the customer is looking for, say so."""

    try:
        response = ollama.chat(
            model=MODEL,
            messages=[{"role": "user", "content": prompt}],
        )
        return response["message"]["content"]
    except Exception as e:
        return f"(Ollama error: {e}. Install from https://ollama.ai and run 'ollama serve')"


def rag(query: str) -> None:
    """Run the full RAG pipeline."""
    print(f"\n{'=' * 60}")
    print(f"Question: {query}")
    print("=" * 60)

    # Retrieve
    items = retrieve(query)
    print(f"\nRetrieved {len(items)} products:")
    for item in items:
        print(f"  • {item.description} (score: {item.score:.2f})")

    # Generate
    context = format_context(items)
    print("\nAnswer:")
    print("-" * 40)
    answer = generate(query, context)
    print(answer)


if __name__ == "__main__":
    print("RAG with django-paradedb + Ollama")
    print(f"Using model: {MODEL}")

    count = setup_mock_items()
    print(f"Loaded {count} products")

    # Demo queries
    rag("What running shoes do you have?")
    rag("I need comfortable shoes for everyday use")
    rag("Do you have any wireless audio products?")
