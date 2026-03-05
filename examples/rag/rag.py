#!/usr/bin/env python
"""RAG example using django-paradedb for retrieval and OpenRouter for generation."""

import os
import sys
from pathlib import Path

import httpx
from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common import MockItem, setup_mock_items

from paradedb.functions import Score
from paradedb.search import ParadeDB, Parse

load_dotenv()

OPENROUTER_API_KEY = os.environ.get("OPENROUTER_API_KEY")
MODEL = os.environ.get("RAG_MODEL", "anthropic/claude-3-haiku")


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
    """Generate answer using OpenRouter."""
    api_key = OPENROUTER_API_KEY or os.environ.get("OPENROUTER_API_KEY")
    if not api_key:
        return "(Set OPENROUTER_API_KEY to enable generation.)"

    prompt = f"""You are a helpful product assistant. Answer the customer's question based only on the product information provided below.

Product Catalog:
{context}

Customer Question: {query}

Provide a helpful, concise answer. If the products don't match what the customer is looking for, say so."""

    try:
        response = httpx.post(
            "https://openrouter.ai/api/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            },
            json={
                "model": MODEL,
                "messages": [{"role": "user", "content": prompt}],
            },
            timeout=60.0,
        )
        response.raise_for_status()
        return response.json()["choices"][0]["message"]["content"]
    except (httpx.HTTPError, KeyError, IndexError, TypeError) as e:
        return f"(OpenRouter error: {e}. Check your API key)"


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
    print("=" * 60)
    print("RAG with django-paradedb + OpenRouter")
    print("=" * 60)
    print(f"Using model: {MODEL}")
    print("Set RAG_MODEL env var to use a different model")
    if not OPENROUTER_API_KEY:
        print("OPENROUTER_API_KEY is not set; generation responses will be skipped.")

    count = setup_mock_items()
    print(f"Loaded {count} products")

    # Demo queries
    rag("What running shoes do you have?")
    rag("I need comfortable shoes for everyday use")
    rag("Do you have any wireless audio products?")

    print("\n" + "=" * 60)
    print("Done!")
