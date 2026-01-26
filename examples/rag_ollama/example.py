#!/usr/bin/env python
"""RAG example using django-paradedb for retrieval and Ollama for generation."""

import os
from urllib.parse import urlparse

import django
import ollama
from django.conf import settings

DATABASE_URL = os.environ.get(
    "DATABASE_URL", "postgres://postgres:postgres@localhost:5432/postgres"
)

parsed = urlparse(DATABASE_URL)
if not settings.configured:
    settings.configure(
        DEBUG=True,
        DATABASES={
            "default": {
                "ENGINE": "django.db.backends.postgresql",
                "NAME": parsed.path.lstrip("/"),
                "USER": parsed.username or "postgres",
                "PASSWORD": parsed.password or "",
                "HOST": parsed.hostname or "localhost",
                "PORT": parsed.port or 5432,
            }
        },
        INSTALLED_APPS=["django.contrib.contenttypes"],
        DEFAULT_AUTO_FIELD="django.db.models.BigAutoField",
    )

django.setup()

from django.db import connection, models  # noqa: E402

from paradedb.functions import Score  # noqa: E402
from paradedb.search import ParadeDB, Parse  # noqa: E402

MODEL = os.environ.get("OLLAMA_MODEL", "llama3.2")


class MockItem(models.Model):
    """ParadeDB's built-in mock_items table."""

    id = models.IntegerField(primary_key=True)
    description = models.TextField()
    category = models.CharField(max_length=100)
    rating = models.IntegerField()
    in_stock = models.BooleanField()
    created_at = models.DateTimeField()
    metadata = models.JSONField(null=True)

    class Meta:
        app_label = "rag"
        managed = False
        db_table = "mock_items"

    def __str__(self):
        return self.description


def setup_mock_data() -> None:
    """Ensure mock_items table exists with BM25 index."""
    with connection.cursor() as cursor:
        cursor.execute("CREATE EXTENSION IF NOT EXISTS pg_search")
        cursor.execute(
            "CALL paradedb.create_bm25_test_table("
            "schema_name => 'public', table_name => 'mock_items')"
        )
    print(f"Loaded {MockItem.objects.count()} products")


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
    print(f"\n{'='*60}")
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

    setup_mock_data()

    # Demo queries
    rag("What running shoes do you have?")
    rag("I need comfortable shoes for everyday use")
    rag("Do you have any wireless audio products?")
