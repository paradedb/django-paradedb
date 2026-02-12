#!/usr/bin/env python
"""Setup script: Create dedicated table for autocomplete demo.

This script creates an autocomplete_items table with product data
from mock_items, optimized for autocomplete with ngram indexes.
"""

import sys
from pathlib import Path

# Add parent directory to path to import common module
sys.path.insert(0, str(Path(__file__).parent.parent))
from models import AutocompleteItem


def setup_autocomplete_table() -> int:
    """Create autocomplete_items table with data from mock_items and ngram index."""
    from django.db import connection

    with connection.cursor() as cursor:
        cursor.execute("CREATE EXTENSION IF NOT EXISTS pg_search")

        # Ensure mock_items exists first
        cursor.execute(
            "CALL paradedb.create_bm25_test_table("
            "schema_name => 'public', table_name => 'mock_items')"
        )

        # Drop and recreate table
        print("\nCreating autocomplete_items table...")
        cursor.execute("DROP TABLE IF EXISTS autocomplete_items CASCADE;")
        cursor.execute(
            """
            CREATE TABLE autocomplete_items (
                id SERIAL PRIMARY KEY,
                description TEXT NOT NULL,
                category VARCHAR(100) NOT NULL,
                rating INTEGER NOT NULL,
                in_stock BOOLEAN NOT NULL DEFAULT true,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
        )
        print("  ✓ Table created")

        # Copy data from mock_items
        print("\nCopying data from mock_items...")
        cursor.execute(
            """
            INSERT INTO autocomplete_items (id, description, category, rating, in_stock, created_at)
            SELECT id, description, category, rating, in_stock, created_at
            FROM mock_items;
            """
        )

        cursor.execute("SELECT COUNT(*) FROM autocomplete_items")
        count = cursor.fetchone()[0]
        print(f"  ✓ Copied {count} products from mock_items")

        print("\nCreating autocomplete-optimized BM25 index...")
    with connection.schema_editor(atomic=False) as schema_editor:
        for index in AutocompleteItem._meta.indexes:
            statement = index.create_sql(
                model=AutocompleteItem, schema_editor=schema_editor
            )
            schema_editor.execute(statement)
        print("  ✓ Created BM25 index with:")
        print("    - description (standard tokenizer)")
        print("    - description_ngram (ngram 3-8 for substring matching)")
        print("    - category (literal for exact matching)")

    return count


if __name__ == "__main__":
    print("=" * 60)
    print("Autocomplete Setup - Creating Dedicated Table")
    print("=" * 60)
    count = setup_autocomplete_table()
    print("\n" + "=" * 60)
    print(f"✓ Setup complete! Created autocomplete_items with {count} products")
    print("=" * 60)
    print("\nRun: python examples/autocomplete/autocomplete.py")
