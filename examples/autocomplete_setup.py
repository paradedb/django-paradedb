#!/usr/bin/env python
"""Setup script: Create dedicated table for autocomplete demo.

This script creates an autocomplete_items table with product data
optimized for autocomplete with ngram indexes.
"""


def setup_autocomplete_table() -> int:
    """Create autocomplete_items table with sample data and ngram index."""
    from django.db import connection

    with connection.cursor() as cursor:
        cursor.execute("CREATE EXTENSION IF NOT EXISTS pg_search")

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

        # Insert sample data optimized for autocomplete demos
        print("\nInserting sample product data...")
        cursor.execute(
            """
            INSERT INTO autocomplete_items (description, category, rating, in_stock) VALUES
            -- Running/Athletic shoes
            ('Sleek running shoes with advanced cushioning', 'Footwear', 5, true),
            ('Comfortable running shoes for athletes', 'Footwear', 4, true),
            ('Lightweight running shoes for marathons', 'Footwear', 5, true),
            ('White jogging shoes with memory foam', 'Footwear', 4, true),
            ('Professional running sneakers', 'Footwear', 5, false),

            -- Wireless/Bluetooth products
            ('Wireless bluetooth earbuds with noise cancellation', 'Electronics', 5, true),
            ('Compact wireless keyboard and mouse set', 'Electronics', 4, true),
            ('Wireless charging pad for smartphones', 'Electronics', 4, true),
            ('Bluetooth speaker with 360-degree sound', 'Electronics', 5, true),
            ('Wireless headphones with premium audio', 'Electronics', 5, false),

            -- General shoes
            ('Casual canvas shoes for everyday wear', 'Footwear', 3, true),
            ('Leather dress shoes for formal occasions', 'Footwear', 4, true),
            ('Hiking boots with waterproof protection', 'Footwear', 5, true),
            ('Comfortable sandals for summer', 'Footwear', 4, true),
            ('Athletic training shoes', 'Footwear', 4, true),

            -- Electronics
            ('Portable bluetooth speaker', 'Electronics', 4, true),
            ('Smart wireless router with WiFi 6', 'Electronics', 5, true),
            ('Wireless gaming mouse with RGB lighting', 'Electronics', 5, true),
            ('Bluetooth fitness tracker watch', 'Electronics', 4, false),
            ('Compact laptop stand', 'Electronics', 3, true),

            -- More products for variety
            ('Durable yoga mat with carrying strap', 'Fitness', 4, true),
            ('Stainless steel water bottle', 'Fitness', 5, true),
            ('Adjustable dumbbell set', 'Fitness', 5, true),
            ('Resistance bands workout kit', 'Fitness', 4, true),
            ('Premium foam roller for recovery', 'Fitness', 4, true),

            -- Products with typo-prone names
            ('Comfortable office chair with lumbar support', 'Furniture', 5, true),
            ('Ergonomic keyboard with wrist rest', 'Electronics', 4, true),
            ('Portable charger power bank', 'Electronics', 4, true),
            ('Waterproof phone case', 'Accessories', 3, true),
            ('Universal tablet stand', 'Accessories', 4, true);
            """
        )

        cursor.execute("SELECT COUNT(*) FROM autocomplete_items")
        count = cursor.fetchone()[0]
        print(f"  ✓ Inserted {count} products")

        # Create BM25 index with ngram tokenizer for autocomplete.
        # "3,8" means index 3- to 8-character ngrams; 1-2 char queries won't match.
        print("\nCreating autocomplete-optimized BM25 index...")
        cursor.execute(
            """
            CREATE INDEX autocomplete_items_idx ON autocomplete_items
            USING bm25 (
                id,
                description,
                (description::pdb.ngram(3,8,'alias=description_ngram')),
                (category::pdb.literal('alias=category'))
            )
            WITH (key_field='id');
            """
        )
        print("  ✓ Created BM25 index with:")
        print("    - description (standard tokenizer)")
        print("    - description_ngram (ngram 3-8 for substring matching)")
        print("    - category (literal for exact matching)")

    return count


if __name__ == "__main__":
    import sys
    from pathlib import Path

    # Setup Django
    sys.path.insert(0, str(Path(__file__).parent))
    from _common import configure_django

    configure_django()

    print("=" * 60)
    print("Autocomplete Setup - Creating Dedicated Table")
    print("=" * 60)
    count = setup_autocomplete_table()
    print("\n" + "=" * 60)
    print(f"✓ Setup complete! Created autocomplete_items with {count} products")
    print("=" * 60)
    print("\nRun: python examples/autocomplete.py")
