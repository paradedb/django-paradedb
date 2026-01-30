#!/usr/bin/env python
"""MoreLikeThis example: find similar documents without vectors.

This example demonstrates ParadeDB's MoreLikeThis feature, which finds similar
documents based on term frequency analysis (TF-IDF), not vector embeddings.

NOTE: MoreLikeThis is used directly in .filter(), not wrapped in ParadeDB().
This is because it's a table-level similarity query that uses the primary key
internally, rather than a field-targeted search expression.

Use cases:
- "Related products" on product pages
- "Similar articles" recommendations
- Content discovery and exploration
"""

from _common import MockItem, setup_mock_items

from paradedb.functions import Score
from paradedb.search import MoreLikeThis


def demo_similar_to_single_product() -> None:
    """Find products similar to a single product by ID.

    This is the most common use case: "Customers who viewed this also viewed..."
    """
    print("\n" + "=" * 60)
    print("Demo 1: Similar to a single product")
    print("=" * 60)

    # Pick a product to find similar items for
    source_id = 3
    source = MockItem.objects.get(id=source_id)
    print(f"\nSource product (id={source_id}):")
    print(f"  '{source.description}' [{source.category}]")

    # Find similar products based on description text
    print("\nSimilar products (by description):")
    similar = (
        MockItem.objects.filter(
            MoreLikeThis(product_id=source_id, fields=["description"])
        )
        .annotate(score=Score())
        .order_by("-score")[:5]
    )

    for item in similar:
        marker = " (source)" if item.id == source_id else ""
        print(f"  {item.id}: {item.description[:50]}... [{item.category}]{marker}")


def demo_similar_to_multiple_products() -> None:
    """Find products similar to multiple products.

    Use case: "Based on your recently viewed items..."
    This finds the union of similar items across all source products.
    """
    print("\n" + "=" * 60)
    print("Demo 2: Similar to multiple products (browsing history)")
    print("=" * 60)

    # Simulate a user's browsing history
    browsed_ids = [3, 12, 29]  # Running shoes, earbuds, yoga mat
    browsed = MockItem.objects.filter(id__in=browsed_ids)

    print("\nUser's browsing history:")
    for item in browsed:
        print(f"  {item.id}: {item.description[:50]}... [{item.category}]")

    # Find products similar to any of these
    print("\nRecommended products (similar to any browsed item):")
    similar = (
        MockItem.objects.filter(
            MoreLikeThis(product_ids=browsed_ids, fields=["description"])
        )
        .exclude(id__in=browsed_ids)  # Exclude already-viewed items
        .annotate(score=Score())
        .order_by("-score")[:5]
    )

    for item in similar:
        print(f"  {item.id}: {item.description[:50]}... [{item.category}]")


def demo_similar_by_text() -> None:
    """Find products similar to a text description.

    Use case: User describes what they want, find matching products.
    This is different from regular search - it uses MLT's term analysis.
    """
    print("\n" + "=" * 60)
    print("Demo 3: Similar to text description")
    print("=" * 60)

    # User describes what they're looking for
    user_description = "comfortable wireless audio for running"
    print(f"\nUser wants: '{user_description}'")

    # MoreLikeThis with text requires fields parameter
    print("\nMatching products:")
    similar = (
        MockItem.objects.filter(
            MoreLikeThis(text=user_description, fields=["description"])
        )
        .annotate(score=Score())
        .order_by("-score")[:5]
    )

    for item in similar:
        print(f"  {item.id}: {item.description[:50]}... [{item.category}]")


def demo_tuning_parameters() -> None:
    """Demonstrate MLT tuning parameters for quality control.

    Parameters:
    - min_term_freq: Minimum times a term must appear in source doc
    - max_query_terms: Maximum number of terms to use in the query
    - min_doc_freq: Minimum docs a term must appear in (filters rare terms)
    - max_doc_freq: Maximum docs a term can appear in (filters common terms)
    """
    print("\n" + "=" * 60)
    print("Demo 4: Tuning MoreLikeThis parameters")
    print("=" * 60)

    source_id = 5  # Sleek running shoes
    source = MockItem.objects.get(id=source_id)
    print(f"\nSource: '{source.description}'")

    # Default behavior
    print("\nDefault MLT (no tuning):")
    default_results = list(
        MockItem.objects.filter(
            MoreLikeThis(product_id=source_id, fields=["description"])
        )
        .annotate(score=Score())
        .order_by("-score")[:3]
    )
    for item in default_results:
        print(f"  {item.id}: {item.description[:50]}...")

    # With tuning: require terms to appear in at least 2 docs
    print("\nTuned MLT (min_doc_freq=2, max_query_terms=5):")
    tuned_results = list(
        MockItem.objects.filter(
            MoreLikeThis(
                product_id=source_id,
                fields=["description"],
                min_doc_freq=2,  # Term must appear in 2+ docs
                max_query_terms=5,  # Use only top 5 terms
            )
        )
        .annotate(score=Score())
        .order_by("-score")[:3]
    )
    for item in tuned_results:
        print(f"  {item.id}: {item.description[:50]}...")


def demo_combined_with_filters() -> None:
    """Combine MoreLikeThis with standard Django ORM filters.

    Use case: "Similar products that are in stock and highly rated"
    """
    print("\n" + "=" * 60)
    print("Demo 5: MoreLikeThis + ORM filters")
    print("=" * 60)

    source_id = 15
    source = MockItem.objects.get(id=source_id)
    print(f"\nSource: '{source.description}' (rating: {source.rating})")

    print("\nSimilar products (in_stock=True, rating >= 4):")
    results = (
        MockItem.objects.filter(
            MoreLikeThis(product_id=source_id, fields=["description"]),
            in_stock=True,
            rating__gte=4,
        )
        .annotate(score=Score())
        .order_by("-score")[:5]
    )

    for item in results:
        stock = "In Stock" if item.in_stock else "Out of Stock"
        print(
            f"  {item.id}: {item.description[:40]}... (rating: {item.rating}, {stock})"
        )


def demo_multifield_similarity() -> None:
    """Find similar products using multiple fields.

    This shows how to analyze similarity across both description and category.
    """
    print("\n" + "=" * 60)
    print("Demo 6: Multi-field similarity")
    print("=" * 60)

    source_id = 3
    source = MockItem.objects.get(id=source_id)
    print(f"\nSource: '{source.description}' [{source.category}]")

    # By description only (default)
    print("\nSimilar by DESCRIPTION only:")
    by_desc = MockItem.objects.filter(
        MoreLikeThis(product_id=source_id, fields=["description"])
    ).exclude(id=source_id)[:3]
    for item in by_desc:
        print(f"  {item.id}: {item.description[:40]}... [{item.category}]")

    # By both description and category
    # Note: This requires both fields to be stored in the BM25 index
    print("\nSimilar by DESCRIPTION + CATEGORY (if both indexed):")
    try:
        by_both = MockItem.objects.filter(
            MoreLikeThis(product_id=source_id, fields=["description", "category"])
        ).exclude(id=source_id)[:3]
        for item in by_both:
            print(f"  {item.id}: {item.description[:40]}... [{item.category}]")
    except Exception:
        print("  (Skipped: category field may not be stored in BM25 index)")
        print("  Note: Only 'description' is stored by default in mock_items")


if __name__ == "__main__":
    print("=" * 60)
    print("django-paradedb MoreLikeThis Example")
    print("Find similar documents without vector embeddings")
    print("=" * 60)

    count = setup_mock_items()
    print(f"Loaded {count} mock items")

    demo_similar_to_single_product()
    demo_similar_to_multiple_products()
    demo_similar_by_text()
    demo_tuning_parameters()
    demo_combined_with_filters()
    demo_multifield_similarity()

    print("\n" + "=" * 60)
    print("Done!")
