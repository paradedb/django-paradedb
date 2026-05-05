"""Test models for SQL generation tests."""

from django.db import models

from paradedb.queryset import ParadeDBManager


class Product(models.Model):
    """Sample model for testing ParadeDB search."""

    description = models.TextField()
    category = models.CharField(max_length=100)
    rating = models.IntegerField()
    in_stock = models.BooleanField()
    price = models.DecimalField(max_digits=10, decimal_places=2)
    created_at = models.DateTimeField(auto_now_add=True)
    metadata = models.JSONField(default=dict)

    objects = ParadeDBManager()

    class Meta:
        app_label = "tests"

    def __str__(self) -> str:
        return self.description[:50]


class MockItem(models.Model):
    """ParadeDB mock data table created via `paradedb.create_bm25_test_table`."""

    id = models.IntegerField(primary_key=True)
    description = models.TextField()
    category = models.CharField(max_length=100)
    rating = models.IntegerField()
    in_stock = models.BooleanField()
    created_at = models.DateTimeField()
    metadata = models.JSONField(null=True)

    objects = ParadeDBManager()

    class Meta:
        app_label = "tests"
        managed = False
        db_table = "mock_items"

    def __str__(self) -> str:
        return f"MockItem(id={self.id})"
