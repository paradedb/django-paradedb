"""Test models."""

from django.db import models

from paradedb.queryset import ParadeDBManager


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
