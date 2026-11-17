"""Test models."""

from django.db import models

from paradedb.queryset import ParadeDBManager
from paradedb.vector import VectorField


class MockItem(models.Model):
    """ParadeDB mock data table created via `paradedb.create_paradedb_test_table`."""

    id = models.IntegerField(primary_key=True)
    description = models.TextField()
    category = models.CharField(max_length=100)
    rating = models.IntegerField()
    in_stock = models.BooleanField()
    created_at = models.DateTimeField()
    metadata = models.JSONField(null=True)
    embedding = VectorField(dimensions=8, null=True)

    objects = ParadeDBManager()

    class Meta:
        app_label = "tests"
        managed = False
        db_table = "mock_items"

    def __str__(self) -> str:
        return f"MockItem(id={self.id})"


# The synonym tests create these tables. `pdb.synonym_rule` is a domain over
# text, so the rule columns are `TextField`s.
class ShopSynonym(models.Model):
    rule = models.TextField()

    class Meta:
        app_label = "tests"
        managed = False
        db_table = "shop_synonyms"
        # `Expand` has to drop a default ordering.
        ordering = ("rule",)

    def __str__(self) -> str:
        return self.rule


class Synonym(models.Model):
    set_name = models.TextField()
    rule = models.TextField()

    class Meta:
        app_label = "tests"
        managed = False
        db_table = "synonyms"

    def __str__(self) -> str:
        return f"{self.set_name}: {self.rule}"
