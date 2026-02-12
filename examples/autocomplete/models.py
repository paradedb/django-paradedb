"""Shared model definitions for autocomplete examples."""

from common import configure_django
from django.db import models

from paradedb.indexes import BM25Index
from paradedb.queryset import ParadeDBManager

configure_django()


class AutocompleteItem(models.Model):
    id = models.IntegerField(primary_key=True)
    description = models.TextField()
    category = models.CharField(max_length=100)
    rating = models.IntegerField()
    in_stock = models.BooleanField()
    created_at = models.DateTimeField()

    objects = ParadeDBManager()

    class Meta:
        app_label = "examples"
        managed = False
        db_table = "autocomplete_items"
        indexes = (
            BM25Index(
                fields={
                    "id": {},
                    "description": {
                        "tokenizers": [
                            {"tokenizer": "unicode_words"},
                            {
                                "tokenizer": "ngram",
                                "args": [3, 8],
                                "alias": "description_ngram",
                            },
                        ]
                    },
                    "category": {"tokenizer": "literal", "alias": "category"},
                },
                key_field="id",
                name="autocomplete_items_idx",
            ),
        )

    def __str__(self) -> str:
        return self.description
