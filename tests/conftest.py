"""Pytest configuration for SQL generation tests.

This plugin tests SQL generation only - no database connection required.
"""

import django
from django.conf import settings


def pytest_configure() -> None:
    """Configure minimal Django settings for SQL generation tests."""
    if not settings.configured:
        settings.configure(
            INSTALLED_APPS=[
                "django.contrib.contenttypes",
                "tests",
            ],
            DATABASES={
                "default": {
                    "ENGINE": "django.db.backends.sqlite3",
                    "NAME": ":memory:",
                }
            },
            DEFAULT_AUTO_FIELD="django.db.models.BigAutoField",
        )
    django.setup()
