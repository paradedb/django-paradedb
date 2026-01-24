"""Minimal Django settings for SQL generation tests.

This plugin tests SQL generation only - no database connection required.
"""

INSTALLED_APPS = [
    "django.contrib.contenttypes",
    "tests",
]

DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.sqlite3",
        "NAME": ":memory:",
    }
}

DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"
