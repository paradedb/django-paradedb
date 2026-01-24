"""Django settings for tests."""

from __future__ import annotations

import os
from urllib.parse import urlparse

INSTALLED_APPS = [
    "django.contrib.contenttypes",
    "tests",
]


def _database_settings() -> dict[str, object]:
    dsn = os.environ.get(
        "PARADEDB_TEST_DSN", "postgres://postgres@localhost:5432/postgres"
    )
    parsed = urlparse(dsn)
    return {
        "ENGINE": "django.db.backends.postgresql",
        "NAME": parsed.path.lstrip("/") or "postgres",
        "USER": parsed.username or "",
        "PASSWORD": parsed.password or "",
        "HOST": parsed.hostname or "",
        "PORT": parsed.port or "",
        "TEST": {"NAME": os.environ.get("PARADEDB_TEST_DB", "paradedb_test")},
    }


DATABASES = {"default": _database_settings()}

DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"
SECRET_KEY = "tests-secret-key"
MIGRATION_MODULES = {"tests": None}
