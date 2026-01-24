"""Pytest configuration shared across unit and integration tests."""

from __future__ import annotations

import os
from urllib.parse import urlparse

import django
from django.conf import settings


def _database_settings() -> dict[str, object]:
    database_url = os.environ.get(
        "PARADEDB_TEST_DSN", "postgres://postgres@localhost:5432/postgres"
    )
    parsed = urlparse(database_url)
    return {
        "ENGINE": "django.db.backends.postgresql",
        "NAME": parsed.path.lstrip("/") or "postgres",
        "USER": parsed.username or "",
        "PASSWORD": parsed.password or "",
        "HOST": parsed.hostname or "",
        "PORT": parsed.port or "",
        "TEST": {"NAME": os.environ.get("PARADEDB_TEST_DB", "paradedb_test")},
    }


def pytest_configure(config: object) -> None:
    """Ensure Django is initialized and register custom markers."""
    config.addinivalue_line(
        "markers",
        "integration: marks tests that require a ParadeDB Postgres instance",
    )

    if not settings.configured:
        settings.configure(
            INSTALLED_APPS=["django.contrib.contenttypes", "tests"],
            DATABASES={"default": _database_settings()},
            DEFAULT_AUTO_FIELD="django.db.models.BigAutoField",
            SECRET_KEY="tests-secret-key",
            MIGRATION_MODULES={"tests": None},
        )
    elif "postgresql" not in settings.DATABASES.get("default", {}).get("ENGINE", ""):
        settings.DATABASES["default"] = _database_settings()

    django.setup()
