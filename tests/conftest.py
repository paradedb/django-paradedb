"""Pytest configuration shared across unit and integration tests."""

from __future__ import annotations

import django
from django.conf import settings

from tests._db_config import database_settings


def pytest_configure(config: object) -> None:
    """Ensure Django is initialized and register custom markers."""
    config.addinivalue_line(
        "markers",
        "integration: marks tests that require a ParadeDB Postgres instance",
    )

    if not settings.configured:
        settings.configure(
            INSTALLED_APPS=["django.contrib.contenttypes", "tests"],
            DATABASES={"default": database_settings()},
            DEFAULT_AUTO_FIELD="django.db.models.BigAutoField",
            SECRET_KEY="tests-secret-key",
            MIGRATION_MODULES={"tests": None},
        )
    elif "postgresql" not in settings.DATABASES.get("default", {}).get("ENGINE", ""):
        settings.DATABASES["default"] = database_settings()

    django.setup()
