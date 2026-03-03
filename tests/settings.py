"""Django settings for tests."""

from __future__ import annotations

from tests._db_config import database_settings

INSTALLED_APPS = [
    "django.contrib.contenttypes",
    "paradedb",
    "tests",
]

DATABASES = {"default": database_settings()}

DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"
SECRET_KEY = "tests-secret-key"
MIGRATION_MODULES = {"tests": None}
