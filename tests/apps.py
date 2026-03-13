"""AppConfig for the tests package used as a demo Django app."""

from django.apps import AppConfig


class TestsConfig(AppConfig):
    name = "tests"
    label = "tests"
    verbose_name = "ParadeDB Demo"
