#!/usr/bin/env python
import os
import sys
from importlib import util


def main():
    """Run administrative tasks."""
    if util.find_spec("tests.settings_local"):
        settings_path = "tests.settings_local"
    else:
        settings_path = "tests.settings"
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", settings_path)

    try:
        from django.core.management import execute_from_command_line
    except ImportError as exc:
        raise ImportError(
            "Couldn't import Django. Are you sure it's installed and "
            "available on your PYTHONPATH environment variable? Did you "
            "forget to activate a virtual environment?"
        ) from exc
    execute_from_command_line(sys.argv)


if __name__ == "__main__":
    main()
