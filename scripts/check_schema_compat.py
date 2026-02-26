#!/usr/bin/env python3
"""
Check that all SQL symbols defined in api.py are present in a pg_search schema file.

The schema is generated in the paradedb repo via:
    cargo pgrx schema -p pg_search pg18 > pg_search.schema.sql

The symbols are read directly from src/paradedb/api.py using the OP_/FN_/TYPE_
naming convention — no intermediate JSON or extraction step needed.

Usage:
    python scripts/check_schema_compat.py <schema.sql> <api.py>
"""

import re
import sys
from pathlib import Path


def normalize(sql: str) -> str:
    """Strip double-quotes around identifiers so pdb."score" matches pdb.score."""
    return re.sub(r'"([^"]+)"', r"\1", sql)


def extract_from_sql_api(path: Path) -> dict:
    """Read OP_/FN_/TYPE_ constants from api.py and return a deps dict."""
    source = path.read_text()

    def _values(prefix: str) -> list[str]:
        return sorted({
            m
            for m in re.findall(
                rf"^{prefix}_\w+\s*=\s*[\"']([^\"']+)[\"']", source, re.MULTILINE
            )
        })

    return {
        "functions": _values("FN"),
        "operators": _values("OP"),
        "types": _values("PDB_TYPE"),
    }


def check_function(schema: str, qualified_name: str) -> bool:
    """
    Return True if a CREATE FUNCTION or CREATE AGGREGATE for qualified_name exists.
    Handles both schema.name and bare name forms.
    """
    dot = qualified_name.rfind(".")
    if dot == -1:
        name_pattern = re.escape(qualified_name)
        schema_pattern = r"\S+\."
    else:
        name_pattern = re.escape(qualified_name[dot + 1 :])
        schema_pattern = re.escape(qualified_name[: dot + 1])

    pattern = (
        rf"(?:FUNCTION|AGGREGATE)\s+{schema_pattern}{name_pattern}\s*\("
    )
    return bool(re.search(pattern, schema, re.IGNORECASE))


def check_operator(schema: str, symbol: str) -> bool:
    """
    Return True if CREATE OPERATOR ... <symbol> exists anywhere in the schema.
    The operator may be prefixed with a schema (e.g. pg_catalog.@@@).
    """
    pattern = rf"OPERATOR\s+(?:\w+\.)?{re.escape(symbol)}\s*\("
    return bool(re.search(pattern, schema, re.IGNORECASE))


def check_type(schema: str, qualified_name: str) -> bool:
    """
    Return True if CREATE TYPE schema.name exists.
    """
    dot = qualified_name.rfind(".")
    if dot == -1:
        pattern = rf"TYPE\s+\S*{re.escape(qualified_name)}\b"
    else:
        pattern = rf"TYPE\s+{re.escape(qualified_name)}\b"
    return bool(re.search(pattern, schema, re.IGNORECASE))


def main() -> int:
    if len(sys.argv) != 3:
        print(f"Usage: {sys.argv[0]} <schema.sql> <api.py>", file=sys.stderr)
        return 1

    schema_path = Path(sys.argv[1])
    sql_api_path = Path(sys.argv[2])

    if not schema_path.exists():
        print(f"❌ Schema file not found: {schema_path}", file=sys.stderr)
        return 1

    if not sql_api_path.exists():
        print(f"❌ api.py not found: {sql_api_path}", file=sys.stderr)
        return 1

    schema = normalize(schema_path.read_text())
    deps = extract_from_sql_api(sql_api_path)

    missing: list[tuple[str, str]] = []

    for fn in deps.get("functions", []):
        if not check_function(schema, fn):
            missing.append(("function", fn))

    for op in deps.get("operators", []):
        if not check_operator(schema, op):
            missing.append(("operator", op))

    for typ in deps.get("types", []):
        if not check_type(schema, typ):
            missing.append(("type", typ))

    total = sum(len(v) for v in deps.values() if isinstance(v, list))

    if missing:
        print(f"❌ {len(missing)}/{total} SQL symbols missing from pg_search schema:")
        for kind, name in missing:
            print(f"   {kind}: {name}")
        print()
        print(
            "These symbols were removed or renamed in this version of pg_search.\n"
            "Update django-paradedb to handle the API change, then update\n"
            "api.py to reflect the new surface."
        )
        return 1

    print(f"✅ All {total} SQL symbols present in pg_search schema.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
