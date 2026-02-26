#!/usr/bin/env python3
"""
Check compatibility between api.json and a pg_search schema file in both directions.

The schema is generated in the paradedb repo via:
    cargo pgrx schema -p pg_search pg18 > pg_search.schema.sql

Two checks are performed:

  Forward:  every symbol in api.json is present in the schema (detects removals/renames).
  Reverse:  every pdb.* symbol in the schema is either in api.json or in apiignore.json
            (surfaces new paradedb APIs that haven't been wrapped yet).

Usage:
    python scripts/check_schema_compat.py <schema.sql> <api.json>

The ignore list is read automatically from apiignore.json (repo root) if it exists.
"""

import json
import re
import sys
from pathlib import Path

_IGNORE_FILE = Path(__file__).parent.parent / "apiignore.json"


def normalize(sql: str) -> str:
    """Strip double-quotes around identifiers so pdb."score" matches pdb.score."""
    return re.sub(r'"([^"]+)"', r"\1", sql)


def extract_from_api(path: Path) -> dict:
    """Read api.json and return {functions: [...], operators: [...], types: [...]}."""
    data = json.loads(path.read_text())
    return {
        "functions": sorted(set(data["functions"].values())),
        "operators": sorted(set(data["operators"].values())),
        "types": sorted(set(data["types"].values())),
    }


def scan_schema_symbols(schema: str) -> dict:
    """Extract all pdb.* functions/aggregates/types and all operators from the schema."""
    functions = sorted({
        m.lower()
        for m in re.findall(r"(?:FUNCTION|AGGREGATE)\s+(pdb\.\w+)\s*\(", schema, re.IGNORECASE)
    })
    types = sorted({
        m.lower()
        for m in re.findall(r"TYPE\s+(pdb\.\w+)\b", schema, re.IGNORECASE)
    })
    operators = sorted({
        m
        for m in re.findall(r"OPERATOR\s+(?:\w+\.)?([^\s(]+)\s*\(", schema, re.IGNORECASE)
    })
    return {"functions": functions, "operators": operators, "types": types}


def check_function(schema: str, qualified_name: str) -> bool:
    dot = qualified_name.rfind(".")
    if dot == -1:
        name_pattern = re.escape(qualified_name)
        schema_pattern = r"\S+\."
    else:
        name_pattern = re.escape(qualified_name[dot + 1:])
        schema_pattern = re.escape(qualified_name[:dot + 1])
    pattern = rf"(?:FUNCTION|AGGREGATE)\s+{schema_pattern}{name_pattern}\s*\("
    return bool(re.search(pattern, schema, re.IGNORECASE))


def check_operator(schema: str, symbol: str) -> bool:
    pattern = rf"OPERATOR\s+(?:\w+\.)?{re.escape(symbol)}\s*\("
    return bool(re.search(pattern, schema, re.IGNORECASE))


def check_type(schema: str, qualified_name: str) -> bool:
    dot = qualified_name.rfind(".")
    if dot == -1:
        pattern = rf"TYPE\s+\S*{re.escape(qualified_name)}\b"
    else:
        pattern = rf"TYPE\s+{re.escape(qualified_name)}\b"
    return bool(re.search(pattern, schema, re.IGNORECASE))


def main() -> int:
    if len(sys.argv) != 3:
        print(f"Usage: {sys.argv[0]} <schema.sql> <api.json>", file=sys.stderr)
        return 1

    schema_path = Path(sys.argv[1])
    api_path = Path(sys.argv[2])

    if not schema_path.exists():
        print(f"❌ Schema file not found: {schema_path}", file=sys.stderr)
        return 1
    if not api_path.exists():
        print(f"❌ api.json not found: {api_path}", file=sys.stderr)
        return 1

    schema = normalize(schema_path.read_text())
    deps = extract_from_api(api_path)
    ignored = json.loads(_IGNORE_FILE.read_text()) if _IGNORE_FILE.exists() else {}

    rc = 0

    # ------------------------------------------------------------------
    # Forward check: every symbol in api.json must exist in the schema.
    # ------------------------------------------------------------------
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

    total_api = sum(len(v) for v in deps.values() if isinstance(v, list))
    if missing:
        print(f"❌ Forward check: {len(missing)}/{total_api} api.json symbols missing from schema:")
        for kind, name in missing:
            print(f"   {kind}: {name}")
        print(
            "\nThese symbols were removed or renamed in this version of pg_search.\n"
            "Update django-paradedb to handle the API change, then update api.json."
        )
        rc = 1
    else:
        print(f"✅ Forward check: all {total_api} api.json symbols present in schema.")

    # ------------------------------------------------------------------
    # Reverse check: every pdb.* symbol in the schema must be in api.json
    # or explicitly ignored in apiignore.json.
    # ------------------------------------------------------------------
    schema_symbols = scan_schema_symbols(schema)
    uncovered: list[tuple[str, str]] = []
    for kind in ("functions", "operators", "types"):
        api_set = set(deps.get(kind, []))
        ignore_set = set(ignored.get(kind, []))
        for sym in schema_symbols.get(kind, []):
            if sym not in api_set and sym not in ignore_set:
                uncovered.append((kind, sym))

    total_schema = sum(len(v) for v in schema_symbols.values())
    if uncovered:
        print(
            f"\n⚠️  Reverse check: {len(uncovered)} schema symbols not covered by api.json:"
        )
        for kind, name in uncovered:
            print(f"   {kind}: {name}")
        print(
            "\nThese are paradedb APIs not yet wrapped by django-paradedb.\n"
            "Either add them to api.json or add them to apiignore.json."
        )
        rc = 1
    else:
        print(f"✅ Reverse check: all {total_schema} schema symbols accounted for.")

    return rc


if __name__ == "__main__":
    sys.exit(main())
