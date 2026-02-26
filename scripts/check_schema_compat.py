#!/usr/bin/env python3
"""
Check compatibility between api.py and a pg_search schema file in both directions.

The schema is generated in the paradedb repo via:
    cargo pgrx schema -p pg_search pg18 > pg_search.schema.sql

The symbols are read directly from src/paradedb/api.py using the OP_/FN_/PDB_TYPE_
naming convention — no intermediate JSON or extraction step needed.

Two checks are performed:

  Forward:  every symbol in api.py is present in the schema (detects removals/renames).
  Reverse:  every pdb.* symbol in the schema is either in api.py or in compat_ignore.json
            (surfaces new paradedb APIs that haven't been wrapped yet).

Usage:
    python scripts/check_schema_compat.py <schema.sql> <api.py>

The ignore list is read automatically from scripts/compat_ignore.json (sibling of this
script) if it exists.
"""

import json
import re
import sys
from pathlib import Path

_IGNORE_FILE = Path(__file__).parent / "compat_ignore.json"


def normalize(sql: str) -> str:
    """Strip double-quotes around identifiers so pdb."score" matches pdb.score."""
    return re.sub(r'"([^"]+)"', r"\1", sql)


def extract_from_api(path: Path) -> dict:
    """Read OP_/FN_/PDB_TYPE_ constants from api.py and return a deps dict."""
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
        print(f"Usage: {sys.argv[0]} <schema.sql> <api.py>", file=sys.stderr)
        return 1

    schema_path = Path(sys.argv[1])
    api_path = Path(sys.argv[2])

    if not schema_path.exists():
        print(f"❌ Schema file not found: {schema_path}", file=sys.stderr)
        return 1
    if not api_path.exists():
        print(f"❌ api.py not found: {api_path}", file=sys.stderr)
        return 1

    schema = normalize(schema_path.read_text())
    deps = extract_from_api(api_path)
    ignored = json.loads(_IGNORE_FILE.read_text()) if _IGNORE_FILE.exists() else {}

    rc = 0

    # ------------------------------------------------------------------
    # Forward check: every symbol in api.py must exist in the schema.
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
        print(f"❌ Forward check: {len(missing)}/{total_api} api.py symbols missing from schema:")
        for kind, name in missing:
            print(f"   {kind}: {name}")
        print(
            "\nThese symbols were removed or renamed in this version of pg_search.\n"
            "Update django-paradedb to handle the API change, then update api.py."
        )
        rc = 1
    else:
        print(f"✅ Forward check: all {total_api} api.py symbols present in schema.")

    # ------------------------------------------------------------------
    # Reverse check: every pdb.* symbol in the schema must be in api.py
    # or explicitly ignored in compat_ignore.json.
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
            f"\n⚠️  Reverse check: {len(uncovered)} schema symbols not covered by api.py:"
        )
        for kind, name in uncovered:
            print(f"   {kind}: {name}")
        print(
            "\nThese are paradedb APIs not yet wrapped by django-paradedb.\n"
            "Either add them to api.py or add them to scripts/compat_ignore.json."
        )
        rc = 1
    else:
        print(f"✅ Reverse check: all {total_schema} schema symbols accounted for.")

    return rc


if __name__ == "__main__":
    sys.exit(main())
