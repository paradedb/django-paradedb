#!/usr/bin/env python3
"""Validate that api.json matches the Django wrapper surface."""

from __future__ import annotations

import ast
import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
API_JSON = ROOT / "api.json"
APIIGNORE_JSON = ROOT / "apiignore.json"
PDB_SYMBOL_RE = re.compile(r"\bpdb\.[A-Za-z_][A-Za-z0-9_]*\b")


def load_json(path: Path) -> object:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise FileNotFoundError(f"{path} not found") from exc
    except json.JSONDecodeError as exc:
        raise ValueError(f"invalid JSON in {path}: {exc}") from exc


def flatten_ignore(section: object, *, kind: str) -> set[str]:
    if section is None:
        return set()
    if isinstance(section, list):
        return {str(item) for item in section}
    if isinstance(section, dict):
        flattened: set[str] = set()
        for values in section.values():
            if not isinstance(values, list):
                raise ValueError(
                    f"apiignore {kind} section values must be arrays when grouped."
                )
            flattened.update(str(item) for item in values)
        return flattened
    raise ValueError(f"apiignore {kind} section must be an array or object of arrays.")


def source_paths() -> list[Path]:
    return sorted(
        path for path in ROOT.glob("paradedb/**/*.py") if path.name != "api.py"
    )


def parse_module(path: Path) -> ast.AST:
    try:
        return ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    except SyntaxError as exc:
        raise SyntaxError(f"failed to parse {path}: {exc}") from exc


def collect_name_references(module: ast.AST) -> set[str]:
    return {node.id for node in ast.walk(module) if isinstance(node, ast.Name)}


def collect_string_literals(module: ast.AST) -> list[str]:
    literals: list[str] = []
    for node in ast.walk(module):
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            literals.append(node.value)
    return literals


def main() -> int:
    try:
        api = load_json(API_JSON)
        apiignore = load_json(APIIGNORE_JSON) if APIIGNORE_JSON.is_file() else {}
    except (FileNotFoundError, ValueError) as exc:
        print(f"❌ {exc}", file=sys.stderr)
        return 1

    if not isinstance(api, dict):
        print("❌ api.json must contain a JSON object.", file=sys.stderr)
        return 1
    if not isinstance(apiignore, dict):
        print("❌ apiignore.json must contain a JSON object.", file=sys.stderr)
        return 1

    try:
        operators = api["operators"]
        functions = api["functions"]
        types = api["types"]
    except KeyError as exc:
        print(f"❌ api.json missing required section: {exc}", file=sys.stderr)
        return 1

    if not all(isinstance(section, dict) for section in (operators, functions, types)):
        print(
            "❌ api.json sections operators/functions/types must all be objects.",
            file=sys.stderr,
        )
        return 1

    api_names = {
        *(str(name) for name in operators),
        *(str(name) for name in functions),
        *(str(name) for name in types),
    }
    expected_api_names = api_names

    try:
        ignored_functions = flatten_ignore(apiignore.get("functions"), kind="functions")
        ignored_types = flatten_ignore(apiignore.get("types"), kind="types")
    except ValueError as exc:
        print(f"❌ {exc}", file=sys.stderr)
        return 1

    referenced_api_names: set[str] = set()
    referenced_symbols: set[str] = set()

    try:
        paths = source_paths()
        for path in paths:
            module = parse_module(path)
            referenced_api_names.update(collect_name_references(module))
            for literal in collect_string_literals(module):
                referenced_symbols.update(PDB_SYMBOL_RE.findall(literal))
    except (SyntaxError, OSError) as exc:
        print(f"❌ {exc}", file=sys.stderr)
        return 1

    missing_api_names = sorted(expected_api_names - referenced_api_names)

    allowed_symbols = {
        *(str(value) for value in functions.values()),
        *(str(value) for value in types.values()),
        *ignored_functions,
        *ignored_types,
    }
    untracked_symbols = sorted(referenced_symbols - allowed_symbols)

    issues: list[str] = []
    if missing_api_names:
        issues.append(
            "api.json names not referenced by Django wrappers: "
            + ", ".join(missing_api_names)
        )
    if untracked_symbols:
        issues.append(
            "pdb.* symbols used in package source but missing from api.json/apiignore.json: "
            + ", ".join(untracked_symbols)
        )

    if issues:
        print("❌ API coverage check failed:", file=sys.stderr)
        for issue in issues:
            print(f"   - {issue}", file=sys.stderr)
        print(
            "\nUpdate api.json, apiignore.json, or the Django wrappers so they stay in sync.",
            file=sys.stderr,
        )
        return 1

    print("✅ API coverage check passed.")
    print(f"   api names referenced: {len(expected_api_names)}/{len(api_names)}")
    print(
        "   "
        f"source files: {len(paths)}, raw pdb.* references checked: {len(referenced_symbols)}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
