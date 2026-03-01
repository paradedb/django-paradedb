#!/usr/bin/env python3
"""Validate that api.json and src/paradedb/api.pyi expose the same constants."""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path

_STUB_SYMBOL_RE = re.compile(r"^([A-Z][A-Z0-9_]*)\s*:\s*str\s*$")


def _load_api_symbols(api_path: Path) -> set[str]:
    data = json.loads(api_path.read_text(encoding="utf-8"))
    symbols: set[str] = set()
    for section in ("operators", "functions", "types"):
        values = data.get(section)
        if not isinstance(values, dict):
            raise ValueError(f"api.json section {section!r} must be an object.")
        symbols.update(values.keys())
    return symbols


def _load_stub_symbols(stub_path: Path) -> set[str]:
    symbols: set[str] = set()
    for line in stub_path.read_text(encoding="utf-8").splitlines():
        match = _STUB_SYMBOL_RE.match(line.strip())
        if match:
            symbols.add(match.group(1))
    return symbols


def main() -> int:
    root = Path(__file__).resolve().parents[1]
    api_path = root / "api.json"
    stub_path = root / "src" / "paradedb" / "api.pyi"

    if not api_path.is_file():
        print(f"❌ api.json not found at {api_path}", file=sys.stderr)
        return 1
    if not stub_path.is_file():
        print(f"❌ api.pyi not found at {stub_path}", file=sys.stderr)
        return 1

    api_symbols = _load_api_symbols(api_path)
    stub_symbols = _load_stub_symbols(stub_path)

    missing_in_stub = sorted(api_symbols - stub_symbols)
    missing_in_api = sorted(stub_symbols - api_symbols)

    if missing_in_stub or missing_in_api:
        print("❌ api.json and api.pyi are out of sync.", file=sys.stderr)
        if missing_in_stub:
            print(
                "   Missing in api.pyi: " + ", ".join(missing_in_stub),
                file=sys.stderr,
            )
        if missing_in_api:
            print(
                "   Missing in api.json: " + ", ".join(missing_in_api),
                file=sys.stderr,
            )
        return 1

    print(f"✅ api.json and api.pyi are in sync ({len(api_symbols)} symbols).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
