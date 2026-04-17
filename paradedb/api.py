"""Load ParadeDB SQL API constants from api.json5.

To add or modify a symbol, edit api.json5 only. Every key in every section
(operators/functions/types) is exposed as a module-level name::

    from paradedb.api import FN_ALL, OP_SEARCH, PDB_TYPE_BOOST
"""

from pathlib import Path

import json5


def _validate_api_payload(payload: object) -> dict[str, dict[str, str]]:
    if not isinstance(payload, dict):
        raise ValueError("paradedb.api payload must be a JSON object.")

    required_sections = ("operators", "functions", "types")
    validated: dict[str, dict[str, str]] = {}
    for section in required_sections:
        section_data = payload.get(section)
        if not isinstance(section_data, dict):
            raise ValueError(f"paradedb.api section {section!r} must be an object.")

        typed_section: dict[str, str] = {}
        for name, value in section_data.items():
            if not isinstance(name, str):
                raise ValueError(
                    f"paradedb.api section {section!r} contains non-string key: {name!r}"
                )
            if not isinstance(value, str):
                raise ValueError(
                    f"paradedb.api symbol {name!r} in section {section!r} must map to a string."
                )
            typed_section[name] = value
        validated[section] = typed_section

    return validated


def _load_api() -> dict[str, dict[str, str]]:
    # In installed wheels, api.json5 is bundled into the package directory.
    packaged_api = Path(__file__).with_name("api.json5")
    if packaged_api.is_file():
        return _validate_api_payload(
            json5.loads(packaged_api.read_text(encoding="utf-8"))
        )

    # In editable/source checkouts, fall back to the repository-root file.
    source_api = Path(__file__).resolve().parents[1] / "api.json5"
    if source_api.is_file():
        return _validate_api_payload(
            json5.loads(source_api.read_text(encoding="utf-8"))
        )

    raise FileNotFoundError("Could not locate api.json5 for paradedb.api.")


_api = _load_api()
for _section in _api.values():
    globals().update(_section)
