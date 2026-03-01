"""Load ParadeDB SQL API constants from api.json.

To add or modify a symbol, edit api.json only. Every key in every section
(operators/functions/types) is exposed as a module-level name::

    from paradedb.api import FN_ALL, OP_SEARCH, PDB_TYPE_BOOST
"""

import json
from pathlib import Path


def _load_api() -> dict[str, dict[str, str]]:
    # In installed wheels, api.json is bundled into the package directory.
    packaged_api = Path(__file__).with_name("api.json")
    if packaged_api.is_file():
        return json.loads(packaged_api.read_text(encoding="utf-8"))

    # In editable/source checkouts, fall back to the repository-root file.
    source_api = Path(__file__).resolve().parents[2] / "api.json"
    if source_api.is_file():
        return json.loads(source_api.read_text(encoding="utf-8"))

    raise FileNotFoundError("Could not locate api.json for paradedb.api.")


_api = _load_api()
for _section in _api.values():
    globals().update(_section)
