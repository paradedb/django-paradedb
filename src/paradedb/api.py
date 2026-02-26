"""Load paradedb SQL API constants from api.json at the repo root.

To add or modify a symbol, edit api.json only.  Every key in every section
(operators/functions/types) is exposed as a module-level name::

    from paradedb.api import FN_ALL, OP_SEARCH, PDB_TYPE_BOOST
"""

import json
from pathlib import Path

_api = json.loads((Path(__file__).parent.parent.parent / "api.json").read_text())
for _section in _api.values():
    globals().update(_section)
