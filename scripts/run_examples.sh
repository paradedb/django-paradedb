#!/bin/bash

set -euo pipefail

: "${DJANGO_SPEC:="Django~=6.0"}"
uv run --with "${DJANGO_SPEC}" python examples/quickstart/quickstart.py
uv run --with "${DJANGO_SPEC}" python examples/autocomplete/setup.py
uv run --with "${DJANGO_SPEC}" python examples/autocomplete/autocomplete.py
uv run --with "${DJANGO_SPEC}" python examples/more_like_this/more_like_this.py
uv run --with "${DJANGO_SPEC}" python examples/faceted_search/faceted_search.py
uv run --with "${DJANGO_SPEC}" python examples/hybrid_rrf/setup.py
uv run --with "${DJANGO_SPEC}" python examples/hybrid_rrf/hybrid_rrf.py
