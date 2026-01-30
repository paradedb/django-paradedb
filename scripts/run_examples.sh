#!/usr/bin/env bash
# Run all examples (for CI and local testing)

set -euo pipefail

EXAMPLES_DIR="${1:-examples}"

# Use python3 if available, otherwise python
PYTHON_CMD="${PYTHON:-python3}"
if ! command -v "$PYTHON_CMD" >/dev/null 2>&1; then
  PYTHON_CMD="python"
fi

echo "Running all examples using: $PYTHON_CMD"

# quickstart
if [[ -f "$EXAMPLES_DIR/quickstart/quickstart.py" ]]; then
  echo "Running quickstart..."
  "$PYTHON_CMD" "$EXAMPLES_DIR/quickstart/quickstart.py"
fi

# autocomplete (needs setup first)
if [[ -f "$EXAMPLES_DIR/autocomplete/setup.py" ]]; then
  echo "Running autocomplete setup..."
  "$PYTHON_CMD" "$EXAMPLES_DIR/autocomplete/setup.py"
fi
if [[ -f "$EXAMPLES_DIR/autocomplete/autocomplete.py" ]]; then
  echo "Running autocomplete..."
  "$PYTHON_CMD" "$EXAMPLES_DIR/autocomplete/autocomplete.py"
fi

# more_like_this
if [[ -f "$EXAMPLES_DIR/more_like_this/more_like_this.py" ]]; then
  echo "Running more_like_this..."
  "$PYTHON_CMD" "$EXAMPLES_DIR/more_like_this/more_like_this.py"
fi

# faceted_search
if [[ -f "$EXAMPLES_DIR/faceted_search/faceted_search.py" ]]; then
  echo "Running faceted_search..."
  "$PYTHON_CMD" "$EXAMPLES_DIR/faceted_search/faceted_search.py"
fi

# hybrid_rrf (needs setup first)
if [[ -f "$EXAMPLES_DIR/hybrid_rrf/setup.py" ]]; then
  echo "Running hybrid_rrf setup..."
  "$PYTHON_CMD" "$EXAMPLES_DIR/hybrid_rrf/setup.py"
fi
if [[ -f "$EXAMPLES_DIR/hybrid_rrf/hybrid_rrf.py" ]]; then
  echo "Running hybrid_rrf..."
  "$PYTHON_CMD" "$EXAMPLES_DIR/hybrid_rrf/hybrid_rrf.py"
fi

echo "All examples completed!"
