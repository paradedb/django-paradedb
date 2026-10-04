import contextlib
import importlib.util
import io
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

SPEC = importlib.util.spec_from_file_location(
    "check_schema_compat",
    Path(__file__).resolve().parents[1] / "check_schema_compat.py",
)
assert SPEC is not None and SPEC.loader is not None
compat = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(compat)


class SchemaCompatibilityTests(unittest.TestCase):
    def check(self, schema, api, ignored):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            schema_path = root / "schema.sql"
            api_path = root / "api.json5"
            ignore_path = root / "apiignore.json5"
            schema_path.write_text(schema)
            api_path.write_text(json.dumps(api))
            ignore_path.write_text(json.dumps(ignored))
            output = io.StringIO()
            with (
                patch.object(compat, "_IGNORE_FILE", ignore_path),
                contextlib.redirect_stdout(output),
            ):
                result = compat.run_checks(schema_path, api_path)
            return result, output.getvalue()

    def test_current_symbols_and_grouped_ignores_pass(self):
        result, _ = self.check(
            'CREATE FUNCTION pdb."score"(bigint); CREATE FUNCTION pdb.internal_fn(); '
            "CREATE TYPE pdb.query; CREATE OPERATOR public.@@@ (LEFTARG = text);",
            {
                "functions": {"score": "pdb.score"},
                "operators": {"search": "@@@"},
                "types": {},
            },
            {"functions": {"internal": ["pdb.internal_fn"]}, "types": ["pdb.query"]},
        )
        self.assertEqual(result, 0)

    def test_stale_ignores_fail_for_every_symbol_kind(self):
        result, output = self.check(
            "",
            {"functions": {}, "operators": {}, "types": {}},
            {
                "functions": ["pdb.alias_recv"],
                "operators": ["@@@"],
                "types": ["pdb.old_type"],
            },
        )
        self.assertEqual(result, 1)
        for symbol in ("pdb.alias_recv", "@@@", "pdb.old_type"):
            self.assertIn(symbol, output)
        self.assertIn("3 ignored symbols no longer exist", output)

    def test_type_does_not_satisfy_function_ignore(self):
        result, output = self.check(
            "CREATE TYPE pdb.query;",
            {"functions": {}, "operators": {}, "types": {}},
            {"functions": ["pdb.query"], "types": ["pdb.query"]},
        )
        self.assertEqual(result, 1)
        self.assertIn("functions: pdb.query", output)

    def test_missing_wrapped_function_still_fails(self):
        result, output = self.check(
            "",
            {"functions": {"score": "pdb.score"}, "operators": {}, "types": {}},
            {},
        )
        self.assertEqual(result, 1)
        self.assertIn("Forward check: 1/1", output)

    def test_new_public_function_still_fails(self):
        result, output = self.check(
            "CREATE FUNCTION pdb.new_public_api();",
            {"functions": {}, "operators": {}, "types": {}},
            {},
        )
        self.assertEqual(result, 1)
        self.assertIn("functions: pdb.new_public_api", output)


if __name__ == "__main__":
    unittest.main()
