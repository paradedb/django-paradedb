import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[1] / "run_paradedb.sh"


class DatabaseStartupTests(unittest.TestCase):
    def run_script(
        self,
        exists=False,
        actual_image="",
        version="0.26.0",
        pg="18",
        image=None,
        sourced=False,
    ):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            docker = root / "docker"
            docker.write_text(
                "#!" + sys.executable + "\n"
                "import json, os, sys\n"
                "with open(os.environ['DOCKER_CALLS'], 'a') as out: out.write(json.dumps(sys.argv[1:]) + '\\n')\n"
                "command = sys.argv[1]\n"
                "if command == 'ps' and os.environ['MOCK_EXISTS'] == '1': print('orm-test')\n"
                "elif command == 'inspect': print(os.environ['MOCK_IMAGE'])\n"
                "elif command == 'port': print('0.0.0.0:55439')\n"
            )
            docker.chmod(0o755)
            calls_path = root / "calls.jsonl"
            env = os.environ.copy()
            for key in ("PARADEDB_IMAGE", "DATABASE_URL"):
                env.pop(key, None)
            env.update(
                {
                    "PATH": str(root) + os.pathsep + env["PATH"],
                    "DOCKER_CALLS": str(calls_path),
                    "MOCK_EXISTS": "1" if exists else "0",
                    "MOCK_IMAGE": actual_image,
                    "PARADEDB_CONTAINER_NAME": "orm-test",
                    "PARADEDB_VERSION": version,
                    "PARADEDB_POSTGRES_VERSION": pg,
                    "PARADEDB_PORT": "55439",
                }
            )
            if image is not None:
                env["PARADEDB_IMAGE"] = image
            command = ["bash", str(SCRIPT)]
            if sourced:
                command = [
                    "bash",
                    "-c",
                    'set +e; set +u; set +o pipefail; before="$(set +o)"; source "$1"; after="$(set +o)"; [[ "$before" == "$after" ]]',
                    "bash",
                    str(SCRIPT),
                ]
            result = subprocess.run(
                command, env=env, capture_output=True, text=True, check=False
            )
            calls = [json.loads(line) for line in calls_path.read_text().splitlines()]
            return result, calls

    def test_version_and_postgres_overrides_select_requested_image(self):
        result, calls = self.run_script(version="0.26.1", pg="17")
        self.assertEqual(result.returncode, 0, result.stderr)
        run = next(call for call in calls if call[0] == "run")
        self.assertEqual(run[-1], "paradedb/paradedb:0.26.1-pg17")

    def test_explicit_image_takes_precedence(self):
        result, calls = self.run_script(image="local/paradedb:test")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(
            next(call for call in calls if call[0] == "run")[-1], "local/paradedb:test"
        )

    def test_matching_existing_container_is_reused(self):
        result, calls = self.run_script(
            exists=True, actual_image="paradedb/paradedb:0.26.0-pg18"
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn(["start", "orm-test"], calls)
        self.assertFalse(any(call[0] in ("run", "rm") for call in calls))

    def test_mismatched_container_is_preserved_and_not_started(self):
        result, calls = self.run_script(
            exists=True, actual_image="paradedb/paradedb:0.25.0-pg18"
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("but paradedb/paradedb:0.26.0-pg18 was requested", result.stderr)
        self.assertFalse(
            any(call[0] in ("run", "rm", "start", "exec") for call in calls)
        )

    @unittest.skipIf(
        "set -euo pipefail\n\nPARADEDB_VERSION" in SCRIPT.read_text(),
        "Drizzle invokes the script directly",
    )
    def test_sourcing_restores_shell_options(self):
        result, _ = self.run_script(sourced=True)
        self.assertEqual(result.returncode, 0, result.stderr)


if __name__ == "__main__":
    unittest.main()
