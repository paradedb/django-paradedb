import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
ORM = "django"
RUNNER = (
    ROOT
    / "scripts"
    / ("run_integration_tests.sh" if ORM == "sqlalchemy" else "run_tests.sh")
)


class TestRunnerTests(unittest.TestCase):
    def run_runner(self, overrides):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            log = root / "calls.jsonl"
            body = (
                "#!" + sys.executable + "\n"
                "import json, os, sys\n"
                "from pathlib import Path\n"
                "tool = Path(sys.argv[0]).name\n"
                "with open(os.environ['RUNNER_CALLS'], 'a') as out: out.write(json.dumps({'tool': tool, 'args': sys.argv[1:], 'dsn': os.environ.get('PARADEDB_TEST_DSN'), 'url': os.environ.get('DATABASE_URL'), 'image': os.environ.get('PARADEDB_IMAGE')}) + '\\n')\n"
                "if tool == 'ruby': print('ruby 4.0.0')\n"
                "if tool == 'docker' and sys.argv[1] == 'ps': print('runner-test')\n"
                "if tool == 'docker' and sys.argv[1] == 'inspect': print('paradedb/paradedb:0.26.0-pg18')\n"
                "if tool == 'docker' and sys.argv[1] == 'port': print('0.0.0.0:55439')\n"
            )
            for tool in (
                "docker",
                "uv",
                "pnpm",
                "dotnet",
                "bundle",
                "ruby",
                "gem",
                "rbenv",
            ):
                executable = root / tool
                executable.write_text(body)
                executable.chmod(0o755)
            env = os.environ.copy()
            for key in (
                "PARADEDB_TEST_DSN",
                "DATABASE_URL",
                "PARADEDB_IMAGE",
                "PARADEDB_VERSION",
                "PARADEDB_POSTGRES_VERSION",
            ):
                env.pop(key, None)
            env.update(
                {
                    "PATH": str(root) + os.pathsep + env["PATH"],
                    "RUNNER_CALLS": str(log),
                    "PARADEDB_CONTAINER_NAME": "runner-test",
                    "PARADEDB_PORT": "55439",
                }
            )
            env.update(overrides)
            result = subprocess.run(
                ["bash", str(RUNNER), "selector"],
                cwd=root,
                env=env,
                capture_output=True,
                text=True,
                check=False,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            calls = [json.loads(line) for line in log.read_text().splitlines()]
            test_call = next(
                call
                for call in reversed(calls)
                if call["tool"] in ("uv", "pnpm", "dotnet", "bundle")
            )
            self.assertIn("selector", test_call["args"])
            return calls, test_call

    @unittest.skipIf(ORM == "drizzle", "Drizzle uses DATABASE_URL")
    def test_external_dsn_is_preserved_without_docker(self):
        calls, test = self.run_runner({"PARADEDB_TEST_DSN": "external-test-dsn"})
        self.assertEqual(test["dsn"], "external-test-dsn")
        self.assertFalse(any(call["tool"] == "docker" for call in calls))

    @unittest.skipIf(ORM == "efcore", "EF Core uses an Npgsql connection string")
    def test_external_url_is_preserved_without_docker(self):
        calls, test = self.run_runner(
            {"DATABASE_URL": "postgresql://external/database"}
        )
        self.assertEqual(test["url"], "postgresql://external/database")
        self.assertFalse(any(call["tool"] == "docker" for call in calls))
        if ORM != "drizzle":
            self.assertEqual(test["dsn"], test["url"])

    @unittest.skipIf(ORM == "efcore", "EF Core uses Testcontainers")
    def test_local_container_connection_reaches_tests(self):
        calls, test = self.run_runner({})
        self.assertTrue(any(call["tool"] == "docker" for call in calls))
        self.assertIn(":55439/", test["url"])
        if ORM != "drizzle":
            self.assertEqual(test["dsn"], test["url"])

    @unittest.skipUnless(ORM == "efcore", "EF Core selects the Testcontainers image")
    def test_default_and_requested_images_reach_dotnet(self):
        _, test = self.run_runner({})
        self.assertEqual(test["image"], "paradedb/paradedb:0.26.0-pg18")
        _, test = self.run_runner(
            {"PARADEDB_VERSION": "0.26.1", "PARADEDB_POSTGRES_VERSION": "17"}
        )
        self.assertEqual(test["image"], "paradedb/paradedb:0.26.1-pg17")
        _, test = self.run_runner({"PARADEDB_IMAGE": "local/paradedb:test"})
        self.assertEqual(test["image"], "local/paradedb:test")


if __name__ == "__main__":
    unittest.main()
