import importlib.util
import json
import subprocess
import unittest
from pathlib import Path
from unittest.mock import patch

SPEC = importlib.util.spec_from_file_location(
    "notify_schema_failure",
    Path(__file__).resolve().parents[1] / "notify_schema_failure.py",
)
assert SPEC is not None and SPEC.loader is not None
notification = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(notification)


class NotificationTests(unittest.TestCase):
    def run_notification(self, pages):
        with patch.object(
            notification, "gh", side_effect=[json.dumps(pages), "", ""]
        ) as gh:
            notification.notify(
                "paradedb/example", "v0.26.0", "https://github.com/example/run/1"
            )
            return gh.call_args_list

    def test_first_failure_creates_issue(self):
        calls = self.run_notification([[]])
        self.assertEqual(calls[1].args[:2], ("issue", "create"))
        self.assertIn("Schema compat failure for ParadeDB v0.26.0", calls[1].args)

    def test_repeat_failure_comments_on_existing_issue(self):
        calls = self.run_notification(
            [
                [
                    {
                        "number": 12,
                        "title": "Schema compat failure for ParadeDB v0.26.0",
                        "state": "open",
                    }
                ]
            ]
        )
        self.assertEqual(calls[1].args[:3], ("issue", "comment", "12"))
        self.assertIn("https://github.com/example/run/1", calls[1].args[-1])

    def test_resolved_issue_reopens_on_new_failure(self):
        calls = self.run_notification(
            [
                [
                    {
                        "number": 12,
                        "title": "Schema compat failure for ParadeDB v0.26.0",
                        "state": "closed",
                    }
                ]
            ]
        )
        self.assertEqual(calls[1].args[:3], ("issue", "reopen", "12"))
        self.assertEqual(calls[2].args[:3], ("issue", "comment", "12"))

    def test_exact_title_and_issues_only_across_pages(self):
        calls = self.run_notification(
            [
                [
                    {
                        "number": 20,
                        "title": "Schema compat failure for ParadeDB v0.26.01",
                        "state": "open",
                    },
                    {
                        "number": 21,
                        "title": "Schema compat failure for ParadeDB v0.26.0",
                        "state": "open",
                        "pull_request": {},
                    },
                ],
                [
                    {
                        "number": 12,
                        "title": "Schema compat failure for ParadeDB v0.26.0",
                        "state": "open",
                    }
                ],
            ]
        )
        self.assertEqual(calls[1].args[:3], ("issue", "comment", "12"))

    def test_open_issue_preferred_over_closed_duplicate(self):
        calls = self.run_notification(
            [
                [
                    {
                        "number": 20,
                        "title": "Schema compat failure for ParadeDB v0.26.0",
                        "state": "closed",
                    },
                    {
                        "number": 12,
                        "title": "Schema compat failure for ParadeDB v0.26.0",
                        "state": "open",
                    },
                ]
            ]
        )
        self.assertEqual(calls[1].args[:3], ("issue", "comment", "12"))

    def test_api_failure_does_not_create_duplicate(self):
        with patch.object(
            notification, "gh", side_effect=subprocess.CalledProcessError(1, "gh")
        ) as gh:
            with self.assertRaises(subprocess.CalledProcessError):
                notification.notify(
                    "paradedb/example", "0.26.0", "https://github.com/example/run/1"
                )
            self.assertEqual(gh.call_count, 1)


if __name__ == "__main__":
    unittest.main()
