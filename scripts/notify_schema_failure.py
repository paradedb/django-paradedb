#!/usr/bin/env python3
"""Keep one schema compatibility failure issue per ParadeDB release."""

import json
import os
import subprocess


def gh(*args: str) -> str:
    return subprocess.run(
        ["gh", *args], check=True, capture_output=True, text=True
    ).stdout


def notify(repository: str, version: str, run_url: str) -> None:
    version = version.removeprefix("v")
    title = f"Schema compat failure for ParadeDB v{version}"
    # Fetch issues directly rather than relying on GitHub's delayed search index.
    issues = json.loads(
        gh(
            "api",
            "--paginate",
            "--slurp",
            f"repos/{repository}/issues?state=all&per_page=100",
        )
    )
    matches = [
        issue
        for page in issues
        for issue in page
        if issue["title"] == title and "pull_request" not in issue
    ]
    matches.sort(key=lambda issue: (issue["state"] != "open", -issue["number"]))
    body = (
        f"The schema compatibility check or integration tests failed against "
        f"ParadeDB **v{version}**.\n\n**Workflow run:** {run_url}\n\n"
        f"Please investigate and update {repository.split('/')[-1]} as needed."
    )
    if not matches:
        gh(
            "issue",
            "create",
            "--repo",
            repository,
            "--title",
            title,
            "--body",
            body,
            "--label",
            "bug",
        )
        return

    issue = matches[0]
    number = str(issue["number"])
    if issue["state"] == "closed":
        gh("issue", "reopen", number, "--repo", repository)
    gh("issue", "comment", number, "--repo", repository, "--body", body)


def main() -> None:
    repository = os.environ["GITHUB_REPOSITORY"]
    run_url = (
        f"{os.environ['GITHUB_SERVER_URL']}/{repository}/actions/runs/"
        f"{os.environ['GITHUB_RUN_ID']}"
    )
    notify(repository, os.environ["PARADEDB_VERSION"], run_url)


if __name__ == "__main__":
    main()
