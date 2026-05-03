"""Default ``DatabricksRunner`` shelling to the ``databricks`` CLI.

Tests substitute a mock; production uses subprocess. Output is the parsed
JSON the CLI prints when invoked with ``--output json``.
"""

from __future__ import annotations

import json
import subprocess
from typing import Any, Mapping


class DatabricksCliRunner:
    def get_run(self, *, run_id: str, profile: str) -> Mapping[str, Any]:
        return self._invoke(
            [
                "databricks",
                "jobs",
                "get-run",
                str(run_id),
                "--profile",
                str(profile),
                "--output",
                "json",
            ]
        )

    def get_run_output(self, *, run_id: str, profile: str) -> Mapping[str, Any]:
        return self._invoke(
            [
                "databricks",
                "jobs",
                "get-run-output",
                str(run_id),
                "--profile",
                str(profile),
                "--output",
                "json",
            ]
        )

    def _invoke(self, cmd: list[str]) -> Mapping[str, Any]:
        proc = subprocess.run(cmd, check=True, capture_output=True, text=True)
        return json.loads(proc.stdout) if proc.stdout.strip() else {}
