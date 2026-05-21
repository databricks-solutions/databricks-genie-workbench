"""Plan 12 PR 8 Task 8.2 — Smoke-list of replay tests that gate every
deploy.

CI fails if any of these tests do not pass. They prove that the
deployed binary's code-path coverage actually exercises the
patch-survival contract end-to-end — unit tests can drift from
production callsites silently, but the deploy gate cannot.

Each entry runs as a subprocess so that one gate's environment
mutations (env vars, registered emitters) do not leak into another's.
"""
import subprocess
import sys

import pytest


DEPLOY_GATE_TESTS = [
    "tests/replay/test_patch_survival_e2e.py",
]


@pytest.mark.parametrize("path", DEPLOY_GATE_TESTS)
def test_deploy_gate_passes(path):
    """Run the deploy-gate test as a subprocess; fail if it does not
    return 0.

    The subprocess isolation matters: a future deploy gate may set
    feature-flag env vars or register emitters that other tests are
    not prepared for. Running each gate as a subprocess prevents
    cross-contamination.
    """
    result = subprocess.run(
        [sys.executable, "-m", "pytest", path, "-q"],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, (
        f"Deploy gate {path} failed (returncode="
        f"{result.returncode}):\n"
        f"---stdout---\n{result.stdout}\n"
        f"---stderr---\n{result.stderr}"
    )
