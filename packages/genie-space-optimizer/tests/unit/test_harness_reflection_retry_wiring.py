"""Pin precise reflection retry."""

from __future__ import annotations

import inspect

from genie_space_optimizer.optimization import harness


def test_uses_patch_retry_signature() -> None:
    src = inspect.getsource(harness._run_lever_loop)
    assert "patch_retry_signature" in src


def test_uses_retry_allowed_after_rollback() -> None:
    src = inspect.getsource(harness._run_lever_loop)
    assert "retry_allowed_after_rollback" in src
