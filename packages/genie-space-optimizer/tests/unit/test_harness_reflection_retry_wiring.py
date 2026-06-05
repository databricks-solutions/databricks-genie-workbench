"""Pin precise reflection retry."""

from __future__ import annotations

from _harness_loop_source import lever_loop_source


def test_uses_patch_retry_signature() -> None:
    src = lever_loop_source()
    assert "patch_retry_signature" in src


def test_uses_retry_allowed_after_rollback() -> None:
    src = lever_loop_source()
    assert "retry_allowed_after_rollback" in src
