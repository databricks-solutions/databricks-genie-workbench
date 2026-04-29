"""Harness must call verify_rollback_restored after rollback."""

from __future__ import annotations

import inspect

from genie_space_optimizer.optimization import harness as harness_mod


def test_harness_imports_verify_rollback_restored() -> None:
    src = inspect.getsource(harness_mod)
    assert "verify_rollback_restored" in src, (
        "harness must call verify_rollback_restored after rollback() "
        "to confirm the Genie Space state actually reverted"
    )


def test_harness_verify_rollback_restored_call_emits_warning_path() -> None:
    src = inspect.getsource(harness_mod)
    rollback_index = src.find("rollback(apply_log, w, space_id, metadata_snapshot)")
    verify_index = src.find("verify_rollback_restored", rollback_index)
    assert rollback_index >= 0
    assert verify_index >= 0
    assert verify_index - rollback_index < 4000, (
        "verify_rollback_restored must follow rollback() within the same block"
    )
