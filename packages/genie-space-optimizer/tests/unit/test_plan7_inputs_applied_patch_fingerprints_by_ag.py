"""Plan 8 Task 9 — plan7_inputs.build_applied_patch_fingerprints_by_ag
extracts content_fingerprints per AG from the harness's apply_log."""
from __future__ import annotations

from genie_space_optimizer.optimization.plan7_inputs import (
    build_applied_patch_fingerprints_by_ag,
)


def test_groups_fingerprints_by_ag():
    apply_log = {"applied": [
        {"patch": {"ag_id": "AG_X", "content_fingerprint": "fp1"}},
        {"patch": {"ag_id": "AG_X", "content_fingerprint": "fp2"}},
        {"patch": {"ag_id": "AG_Y", "content_fingerprint": "fp3"}},
    ]}
    out = build_applied_patch_fingerprints_by_ag(apply_log)
    assert out == {"AG_X": {"fp1", "fp2"}, "AG_Y": {"fp3"}}


def test_drops_entries_without_fingerprint():
    apply_log = {"applied": [
        {"patch": {"ag_id": "AG_X", "content_fingerprint": "fp1"}},
        {"patch": {"ag_id": "AG_X"}},
    ]}
    out = build_applied_patch_fingerprints_by_ag(apply_log)
    assert out == {"AG_X": {"fp1"}}
