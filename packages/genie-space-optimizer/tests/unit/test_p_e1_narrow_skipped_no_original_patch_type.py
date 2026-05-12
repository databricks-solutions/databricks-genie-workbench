"""P-E1 Task 5/6 — narrow_replacement_diagnosis distinguishes
'no original patch_type' from 'unrecognized patch_type'.
"""
from __future__ import annotations


def _patch_empty_ptype() -> dict:
    return {"proposal_id": "X", "patch_type": ""}


def _patch_unknown_ptype() -> dict:
    return {"proposal_id": "X", "patch_type": "not_a_real_patch_type"}


def test_empty_ptype_emits_new_reason_when_flag_on(monkeypatch):
    monkeypatch.setenv("GSO_NARROW_SKIPPED_NO_ORIGINAL_PATCH_TYPE", "1")
    from genie_space_optimizer.optimization.cluster_driven_synthesis import (
        narrow_replacement_diagnosis,
    )
    diag = narrow_replacement_diagnosis(
        original_patch=_patch_empty_ptype(),
        ag_target_qids=("q1",),
        root_cause="missing_filter",
    )
    assert diag["applicable"] is False
    assert diag["reason"] == "narrow_skipped_no_original_patch_type"
    assert diag["original_patch_type"] == ""


def test_empty_ptype_preserves_legacy_reason_when_flag_off(monkeypatch):
    monkeypatch.setenv("GSO_NARROW_SKIPPED_NO_ORIGINAL_PATCH_TYPE", "0")
    from genie_space_optimizer.optimization.cluster_driven_synthesis import (
        narrow_replacement_diagnosis,
    )
    diag = narrow_replacement_diagnosis(
        original_patch=_patch_empty_ptype(),
        ag_target_qids=("q1",),
        root_cause="missing_filter",
    )
    assert diag["reason"] == "unrecognized_patch_type"


def test_nonempty_unknown_ptype_still_unrecognized_when_flag_on(monkeypatch):
    monkeypatch.setenv("GSO_NARROW_SKIPPED_NO_ORIGINAL_PATCH_TYPE", "1")
    from genie_space_optimizer.optimization.cluster_driven_synthesis import (
        narrow_replacement_diagnosis,
    )
    diag = narrow_replacement_diagnosis(
        original_patch=_patch_unknown_ptype(),
        ag_target_qids=("q1",),
        root_cause="missing_filter",
    )
    assert diag["reason"] == "unrecognized_patch_type"


def test_harness_emits_new_record_for_empty_ptype(monkeypatch):
    """When diag.reason is narrow_skipped_no_original_patch_type the
    harness must emit the typed record + marker rather than
    narrow_not_applicable.
    """
    monkeypatch.setenv("GSO_NARROW_SKIPPED_NO_ORIGINAL_PATCH_TYPE", "1")
    monkeypatch.setenv("GSO_L6_NARROW_REPLACEMENT_PATCH_AWARE", "1")
    from genie_space_optimizer.optimization.harness import (
        _emit_narrow_replacement_diagnostic_for_test,
    )
    iter_inputs = {"decision_records": [], "markers": []}
    _emit_narrow_replacement_diagnostic_for_test(
        diag={
            "applicable": False,
            "reason": "narrow_skipped_no_original_patch_type",
            "original_patch_type": "",
        },
        run_id="r1",
        iteration=2,
        ag_id="AG_X",
        cluster_id="H004",
        ag_root_cause="missing_filter",
        iter_inputs=iter_inputs,
    )
    rcs = [r["reason_code"] for r in iter_inputs["decision_records"]]
    assert "narrow_skipped_no_original_patch_type" in rcs
    assert "narrow_not_applicable" not in rcs
    assert any(
        m.startswith("GSO_NARROW_SKIPPED_NO_ORIGINAL_PATCH_TYPE_V1 ")
        for m in iter_inputs["markers"]
    )
