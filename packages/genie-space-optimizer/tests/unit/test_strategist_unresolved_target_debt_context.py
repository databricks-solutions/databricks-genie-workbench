"""Cycle 14-C T6 — strategist context includes
unresolved_target_debt_qids slot when the flag is on.

Default-off behaviour flag: with flag off, the slot is fully
absent from the context dict so the strategist prompt is byte-
identical to pre-14-C."""
from __future__ import annotations


def _minimal_context_inputs(metadata_snapshot: dict) -> dict:
    """Minimal kwargs to call _build_context_data successfully."""
    return {
        "clusters": [],
        "soft_signal_clusters": [],
        "metadata_snapshot": metadata_snapshot,
        "reflection_buffer": [],
        "priority_ranking": [],
        "blame_set": None,
        "success_summary": "",
        "reflection_text": "",
        "persistence_text": "",
        "proven_patterns_text": "",
        "suggestions_text": "",
        "iq_scan_text": "",
        "rca_theme_context": "",
    }


def test_slot_present_when_flag_on_and_debt_populated(monkeypatch) -> None:
    monkeypatch.setenv("GSO_UNRESOLVED_TARGET_DEBT_STRATEGIST", "1")
    from genie_space_optimizer.optimization.optimizer import _build_context_data
    metadata = {"_unresolved_target_debt_qids": ["gs_024"]}
    ctx = _build_context_data(**_minimal_context_inputs(metadata))
    assert ctx.get("unresolved_target_debt_qids") == ["gs_024"]


def test_slot_absent_when_flag_off_even_if_debt_populated(monkeypatch) -> None:
    """Byte-stability with flag off: the new slot is not even a
    key in the dict, so JSON serialisation is identical to
    pre-14-C output."""
    monkeypatch.setenv("GSO_UNRESOLVED_TARGET_DEBT_STRATEGIST", "0")
    from genie_space_optimizer.optimization.optimizer import _build_context_data
    metadata = {"_unresolved_target_debt_qids": ["gs_024"]}
    ctx = _build_context_data(**_minimal_context_inputs(metadata))
    assert "unresolved_target_debt_qids" not in ctx


def test_slot_absent_when_flag_on_but_no_debt(monkeypatch) -> None:
    """When the prior iteration had no unresolved debt, the slot
    is absent (not `null`/`[]`) so the strategist's prompt is not
    polluted with empty signals."""
    monkeypatch.setenv("GSO_UNRESOLVED_TARGET_DEBT_STRATEGIST", "1")
    from genie_space_optimizer.optimization.optimizer import _build_context_data
    metadata: dict = {}
    ctx = _build_context_data(**_minimal_context_inputs(metadata))
    assert "unresolved_target_debt_qids" not in ctx


def test_slot_absent_when_flag_on_and_debt_empty_list(monkeypatch) -> None:
    """Empty list also suppressed to keep the prompt clean."""
    monkeypatch.setenv("GSO_UNRESOLVED_TARGET_DEBT_STRATEGIST", "1")
    from genie_space_optimizer.optimization.optimizer import _build_context_data
    metadata = {"_unresolved_target_debt_qids": []}
    ctx = _build_context_data(**_minimal_context_inputs(metadata))
    assert "unresolved_target_debt_qids" not in ctx
