"""Cycle 14-C T6 — strategist context includes
unresolved_target_debt_qids slot when the flag is on.

Default-on behaviour flag (flipped 2026-05-10 for the combined
C14-W + C14-C pre-pilot test): with flag explicitly off, the slot
is fully absent from the context dict so the strategist prompt is
byte-identical to pre-14-C."""
from __future__ import annotations

import os
from unittest import mock


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


def test_slot_absent_when_flag_explicitly_off(monkeypatch) -> None:
    """Byte-stability with flag explicitly off
    (``GSO_UNRESOLVED_TARGET_DEBT_STRATEGIST=0``): the new slot is
    not even a key in the dict, so JSON serialisation is identical
    to pre-14-C output. After the 2026-05-10 default-on flip this
    is the explicit-override path for replay byte-stability."""
    monkeypatch.setenv("GSO_UNRESOLVED_TARGET_DEBT_STRATEGIST", "0")
    from genie_space_optimizer.optimization.optimizer import _build_context_data
    metadata = {"_unresolved_target_debt_qids": ["gs_024"]}
    ctx = _build_context_data(**_minimal_context_inputs(metadata))
    assert "unresolved_target_debt_qids" not in ctx


def test_default_on_after_2026_05_10_flip() -> None:
    """Default flipped ON in 2026-05-10 for combined C14-W + C14-C
    pre-pilot test. With no env var set, the flag returns True."""
    with mock.patch.dict(os.environ, {}, clear=True):
        from genie_space_optimizer.common.config import (
            unresolved_target_debt_strategist_enabled,
        )
        assert unresolved_target_debt_strategist_enabled() is True


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
