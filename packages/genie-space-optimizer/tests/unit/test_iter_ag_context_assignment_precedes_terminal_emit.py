"""Phase 1 (2026-05-16) — structural guard that the AG-context
capture runs BEFORE the first terminal-emit predicate.

Source-inspection style mirrors
``tests/unit/test_phase_h_acceptance_dict_carries_accepted_field.py``.
"""

from __future__ import annotations

from pathlib import Path


HARNESS_PATH = (
    Path(__file__).parent.parent.parent
    / "src"
    / "genie_space_optimizer"
    / "optimization"
    / "harness.py"
)


def test_capture_iter_ag_context_call_precedes_per_ag_terminal_emits():
    """The capture must run BEFORE every per-AG terminal emit (the
    Bug 4 regression class). Pre-strategist early-exit branches that
    emit ``_iter_terminal_emitted = True`` before the AG loop opens
    don't have an AG to capture from and are out-of-scope for this
    guard — they leave the iter-locals at their iter-top defaults.
    """
    src = HARNESS_PATH.read_text()
    lines = src.splitlines()

    capture_line = next(
        (i for i, line in enumerate(lines, start=1)
         if "capture_iter_ag_context(" in line),
        None,
    )
    assert capture_line is not None, (
        "Phase 1 Task 2 wiring missing — no call to "
        "capture_iter_ag_context() found in harness.py."
    )

    # Find every `_iter_terminal_emitted = True` line BELOW the capture
    # call. The capture lives just after `ag_id = ag.get(...)` so any
    # per-AG terminal emit reachable after an AG is in scope is also
    # below the capture. Emits ABOVE the capture line are out-of-scope
    # (pre-strategist exit paths that have no AG to capture from —
    # e.g., the reserved-recovery branch at ~18666 and the
    # `no_action_group_emitted` branch at ~21856).
    per_ag_emit_lines = [
        i for i, line in enumerate(lines, start=1)
        if "_iter_terminal_emitted = True" in line
        and i > capture_line
    ]
    assert per_ag_emit_lines, (
        "no per-AG terminal-emit sites found below capture_iter_ag_context()"
    )
    for emit_line in per_ag_emit_lines:
        assert capture_line < emit_line, (
            f"Bug 4 regression — capture_iter_ag_context() at "
            f"harness.py:{capture_line} must precede every per-AG "
            f"`_iter_terminal_emitted = True` (found one at "
            f"harness.py:{emit_line})."
        )


def test_capture_writes_all_five_iter_locals_in_harness():
    """The harness call site must unpack the helper's result into ALL
    five ``_iter_*_for_ledger`` locals."""
    src = HARNESS_PATH.read_text()

    idx = src.find("capture_iter_ag_context(")
    assert idx != -1, "capture_iter_ag_context() call not found"
    window = src[idx : idx + 800]

    for local in (
        "_iter_ag_id_for_ledger",
        "_iter_cluster_ids_for_ledger",
        "_iter_target_qids_for_ledger",
        "_iter_levers_for_ledger",
        "_iter_root_cause_for_ledger",
    ):
        assert local in window, (
            f"Phase 1 Task 2 wiring incomplete — `{local}` is not "
            f"assigned within 800 chars of the capture_iter_ag_context() "
            f"call."
        )
