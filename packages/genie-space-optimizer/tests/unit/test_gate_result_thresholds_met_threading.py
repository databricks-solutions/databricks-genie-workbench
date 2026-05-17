"""Phase 1 — Acceptance Unification Task 6.

Source-inspection guards for ``thresholds_met`` threading from
``_run_gate_checks`` through the lever-loop into ``AcceptanceInput``.

Before Task 6, the harness built ``AcceptanceInput`` at the
lever-loop's full-eval site without forwarding ``thresholds_met`` —
the field defaulted to ``True`` regardless of the actual
``GSO_TARGET_AWARE_ACCEPTANCE`` check inside ``_run_gate_checks``.
That broke the control-plane gate's attribution-drift rejection
branch on iterations where the candidate accuracy failed the overall
bar (95.0%): the gate's canonical decision saw the false flag, but
the harness's downstream ``stages.acceptance`` recompute saw the
default and could disagree.

Task 6 (this test, post-RED):
1. Both ``_run_gate_checks`` returns must carry a ``thresholds_met``
   key set to ``_gate_thresholds_met`` (computed at ~line 16430).
2. The lever-loop must extract that value into ``_iter_thresholds_met``
   after the gate call.
3. The lever-loop must pass ``thresholds_met=_iter_thresholds_met``
   into the ``AcceptanceInput`` constructor at ~line 29044.
"""

from __future__ import annotations

import re
from pathlib import Path


HARNESS_PATH = (
    Path(__file__).parent.parent.parent
    / "src"
    / "genie_space_optimizer"
    / "optimization"
    / "harness.py"
)


def test_both_gate_returns_carry_thresholds_met_key():
    """Both fork paths of ``_run_gate_checks`` must include
    ``"thresholds_met": _gate_thresholds_met`` in the return dict."""
    src = HARNESS_PATH.read_text()
    matches = [
        i for i, line in enumerate(src.splitlines(), start=1)
        if '"thresholds_met": _gate_thresholds_met' in line
    ]
    assert len(matches) >= 2, (
        f"Phase 1 Task 6 wiring incomplete — expected >=2 returns of "
        f"`\"thresholds_met\": _gate_thresholds_met` (one per fork "
        f"path of _run_gate_checks); found {len(matches)} at "
        f"harness.py:{matches}."
    )


def test_lever_loop_extracts_iter_thresholds_met_from_gate_result():
    """The lever-loop must read ``thresholds_met`` off the gate dict
    into a local named ``_iter_thresholds_met``."""
    src = HARNESS_PATH.read_text()
    pattern = re.compile(
        r"_iter_thresholds_met\s*=\s*bool\(\s*"
        r"\(?\s*gate_result\b.*?\.get\(\s*\"thresholds_met\"",
        re.DOTALL,
    )
    assert pattern.search(src), (
        "Phase 1 Task 6 wiring incomplete — `_iter_thresholds_met = "
        "bool((gate_result or {}).get(\"thresholds_met\", ...))` not "
        "found in harness.py."
    )


def test_acceptance_input_receives_iter_thresholds_met():
    """``AcceptanceInput(...)`` construction must pass
    ``thresholds_met=_iter_thresholds_met``."""
    src = HARNESS_PATH.read_text()

    accept_inp_idx = src.find("AcceptanceInput(")
    assert accept_inp_idx != -1, "AcceptanceInput(...) call not found"
    # Find the matching closing paren — pragmatic: scan a 4 KB window.
    window = src[accept_inp_idx : accept_inp_idx + 4000]
    assert "thresholds_met=_iter_thresholds_met" in window, (
        "Phase 1 Task 6 wiring incomplete — AcceptanceInput(...) "
        "construction does not pass thresholds_met=_iter_thresholds_met. "
        "Without this, the field defaults to True and the control-plane "
        "gate's attribution-drift branch silently disables itself."
    )
