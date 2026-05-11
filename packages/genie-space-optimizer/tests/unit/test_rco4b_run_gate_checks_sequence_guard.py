"""RCO-4b Phase A Task 10 — grep-guard pinning the production firing
order of ``gate_name=`` sentinels inside ``_run_gate_checks``.

Mirrors the RCO-4 sequencing-guard pattern at
``tests/unit/test_rco4_sequencing_grep_guard.py``. The guard exists
because ``_run_gate_checks`` cannot be cleanly instrumented (it has
side effects, mlflow lifecycle calls, and three early-return paths).
A textual scan of the function body is the most reliable way to
detect order changes until the function is fully decomposed into
pure helpers (Phases B-E).

When Phases B-E land and the function shrinks below ~200 lines, this
guard can be retired and replaced with a direct instrumentation test.
"""

from __future__ import annotations

import inspect
import re

from genie_space_optimizer.optimization import harness


# Production firing order of gate_name= sentinels INSIDE _run_gate_checks,
# as of RCO-4b Phase A landing. Update this list (and the inventory doc
# at docs/2026-05-12-rco-4b-gate-stage-inventory.md) if the order
# legitimately changes; do NOT relax the guard.
#
# Position 5 captures the bare identifier ``_asi_audit_1`` rather than
# the literal fallback ``"asi_extraction"``. The source line is:
#     gate_name=_asi_audit_1.get("gate_name") or "asi_extraction"
# The regex anchors on ``gate_name=`` and captures the identifier
# directly following the equals sign. The semantic gate name is still
# ``asi_extraction``; the wire identifier captured here is the variable
# that supplies it at runtime.
# Note: the propagation_wait block currently appears FOUR times in the
# source — two in the flag-on (RCO-4b pure-helper) branch and two in
# the legacy else branch. Once every Phase B-E extraction lands and the
# legacy else branches are removed, the order will collapse to a single
# pair per gate; this guard list is the canary that fires the day the
# collapse happens (intentionally, prompting an update here in the
# same commit).
_EXPECTED_ORDER = [
    "propagation_wait",       # flag-on: confirmed-fast emission
    "propagation_wait",       # flag-on: full-budget emission
    "propagation_wait",       # legacy: confirmed-fast emission
    "propagation_wait",       # legacy: full-budget emission
    "slice_gate",
    "p0_gate",
    "_asi_audit_1",           # asi_extraction (via ``or "asi_extraction"`` fallback)
    "baseline_drift_diagnostic",
    "full_eval_acceptance",   # first emission
    "pre_arbiter_regression_guardrail",
    "full_eval_acceptance",   # second emission
    "full_eval_acceptance",   # third emission
]


def _all_gate_name_occurrences(text: str) -> list[str]:
    pattern = re.compile(r'gate_name\s*=\s*[\'"]?([A-Za-z0-9_]+)[\'"]?')
    return [m.group(1) for m in pattern.finditer(text)]


def test_run_gate_checks_gate_name_firing_order() -> None:
    src = inspect.getsource(harness._run_gate_checks)
    occurrences = _all_gate_name_occurrences(src)
    # Strip out the audit-emit helper signature's ``gate_name: str``
    # parameter declaration (which is not a firing site).
    occurrences = [
        o for o in occurrences if o not in {"str", "None"}
    ]
    assert occurrences == _EXPECTED_ORDER, (
        f"_run_gate_checks gate_name firing order changed.\n"
        f"Expected: {_EXPECTED_ORDER}\n"
        f"Observed: {occurrences}\n"
        f"If this change is intentional, update both this list AND "
        f"docs/2026-05-12-rco-4b-gate-stage-inventory.md in the same "
        f"commit."
    )


def test_inventory_doc_references_match_guard_list() -> None:
    """Belt-and-suspenders: the guard list above is the single source
    of truth for production firing order. The inventory doc must list
    the same gate_name= sentinels in the same order."""
    import pathlib
    doc_path = (
        pathlib.Path(__file__).resolve().parents[2]
        / "docs"
        / "2026-05-12-rco-4b-gate-stage-inventory.md"
    )
    doc = doc_path.read_text()
    # Coarse check: every unique gate_name from the guard list must
    # appear at least once in the doc.
    for name in set(_EXPECTED_ORDER):
        assert name in doc, (
            f"Inventory doc {doc_path} is missing the gate name {name!r}. "
            f"The guard list and the inventory must stay in sync."
        )
