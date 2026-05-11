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
    "p0_gate",                # Phase C: flag-on branch
    "p0_gate",                # Phase C: legacy branch
    "_rco4b_asi_out",         # Phase D: flag-on branch (gate_name=_rco4b_asi_out.gate_name)
    "_asi_audit_1",           # Phase D: legacy branch (or "asi_extraction" fallback)
    "baseline_drift_diagnostic",  # Phase D: flag-on branch
    "baseline_drift_diagnostic",  # Phase D: legacy branch
    "full_eval_acceptance",   # Phase E: verdict-emit flag-on
    "full_eval_acceptance",   # Phase E: verdict-emit legacy
    "pre_arbiter_regression_guardrail",
    "full_eval_acceptance",   # Phase E: rollback-emit flag-on
    "full_eval_acceptance",   # Phase E: rollback-emit legacy
    "full_eval_acceptance",   # Phase E: accept-emit flag-on
    "full_eval_acceptance",   # Phase E: accept-emit legacy
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


# ---------------------------------------------------------------------------
# RCO-4b Phase B — slice_gate position assertions
# ---------------------------------------------------------------------------


def test_slice_gate_audit_emit_appears_exactly_once_in_rollback_path() -> None:
    """RCO-4b Phase B — Phase B must NOT duplicate the slice_gate audit
    row. Even though the wiring has two flag branches (flag-on / legacy
    else), only the rollback path emits, and there is exactly one
    rollback path. The literal ``gate_name="slice_gate"`` must appear
    exactly once in harness.py.
    """
    import pathlib
    src = pathlib.Path(
        "src/genie_space_optimizer/optimization/harness.py"
    ).read_text(encoding="utf-8")
    assert src.count('gate_name="slice_gate"') == 1


def test_slice_gate_audit_emit_position_unchanged_relative_to_propagation_wait() -> None:
    """Phase A pinned ``gate_name="propagation_wait"`` to fire
    BEFORE ``gate_name="slice_gate"``. Phase B must not reorder."""
    import pathlib
    src = pathlib.Path(
        "src/genie_space_optimizer/optimization/harness.py"
    ).read_text(encoding="utf-8")
    prop_idx = src.find('gate_name="propagation_wait"')
    slice_idx = src.find('gate_name="slice_gate"')
    if prop_idx == -1 or slice_idx == -1:
        # If propagation_wait's audit was inlined into the helper rather
        # than the harness, this assertion has no anchor — skip rather
        # than fail. The base sequence-guard catches that case.
        import pytest
        pytest.skip("propagation_wait or slice_gate audit not found in harness")
    assert prop_idx < slice_idx, (
        "Phase B must preserve the propagation_wait -> slice_gate ordering"
    )


# ---------------------------------------------------------------------------
# RCO-4b Phase C — p0_gate position assertions
# ---------------------------------------------------------------------------


def test_p0_gate_audit_emit_appears_twice_one_per_flag_branch() -> None:
    """RCO-4b Phase C — the P0-gate wiring duplicates the audit call
    (one per flag branch) because the legacy reason_detail format
    constructs ``f"p0_gate: {N} failures"`` inline while the
    helper-on path reads the same string from the typed outcome.
    Unifying the two would require a 3-arg helper that's worse than
    the duplication.

    Only one branch fires per run; the parity test verifies they
    produce identical audit rows for the same input.
    """
    import pathlib
    src = pathlib.Path(
        "src/genie_space_optimizer/optimization/harness.py"
    ).read_text(encoding="utf-8")
    assert src.count('gate_name="p0_gate"') == 2


def test_p0_gate_audit_emit_position_unchanged_relative_to_slice_gate() -> None:
    """Phase B pinned ``gate_name="slice_gate"`` to fire BEFORE
    ``gate_name="p0_gate"``. Phase C must not reorder.

    Both p0_gate audit positions must be after the slice_gate audit
    position.
    """
    import pathlib
    src = pathlib.Path(
        "src/genie_space_optimizer/optimization/harness.py"
    ).read_text(encoding="utf-8")
    slice_idx = src.find('gate_name="slice_gate"')
    first_p0_idx = src.find('gate_name="p0_gate"')
    last_p0_idx = src.rfind('gate_name="p0_gate"')
    if slice_idx == -1 or first_p0_idx == -1:
        import pytest
        pytest.skip("slice_gate or p0_gate audit not found in harness")
    assert slice_idx < first_p0_idx, (
        "Phase C must preserve the slice_gate -> p0_gate ordering"
    )
    assert slice_idx < last_p0_idx, (
        "Both p0_gate audit positions must follow the slice_gate audit"
    )


# ---------------------------------------------------------------------------
# RCO-4b Phase D — asi_extraction + baseline_drift_diagnostic assertions
# ---------------------------------------------------------------------------


def test_asi_extraction_wiring_present_in_both_branches() -> None:
    """RCO-4b Phase D — the ASI forwarder wiring has two branches.
    The legacy branch constructs ``gate_name=... or "asi_extraction"``
    inline; the helper-on branch uses ``gate_name=_rco4b_asi_out.gate_name``
    (the default `"asi_extraction"` is embedded in the typed outcome's
    dataclass field default, not the harness source). The literal
    ``"asi_extraction"`` appears once in harness.py (legacy branch).
    Both branches must reference the wiring sentinels.
    """
    import pathlib
    src = pathlib.Path(
        "src/genie_space_optimizer/optimization/harness.py"
    ).read_text(encoding="utf-8")
    # Legacy branch sentinel: the `or "asi_extraction"` fallback string.
    assert '"asi_extraction"' in src
    # Helper branch sentinel: the typed-outcome reference.
    assert "_rco4b_asi_out.gate_name" in src
    # Both branches forward through the helper or legacy mechanism.
    assert "forward_asi_extraction_audit" in src
    assert "_asi_audit_1" in src


def test_baseline_drift_audit_appears_twice_one_per_flag_branch() -> None:
    """RCO-4b Phase D — same pattern for baseline-drift."""
    import pathlib
    src = pathlib.Path(
        "src/genie_space_optimizer/optimization/harness.py"
    ).read_text(encoding="utf-8")
    assert src.count('gate_name="baseline_drift_diagnostic"') == 2


def test_phase_d_audits_fire_after_phase_c_p0_gate() -> None:
    """Phase D audits (asi_extraction and baseline_drift_diagnostic)
    must fire AFTER the p0_gate audit pinned in Phase C, preserving
    the conceptual stage order from Phase A's inventory."""
    import pathlib
    src = pathlib.Path(
        "src/genie_space_optimizer/optimization/harness.py"
    ).read_text(encoding="utf-8")
    p0_idx = src.find('gate_name="p0_gate"')
    asi_idx = src.find('"asi_extraction"')
    drift_idx = src.find('gate_name="baseline_drift_diagnostic"')
    if p0_idx == -1 or asi_idx == -1 or drift_idx == -1:
        import pytest
        pytest.skip("one of the expected audit gate_names is missing")
    assert p0_idx < asi_idx, (
        "asi_extraction must fire after p0_gate (Phase A inventory order)"
    )
    assert asi_idx < drift_idx, (
        "baseline_drift_diagnostic must fire after asi_extraction"
    )


def test_baseline_drift_audit_fires_after_asi_extraction_in_both_branches() -> None:
    """Pin the relative order: every asi_extraction audit precedes
    every baseline_drift_diagnostic audit. With both audits appearing
    twice, the test asserts the LAST asi_extraction comes before the
    FIRST baseline_drift_diagnostic."""
    import pathlib
    src = pathlib.Path(
        "src/genie_space_optimizer/optimization/harness.py"
    ).read_text(encoding="utf-8")
    last_asi = src.rfind('"asi_extraction"')
    first_drift = src.find('gate_name="baseline_drift_diagnostic"')
    if last_asi == -1 or first_drift == -1:
        import pytest
        pytest.skip("one of the expected audit gate_names is missing")
    assert last_asi < first_drift, (
        "Phase D must preserve the asi_extraction -> baseline_drift_diagnostic "
        "ordering across both flag branches"
    )


# ---------------------------------------------------------------------------
# RCO-4b Phase E — full_eval_acceptance position assertions
# ---------------------------------------------------------------------------


def test_full_eval_acceptance_audits_appear_six_times() -> None:
    """RCO-4b Phase E — three audit-emission sites (verdict, rollback,
    accept), each duplicated across the helper-on and legacy branches.
    Only one verdict + one branch-specific audit fires per iteration;
    the parity test verifies identical behavior."""
    import pathlib
    src = pathlib.Path(
        "src/genie_space_optimizer/optimization/harness.py"
    ).read_text(encoding="utf-8")
    assert src.count('gate_name="full_eval_acceptance"') == 6


def test_full_eval_acceptance_audits_fire_after_baseline_drift() -> None:
    """RCO-4b Phase E — the full-eval-acceptance audits must follow
    the baseline_drift_diagnostic audit pinned in Phase D, preserving
    the conceptual stage order."""
    import pathlib
    src = pathlib.Path(
        "src/genie_space_optimizer/optimization/harness.py"
    ).read_text(encoding="utf-8")
    drift_idx = src.find('gate_name="baseline_drift_diagnostic"')
    first_full_eval = src.find('gate_name="full_eval_acceptance"')
    if drift_idx == -1 or first_full_eval == -1:
        import pytest
        pytest.skip("expected audit gate_names missing")
    assert drift_idx < first_full_eval, (
        "Phase E must preserve the baseline_drift_diagnostic -> "
        "full_eval_acceptance ordering"
    )


def test_full_eval_verdict_audit_fires_before_rollback_and_accept() -> None:
    """RCO-4b Phase E — the three audit-emission sites must appear in
    source order: verdict-emit → rollback-emit → accept-emit. Each
    site has 2 occurrences (flag-on + legacy) of
    ``gate_name="full_eval_acceptance"``. The 6 occurrences cluster
    into 3 site-clusters; cluster 1 = verdict, cluster 2 = rollback,
    cluster 3 = accept.

    Note: ``decision="rolled_back"`` and ``decision="accepted"`` are
    NOT unique to full_eval (slice_gate / p0_gate use the same
    decision literals), so this test anchors on the
    ``gate_name="full_eval_acceptance"`` site clusters directly.
    """
    import pathlib
    src = pathlib.Path(
        "src/genie_space_optimizer/optimization/harness.py"
    ).read_text(encoding="utf-8")
    positions: list[int] = []
    start = 0
    needle = 'gate_name="full_eval_acceptance"'
    while True:
        idx = src.find(needle, start)
        if idx == -1:
            break
        positions.append(idx)
        start = idx + 1
    if len(positions) != 6:
        import pytest
        pytest.skip(f"expected 6 full_eval_acceptance audits, got {len(positions)}")
    # Three clusters of 2: positions[0:2] = verdict, [2:4] = rollback, [4:6] = accept.
    verdict_first, verdict_last = positions[0], positions[1]
    rollback_first, rollback_last = positions[2], positions[3]
    accept_first, accept_last = positions[4], positions[5]
    assert verdict_last < rollback_first, (
        "verdict-emit site must precede rollback-emit site in source order"
    )
    assert rollback_last < accept_first, (
        "rollback-emit site must precede accept-emit site in source order "
        "(rollback branch returns early; accept-emit is unreachable "
        "from inside the rollback if-block)"
    )


def test_full_eval_acceptance_audits_appear_at_three_distinct_positions() -> None:
    """RCO-4b Phase E — beyond the count check, assert the three SITES
    are at distinct positions (not all clustered in one place). Each
    of the 6 occurrences should map to one of three distinct line
    ranges roughly 1000+ chars apart."""
    import pathlib
    src = pathlib.Path(
        "src/genie_space_optimizer/optimization/harness.py"
    ).read_text(encoding="utf-8")
    positions: list[int] = []
    start = 0
    needle = 'gate_name="full_eval_acceptance"'
    while True:
        idx = src.find(needle, start)
        if idx == -1:
            break
        positions.append(idx)
        start = idx + 1
    assert len(positions) == 6
    min_cluster_gap = 1000
    clusters: list[list[int]] = [[positions[0]]]
    for p in positions[1:]:
        if p - clusters[-1][-1] < min_cluster_gap:
            clusters[-1].append(p)
        else:
            clusters.append([p])
    assert len(clusters) == 3, (
        f"Expected 3 distinct audit sites; found {len(clusters)} "
        f"clusters at positions {positions}"
    )
    for c in clusters:
        assert len(c) == 2, (
            f"Each site should have 2 occurrences (helper-on + legacy); "
            f"cluster {c} has {len(c)}"
        )
