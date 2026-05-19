"""P-E2 — AG-selection-blocks negative control.

The iter-level guard at ``harness.py:19752`` short-circuits forbidden
AGs *before* the sub-AG proposal generators run. This test pins that
when the guard fires, the sub-AG generators never enter, and the
P-E2 observe-only check therefore emits zero records — even when
the AG's identity would clearly match the forbidden set at the
sub-AG site.

We exercise this at the helper level: ``_check_proposal_stage_
forbidden_ag_leakage`` is the only Python surface that can emit an
observe-only record. The harness wiring (Tasks 6 + 7) places this
helper *inside* the per-AG body (lexically after the guard); if the
guard fires, control never reaches the helper. The negative control
below confirms the helper is the sole emission surface by checking
the per-AG loop's behaviour structurally.
"""
from __future__ import annotations


def test_helper_only_emission_surface_for_observe_records():
    """Pin the single-emission-surface property: searching the
    harness module for the record factory name should yield exactly
    one production call site (inside _check_proposal_stage_
    forbidden_ag_leakage). Any second call site is a wiring bug —
    only the helper is allowed to emit observe-only records.
    """
    import pathlib
    harness_path = pathlib.Path(
        "src/genie_space_optimizer/optimization/harness.py"
    )
    if not harness_path.exists():
        # Repo root may differ in CI; try the absolute-from-package path.
        harness_path = (
            pathlib.Path(__file__).resolve().parents[2]
            / "src" / "genie_space_optimizer" / "optimization" / "harness.py"
        )
    src = harness_path.read_text()
    n_calls = src.count("proposal_stage_forbidden_ag_observed_record(")
    # Exactly one production call: inside _check_proposal_stage_
    # forbidden_ag_leakage.
    assert n_calls == 1, (
        f"Expected exactly one production call site for "
        f"proposal_stage_forbidden_ag_observed_record(...), found {n_calls}. "
        "Only the helper _check_proposal_stage_forbidden_ag_leakage "
        "is allowed to emit observe-only records."
    )


def test_iter_level_guard_short_circuits_before_sub_ag_helper():
    """Pin the lexical ordering in harness.py: the iter-level guard
    (``_collision_pair_matches`` at line ~19752) appears before the
    cluster-driven-synthesis call site (~20342) and the force-L6
    call site (~20546). Any rearrangement that swaps this order
    would let forbidden AGs reach sub-AG generators unconditionally."""
    import pathlib
    harness_path = pathlib.Path(
        "src/genie_space_optimizer/optimization/harness.py"
    )
    if not harness_path.exists():
        harness_path = (
            pathlib.Path(__file__).resolve().parents[2]
            / "src" / "genie_space_optimizer" / "optimization" / "harness.py"
        )
    src = harness_path.read_text().splitlines()
    # Find the iter-level guard line.
    guard_line = next(
        (i for i, ln in enumerate(src)
         if "_collision_pair_matches(_collision_pair" in ln),
        None,
    )
    # P-E2 refactor: the sub-AG cluster-driven synthesis call site is
    # now ``dispatch_forced_structural_synthesis(`` at harness.py
    # ~line 23752 (the harness run_lever_loop assigns the result to
    # ``_dispatch_result``). The same symbol also appears at ~13081
    # inside the ``_maybe_dispatch_forced_structural_synthesis``
    # helper definition; we filter on the assignment prefix to pin
    # the in-loop call site rather than the helper internals.
    cds_line = next(
        (i for i, ln in enumerate(src)
         if "= dispatch_forced_structural_synthesis(" in ln),
        None,
    )
    force_l6_line = next(
        (i for i, ln in enumerate(src)
         if "_maybe_force_lever6_with_cache(" in ln
         and "def " not in ln),
        None,
    )
    assert guard_line is not None, "iter-level guard not found"
    assert cds_line is not None, "cluster-driven synthesis call not found"
    assert force_l6_line is not None, (
        "_maybe_force_lever6_with_cache call site not found"
    )
    assert guard_line < cds_line, (
        f"iter-level guard at line {guard_line + 1} must come before "
        f"cluster-driven synthesis at line {cds_line + 1}"
    )
    assert guard_line < force_l6_line, (
        f"iter-level guard at line {guard_line + 1} must come before "
        f"force-L6 call at line {force_l6_line + 1}"
    )


def test_helper_returns_none_when_guard_would_block(monkeypatch):
    """Behavioural pin: even if a caller invokes the helper directly
    with a matching pair, the helper returns ``None`` and emits
    nothing when the flag is off. This is the byte-stability guarantee
    for fixtures captured under the legacy (pre-P-E2) regime.
    """
    monkeypatch.setenv("GSO_PROPOSAL_STAGE_FORBIDDEN_AG_OBSERVED", "0")
    from genie_space_optimizer.optimization.harness import (
        _ag_collision_key_pair,
        _ForbiddenSetPair,
        _check_proposal_stage_forbidden_ag_leakage,
        _normalise_blame,
    )
    ag = {"id": "AG_X", "source_cluster_signatures": ["sig_A"]}
    pair = _ag_collision_key_pair(
        ag=ag, ag_root_cause="missing_filter",
        ag_blame_set=["t.col"], lever_keys=["5", "6"],
    )
    forbidden = _ForbiddenSetPair(
        by_root_cause=frozenset({(
            "missing_filter", _normalise_blame(["t.col"]),
            frozenset([5, 6]),
        )}),
        by_signature=frozenset(),
    )
    iter_inputs = {"decision_records": [], "markers": []}
    axis = _check_proposal_stage_forbidden_ag_leakage(
        run_id="r1", iteration=2, ag_id="AG_X",
        cluster_id="H004", root_cause="missing_filter",
        collision_pair=pair, forbidden_pair=forbidden,
        cluster_signature="sig_A", lever_set=(5, 6),
        call_site="cluster_driven_synthesis",
        iter_inputs=iter_inputs,
    )
    assert axis is None
    assert iter_inputs["decision_records"] == []
    assert iter_inputs["markers"] == []
