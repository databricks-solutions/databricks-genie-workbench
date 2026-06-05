"""Unit tests for Trial 21 W2 — proposal_slate_compiler.

The compiler is the single decision boundary between Stage 3 raw
``RepairProposals`` and the applier. These tests pin:

* The DropReason → TerminalReason mapping is total over the enum.
* Each of the seven pipeline checks fires the right DropReason when its
  upstream evidence is present.
* The compiler degrades gracefully when upstream evidence is absent
  (proposals survive; no exceptions).
* Empty-slate ``terminal_reason_if_empty`` follows the precedence order.
* The compiler NEVER emits ``TerminalReason.NO_APPLIED_PATCHES`` for
  any DropReason — Trial 21 W2 bright-line condition #4.
* The summary marker carries survivor + drop counts so postmortems can
  group drops.
"""
from __future__ import annotations

from typing import Any
from unittest import mock

import pytest

from genie_space_optimizer.optimization.proposal_slate_compiler import (
    DropReason,
    SlateCompilerContext,
    SlateCompilerResult,
    TypedFeedback,
    compile_slate,
    drop_reason_to_terminal_reason,
)
from genie_space_optimizer.optimization.repair_intent import (
    PatchType,
    RepairShape,
)
from genie_space_optimizer.optimization.repair_proposal_typed import (
    RepairProposal,
)
from genie_space_optimizer.optimization.terminal_reason import TerminalReason


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------


def _make_proposal(
    *,
    intent_id: str = "intent_1",
    patch_type: str = "add_example_sql",
    qids: tuple[str, ...] = ("qid_a",),
    selected_lever: str = "lever-5",
    bundle_id: str = "",
    patch_body: dict[str, Any] | None = None,
    selected_levers: tuple[str, ...] = (),
) -> RepairProposal:
    levers: tuple[str, ...]
    if selected_levers:
        levers = selected_levers
    elif selected_lever:
        levers = (selected_lever,)
    else:
        levers = ()
    return RepairProposal(
        intent_id=intent_id,
        intent_name=f"intent {intent_id}",
        intent_description="",
        repair_shape=RepairShape.OTHER,
        patch_type=PatchType(patch_type),
        rationale="r",
        confidence="medium",
        patch_body=patch_body or {},
        blame_set=qids,
        target_qids=qids,
        selected_lever=selected_lever,
        selected_levers=levers,
        bundle_id=bundle_id,
    )


# ---------------------------------------------------------------------
# DropReason → TerminalReason mapping
# ---------------------------------------------------------------------


def test_drop_reason_to_terminal_reason_is_total():
    """Every DropReason must have a TerminalReason mapping."""
    for dr in DropReason:
        tr = drop_reason_to_terminal_reason(dr)
        assert isinstance(tr, TerminalReason)


def test_drop_reason_never_maps_to_no_applied_patches():
    """Trial 21 W2 bright-line #4: no DropReason resolves to
    ``NO_APPLIED_PATCHES``. That catch-all is exactly what the
    compiler replaces."""
    for dr in DropReason:
        assert (
            drop_reason_to_terminal_reason(dr)
            != TerminalReason.NO_APPLIED_PATCHES
        )


# ---------------------------------------------------------------------
# Empty input
# ---------------------------------------------------------------------


def test_empty_input_returns_empty_slate_with_proposal_generation_empty():
    """No proposals in → no proposals out, terminal reason is
    ``PROPOSAL_GENERATION_EMPTY`` (the strategist side handles
    this; the Actuator just labels)."""
    result = compile_slate([], SlateCompilerContext())
    assert result.surviving_proposals == ()
    assert result.dropped_proposals == ()
    assert (
        result.terminal_reason_if_empty
        == TerminalReason.PROPOSAL_GENERATION_EMPTY
    )


# ---------------------------------------------------------------------
# Pass-through (no upstream verdicts wired)
# ---------------------------------------------------------------------


def test_proposal_survives_when_no_upstream_evidence_is_wired():
    """All seven checks no-op when their inputs are absent; the
    proposal must pass through unchanged. This is the "W3-W9 not
    landed yet" baseline."""
    proposal = _make_proposal(patch_type="add_example_sql")
    result = compile_slate([proposal], SlateCompilerContext())
    assert result.surviving_proposals == (proposal,)
    assert result.dropped_proposals == ()
    assert result.terminal_reason_if_empty is None


# ---------------------------------------------------------------------
# Step 1 — prompt budget
# ---------------------------------------------------------------------


def test_step1_prompt_over_cap_drops_with_prompt_split_required():
    proposal = _make_proposal()
    ctx = SlateCompilerContext(
        cluster_id="cluster_x",
        prompt_size_verdict_by_cluster={
            "cluster_x": {"over_cap": True, "sub_cluster_split_needed": False}
        },
    )
    result = compile_slate([proposal], ctx)
    assert result.surviving_proposals == ()
    assert result.dropped_proposals[0][1] == DropReason.PROMPT_SPLIT_REQUIRED
    assert (
        result.terminal_reason_if_empty
        == TerminalReason.PROPOSAL_GENERATION_EMPTY
    )


def test_step1_sub_cluster_split_needed_drops_with_prompt_split_required():
    proposal = _make_proposal()
    ctx = SlateCompilerContext(
        cluster_id="cluster_x",
        prompt_size_verdict_by_cluster={
            "cluster_x": {"over_cap": False, "sub_cluster_split_needed": True}
        },
    )
    result = compile_slate([proposal], ctx)
    assert result.dropped_proposals[0][1] == DropReason.PROMPT_SPLIT_REQUIRED


def test_step1_prompt_budget_satisfied_passes_through():
    proposal = _make_proposal()
    ctx = SlateCompilerContext(
        cluster_id="cluster_x",
        prompt_size_verdict_by_cluster={
            "cluster_x": {"over_cap": False, "sub_cluster_split_needed": False}
        },
    )
    result = compile_slate([proposal], ctx)
    assert result.surviving_proposals == (proposal,)


# ---------------------------------------------------------------------
# Step 2 — metadata target resolution
# ---------------------------------------------------------------------


def test_step2_unresolvable_metadata_target_drops():
    """When the metadata target cannot be resolved against the
    snapshot, the proposal drops with ``UNRESOLVABLE_TARGET``."""
    proposal = _make_proposal(
        patch_type="add_column_description",
        patch_body={
            "type": "add_column_description",
            "table": "main.sales.does_not_exist",
            "column": "ghost",
            "description": "x",
        },
    )
    ctx = SlateCompilerContext(
        metadata_snapshot={
            "data_sources": {
                "tables": [{"identifier": "main.sales.real_table"}],
                "metric_views": [],
            }
        },
    )
    # Stub the applyability check so we don't depend on applier internals.
    with mock.patch(
        "genie_space_optimizer.optimization.patch_applyability."
        "check_patch_applyability",
        side_effect=lambda **kw: type(
            "D", (), {"applyable": False, "reason": "missing_table", "table": ""}
        )(),
    ):
        result = compile_slate([proposal], ctx)
    assert result.dropped_proposals[0][1] == DropReason.UNRESOLVABLE_TARGET


# ---------------------------------------------------------------------
# Step 3 — snippet validator
# ---------------------------------------------------------------------


def test_step3_declined_snippet_drops_with_snippet_invalid():
    proposal = _make_proposal(
        intent_id="snip_1",
        patch_type="add_sql_snippet_filter",
        patch_body={
            "type": "add_sql_snippet_filter",
            "sql_expression": "SELECT * FROM bogus",
        },
    )
    ctx = SlateCompilerContext(
        snippet_validator_verdict_by_proposal_id={
            "snip_1": {"outcome": "declined", "abstain_reason": "snippet_invalid"}
        },
    )
    result = compile_slate([proposal], ctx)
    assert result.dropped_proposals[0][1] == DropReason.SNIPPET_INVALID
    # Empty slate must NOT carry no_applied_patches.
    assert (
        result.terminal_reason_if_empty
        != TerminalReason.NO_APPLIED_PATCHES
    )


# ---------------------------------------------------------------------
# Step 4 — required assets
# ---------------------------------------------------------------------


def test_step4_metadata_patch_with_no_implicated_assets_drops():
    proposal = _make_proposal(
        intent_id="meta_1",
        patch_type="add_column_description",
        patch_body={"type": "add_column_description"},
    )
    ctx = SlateCompilerContext(
        implicated_assets_by_proposal_id={"meta_1": []},
    )
    result = compile_slate([proposal], ctx)
    assert (
        result.dropped_proposals[0][1] == DropReason.MISSING_IMPLICATED_ASSETS
    )


def test_step4_instruction_patch_without_justification_drops():
    proposal = _make_proposal(
        intent_id="inst_1",
        patch_type="add_instruction",
    )
    ctx = SlateCompilerContext(
        justification_by_proposal_id={"inst_1": ""},
    )
    result = compile_slate([proposal], ctx)
    assert (
        result.dropped_proposals[0][1] == DropReason.UNJUSTIFIED_SINGLE_LEVER
    )


def test_step4_instruction_patch_with_justification_passes():
    proposal = _make_proposal(
        intent_id="inst_1",
        patch_type="add_instruction",
    )
    ctx = SlateCompilerContext(
        justification_by_proposal_id={"inst_1": "non-empty justification"},
    )
    result = compile_slate([proposal], ctx)
    assert result.surviving_proposals == (proposal,)


# ---------------------------------------------------------------------
# Step 5 — mechanism coverage
# ---------------------------------------------------------------------


def test_step5_uncovered_mechanism_drops():
    """add_column_description (metadata_description mechanism) against
    a rank/order/top-N behavior_delta is uncovered."""
    proposal = _make_proposal(
        intent_id="m1",
        patch_type="add_column_description",
        qids=("qid_topn",),
        patch_body={"type": "add_column_description", "table": "t", "column": "c"},
    )
    ctx = SlateCompilerContext(
        behavior_delta_by_qid={
            "qid_topn": "result has wrong sort order; top-N collapse mismatch"
        },
    )
    result = compile_slate([proposal], ctx)
    assert result.dropped_proposals[0][1] == DropReason.UNCOVERED_MECHANISM


# ---------------------------------------------------------------------
# Step 6 — mechanism repeat memory
# ---------------------------------------------------------------------


def test_step6_repeated_qid_patch_type_lever_triple_drops():
    """Run B gs_026 scenario distilled: lever-5/add_example_sql repeats
    on the same QID; the Actuator must drop with
    ``REPEATED_FAILED_MECHANISM``."""
    proposal = _make_proposal(
        intent_id="repeat_1",
        patch_type="add_example_sql",
        qids=("gs_026",),
        selected_lever="lever-5",
    )
    ctx = SlateCompilerContext(
        prior_mechanism_attempts=[
            {
                "qid": "gs_026",
                "patch_type": "add_example_sql",
                "selected_lever": "lever-5",
                "behavioral_diff": "unchanged",
                "rca_kind": "top_n_cardinality_collapse",
            }
        ],
    )
    result = compile_slate([proposal], ctx)
    assert (
        result.dropped_proposals[0][1] == DropReason.REPEATED_FAILED_MECHANISM
    )


def test_step6_behavior_delta_fingerprint_distinguishes_paraphrases():
    """W5 — when the prior attempt's RCA-kind+behavior_delta and the
    current proposal's plumbed RCA-kind+behavior_delta hash to
    DIFFERENT fingerprints, the repeat guard does NOT drop. Two
    semantically-different behavior deltas under the same QID +
    patch_type + lever triple are allowed.
    """
    proposal = _make_proposal(
        intent_id="not_repeat",
        patch_type="add_example_sql",
        qids=("qid_a",),
        selected_lever="lever-5",
    )
    ctx = SlateCompilerContext(
        prior_mechanism_attempts=[
            {
                "qid": "qid_a",
                "patch_type": "add_example_sql",
                "selected_lever": "lever-5",
                "rca_kind": "top_n_cardinality_collapse",
                "behavioral_diff": "unchanged",
            }
        ],
        rca_kind_label_by_qid={"qid_a": "filter_predicate_missing"},
        behavior_delta_by_qid={
            "qid_a": "wrong filter predicate was applied"
        },
    )
    result = compile_slate([proposal], ctx)
    # Different fingerprint → proposal survives.
    assert result.surviving_proposals == (proposal,)


def test_step6_behavior_delta_fingerprint_matches_canonical_repeat():
    """W5 — when both sides hash to the same fingerprint, the drop
    fires. Trial 21 fixture replay (gs_026 case) lives in the
    integration suite; this is the deterministic unit-level pin."""
    proposal = _make_proposal(
        intent_id="canonical_repeat",
        patch_type="add_example_sql",
        qids=("qid_a",),
        selected_lever="lever-5",
    )
    ctx = SlateCompilerContext(
        prior_mechanism_attempts=[
            {
                "qid": "qid_a",
                "patch_type": "add_example_sql",
                "selected_lever": "lever-5",
                "rca_kind": "top_n_cardinality_collapse",
                "behavioral_diff": "unchanged",
            }
        ],
        rca_kind_label_by_qid={"qid_a": "top_n_cardinality_collapse"},
        behavior_delta_by_qid={"qid_a": "unchanged"},
    )
    result = compile_slate([proposal], ctx)
    assert (
        result.dropped_proposals[0][1] == DropReason.REPEATED_FAILED_MECHANISM
    )


def test_step6_different_lever_passes():
    proposal = _make_proposal(
        intent_id="ok_1",
        patch_type="add_example_sql",
        qids=("gs_026",),
        selected_lever="lever-3",
    )
    ctx = SlateCompilerContext(
        prior_mechanism_attempts=[
            {
                "qid": "gs_026",
                "patch_type": "add_example_sql",
                "selected_lever": "lever-5",
                "rca_kind": "top_n_cardinality_collapse",
            }
        ],
    )
    result = compile_slate([proposal], ctx)
    assert result.surviving_proposals == (proposal,)


# ---------------------------------------------------------------------
# Trial 22 W2 — group-aware bundle invariants + Phase 1.5 cohesion sweep
#
# These tests mirror the PRODUCTION wire shape: Stage 3 emits bundles
# as N sibling RepairProposals sharing one ``bundle_id``, each with
# exactly one lever in ``selected_levers``. Trial 21's tests pinned a
# single-proposal multi-lever shape that production never emits — that
# fixture/runtime mismatch is what shipped the Trial 21 bug. The
# deleted ``test_step7_bundle_with_single_lever_violates_invariant``
# encoded the WRONG contract; see Trial 22 plan / postmortem for
# details.
# ---------------------------------------------------------------------


def test_w2_bundle_group_with_two_siblings_distinct_levers_both_survive():
    """W2 production-shape — 2 sibling proposals share one bundle_id,
    each with one distinct lever. The lever UNION across the bundle
    is {lever-1, lever-5}; the bundle is valid; BOTH members must
    survive."""
    p1 = _make_proposal(
        intent_id="b1_member_lever1",
        patch_type="add_column_description",
        bundle_id="b1",
        selected_lever="lever-1",
        selected_levers=("lever-1",),
        patch_body={"table": "t", "column": "c", "description": "d"},
    )
    p5 = _make_proposal(
        intent_id="b1_member_lever5",
        patch_type="add_example_sql",
        bundle_id="b1",
        selected_lever="lever-5",
        selected_levers=("lever-5",),
        patch_body={"example_question": "q", "example_sql": "SELECT 1"},
    )
    result = compile_slate([p1, p5], SlateCompilerContext())
    surviving = {p.intent_id for p in result.surviving_proposals}
    assert surviving == {"b1_member_lever1", "b1_member_lever5"}, (
        f"Both bundle siblings must survive. Got {surviving}. "
        f"Drops: {result.dropped_proposals}"
    )


def test_w2_bundle_group_with_two_siblings_same_lever_both_drop(monkeypatch):
    """W2 — 2 sibling proposals share bundle_id, both carry the same
    lever. Union cardinality == 1; bundle violates Trial 20 contract;
    BOTH members drop with BUNDLE_INVARIANT_VIOLATED.

    Trial 23 W9 recomposes same-lever bundles to solo by default; this
    test pins the strict-drop ROLLBACK path
    (``GSO_TRIAL23_BUNDLE_REPAIR=0``)."""
    monkeypatch.setenv("GSO_TRIAL23_BUNDLE_REPAIR", "0")
    p1 = _make_proposal(
        intent_id="b2_member_a",
        patch_type="add_example_sql",
        bundle_id="b2",
        selected_lever="lever-5",
        selected_levers=("lever-5",),
        patch_body={"example_question": "q", "example_sql": "SELECT 1"},
    )
    p2 = _make_proposal(
        intent_id="b2_member_b",
        patch_type="add_example_sql",
        bundle_id="b2",
        selected_lever="lever-5",
        selected_levers=("lever-5",),
        patch_body={"example_question": "q2", "example_sql": "SELECT 2"},
    )
    result = compile_slate([p1, p2], SlateCompilerContext())
    assert result.surviving_proposals == ()
    dropped_reasons = [r for _, r in result.dropped_proposals]
    assert dropped_reasons == [
        DropReason.BUNDLE_INVARIANT_VIOLATED,
        DropReason.BUNDLE_INVARIANT_VIOLATED,
    ]


def test_w2_phase15_cohesion_sweep_cascades_remaining_bundle_member():
    """W2 Phase 1.5 — when Phase 1 drops one bundle sibling (e.g. for
    MISSING_IMPLICATED_ASSETS), the OTHER otherwise-valid sibling is
    cascaded with BUNDLE_MEMBER_DROPPED_CASCADE (NOT
    BUNDLE_INVARIANT_VIOLATED). The cascade feedback names the
    originating sibling and reason."""
    p1 = _make_proposal(
        intent_id="b3_member_lever1_no_assets",
        patch_type="add_column_description",
        bundle_id="b3",
        selected_lever="lever-1",
        selected_levers=("lever-1",),
        patch_body={"table": "t", "column": "c", "description": "d"},
    )
    p5 = _make_proposal(
        intent_id="b3_member_lever5_valid",
        patch_type="add_example_sql",
        bundle_id="b3",
        selected_lever="lever-5",
        selected_levers=("lever-5",),
        patch_body={"example_question": "q", "example_sql": "SELECT 1"},
    )
    ctx = SlateCompilerContext(
        implicated_assets_by_proposal_id={
            "b3_member_lever1_no_assets": [],  # triggers Phase 1 drop
            "b3_member_lever5_valid": ["main.sales.payments"],
        },
        justification_by_proposal_id={
            "b3_member_lever1_no_assets": "",
            "b3_member_lever5_valid": "exemplar for column doc",
        },
    )
    result = compile_slate([p1, p5], ctx)
    drops_by_intent = {p.intent_id: r for p, r in result.dropped_proposals}

    assert drops_by_intent.get("b3_member_lever1_no_assets") == (
        DropReason.MISSING_IMPLICATED_ASSETS
    ), f"setup: {drops_by_intent}"
    assert drops_by_intent.get("b3_member_lever5_valid") == (
        DropReason.BUNDLE_MEMBER_DROPPED_CASCADE
    ), (
        "Phase 1.5 must cascade the valid sibling with "
        f"BUNDLE_MEMBER_DROPPED_CASCADE. Got {drops_by_intent}"
    )
    # Feedback names originator + reason.
    fb_for_cascade = [
        fb
        for fb in result.typed_feedback_for_retry
        if fb.proposal_id == "b3_member_lever5_valid"
    ]
    assert fb_for_cascade and (
        "b3_member_lever1_no_assets" in fb_for_cascade[0].feedback_text
    )
    assert "missing_implicated_assets" in fb_for_cascade[0].feedback_text


def test_w2_one_member_bundle_dissolves_into_solo_survivor():
    """Trial 22 follow-up (live fevm-prashanth finding) — a bundle that
    arrives at the Phase 2 group check reduced to a single member was
    NOT born invalid: its sibling(s) were dropped upstream (producer
    snippet validator, W6 asset gate, etc.), leaving one member that
    already passed every per-proposal Phase 1 check. Dropping it as
    BUNDLE_INVARIANT_VIOLATED reproduces the d139 flatline.

    The compiler must instead DISSOLVE the singleton bundle: clear the
    now-meaningless bundle_id and admit the lone member as a solo
    proposal (functionally identical to an empty-bundle_id proposal,
    which already passes Phase 2 untouched). A
    ``GSO_TRIAL22_BUNDLE_DISSOLVED_V1`` marker records the dissolution
    for postmortems."""
    p = _make_proposal(
        intent_id="b4_solo_in_bundle",
        patch_type="add_example_sql",
        bundle_id="b4",
        selected_lever="lever-5",
        selected_levers=("lever-5",),
        patch_body={"example_question": "q", "example_sql": "SELECT 1"},
    )
    result = compile_slate([p], SlateCompilerContext())
    # The lone member survives — as a SOLO proposal (bundle_id cleared).
    assert len(result.surviving_proposals) == 1, (
        f"singleton bundle must dissolve+survive, not drop. "
        f"Drops: {result.dropped_proposals}"
    )
    survivor = result.surviving_proposals[0]
    assert survivor.intent_id == "b4_solo_in_bundle"
    assert survivor.bundle_id == "", (
        "dissolution must clear bundle_id so the group invariant "
        "never re-fires downstream"
    )
    assert result.dropped_proposals == ()
    # A dissolution marker is emitted naming the original bundle.
    dissolved_markers = [
        m
        for m in result.actuator_markers
        if m.get("marker") == "GSO_TRIAL22_BUNDLE_DISSOLVED_V1"
    ]
    assert len(dissolved_markers) == 1, (
        f"expected one dissolution marker, got {result.actuator_markers}"
    )
    assert dissolved_markers[0].get("bundle_id") == "b4"
    assert dissolved_markers[0].get("proposal_id") == "b4_solo_in_bundle"


def test_w2_empty_bundle_id_proposal_passes_regardless_of_lever_count():
    """W2 — solo proposals (empty bundle_id) are exempt from the
    bundle invariant. A single-lever solo proposal passes Phase 2."""
    p = _make_proposal(
        intent_id="solo_1",
        patch_type="add_example_sql",
        bundle_id="",
        selected_lever="lever-5",
        selected_levers=("lever-5",),
        patch_body={"example_question": "q", "example_sql": "SELECT 1"},
    )
    result = compile_slate([p], SlateCompilerContext())
    assert result.surviving_proposals == (p,)


def test_w2_solo_unaffected_by_cascade_in_other_bundle():
    """W2 — when a bundle is fully cascade-dropped in Phase 1.5, a
    solo proposal with empty bundle_id sitting alongside is
    unaffected (it survives if its per-proposal checks passed)."""
    p1 = _make_proposal(
        intent_id="b5_member_lever1_no_assets",
        patch_type="add_column_description",
        bundle_id="b5",
        selected_lever="lever-1",
        selected_levers=("lever-1",),
        patch_body={"table": "t", "column": "c", "description": "d"},
    )
    p5 = _make_proposal(
        intent_id="b5_member_lever5_valid",
        patch_type="add_example_sql",
        bundle_id="b5",
        selected_lever="lever-5",
        selected_levers=("lever-5",),
        patch_body={"example_question": "q", "example_sql": "SELECT 1"},
    )
    solo = _make_proposal(
        intent_id="solo_alongside_b5",
        patch_type="add_example_sql",
        bundle_id="",
        selected_lever="lever-5",
        selected_levers=("lever-5",),
        patch_body={"example_question": "q3", "example_sql": "SELECT 3"},
    )
    ctx = SlateCompilerContext(
        implicated_assets_by_proposal_id={
            "b5_member_lever1_no_assets": [],
            "b5_member_lever5_valid": ["main.sales.payments"],
            "solo_alongside_b5": ["main.sales.orders"],
        },
        justification_by_proposal_id={
            "b5_member_lever1_no_assets": "",
            "b5_member_lever5_valid": "exemplar",
            "solo_alongside_b5": "exemplar",
        },
    )
    result = compile_slate([p1, p5, solo], ctx)
    surviving = {p.intent_id for p in result.surviving_proposals}
    assert surviving == {"solo_alongside_b5"}, (
        "Solo proposal must survive even when an adjacent bundle is "
        f"fully cascade-dropped. surviving={surviving} "
        f"drops={result.dropped_proposals}"
    )


# ---------------------------------------------------------------------
# Markers + feedback
# ---------------------------------------------------------------------


def test_drop_emits_typed_feedback_and_marker():
    proposal = _make_proposal(
        intent_id="snip_2",
        patch_type="add_sql_snippet_filter",
        patch_body={"type": "add_sql_snippet_filter"},
    )
    ctx = SlateCompilerContext(
        optimization_run_id="run_id_test",
        iteration=2,
        cluster_id="cluster_y",
        snippet_validator_verdict_by_proposal_id={
            "snip_2": {"outcome": "declined"}
        },
    )
    result = compile_slate([proposal], ctx)

    # Typed feedback for retry
    assert len(result.typed_feedback_for_retry) == 1
    fb = result.typed_feedback_for_retry[0]
    assert fb.proposal_id == "snip_2"
    assert fb.drop_reason == "snippet_invalid"
    assert fb.feedback_text  # non-empty

    # Per-proposal marker + summary marker
    per_proposal = [
        m for m in result.actuator_markers if not m.get("is_summary")
    ]
    summary = [m for m in result.actuator_markers if m.get("is_summary")]
    assert len(per_proposal) == 1
    assert per_proposal[0]["drop_reason"] == "snippet_invalid"
    assert per_proposal[0]["failing_check"] == "snippet_validator"
    assert per_proposal[0]["optimization_run_id"] == "run_id_test"
    assert per_proposal[0]["iteration"] == 2
    assert per_proposal[0]["cluster_id"] == "cluster_y"

    assert len(summary) == 1
    assert summary[0]["survivor_count"] == 0
    assert summary[0]["drop_count"] == 1
    assert summary[0]["drop_reason_counts"] == {"snippet_invalid": 1}


def test_summary_marker_is_always_emitted_even_when_all_survive():
    proposal = _make_proposal()
    result = compile_slate([proposal], SlateCompilerContext())
    summary = [m for m in result.actuator_markers if m.get("is_summary")]
    assert len(summary) == 1
    assert summary[0]["survivor_count"] == 1
    assert summary[0]["drop_count"] == 0


# ---------------------------------------------------------------------
# Mixed slate
# ---------------------------------------------------------------------


def test_mixed_slate_partial_survival():
    """One pass-through proposal + one declined snippet → survivor==1,
    drop==1, terminal_reason_if_empty is None (slate is non-empty)."""
    good = _make_proposal(intent_id="good")
    bad = _make_proposal(
        intent_id="bad",
        patch_type="add_sql_snippet_filter",
    )
    ctx = SlateCompilerContext(
        snippet_validator_verdict_by_proposal_id={"bad": {"outcome": "declined"}},
    )
    result = compile_slate([good, bad], ctx)
    assert result.surviving_proposals == (good,)
    assert result.dropped_proposals[0][0].intent_id == "bad"
    assert result.terminal_reason_if_empty is None


# ---------------------------------------------------------------------
# Precedence
# ---------------------------------------------------------------------


def test_terminal_reason_precedence_picks_prompt_split_over_snippet_invalid():
    """When the same slate has both PROMPT_SPLIT_REQUIRED and
    SNIPPET_INVALID drops, the empty-slate terminal reason picks the
    higher-precedence one (PROMPT_SPLIT_REQUIRED →
    PROPOSAL_GENERATION_EMPTY)."""
    snip = _make_proposal(
        intent_id="snip",
        patch_type="add_sql_snippet_filter",
    )
    other = _make_proposal(intent_id="other")
    ctx = SlateCompilerContext(
        cluster_id="cl",
        prompt_size_verdict_by_cluster={
            "cl": {"over_cap": True, "sub_cluster_split_needed": False}
        },
        snippet_validator_verdict_by_proposal_id={
            "snip": {"outcome": "declined"}
        },
    )
    result = compile_slate([snip, other], ctx)
    assert result.surviving_proposals == ()
    # PROMPT_SPLIT_REQUIRED dominates → PROPOSAL_GENERATION_EMPTY.
    assert (
        result.terminal_reason_if_empty
        == TerminalReason.PROPOSAL_GENERATION_EMPTY
    )


# ---------------------------------------------------------------------
# W6 — required-assets gate activation + rollback flag
# ---------------------------------------------------------------------


def test_w6_metadata_patch_missing_assets_drops_when_gate_on(monkeypatch):
    """W6 — once the harness wires per-proposal assets, an
    add_column_description proposal with NO implicated assets drops
    with MISSING_IMPLICATED_ASSETS instead of short-circuiting."""
    monkeypatch.delenv("GSO_TRIAL22_ASSET_GATE", raising=False)
    p = _make_proposal(
        intent_id="meta_no_assets",
        patch_type="add_column_description",
        selected_lever="lever-1",
        selected_levers=("lever-1",),
        patch_body={"table": "t", "column": "c", "description": "d"},
    )
    ctx = SlateCompilerContext(
        implicated_assets_by_proposal_id={"meta_no_assets": []},
        justification_by_proposal_id={"meta_no_assets": ""},
    )
    result = compile_slate([p], ctx)
    assert result.surviving_proposals == ()
    assert result.dropped_proposals[0][1] == (
        DropReason.MISSING_IMPLICATED_ASSETS
    )


def test_w6_asset_gate_off_restores_short_circuit(monkeypatch):
    """W6 rollback — with GSO_TRIAL22_ASSET_GATE=0 the gate no-ops
    even when assets/justification are wired, so the same proposal
    survives (pre-W6 behavior)."""
    monkeypatch.setenv("GSO_TRIAL22_ASSET_GATE", "0")
    p = _make_proposal(
        intent_id="meta_no_assets",
        patch_type="add_column_description",
        selected_lever="lever-1",
        selected_levers=("lever-1",),
        patch_body={"table": "t", "column": "c", "description": "d"},
    )
    ctx = SlateCompilerContext(
        implicated_assets_by_proposal_id={"meta_no_assets": []},
        justification_by_proposal_id={"meta_no_assets": ""},
    )
    result = compile_slate([p], ctx)
    assert {x.intent_id for x in result.surviving_proposals} == {
        "meta_no_assets"
    }
