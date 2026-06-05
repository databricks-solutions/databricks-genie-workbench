"""P4 Success-Replay tests — Wave 1 + Wave 2 invariants AND
Wave 3 (C6) observe-first marker emission.

These tests do not require live d139/e943 fixtures. They assemble
minimal synthetic inputs that exercise each P4 contract item and
pin the invariant the plan declares. A future operator-driven
replay over the real d139 + e943 trace bundles confirms the same
invariants empirically.

Layout — one test per P4 contract item, structured so a postmortem
operator can grep ``wave_1``, ``wave_2``, ``wave_3`` to find the
respective invariants.
"""
from __future__ import annotations

import json
from dataclasses import dataclass

import pytest


# ────────────────────────────────────────────────────────────────────
# Wave 1 — Patch Production Must Satisfy Existing Gates
# ────────────────────────────────────────────────────────────────────


def test_wave_1_c1_repair_diagnosis_admits_sufficient_intent():
    """C1: a RepairDiagnosis with implicated_assets non-empty AND
    sql_shape_delta non-empty passes the structural-repair gate."""
    from genie_space_optimizer.optimization.repair_diagnosis import (
        AssetRef,
        EvidenceRef,
        RepairDiagnosis,
        gate_repair_diagnosis_sufficient,
    )

    diag = RepairDiagnosis(
        cluster_id="c_001",
        rca_freeform="Top-3 results ordered incorrectly.",
        behavior_delta="results need top-3 ordering by amount",
        sql_shape_delta="ORDER BY amount DESC LIMIT 3",
        implicated_assets=(
            AssetRef(
                catalog="main",
                schema="public",
                table="orders",
                column="amount",
            ),
        ),
        evidence_citations=(
            EvidenceRef(source="judge_asi", ref_id="t_001", detail=""),
        ),
        candidate_mechanisms=("example_sql", "instruction_text"),
    )
    verdict = gate_repair_diagnosis_sufficient(diag)
    assert verdict.outcome == "admitted"
    assert verdict.missing_fields == ()


def test_wave_1_c1_repair_diagnosis_abstains_on_empty():
    """C1: empty implicated_assets AND empty sql_shape_delta →
    indeterminate, caller must abstain with
    REPAIR_INTENT_INDETERMINATE."""
    from genie_space_optimizer.optimization.repair_diagnosis import (
        RepairDiagnosis,
        gate_repair_diagnosis_sufficient,
    )
    from genie_space_optimizer.optimization.llm_abstain import AbstainReason

    diag = RepairDiagnosis(
        cluster_id="c_002",
        rca_freeform="",
        behavior_delta="",
        sql_shape_delta="",
        implicated_assets=(),
        evidence_citations=(),
        candidate_mechanisms=(),
    )
    verdict = gate_repair_diagnosis_sufficient(diag)
    assert verdict.outcome == "indeterminate"
    assert "implicated_assets" in verdict.missing_fields
    assert "sql_shape_delta" in verdict.missing_fields
    # The reviewer-required AbstainReason exists.
    assert AbstainReason.REPAIR_INTENT_INDETERMINATE


def test_wave_1_c8_stage3_prompt_under_40k_cap():
    """C8: Stage 3 prompt sizer keeps total token estimate ≤ cap
    on a minimal input."""
    from genie_space_optimizer.optimization.stage3_prompt_sizer import (
        STAGE3_PROMPT_TOTAL_CAP,
        Stage3PromptInput,
        build_stage3_prompt_budget,
    )

    inp = Stage3PromptInput(
        cluster_id="c1",
        cluster_qids=("gs_001",),
        history=(),
        rca_cards=(),
        lever_menu_text="LEVER_MENU",
        archetype_catalog=(),
        schema_columns=(),
    )
    breakdown, _slices = build_stage3_prompt_budget(inp)
    assert breakdown.total_tokens <= STAGE3_PROMPT_TOTAL_CAP


def test_wave_1_c3_producer_stamps_snippet_validation_in_place():
    """C3: stamp_snippet_validation_on_body mutates the patch body
    with the four required applier fields."""
    from genie_space_optimizer.optimization.producer_snippet_validator import (
        stamp_snippet_validation_on_body,
    )

    body: dict = {"patch_type": "add_sql_snippet_filter"}
    stamp_snippet_validation_on_body(
        body,
        intent_id="intent_001",
        snippet_name="snip",
        normalized_sql="SELECT 1",
        snippet_type="filter",
        description="",
    )
    assert body["validation_passed"] is True
    assert body["snippet_id"]
    assert body["sql_snippet"]["sql"] == "SELECT 1"


def test_wave_1_c4_producer_stamps_target_resolved_in_place():
    """C4: producer helper stamps target_resolved=True on metadata
    patch bodies and back-fills canonical table/column."""
    from genie_space_optimizer.optimization.metadata_target_resolver import (
        stamp_target_resolved_on_body,
    )

    body: dict = {"patch_type": "update_column_description"}
    stamp_target_resolved_on_body(
        body, resolved_table="orders", resolved_column="amount",
    )
    assert body["target_resolved"] is True
    assert body["table"] == "orders"
    assert body["column"] == "amount"


def test_wave_1_c2_mechanism_repeat_blocked_after_unproductive_attempt():
    """C2: re-emitting the same (qid, behavior_delta, mechanism)
    after no_applied_patches is blocked unless a NEW mechanism is
    paired with the repeat."""
    from genie_space_optimizer.optimization.patch_mechanism import (
        MechanismAttempt,
        PatchMechanism,
        behavior_delta_hash,
        check_mechanism_repeat_guard,
    )

    bd = "results need top-3 ordering by amount"
    bd_h = behavior_delta_hash(bd)

    prior = (
        MechanismAttempt(
            qid="gs_001",
            behavior_delta_hash=bd_h,
            mechanism=PatchMechanism.INSTRUCTION_TEXT,
            outcome="no_applied_patches",
        ),
    )

    # Same mechanism, no pairing → blocked.
    v_blocked = check_mechanism_repeat_guard(
        qid="gs_001",
        behavior_delta=bd,
        proposed_mechanisms=(PatchMechanism.INSTRUCTION_TEXT,),
        mechanism_change_justification="",
        prior_attempts=prior,
    )
    assert v_blocked.outcome == "blocked"
    assert v_blocked.forbidden_mechanism == PatchMechanism.INSTRUCTION_TEXT

    # Same mechanism paired with a new one → allowed.
    v_paired = check_mechanism_repeat_guard(
        qid="gs_001",
        behavior_delta=bd,
        proposed_mechanisms=(
            PatchMechanism.INSTRUCTION_TEXT,
            PatchMechanism.EXAMPLE_SQL,
        ),
        mechanism_change_justification="paired example_sql to ground the rule",
        prior_attempts=prior,
    )
    assert v_paired.outcome == "allowed"


def test_wave_1_c5_mechanism_coverage_rejects_instruction_only_for_rank_topn():
    """C5: ``instruction_text`` alone does not cover a
    ``RANK_ORDER_TOPN`` behavior_delta — coverage rejection."""
    from genie_space_optimizer.optimization.mechanism_coverage import (
        BehaviorDeltaCategory,
        check_mechanism_coverage,
        classify_behavior_delta,
    )
    from genie_space_optimizer.optimization.patch_mechanism import (
        PatchMechanism,
    )

    bd = "results need top-3 ordering by amount"
    cat = classify_behavior_delta(bd)
    assert cat == BehaviorDeltaCategory.RANK_ORDER_TOPN

    v = check_mechanism_coverage(
        behavior_delta=bd,
        proposed_mechanisms=(PatchMechanism.INSTRUCTION_TEXT,),
    )
    assert v.outcome == "uncovered"


def test_wave_1_c5_mechanism_coverage_accepts_example_sql_for_rank_topn():
    """C5: ``example_sql`` covers ``RANK_ORDER_TOPN``."""
    from genie_space_optimizer.optimization.mechanism_coverage import (
        check_mechanism_coverage,
    )
    from genie_space_optimizer.optimization.patch_mechanism import (
        PatchMechanism,
    )

    v = check_mechanism_coverage(
        behavior_delta="results need top-3 ordering by amount",
        proposed_mechanisms=(PatchMechanism.EXAMPLE_SQL,),
    )
    assert v.outcome == "covered"


def test_wave_1_c5_mechanism_coverage_override_path():
    """C5: a non-empty override justification flips ``uncovered`` to
    ``override`` for postmortem audit."""
    from genie_space_optimizer.optimization.mechanism_coverage import (
        check_mechanism_coverage,
    )
    from genie_space_optimizer.optimization.patch_mechanism import (
        PatchMechanism,
    )

    v = check_mechanism_coverage(
        behavior_delta="results need top-3 ordering by amount",
        proposed_mechanisms=(PatchMechanism.INSTRUCTION_TEXT,),
        mechanism_coverage_override_justification=(
            "instruction directly cites ORDER BY ... LIMIT 3"
        ),
    )
    assert v.outcome == "override"
    assert v.override_justification


# ────────────────────────────────────────────────────────────────────
# Wave 2 — Outcome And Evidence Hygiene
# ────────────────────────────────────────────────────────────────────


def test_wave_2_c7_outcome_returns_target_debt_on_partial_fix():
    """C7: e943 scenario — aggregate gain accepted, target not yet
    fixed → ``OPTIMIZER_AGGREGATE_GAIN_TARGET_DEBT``."""
    from genie_space_optimizer.optimization.state_machine.funnel import (
        FunnelStage,
    )
    from genie_space_optimizer.optimization.state_machine.outcome import (
        classify_run_outcome,
    )

    @dataclass
    class _A:
        decision: str

    @dataclass
    class _E:
        pre_apply_score: float
        post_apply_score: float

    @dataclass
    class _It:
        current_stage: FunnelStage
        accepted: _A | None
        evaluated: _E | None
        applied: object = None
        terminal: object = None

    @dataclass
    class _Tr:
        iterations: tuple
        deepest_stage_ever: FunnelStage = FunnelStage.ACCEPTED

    traj = _Tr(iterations=(
        _It(
            current_stage=FunnelStage.ACCEPTED,
            accepted=_A(decision="accepted"),
            evaluated=_E(pre_apply_score=0.875, post_apply_score=0.957),
        ),
    ))

    outcome = classify_run_outcome(
        trajectories=(traj,),
        target_qids=("gs_009", "gs_010", "gs_011"),
        target_fixed_qids=("gs_010", "gs_011"),
    )
    assert outcome == "OPTIMIZER_AGGREGATE_GAIN_TARGET_DEBT"


def test_wave_2_c9_phase_h_assembly_survives_list_shaped_iteration():
    """C9: e943 anchor — list-shaped iteration blob does not raise."""
    from genie_space_optimizer.optimization.run_output_bundle import (
        assemble_bundle_for_replay,
    )

    bundle = assemble_bundle_for_replay({
        "fixture_id": "fx_e943",
        "baseline_accuracy": 0.875,
        "final_accuracy": 0.957,
        "delta_pp": 8.2,
        "iterations": [
            [{
                "iteration": 1,
                "decision_records": [{"id": "r1"}],
                "journey_violations": [],
                "stages": {"stage_a": {"payload": "ok"}},
            }],
        ],
    })
    assert "manifest" in bundle


def test_wave_2_c10_evidence_bundle_honors_requested_task_run_id():
    """C10: when the requested task_run_id is present, the selector
    returns that exact attempt and does NOT emit stale_anchor."""
    from genie_space_optimizer.tools.evidence_bundle import (
        select_lever_loop_task,
    )

    tasks = [
        {"task_key": "lever_loop", "task_run_id": "754823839975023",
         "run_id": "r_a", "state": {"result_state": "SUCCESS"},
         "end_time": 100, "start_time": 50},
        {"task_key": "lever_loop", "task_run_id": "683631917836228",
         "run_id": "r_b", "state": {"result_state": "FAILED"},
         "end_time": 50, "start_time": 25},
    ]
    sel = select_lever_loop_task(
        tasks, requested_task_run_id="683631917836228",
    )
    assert sel.chosen["task_run_id"] == "683631917836228"
    assert sel.honored_requested_id is True
    assert sel.stale_anchor_reason == ""


def test_wave_2_c10_evidence_bundle_emits_stale_anchor_on_fallback():
    """C10 fallback path: requested id not present in attempts →
    chooses latest by heuristic AND emits stale_anchor_reason."""
    from genie_space_optimizer.tools.evidence_bundle import (
        select_lever_loop_task,
    )

    tasks = [
        {"task_key": "lever_loop", "task_run_id": "754823839975023",
         "run_id": "r_a", "state": {"result_state": "SUCCESS"},
         "end_time": 100, "start_time": 50},
    ]
    sel = select_lever_loop_task(
        tasks, requested_task_run_id="683631917836228",  # not present
    )
    assert sel.chosen["task_run_id"] == "754823839975023"
    assert sel.honored_requested_id is False
    assert "683631917836228" in sel.stale_anchor_reason


# ────────────────────────────────────────────────────────────────────
# Wave 3 — Observe-First, No Behavior Change
# ────────────────────────────────────────────────────────────────────


def test_wave_3_c6_hcrf_diagnostic_marker_emits_on_rejection():
    """C6 OBSERVE-FIRST: a hypothetical HCRF rejection produces a
    GSO_HCRF_DIAGNOSTIC_V1 line. No behavior is changed by the
    marker itself."""
    from genie_space_optimizer.optimization.hcrf_diagnostic import (
        HCRF_DIAGNOSTIC_MARKER_PREFIX,
        hcrf_diagnostic_marker_from_verdict,
    )

    verdict = {
        "safe": False,
        "reason": "high_collateral_risk_flagged",
        "passing_dependents_outside_target": ["gs_009"],
    }
    line = hcrf_diagnostic_marker_from_verdict(
        verdict=verdict,
        patch_type="instruction_text",
        intent_id="intent_001",
        live_hard_qids=["gs_009"],
    )
    assert line is not None
    assert line.startswith(HCRF_DIAGNOSTIC_MARKER_PREFIX + " ")
    payload = json.loads(line[len(HCRF_DIAGNOSTIC_MARKER_PREFIX) + 1:])
    # The downgrade rule WOULD have stamped — non-trivial signal.
    assert payload["would_have_stamped"] is True


def test_wave_3_c6_observe_first_no_behavior_change_on_safe_paths():
    """C6: safe verdicts do NOT emit the diagnostic marker — silent
    success path stays silent (byte-stable)."""
    from genie_space_optimizer.optimization.hcrf_diagnostic import (
        hcrf_diagnostic_marker_from_verdict,
    )

    verdict = {"safe": True, "reason": "within_threshold"}
    line = hcrf_diagnostic_marker_from_verdict(
        verdict=verdict,
        patch_type="instruction_text",
        intent_id="intent_001",
        live_hard_qids=[],
    )
    assert line is None


# ────────────────────────────────────────────────────────────────────
# Aggregate — pin marker prefixes across all P4 contract items
# ────────────────────────────────────────────────────────────────────


def test_p4_marker_prefixes_pinned():
    """Every P4 marker prefix begins with ``GSO_*_V1``."""
    from genie_space_optimizer.optimization.hcrf_diagnostic import (
        HCRF_DIAGNOSTIC_MARKER_PREFIX,
        hcrf_diagnostic_marker_from_verdict,
    )
    from genie_space_optimizer.optimization.patch_mechanism import (
        MechanismRepeatVerdict,
        PatchMechanism,
        mechanism_repeat_guard_marker,
    )
    from genie_space_optimizer.optimization.mechanism_coverage import (
        BehaviorDeltaCategory,
        MechanismCoverageVerdict,
        mechanism_coverage_marker,
    )
    from genie_space_optimizer.optimization.stage3_prompt_sizer import (
        Stage3PromptSegment,
        Stage3PromptSizeBreakdown,
        stage3_prompt_size_breakdown_marker,
    )
    from genie_space_optimizer.tools.evidence_bundle import (
        stale_anchor_diagnostic_marker,
    )

    assert HCRF_DIAGNOSTIC_MARKER_PREFIX == "GSO_HCRF_DIAGNOSTIC_V1"

    # mechanism_repeat_guard_marker — actual signature.
    verdict_repeat = MechanismRepeatVerdict(
        outcome="allowed",
        forbidden_mechanism=None,
        feedback="",
    )
    line_repeat = mechanism_repeat_guard_marker(
        optimization_run_id="opt",
        iteration=1,
        qid="gs_001",
        behavior_delta="x",
        proposed_mechanisms=(PatchMechanism.INSTRUCTION_TEXT,),
        verdict=verdict_repeat,
        mechanism_change_justification="",
    )
    assert line_repeat.startswith("GSO_MECHANISM_REPEAT_GUARD_V1 ")

    # mechanism_coverage_marker — actual signature.
    verdict_cov = MechanismCoverageVerdict(
        outcome="covered",
        inferred_category=BehaviorDeltaCategory.OTHER,
        proposed_mechanisms=(PatchMechanism.INSTRUCTION_TEXT,),
        adequate_mechanisms=(PatchMechanism.INSTRUCTION_TEXT,),
        override_justification="",
        feedback="",
    )
    line_cov = mechanism_coverage_marker(
        optimization_run_id="opt",
        iteration=1,
        qid="gs_001",
        behavior_delta="x",
        verdict=verdict_cov,
    )
    assert line_cov.startswith("GSO_MECHANISM_COVERAGE_V1 ")

    # stage3_prompt_size_breakdown_marker — actual signature.
    breakdown = Stage3PromptSizeBreakdown(
        cluster_id="c1",
        history_tokens=0,
        rca_card_tokens=0,
        lever_menu_tokens=0,
        archetype_catalog_tokens=0,
        schema_column_tokens=0,
        total_tokens=0,
        cap=40000,
        cacheable_block_tokens=0,
        segment_caps={s: 1000 for s in Stage3PromptSegment},
        sub_cluster_split_needed=False,
    )
    line_s3 = stage3_prompt_size_breakdown_marker(
        optimization_run_id="opt",
        iteration=1,
        breakdown=breakdown,
    )
    assert line_s3.startswith("GSO_STAGE3_PROMPT_SIZE_BREAKDOWN_V1 ")

    line_stale = stale_anchor_diagnostic_marker(
        optimization_run_id="opt",
        requested_task_run_id="r",
        chosen_task_run_id="c",
        reason="x",
    )
    assert line_stale.startswith("GSO_STALE_ANCHOR_DIAGNOSTIC_V1 ")

    # HCRF marker emission for the prefix sanity-check.
    line_hcrf = hcrf_diagnostic_marker_from_verdict(
        verdict={
            "safe": False,
            "reason": "high_collateral_risk_flagged",
            "passing_dependents_outside_target": ["q1"],
        },
        patch_type="x",
        intent_id="y",
        live_hard_qids=[],
    )
    assert line_hcrf is not None
    assert line_hcrf.startswith("GSO_HCRF_DIAGNOSTIC_V1 ")
