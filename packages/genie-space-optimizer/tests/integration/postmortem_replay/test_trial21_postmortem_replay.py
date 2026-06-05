"""Trial 21 postmortem-replay regression suite — the new merge gate.

Replays the two production lever_loop postmortems through the Trial 21
Evidence Actuator and asserts the seven bright-line conditions from the
Trial 21 plan:

1. ``GSO_STAGE3_PROMPT_SIZE_BREAKDOWN_V1.over_cap=false`` on every
   Stage 3 call after slicing.
2. Zero ``GSO_PATCH_OUTCOME_V1.reason`` containing
   ``without validation_passed=True``.
3. Zero ``(qid, behavior_delta_fingerprint, patch_mechanism)`` triple
   appearing as ``kept_insufficient`` more than once.
4. Every empty-slate iteration carries a typed ``DropReason``; zero
   ``terminal_reason=no_applied_patches``.
5. ``observe_only=true`` removed from ``GSO_REPAIR_DIAGNOSIS_GATE_V1``
   for patch families that require concrete assets.
6. ``accepted_with_attribution_drift`` with non-empty
   ``unresolved_target_debt_qids`` classifies as
   ``OPTIMIZER_AGGREGATE_GAIN_TARGET_DEBT``.
7. ``candidate_deploy_eligible=false`` blocks deploy without failing
   the optimizer task.

These tests are RED until W2-W9 land. Each assertion uses
``pytest.mark.xfail(strict=True)`` so the suite is permitted to fail
without blocking unrelated CI; the xfail markers come off as each
W-item lands. The whole file going green is the Phase 5 merge
criterion.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

FIXTURE_DIR = Path(__file__).parent / "fixtures"
RUN_A_FIXTURE = FIXTURE_DIR / "run_a_919039845318742.json"
RUN_B_FIXTURE = FIXTURE_DIR / "run_b_452249357578743.json"


def _load(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text())


@pytest.fixture(scope="module")
def run_a() -> dict[str, Any]:
    return _load(RUN_A_FIXTURE)


@pytest.fixture(scope="module")
def run_b() -> dict[str, Any]:
    return _load(RUN_B_FIXTURE)


@pytest.fixture(autouse=True)
def _trial21_flags_on(monkeypatch):
    monkeypatch.setenv("GSO_TRIAL21_ACTUATOR", "1")
    yield


# ---------------------------------------------------------------------
# Bright-line #1 — Stage 3 prompt sizer slices for real
# ---------------------------------------------------------------------


def test_w3_c8_stage3_prompt_sizer_slices_run_b_over_cap_payload(run_b) -> None:
    """W3+C8 bright-line #1 — after slicing, ``over_cap=False`` AND
    ``observe_only=False`` on Run B's 104k payload. The slicer
    allocates the cap to system + cacheable and shrinks the user
    prompt; when the user prompt was over its post-slice budget,
    ``sub_cluster_split_needed=True`` so the Actuator drops with
    ``PROMPT_SPLIT_REQUIRED``."""
    from genie_space_optimizer.optimization.stage3_prompt_sizer import (
        STAGE3_PROMPT_TOTAL_CAP,
        slice_segments,
    )

    f = run_b["stage3_prompt_sizer_fixture"]
    bd = f["sample_over_cap_breakdown"]
    sliced = slice_segments(
        system_msg_tokens=int(bd["system_msg_tokens"]),
        user_prompt_tokens=int(bd["user_prompt_tokens"]),
        cacheable_block_tokens=int(bd["cacheable_block_tokens"]),
        cap=STAGE3_PROMPT_TOTAL_CAP,
    )

    assert sliced["over_cap"] is False, (
        "W3 bright-line #1: after slice_segments, over_cap must be "
        f"false on Run B's 104k payload; got {sliced}"
    )
    assert sliced["observe_only"] is False, (
        "W3 bright-line #1: observe_only must be flipped to false"
    )
    assert sliced["sub_cluster_split_needed"] is True, (
        "W3 bright-line #1: user prompt was over-budget pre-slice; the "
        "verdict must flag sub_cluster_split_needed so the Actuator "
        "drops with PROMPT_SPLIT_REQUIRED"
    )
    assert sliced["total_tokens"] <= sliced["cap"], (
        "W3 bright-line #1: total_tokens must be <= cap post-slice"
    )


# ---------------------------------------------------------------------
# Bright-line #2 — Snippet validator drops at producer + defense in depth
# ---------------------------------------------------------------------


def test_w4_c3_declined_snippet_dropped_before_applier(run_b) -> None:
    """W4+C3 bright-line #2 — the Actuator drops snippet proposals
    whose producer-side validator declined with a typed
    :class:`DropReason.SNIPPET_INVALID`."""
    from genie_space_optimizer.optimization.proposal_slate_compiler import (
        DropReason,
        SlateCompilerContext,
        compile_slate,
    )
    from genie_space_optimizer.optimization.repair_intent import (
        PatchType,
        RepairShape,
    )
    from genie_space_optimizer.optimization.repair_proposal_typed import (
        RepairProposal,
    )

    f = run_b["snippet_validator_fixture"]
    proposal = RepairProposal(
        intent_id="run_b_snippet_proposal",
        intent_name="snippet proposal",
        intent_description="",
        repair_shape=RepairShape.OTHER,
        patch_type=PatchType(f["patch_body"]["patch_type"]),
        rationale="r",
        confidence="medium",
        patch_body=dict(f["patch_body"]),
        blame_set=("7now_delivery_analytics_space_gs_018",),
        target_qids=("7now_delivery_analytics_space_gs_018",),
    )
    ctx = SlateCompilerContext(
        snippet_validator_verdict_by_proposal_id={
            "run_b_snippet_proposal": f["validator_verdict"],
        },
    )
    result = compile_slate([proposal], ctx)

    dropped = [(p.intent_id, r) for p, r in result.dropped_proposals]
    assert ("run_b_snippet_proposal", DropReason.SNIPPET_INVALID) in dropped, (
        "W4 bright-line #2: declined snippet must be dropped with "
        f"DropReason.SNIPPET_INVALID; got {dropped}"
    )
    assert proposal not in result.surviving_proposals, (
        "W4 bright-line #2: declined snippet must not survive to applier"
    )


# ---------------------------------------------------------------------
# Bright-line #3 — Repeated kept_insufficient triple dropped
# ---------------------------------------------------------------------


def test_w5_repeated_qid_fingerprint_mechanism_triple_dropped(run_b) -> None:
    """W5+C2+C5 bright-line #3 — the Actuator drops proposals whose
    ``(qid, behavior_delta_fingerprint, patch_mechanism)`` triple
    appears in ``prior_mechanism_attempts`` (the kept_insufficient
    ledger projection)."""
    from genie_space_optimizer.optimization.proposal_slate_compiler import (
        DropReason,
        SlateCompilerContext,
        compile_slate,
    )
    from genie_space_optimizer.optimization.repair_intent import (
        PatchType,
        RepairShape,
    )
    from genie_space_optimizer.optimization.repair_proposal_typed import (
        RepairProposal,
    )

    f = run_b["repeated_kept_insufficient_fixture"]
    qid = f["qid"]
    proposal = RepairProposal(
        intent_id="gs_026_iter_2_repeat",
        intent_name="repeated mechanism",
        intent_description="",
        repair_shape=RepairShape.OTHER,
        patch_type=PatchType.ADD_EXAMPLE_SQL,
        rationale="r",
        confidence="medium",
        patch_body={},
        blame_set=(qid,),
        target_qids=(qid,),
        selected_lever="lever-5",
        selected_levers=("lever-5",),
    )
    ctx = SlateCompilerContext(
        prior_mechanism_attempts=f["kept_insufficient_history"],
        rca_kind_label_by_qid={
            qid: f["behavior_delta_fingerprint_inputs"]["rca_kind"],
        },
        behavior_delta_by_qid={
            qid: f["behavior_delta_fingerprint_inputs"]["behavioral_diff"],
        },
    )

    result = compile_slate([proposal], ctx)
    dropped = [(p.intent_id, r) for p, r in result.dropped_proposals]
    assert (
        "gs_026_iter_2_repeat",
        DropReason.REPEATED_FAILED_MECHANISM,
    ) in dropped, (
        "W5 bright-line #3: (qid, behavior_delta_fingerprint, "
        "patch_mechanism) repeat must be dropped; got "
        f"{dropped}"
    )


# ---------------------------------------------------------------------
# Bright-line #4 — Empty slates carry typed DropReason; no no_applied_patches
# ---------------------------------------------------------------------


def test_w2_empty_slate_carries_typed_drop_reason_not_no_applied_patches(run_b) -> None:
    """W2 bright-line #4 — every empty-slate iteration carries a typed
    :class:`DropReason` and ``terminal_reason_if_empty`` is NEVER
    ``no_applied_patches``."""
    from genie_space_optimizer.optimization.proposal_slate_compiler import (
        DropReason,
        SlateCompilerContext,
        compile_slate,
    )
    from genie_space_optimizer.optimization.repair_intent import (
        PatchType,
        RepairShape,
    )
    from genie_space_optimizer.optimization.repair_proposal_typed import (
        RepairProposal,
    )
    from genie_space_optimizer.optimization.terminal_reason import TerminalReason

    f = run_b["snippet_validator_fixture"]
    proposal = RepairProposal(
        intent_id="lonely_declined_snippet",
        intent_name="snippet",
        intent_description="",
        repair_shape=RepairShape.OTHER,
        patch_type=PatchType(f["patch_body"]["patch_type"]),
        rationale="r",
        confidence="medium",
        patch_body=dict(f["patch_body"]),
        blame_set=("7now_delivery_analytics_space_gs_018",),
        target_qids=("7now_delivery_analytics_space_gs_018",),
    )

    ctx = SlateCompilerContext(
        snippet_validator_verdict_by_proposal_id={
            "lonely_declined_snippet": f["validator_verdict"],
        },
    )
    result = compile_slate([proposal], ctx)

    assert result.surviving_proposals == (), (
        "W2 bright-line #4 setup: slate must be empty after dropping "
        "declined snippet"
    )
    assert any(
        r == DropReason.SNIPPET_INVALID for _, r in result.dropped_proposals
    ), "W2 bright-line #4: empty slate must carry typed DropReason"
    assert (
        result.terminal_reason_if_empty != TerminalReason.NO_APPLIED_PATCHES
    ), (
        "W2 bright-line #4: terminal_reason=no_applied_patches must be "
        f"replaced by typed reason; got {result.terminal_reason_if_empty}"
    )


# ---------------------------------------------------------------------
# Bright-line #5 — RepairDiagnosis observe_only removed for required-asset families
# ---------------------------------------------------------------------


def test_w6_c1_required_asset_families_drop_when_missing(run_b) -> None:
    """W6+C1 bright-line #5 — every patch family with a required-asset
    expectation drops when the expected evidence is absent. The
    expected drop_reason vocabulary is pinned by the postmortem-replay
    fixture's ``expected_after_w6_per_family`` table."""
    from genie_space_optimizer.optimization.repair_diagnosis import (
        required_assets_for_patch_family,
    )

    table = run_b["repair_diagnosis_gate_fixture"][
        "expected_after_w6_per_family"
    ]
    for patch_type, expected in table.items():
        verdict = required_assets_for_patch_family(
            patch_type=patch_type,
            implicated_assets=[],
            justification="",
            sql_shape_delta="",
        )
        assert verdict.outcome == "drop", (
            f"W6 bright-line #5: {patch_type} with no assets must drop; "
            f"got {verdict}"
        )
        expected_reason = expected.get(
            "drop_reason_if_missing"
        ) or expected.get("drop_reason_if_shape_absent")
        assert verdict.drop_reason == expected_reason, (
            f"W6 bright-line #5: {patch_type} drop reason mismatch; "
            f"expected {expected_reason}, got {verdict.drop_reason}"
        )


# ---------------------------------------------------------------------
# Bright-line #6 — attribution_drift + target_debt classifies as AGGREGATE_GAIN_TARGET_DEBT
# ---------------------------------------------------------------------


def test_w8_c7_run_a_classifies_as_aggregate_gain_target_debt(run_a) -> None:
    """W8+C7 bright-line #6 — Run A's accept-with-attribution-drift,
    where ``target_still_hard_qids`` is non-empty, classifies as
    ``OPTIMIZER_AGGREGATE_GAIN_TARGET_DEBT`` (not
    ``OPTIMIZER_TRIED_INSUFFICIENT_GAIN``)."""
    from genie_space_optimizer.optimization.state_machine.outcome import (
        classify_run_outcome_from_aggregates,
    )

    f = run_a["outcome_classification"]["trajectory_inputs"]
    outcome = classify_run_outcome_from_aggregates(
        any_iteration_accepted=bool(f["any_iteration_accepted"]),
        any_iteration_post_gt_pre=bool(f["any_iteration_post_gt_pre"]),
        last_accepted_decision=f["any_accepted_decision_value"],
        target_qids=tuple(f["target_qids_union"]),
        target_fixed_qids=tuple(f["target_fixed_qids"]),
        target_still_hard_qids=tuple(f["target_still_hard_qids"]),
    )
    assert outcome == "OPTIMIZER_AGGREGATE_GAIN_TARGET_DEBT", (
        "W8 bright-line #6: Run A's attribution_drift accept with "
        f"target debt must classify as OPTIMIZER_AGGREGATE_GAIN_TARGET_DEBT; "
        f"got {outcome}"
    )


# ---------------------------------------------------------------------
# Bright-line #7 — candidate_deploy_eligible=false blocks deploy without failing task
# ---------------------------------------------------------------------


def test_w9_run_a_contract_health_blocks_deploy_but_task_succeeds(run_a) -> None:
    """W9 bright-line #7 — Run A had merge_gate_blocked +
    assembly_failed but the optimizer task today reported success and
    the deploy task deployed a busted candidate. After W9, the
    optimizer task stays SUCCESS but the deploy task receives
    ``candidate_deploy_eligible=False`` and skips with reason
    ``"contract_health_blocked"``."""
    from genie_space_optimizer.optimization.harness import (
        compute_deploy_eligibility,
    )

    f = run_a["contract_health"]
    eligibility = compute_deploy_eligibility(
        merge_gate_status=f["merge_gate_status"],
        bundle_status=f["bundle_status"],
        run_outcome=run_a["outcome_classification"]["current_outcome"],
    )
    assert eligibility.optimizer_task_status == "success", (
        "W9 bright-line #7: optimizer task must report success"
    )
    assert eligibility.candidate_deploy_eligible is False, (
        "W9 bright-line #7: candidate_deploy_eligible must be false on "
        "merge_gate_blocked + assembly_failed"
    )
    assert eligibility.deploy_skip_reason == "contract_health_blocked", (
        "W9 bright-line #7: deploy_skip_reason must be "
        f"contract_health_blocked; got {eligibility.deploy_skip_reason}"
    )


# ---------------------------------------------------------------------
# Bonus — W7 resolver positive case (the actuator's check #2 depends on this)
# ---------------------------------------------------------------------


def test_w7_resolver_canonicalizes_bare_table_name_to_fqn(run_a) -> None:
    """W7 (precondition for W2 check #2): the LLM emits the bare table
    name ``mv_7now_store_sales`` while the deployed Genie config stores
    the FQN ``<catalog>.<schema>.mv_7now_store_sales``. The resolver must
    canonicalize before declaring missing_table."""
    from genie_space_optimizer.optimization.metadata_target_resolver import (
        validate_and_stamp_metadata_patch_target,
    )

    f = run_a["metadata_target_resolver_fixture"]
    body = dict(f["patch_body"])
    verdict = validate_and_stamp_metadata_patch_target(
        body,
        patch_type_wire=body["patch_type"],
        metadata_snapshot=f["deployed_genie_metadata_snapshot"],
        space_id="run_a_test_space",
    )
    expected = f["expected_verdict_after_w7"]
    assert verdict.outcome == expected["outcome"], (
        "W7: resolver must resolve bare-name MV to FQN; "
        f"got {verdict}"
    )
    assert verdict.resolved_table == expected["resolved_table"]
    assert verdict.resolved_column == expected["resolved_column"]
    assert body["target_resolved"] is True
    assert body["table"] == expected["resolved_table"]
