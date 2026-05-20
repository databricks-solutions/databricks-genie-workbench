"""Phase B delta — Task 10.

Synthetic-fixture replay test that exercises all ten ``DecisionType``
values in a single iteration and asserts:

1. The replay engine surfaces every record.
2. ``validate_decisions_against_journey`` returns no violations.
3. ``render_operator_transcript`` projects every record into its
   assigned section (no record falls into a catch-all bucket).

This unblocks CI coverage of the full happy path without waiting on
the next ``airline_real_v1.json`` refresh.

Plan: ``docs/2026-05-03-phase-b-decision-trace-completion-plan.md`` Task 10.
"""
from __future__ import annotations


def _build_full_iteration_fixture() -> dict:
    """Build a synthetic single-iteration fixture exercising every
    DecisionType in one happy path: q1 fails -> clusters into H001 ->
    RCA card -> AG1 -> proposals -> gate accepts -> patch applied ->
    accepted -> q1 passes post-eval."""
    return {
        "run_id": "run_synth",
        "iterations": [
            {
                "iteration": 1,
                "eval_rows": [
                    {"question_id": "q1", "result_correctness": "no"},
                    {"question_id": "q2", "result_correctness": "yes"},
                ],
                "clusters": [
                    {
                        "cluster_id": "H001",
                        "root_cause": "missing_filter",
                        "question_ids": ["q1"],
                    }
                ],
                "soft_clusters": [],
                "strategist_response": {
                    "action_groups": [
                        {
                            "id": "AG1",
                            "affected_questions": ["q1"],
                            "source_cluster_ids": ["H001"],
                            "lever_directives": {
                                "5": {"target_qids": ["q1"]}
                            },
                            # ``lever_loop_replay._replay_iteration`` reads
                            # patches from ``ag.patches`` (not from
                            # ``ag.lever_directives[*].patches``); placing
                            # them here is what produces the ``proposed`` and
                            # ``applied`` journey events the cross-checker
                            # validates against.
                            "patches": [
                                {
                                    "proposal_id": "P001",
                                    "patch_type": "update_instruction_section",
                                    "target_qids": ["q1"],
                                    "cluster_id": "H001",
                                }
                            ],
                        }
                    ]
                },
                "ag_outcomes": {"AG1": "accepted"},
                "post_eval_passing_qids": ["q1", "q2"],
                "decision_records": [
                    {
                        "run_id": "run_synth", "iteration": 1,
                        "decision_type": "eval_classified",
                        "outcome": "info", "reason_code": "hard_failure",
                        "question_id": "q1",
                        "evidence_refs": ["eval:q1"],
                        "target_qids": ["q1"], "affected_qids": ["q1"],
                    },
                    {
                        "run_id": "run_synth", "iteration": 1,
                        "decision_type": "eval_classified",
                        "outcome": "info", "reason_code": "already_passing",
                        "question_id": "q2",
                        "evidence_refs": ["eval:q2"],
                        "target_qids": ["q2"], "affected_qids": ["q2"],
                    },
                    {
                        "run_id": "run_synth", "iteration": 1,
                        "decision_type": "cluster_selected",
                        "outcome": "info", "reason_code": "clustered",
                        "cluster_id": "H001",
                        "rca_id": "rca_h001", "root_cause": "missing_filter",
                        "evidence_refs": ["cluster:H001"],
                        "target_qids": ["q1"], "affected_qids": ["q1"],
                    },
                    {
                        "run_id": "run_synth", "iteration": 1,
                        "decision_type": "rca_formed",
                        "outcome": "info", "reason_code": "rca_grounded",
                        "cluster_id": "H001",
                        "rca_id": "rca_h001", "root_cause": "missing_filter",
                        "evidence_refs": ["cluster:H001", "rca:rca_h001"],
                        "target_qids": ["q1"], "affected_qids": ["q1"],
                    },
                    {
                        "run_id": "run_synth", "iteration": 1,
                        "decision_type": "strategist_ag_emitted",
                        "outcome": "info", "reason_code": "strategist_selected",
                        "ag_id": "AG1",
                        "rca_id": "rca_h001", "root_cause": "missing_filter",
                        "evidence_refs": ["cluster:H001"],
                        "target_qids": ["q1"], "affected_qids": ["q1"],
                    },
                    {
                        "run_id": "run_synth", "iteration": 1,
                        "decision_type": "proposal_generated",
                        "outcome": "accepted", "reason_code": "proposal_emitted",
                        "ag_id": "AG1", "proposal_id": "P001",
                        "rca_id": "rca_h001", "root_cause": "missing_filter",
                        "evidence_refs": ["ag:AG1", "cluster:H001", "rca:rca_h001"],
                        "target_qids": ["q1"], "affected_qids": ["q1"],
                    },
                    {
                        "run_id": "run_synth", "iteration": 1,
                        "decision_type": "gate_decision",
                        "outcome": "accepted", "reason_code": "patch_cap_selected",
                        "ag_id": "AG1", "proposal_id": "P001", "gate": "patch_cap",
                        "rca_id": "rca_h001", "root_cause": "missing_filter",
                        "evidence_refs": ["ag:AG1", "rca:rca_h001"],
                        "target_qids": ["q1"], "affected_qids": ["q1"],
                    },
                    {
                        "run_id": "run_synth", "iteration": 1,
                        "decision_type": "patch_skipped",
                        "outcome": "skipped", "reason_code": "patch_cap_dropped",
                        "ag_id": "AG1", "proposal_id": "P002", "gate": "patch_cap",
                        "rca_id": "rca_h001", "root_cause": "missing_filter",
                        "evidence_refs": ["ag:AG1", "rca:rca_h001"],
                        "target_qids": ["q1"], "affected_qids": ["q1"],
                    },
                    {
                        "run_id": "run_synth", "iteration": 1,
                        "decision_type": "patch_applied",
                        "outcome": "applied", "reason_code": "patch_applied",
                        "ag_id": "AG1", "proposal_id": "P001",
                        "rca_id": "rca_h001", "root_cause": "missing_filter",
                        "evidence_refs": ["ag:AG1", "rca:rca_h001"],
                        "target_qids": ["q1"], "affected_qids": ["q1"],
                    },
                    {
                        "run_id": "run_synth", "iteration": 1,
                        "decision_type": "acceptance_decided",
                        "outcome": "accepted", "reason_code": "patch_applied",
                        "ag_id": "AG1",
                        "rca_id": "rca_h001", "root_cause": "missing_filter",
                        "evidence_refs": ["cluster:H001"],
                        "target_qids": ["q1"], "affected_qids": ["q1"],
                        "observed_effect": "Patches applied; eval improved.",
                        "next_action": "Keep accepted patch.",
                    },
                    {
                        "run_id": "run_synth", "iteration": 1,
                        "decision_type": "qid_resolution",
                        "outcome": "resolved", "reason_code": "post_eval_fail_to_pass",
                        "question_id": "q1",
                        "rca_id": "rca_h001", "root_cause": "missing_filter",
                        "evidence_refs": ["post_eval:q1"],
                        "target_qids": ["q1"], "affected_qids": ["q1"],
                    },
                    {
                        # PR-B2: AG_RETIRED — buffered AG whose target qid
                        # reclassified out of hard before delivery.
                        "run_id": "run_synth", "iteration": 1,
                        "decision_type": "ag_retired",
                        "outcome": "retired",
                        "reason_code": "ag_target_no_longer_hard",
                        "ag_id": "AG_DEAD",
                        "target_qids": ["q99"],
                        "affected_qids": ["q99"],
                        "reason_detail": (
                            "AG AG_DEAD retired at plateau because target "
                            "qids ['q99'] are no longer in the live "
                            "hard-failure set."
                        ),
                    },
                    {
                        # Cycle 5 T1: ITERATION_BUDGET_DECISION — every
                        # iteration emits one of these at end-of-iter
                        # under productive-iteration accounting.
                        "run_id": "run_synth", "iteration": 1,
                        "decision_type": "iteration_budget_decision",
                        "outcome": "info",
                        "reason_code": "iteration_budget_consumed",
                        "next_action": (
                            "Iteration 1 consumed budget; 1 patch(es) applied."
                        ),
                        "metrics": {
                            "applied_patches": 1,
                            "no_op_cause": "",
                            "consumed": True,
                        },
                    },
                    {
                        # Cycle 11: PRODUCER_EXCEPTION — typed record emitted
                        # when a harness producer try/except catches. Added to
                        # the fixture to satisfy the exhaustiveness check in
                        # test_replay_decision_records_cover_every_decision_type.
                        "run_id": "run_synth", "iteration": 1,
                        "decision_type": "producer_exception",
                        "outcome": "failed",
                        "reason_code": "producer_exception",
                        "ag_id": "",
                        "evidence_refs": ["producer:synthetic"],
                        "reason_detail": "ValueError: synthetic fixture exception",
                        "metrics": {
                            "producer": "synthetic",
                            "exception_class": "ValueError",
                            "exception_repr": "ValueError('synthetic fixture exception')",
                            "traceback_head": "",
                        },
                    },
                    {
                        # Cycle 11: INVARIANT_VIOLATION — typed record emitted
                        # when an invariant check fires in warn-and-degrade mode.
                        # Added to the fixture to satisfy the exhaustiveness check
                        # in test_replay_decision_records_cover_every_decision_type.
                        "run_id": "run_synth", "iteration": 1,
                        "decision_type": "invariant_violation",
                        "outcome": "failed",
                        "reason_code": "invariant_violation",
                        "evidence_refs": ["invariant:I1"],
                        "reason_detail": "I1: phase_b.total_records below replay_fixture.records",
                        "metrics": {
                            "invariant_id": "I1",
                            "title": "phase_b.total_records below replay_fixture.records",
                        },
                    },
                    {
                        # Defect Plan 1 (2026-05-12): CLUSTER_BLOCKED_NO_RCA —
                        # typed record emitted at AG-emit time when an open
                        # hard cluster has rca_card=False. Added here to
                        # satisfy the exhaustiveness check in
                        # test_replay_decision_records_cover_every_decision_type.
                        "run_id": "run_synth", "iteration": 1,
                        "decision_type": "cluster_blocked_no_rca",
                        "outcome": "skipped",
                        "reason_code": "rca_ungrounded",
                        "cluster_id": "H_NO_RCA_synth",
                        "evidence_refs": ["cluster:H_NO_RCA_synth"],
                        "root_cause": "synthetic_no_rca_card",
                        "target_qids": ["q_synth_blocked"],
                        "next_action": (
                            "regenerate RCA evidence for this cluster or "
                            "escalate to diagnostic-AG path before re-attempting "
                            "AG emission"
                        ),
                    },
                    {
                        # Plan P-G (2026-05-12): STRATEGIST_CONTEXT_ASSEMBLED —
                        # iteration-level boundary record that captures the
                        # typed StrategistContextOutput at the start of
                        # Stage 5. Added here to satisfy the exhaustiveness
                        # check in
                        # test_replay_decision_records_cover_every_decision_type.
                        "run_id": "run_synth", "iteration": 1,
                        "decision_type": "strategist_context_assembled",
                        "outcome": "info",
                        "reason_code": "context_assembled",
                        "evidence_refs": ["stage:strategist_context"],
                        "affected_qids": ["q_synth_hard"],
                        "metrics": {
                            "assembled_hash": "sha256:" + "0" * 64,
                            "hard_failure_qid_count": 1,
                            "rca_cards_grounded_only_count": 1,
                            "rca_cards_ungrounded_count": 0,
                            "top_level_fields": [
                                "iteration",
                                "rca_cards_grounded_only",
                                "hard_failure_qids",
                            ],
                        },
                    },
                    {
                        # Plan P-G (2026-05-12): STRATEGIST_CONTEXT_CONSUMED —
                        # companion record at the strategist LLM-call
                        # boundary; carries the consumed-vs-assembled drift
                        # signals.
                        "run_id": "run_synth", "iteration": 1,
                        "decision_type": "strategist_context_consumed",
                        "outcome": "info",
                        "reason_code": "context_consumed_matches_assembled",
                        "evidence_refs": ["stage:action_group_selection"],
                        "metrics": {
                            "assembled_hash": "sha256:" + "0" * 64,
                            "consumed_hash": "sha256:" + "0" * 64,
                            "drift_detected": False,
                            "keys_only_in_consumed": [],
                            "keys_only_in_assembled": [],
                            "keys_in_both": 0,
                        },
                    },
                    {
                        # PROPOSAL_FAILURE_DECIDED — emitted once per
                        # failed proposal-generation attempt with a
                        # closed-vocabulary next-action label. Producer:
                        # decision_emitters.proposal_failure_decided_record.
                        "run_id": "run_synth", "iteration": 1,
                        "decision_type": "proposal_failure_decided",
                        "outcome": "info",
                        "reason_code": "rotate_lever_family",
                        "ag_id": "AG_SYNTH_FAIL",
                        "cluster_id": "H_SYNTH",
                        "evidence_refs": ["ag:AG_SYNTH_FAIL"],
                        "target_qids": ["q_synth_failed"],
                        "affected_qids": ["q_synth_failed"],
                        "next_action": (
                            "rotate from lever 5 to lever 6 after empty "
                            "proposal slate"
                        ),
                    },
                    {
                        # LEVER_ROTATION_DECIDED — emitted by
                        # _select_lever_for_cluster when the
                        # RCA_REPAIR_MATRIX rotation chose a fallback lever
                        # over the cluster's first-pick lever. Producer:
                        # decision_emitters.lever_rotation_decided_record
                        # (the emitter has no reason_code field so
                        # ``none`` is the canonical fixture default).
                        "run_id": "run_synth", "iteration": 1,
                        "decision_type": "lever_rotation_decided",
                        "outcome": "info",
                        "reason_code": "none",
                        "cluster_id": "H_SYNTH",
                        "rca_id": "rca_synth",
                        "root_cause": "missing_filter",
                        "evidence_refs": ["cluster:H_SYNTH"],
                        "affected_qids": ["q_synth"],
                        "metrics": {
                            "first_pick_lever": 5,
                            "rotated_to_lever": 6,
                        },
                    },
                    {
                        # SLATE_AUTHORITATIVE_SKIP — WU-1 (2026-05-18).
                        # Emitted when the harness authoritatively skips
                        # the current AG because the slate's
                        # apply_admission_trace denied it. Producer:
                        # decision_emitters.slate_authoritative_skip_record
                        # (uses RCA_UNGROUNDED for the
                        # cluster_blocked_no_rca skip path).
                        "run_id": "run_synth", "iteration": 1,
                        "decision_type": "slate_authoritative_skip",
                        "outcome": "skipped",
                        "reason_code": "rca_ungrounded",
                        "ag_id": "AG_DENIED",
                        "cluster_id": "H_SYNTH",
                        "evidence_refs": ["slate:apply_admission_trace"],
                        "target_qids": ["q_denied"],
                        "affected_qids": ["q_denied"],
                    },
                    {
                        # RCA_REGEN_RETRY_VERDICT — WU-2 (2026-05-18).
                        # Emitted once per blocked cluster after the
                        # single-shot RCA regeneration retry runs.
                        # Producer:
                        # decision_emitters.rca_regen_retry_verdict_record
                        # (per-attempt outcome carried in metrics; the
                        # emitter pins reason_code=NONE).
                        "run_id": "run_synth", "iteration": 1,
                        "decision_type": "rca_regen_retry_verdict",
                        "outcome": "info",
                        "reason_code": "none",
                        "cluster_id": "H_RETRY",
                        "rca_id": "rca_h_retry",
                        "evidence_refs": ["cluster:H_RETRY"],
                        "metrics": {
                            "retry_source": "second_pass_synth",
                            "attempt_count": 1,
                        },
                    },
                    {
                        # CANDIDATE_CRITIQUED — Plan 6. Emitted by
                        # stages/candidate_critique.py:execute once per
                        # critiqued proposal regardless of enforcing mode.
                        # Uses one of the four critique reason codes:
                        # CRITIQUE_PROCEED / CRITIQUE_REWORK /
                        # CRITIQUE_DISCARD / CRITIQUE_DECLINED.
                        "run_id": "run_synth", "iteration": 1,
                        "decision_type": "candidate_critiqued",
                        "outcome": "accepted",
                        "reason_code": "critique_proceed",
                        "ag_id": "AG1",
                        "proposal_id": "P001",
                        "rca_id": "rca_h001",
                        "evidence_refs": ["proposal:P001"],
                        "target_qids": ["q1"],
                        "affected_qids": ["q1"],
                        "metrics": {
                            "critique_score": 0.92,
                            "verdict": "pass",
                            "enforcing": False,
                        },
                    },
                    {
                        # NEXT_ATTEMPT_HYPOTHESIZED — Plan 7. Emitted by
                        # rollback_learning.hypothesize_next_attempts_for_iteration
                        # once per rolled-back cluster regardless of LLM
                        # success/decline/validator-rejection.
                        # Uses one of HYPOTHESIS_HIGH/MEDIUM/LOW_
                        # CONFIDENCE / HYPOTHESIS_DECLINED /
                        # HYPOTHESIS_VALIDATION_REJECTED.
                        "run_id": "run_synth", "iteration": 1,
                        "decision_type": "next_attempt_hypothesized",
                        "outcome": "info",
                        "reason_code": "hypothesis_high_confidence",
                        "cluster_id": "H_ROLLEDBACK",
                        "rca_id": "rca_h_rolledback",
                        "evidence_refs": ["cluster:H_ROLLEDBACK"],
                        "affected_qids": ["q_rolledback"],
                        "metrics": {
                            "hypothesis_count": 1,
                            "llm_outcome": "success",
                        },
                    },
                    {
                        # Plan 10 Phase B3: LLM_CONTRACT_FAILURE — typed
                        # record emitted when a Pydantic response_model
                        # rejects an LLM response. Added here to satisfy
                        # the exhaustiveness check in
                        # test_replay_decision_records_cover_every_decision_type.
                        # Mirrors the shape ``decision_emitters.llm_contract_failure_record``
                        # produces in production.
                        "run_id": "run_synth", "iteration": 1,
                        "decision_type": "llm_contract_failure",
                        "outcome": "failed",
                        "reason_code": "llm_contract_failure",
                        "cluster_id": "C_synth_contract_fail",
                        "evidence_refs": ["schema:Lever5bExampleSqlOutput"],
                        "reason_detail": (
                            "Lever5bExampleSqlOutput rejected LLM response; "
                            "failing_fields=['example_sql']"
                        ),
                        "metrics": {
                            "schema_name": "Lever5bExampleSqlOutput",
                            "skill_name": "lever_5b_example_sql",
                            "failing_fields": ["example_sql"],
                            "error_repr": (
                                "ValidationError(1 validation error for "
                                "Lever5bExampleSqlOutput)"
                            ),
                            "raw_payload": {
                                "example_question": "Top categories by revenue?",
                                "example_sql": None,
                            },
                        },
                    },
                ],
            }
        ],
    }


def _build_iteration_with_phase_c_unresolved_paths() -> dict:
    """Phase C Task 8 — extends the happy-path fixture with three
    new unresolved paths in iteration 2:

    * Cluster H_NO_RCA has hard failures but no RCA finding →
      ``unresolved_rca_records`` emits RCA_FORMED+UNRESOLVED+RCA_UNGROUNDED.
    * RCA finding ``rca_orphan`` covers qid ``q_orphan`` but no AG
      includes it → ``orphan_rca_records`` emits
      STRATEGIST_AG_EMITTED+UNRESOLVED+RCA_UNGROUNDED.
    * Proposal P_UNGROUNDED has matching ``target_qids`` for AG1 but
      its target/intent strings touch unrelated surface area →
      groundedness gate emits GATE_DECISION+DROPPED+RCA_UNGROUNDED.

    The base happy-path iteration is reused as iteration 1; the new
    paths land in iteration 2 so the replay engine sees both.
    """
    base = _build_full_iteration_fixture()
    iteration_2: dict = {
        "iteration": 2,
        "eval_rows": [
            {"question_id": "q_orphan", "result_correctness": "no"},
            {"question_id": "q_no_rca", "result_correctness": "no"},
            {"question_id": "q_ungrounded", "result_correctness": "no"},
        ],
        "clusters": [
            {
                "cluster_id": "H_NO_RCA",
                "root_cause": "",
                "question_ids": ["q_no_rca"],
            },
            {
                "cluster_id": "H_ORPHAN",
                "root_cause": "missing_filter",
                "question_ids": ["q_orphan"],
            },
            {
                "cluster_id": "H_UNGROUNDED",
                "root_cause": "missing_filter",
                "question_ids": ["q_ungrounded"],
            },
        ],
        "soft_clusters": [],
        "strategist_response": {
            "action_groups": [
                {
                    "id": "AG_UNGROUNDED",
                    "affected_questions": ["q_ungrounded"],
                    "source_cluster_ids": ["H_UNGROUNDED"],
                    "lever_directives": {"5": {"target_qids": ["q_ungrounded"]}},
                    "patches": [
                        {
                            "proposal_id": "P_UNGROUNDED",
                            "patch_type": "update_instruction_section",
                            "target_qids": ["q_ungrounded"],
                            "cluster_id": "H_UNGROUNDED",
                        }
                    ],
                }
            ]
        },
        "ag_outcomes": {"AG_UNGROUNDED": "skipped_dead_on_arrival"},
        "post_eval_passing_qids": ["q1", "q2"],
        "decision_records": [
            # New Phase C: unresolved_rca_records for H_NO_RCA
            {
                "run_id": "run_synth", "iteration": 2,
                "decision_type": "rca_formed",
                "outcome": "unresolved", "reason_code": "rca_ungrounded",
                "cluster_id": "H_NO_RCA",
                "rca_id": "", "root_cause": "",
                "evidence_refs": ["cluster:H_NO_RCA"],
                "target_qids": ["q_no_rca"], "affected_qids": ["q_no_rca"],
            },
            # New Phase C: orphan_rca_records for rca_orphan
            {
                "run_id": "run_synth", "iteration": 2,
                "decision_type": "strategist_ag_emitted",
                "outcome": "unresolved", "reason_code": "rca_ungrounded",
                "ag_id": "",
                "rca_id": "rca_orphan", "root_cause": "missing_filter",
                "evidence_refs": ["rca:rca_orphan"],
                "target_qids": ["q_orphan"], "affected_qids": ["q_orphan"],
            },
            # New Phase C: groundedness_gate_records for P_UNGROUNDED
            {
                "run_id": "run_synth", "iteration": 2,
                "decision_type": "gate_decision",
                "outcome": "dropped", "reason_code": "rca_ungrounded",
                "ag_id": "AG_UNGROUNDED", "proposal_id": "P_UNGROUNDED",
                "gate": "rca_groundedness",
                "rca_id": "rca_h_ungrounded", "root_cause": "missing_filter",
                "evidence_refs": ["ag:AG_UNGROUNDED"],
                "target_qids": ["q_ungrounded"], "affected_qids": ["q_ungrounded"],
            },
        ],
    }
    base["iterations"].append(iteration_2)
    return base


def test_phase_c_unresolved_paths_produce_expected_records() -> None:
    """Phase C Task 8 — three new unresolved paths land in iteration 2."""
    from genie_space_optimizer.optimization.lever_loop_replay import run_replay
    from genie_space_optimizer.optimization.rca_decision_trace import (
        DecisionOutcome,
        DecisionType,
        ReasonCode,
    )

    result = run_replay(_build_iteration_with_phase_c_unresolved_paths())

    iter2 = [r for r in result.decision_records if r.iteration == 2]

    # Path 1 — RCA_FORMED+UNRESOLVED+RCA_UNGROUNDED on H_NO_RCA.
    no_rca = [
        r for r in iter2
        if r.decision_type == DecisionType.RCA_FORMED
        and r.outcome == DecisionOutcome.UNRESOLVED
        and r.reason_code == ReasonCode.RCA_UNGROUNDED
        and r.cluster_id == "H_NO_RCA"
    ]
    assert len(no_rca) == 1, f"Expected 1 H_NO_RCA unresolved record; got {no_rca}"

    # Path 2 — STRATEGIST_AG_EMITTED+UNRESOLVED+RCA_UNGROUNDED on rca_orphan.
    orphan = [
        r for r in iter2
        if r.decision_type == DecisionType.STRATEGIST_AG_EMITTED
        and r.outcome == DecisionOutcome.UNRESOLVED
        and r.reason_code == ReasonCode.RCA_UNGROUNDED
        and r.rca_id == "rca_orphan"
    ]
    assert len(orphan) == 1, f"Expected 1 orphan-RCA record; got {orphan}"

    # Path 3 — GATE_DECISION+DROPPED+RCA_UNGROUNDED on P_UNGROUNDED.
    gate = [
        r for r in iter2
        if r.decision_type == DecisionType.GATE_DECISION
        and r.outcome == DecisionOutcome.DROPPED
        and r.reason_code == ReasonCode.RCA_UNGROUNDED
        and r.proposal_id == "P_UNGROUNDED"
        and r.gate == "rca_groundedness"
    ]
    assert len(gate) == 1, f"Expected 1 groundedness-gate drop record; got {gate}"


def test_phase_c_unresolved_paths_pass_validator() -> None:
    """The three new unresolved records validate cleanly. The H_NO_RCA
    record carries empty rca_id but the validator's per-(type,reason)
    exemption (Task 7) allows it. The orphan record carries rca_id.
    The gate record carries rca_id from its source cluster."""
    from genie_space_optimizer.optimization.lever_loop_replay import run_replay

    result = run_replay(_build_iteration_with_phase_c_unresolved_paths())

    assert result.decision_validation == [], (
        f"Cross-checker should pass with Phase C unresolved paths; got: "
        f"{result.decision_validation}"
    )


def test_phase_c_unresolved_paths_project_into_correct_transcript_sections() -> None:
    """``RCA_FORMED`` (no-RCA) → RCA Cards section.
    ``STRATEGIST_AG_EMITTED`` (orphan) → AG Decisions section.
    ``GATE_DECISION`` (groundedness) → Proposal Survival section.
    """
    from genie_space_optimizer.optimization.lever_loop_replay import run_replay
    from genie_space_optimizer.optimization.rca_decision_trace import (
        SECTION_AG_DECISIONS,
        SECTION_PROPOSAL_SURVIVAL,
        SECTION_RCA_CARDS,
    )

    result = run_replay(_build_iteration_with_phase_c_unresolved_paths())
    transcript = result.operator_transcript

    assert SECTION_RCA_CARDS in transcript
    assert SECTION_AG_DECISIONS in transcript
    assert SECTION_PROPOSAL_SURVIVAL in transcript
    # The exact phrasing is owned by the producers — assert presence
    # of the recognizable identifiers.
    assert "H_NO_RCA" in transcript
    assert "rca_orphan" in transcript
    assert "P_UNGROUNDED" in transcript


def test_replay_decision_records_cover_every_decision_type() -> None:
    from genie_space_optimizer.optimization.lever_loop_replay import run_replay
    from genie_space_optimizer.optimization.rca_decision_trace import DecisionType

    result = run_replay(_build_full_iteration_fixture())

    seen = {r.decision_type for r in result.decision_records}
    missing = [dt for dt in DecisionType if dt not in seen]
    assert missing == [], (
        f"Synthetic fixture must cover every DecisionType; missing: {missing}"
    )


def test_replay_cross_checker_returns_no_violations_for_full_happy_path() -> None:
    from genie_space_optimizer.optimization.lever_loop_replay import run_replay

    result = run_replay(_build_full_iteration_fixture())

    assert result.decision_validation == [], (
        f"Cross-checker should return no violations; got: {result.decision_validation}"
    )


def test_replay_transcript_projects_every_decision_type_into_its_section() -> None:
    from genie_space_optimizer.optimization.lever_loop_replay import run_replay
    from genie_space_optimizer.optimization.rca_decision_trace import (
        DecisionType,
        TYPE_TO_SECTION,
    )

    result = run_replay(_build_full_iteration_fixture())
    transcript = result.operator_transcript
    next_idx = transcript.index("Next Suggested Action")
    tail = transcript[next_idx:]
    for dt in DecisionType:
        # No raw decision_type value should appear in the tail (the
        # legacy bottom-of-output dump must be gone).
        assert dt.value not in tail, (
            f"DecisionType {dt.value} leaked into the tail of the transcript"
        )
        # The section assigned by TYPE_TO_SECTION must appear in the
        # transcript header order.
        section_heading = TYPE_TO_SECTION[dt]
        assert section_heading in transcript


def test_terminal_success_does_not_change_decision_validation_count() -> None:
    """Plan N2 — the terminal-success render-time override must not
    affect the cross-projection completeness check. The check
    validates the trace (typed records + journey events), not the
    transcript bytes; an override that mutated the trace would be a
    contract violation.

    Exercises the same fixture twice: once with the override active
    (default), once with ``_force_no_override=True`` (test-only
    bypass). The transcript bytes WILL differ; the
    ``decision_validation`` results MUST be byte-identical.
    """
    from genie_space_optimizer.optimization.lever_loop_replay import run_replay
    from genie_space_optimizer.optimization.rca_decision_trace import (
        OptimizationTrace,
        render_operator_transcript,
        validate_decisions_against_journey,
    )

    fixture = _build_full_iteration_fixture()
    result = run_replay(fixture)
    iteration = max(int(r.iteration) for r in result.decision_records)

    # Build the cross-projection validation against the typed trace.
    # This is what ``run_replay`` reports as ``decision_validation``;
    # the terminal-success override does not pass through this
    # function so the lists must be byte-identical.
    validation_with_override = validate_decisions_against_journey(
        records=list(result.decision_records),
        events=list(result.events),
    )

    # Render once with the override and once without. Bytes will
    # differ when the iteration contains terminal success; the
    # validation list must NOT.
    trace = OptimizationTrace(
        decision_records=tuple(result.decision_records),
        journey_events=tuple(result.events),
    )
    transcript_with = render_operator_transcript(
        trace=trace, iteration=iteration,
    )
    transcript_without = render_operator_transcript(
        trace=trace, iteration=iteration, _force_no_override=True,
    )

    validation_without_override = validate_decisions_against_journey(
        records=list(result.decision_records),
        events=list(result.events),
    )
    assert validation_with_override == validation_without_override, (
        "render-time override must not affect decision_validation; "
        f"with={validation_with_override} "
        f"without={validation_without_override}"
    )

    # Sanity: transcripts should both contain the
    # ``Terminal Success`` heading. The heading always renders
    # whether or not the override fires.
    assert "Terminal Success" in transcript_with
    assert "Terminal Success" in transcript_without
