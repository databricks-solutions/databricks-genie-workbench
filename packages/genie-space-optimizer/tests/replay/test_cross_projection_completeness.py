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
                ],
            }
        ],
    }


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
