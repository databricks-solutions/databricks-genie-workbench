"""Pin the pure replay driver behavior on a synthetic minimal fixture."""

from __future__ import annotations


def _minimal_fixture() -> dict:
    return {
        "fixture_id": "tiny",
        "iterations": [
            {
                "iteration": 1,
                "eval_rows": [
                    {"question_id": "q1", "result_correctness": "yes",
                     "arbiter": "both_correct"},
                    {"question_id": "q2", "result_correctness": "no",
                     "arbiter": "ground_truth_correct"},
                ],
                "clusters": [
                    {"cluster_id": "H1", "root_cause": "missing_filter",
                     "question_ids": ["q2"]},
                ],
                "soft_clusters": [],
                "strategist_response": {
                    "action_groups": [
                        {
                            "id": "AG1",
                            "source_cluster_ids": ["H1"],
                            "affected_questions": ["q2"],
                            "lever_directives": {"6": []},
                            "patches": [
                                {"proposal_id": "P1",
                                 "patch_type": "add_sql_snippet_filter",
                                 "cluster_id": "H1",
                                 "target_qids": ["q2"]},
                            ],
                        }
                    ]
                },
                "post_eval_passing_qids": ["q1", "q2"],
                "ag_outcomes": {"AG1": "accepted"},
            }
        ],
    }


def test_replay_driver_emits_evaluated_for_every_qid() -> None:
    from genie_space_optimizer.optimization.lever_loop_replay import (
        run_replay,
    )

    result = run_replay(_minimal_fixture())
    qids_evaluated = {
        ev.question_id for ev in result.events if ev.stage == "evaluated"
    }
    assert qids_evaluated == {"q1", "q2"}


def test_replay_driver_produces_complete_journey_for_resolved_hard_qid() -> None:
    from genie_space_optimizer.optimization.lever_loop_replay import (
        run_replay,
    )

    result = run_replay(_minimal_fixture())
    q2_stages = [ev.stage for ev in result.events if ev.question_id == "q2"]
    assert "evaluated" in q2_stages
    assert "clustered" in q2_stages
    assert "ag_assigned" in q2_stages
    assert "proposed" in q2_stages
    assert "applied" in q2_stages
    assert "accepted" in q2_stages
    assert "post_eval" in q2_stages


def test_replay_driver_validation_report_is_clean_for_minimal_fixture() -> None:
    from genie_space_optimizer.optimization.lever_loop_replay import (
        run_replay,
    )

    result = run_replay(_minimal_fixture())
    assert result.validation.is_valid, (
        f"violations={result.validation.violations}, "
        f"missing={result.validation.missing_qids}"
    )
