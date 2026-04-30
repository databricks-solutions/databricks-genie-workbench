from genie_space_optimizer.optimization.scorers.arbiter import (
    ARBITER_FAIL_VERDICTS,
    ARBITER_PASS_VERDICTS,
    ARBITER_VERDICTS,
    is_arbiter_pass_verdict,
)


def test_arbiter_verdict_sets_are_complete_and_disjoint() -> None:
    assert ARBITER_VERDICTS == {
        "genie_correct",
        "ground_truth_correct",
        "both_correct",
        "neither_correct",
        "skipped",
    }
    assert ARBITER_PASS_VERDICTS == {"genie_correct", "both_correct"}
    assert ARBITER_FAIL_VERDICTS == {"ground_truth_correct", "neither_correct"}
    assert not (ARBITER_PASS_VERDICTS & ARBITER_FAIL_VERDICTS)


def test_is_arbiter_pass_verdict() -> None:
    assert is_arbiter_pass_verdict("genie_correct") is True
    assert is_arbiter_pass_verdict("both_correct") is True
    assert is_arbiter_pass_verdict("ground_truth_correct") is False
    assert is_arbiter_pass_verdict("neither_correct") is False
    assert is_arbiter_pass_verdict("skipped") is False


def test_expected_judge_set_is_fixed_and_ordered() -> None:
    from genie_space_optimizer.optimization.scorers import EXPECTED_JUDGE_SET

    assert EXPECTED_JUDGE_SET == (
        "syntax_validity",
        "schema_accuracy",
        "logical_accuracy",
        "semantic_equivalence",
        "completeness",
        "response_quality",
        "asset_routing",
        "result_correctness",
        "arbiter",
    )


def test_build_arbiter_quorum_shadow_records_non_decisive_signal() -> None:
    from genie_space_optimizer.optimization.scorers.arbiter import (
        build_arbiter_quorum_shadow,
    )

    shadow = build_arbiter_quorum_shadow(
        arbiter_verdict="ground_truth_correct",
        judge_values={
            "result_correctness": "no",
            "logical_accuracy": "yes",
            "semantic_equivalence": "yes",
        },
    )

    assert shadow == {
        "enabled": True,
        "decision_effect": "none_shadow_only",
        "arbiter_verdict": "ground_truth_correct",
        "supporting_yes_count": 2,
        "supporting_no_count": 1,
        "suggested_tiebreaker": "genie_shape_supported",
    }
