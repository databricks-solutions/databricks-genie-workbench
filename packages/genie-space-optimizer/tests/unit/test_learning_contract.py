from genie_space_optimizer.optimization.stages.learning import (
    AcceptanceVerdict,
    IterationSummary,
    LearningInput,
    LearningOutput,
)
from genie_space_optimizer.optimization.stages._json_io import JsonRoundTrip


def test_input_and_output_mix_jsonroundtrip() -> None:
    assert issubclass(LearningInput, JsonRoundTrip)
    assert issubclass(LearningOutput, JsonRoundTrip)


def test_acceptance_verdict_is_strenum() -> None:
    assert AcceptanceVerdict.ACCEPTED.value == "accepted"
    assert AcceptanceVerdict.ACCEPTED_WITH_ATTRIBUTION_DRIFT.value == "accepted_with_attribution_drift"
    assert AcceptanceVerdict.ACCEPTED_WITH_REGRESSION_DEBT.value == "accepted_with_regression_debt"
    assert AcceptanceVerdict.ROLLED_BACK.value == "rolled_back"


def test_iteration_summary_round_trip() -> None:
    s = IterationSummary(
        iteration=1,
        attempted=True,
        verdict=AcceptanceVerdict.ACCEPTED_WITH_ATTRIBUTION_DRIFT,
        candidate_accuracy=0.958,
        baseline_accuracy=0.833,
        accidentally_improved_qids=("gs_007", "gs_009", "gs_016"),
        unresolved_target_debt_qids=("gs_024",),
    )
    payload = s.to_json()
    restored = IterationSummary.from_json(payload)
    assert restored == s


def test_learning_output_one_summary_per_attempted_iteration() -> None:
    out = LearningOutput(
        iteration_summaries=(
            IterationSummary(iteration=1, attempted=True,
                             verdict=AcceptanceVerdict.ACCEPTED_WITH_ATTRIBUTION_DRIFT,
                             candidate_accuracy=0.958, baseline_accuracy=0.833),
            IterationSummary(iteration=2, attempted=True,
                             verdict=AcceptanceVerdict.ROLLED_BACK,
                             candidate_accuracy=0.917, baseline_accuracy=0.958),
            IterationSummary(iteration=3, attempted=True,
                             verdict=AcceptanceVerdict.ROLLED_BACK,
                             candidate_accuracy=0.917, baseline_accuracy=0.958),
        ),
        terminate=True,
        terminate_reason="plateau_unresolved_hard_failures_quarantined",
    )
    assert len(out.iteration_summaries) == 3
    assert out.terminate
