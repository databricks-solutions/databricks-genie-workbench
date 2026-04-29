from genie_space_optimizer.optimization.rca_terminal import (
    RcaTerminalStatus,
    classify_terminal_state,
)


def test_terminal_state_converged_when_accuracy_reaches_target() -> None:
    decision = classify_terminal_state(
        post_arbiter_accuracy=100.0,
        max_iterations=5,
        iteration_counter=3,
        actionable_plan_count=2,
        repeated_failure_count=0,
        judge_failure_count=0,
        benchmark_issue_count=0,
        unpatchable_count=0,
    )

    assert decision.status is RcaTerminalStatus.CONVERGED
    assert decision.should_continue is False


def test_terminal_state_exhausted_budget_when_max_iterations_reached_with_actionable_plans() -> None:
    decision = classify_terminal_state(
        post_arbiter_accuracy=80.0,
        max_iterations=5,
        iteration_counter=5,
        actionable_plan_count=1,
        repeated_failure_count=1,
        judge_failure_count=0,
        benchmark_issue_count=0,
        unpatchable_count=0,
    )

    assert decision.status is RcaTerminalStatus.EXHAUSTED_BUDGET
    assert decision.should_continue is False
    assert "5" in decision.reason


def test_terminal_state_unpatchable_when_no_actionable_plans_remain() -> None:
    decision = classify_terminal_state(
        post_arbiter_accuracy=66.7,
        max_iterations=5,
        iteration_counter=2,
        actionable_plan_count=0,
        repeated_failure_count=0,
        judge_failure_count=0,
        benchmark_issue_count=0,
        unpatchable_count=2,
    )

    assert decision.status is RcaTerminalStatus.UNPATCHABLE_WITH_SIX_LEVERS
    assert decision.should_continue is False


def test_terminal_state_judge_unreliable_precedes_patchable_progress() -> None:
    decision = classify_terminal_state(
        post_arbiter_accuracy=50.0,
        max_iterations=5,
        iteration_counter=2,
        actionable_plan_count=2,
        repeated_failure_count=0,
        judge_failure_count=3,
        benchmark_issue_count=0,
        unpatchable_count=0,
    )

    assert decision.status is RcaTerminalStatus.JUDGE_UNRELIABLE
    assert decision.should_continue is False


def test_terminal_state_patchable_in_progress_when_plans_remain() -> None:
    decision = classify_terminal_state(
        post_arbiter_accuracy=75.0,
        max_iterations=5,
        iteration_counter=2,
        actionable_plan_count=1,
        repeated_failure_count=1,
        judge_failure_count=0,
        benchmark_issue_count=0,
        unpatchable_count=0,
    )

    assert decision.status is RcaTerminalStatus.PATCHABLE_IN_PROGRESS
    assert decision.should_continue is True
