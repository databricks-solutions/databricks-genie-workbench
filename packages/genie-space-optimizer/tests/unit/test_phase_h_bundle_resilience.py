"""F-6 — Phase H bundle resilience.

Three confirmed defects on run 833969815458299:
  1. RecursionError during stage_io_capture[action_group_selection]
     because nested back-pointers in ActionGroupsInput's clusters
     hit Python's recursion limit; the bundle's manifest then claimed
     ``missing_pieces=[]`` even though the artifacts were never
     written (silent lying).
  2. operator_transcript.md truncated at 17 lines because of (1) but
     manifest doesn't surface the gap.
  3. baseline accuracy printed as ``8947.0%`` because the bundle
     write multiplied a 0-100 percent by 100 again — ``build_run_summary``
     accepted both 0-1 fractions and 0-100 percents without
     normalising.
"""
from __future__ import annotations


def test_serialize_io_handles_self_referential_dict() -> None:
    """A dict containing a back-pointer to itself must not infinite-
    recurse; the serialized payload contains a cycle marker."""
    from genie_space_optimizer.optimization.stage_io_capture import (
        _serialize_io,
    )
    obj: dict = {"a": 1}
    obj["self"] = obj
    out = _serialize_io(obj)
    assert isinstance(out, str)
    assert len(out) > 0
    assert "<cycle:" in out


def test_serialize_io_handles_deep_nesting_below_python_limit() -> None:
    """Nest dicts 200 levels deep — well under sys.getrecursionlimit()
    but enough to trigger naive walkers. The depth cap kicks in and
    produces a truncation marker rather than RecursionError."""
    from genie_space_optimizer.optimization.stage_io_capture import (
        _serialize_io,
    )
    obj: dict = {}
    cur = obj
    for _ in range(200):
        cur["next"] = {}
        cur = cur["next"]
    out = _serialize_io(obj)
    assert isinstance(out, str)
    assert len(out) > 0
    assert "<truncated:depth>" in out


def test_record_and_consume_capture_failures() -> None:
    """record_capture_failure stamps a structured row; consume drains."""
    from genie_space_optimizer.optimization.stage_io_capture import (
        consume_capture_failures,
        record_capture_failure,
    )
    consume_capture_failures()  # drain anything left from prior tests
    record_capture_failure(
        stage_key="action_group_selection",
        artifact_path=(
            "iter_1/stages/04_action_group_selection/input.json"
        ),
        error_class="RecursionError",
    )
    failures = consume_capture_failures()
    assert any(
        f["stage_key"] == "action_group_selection"
        and f["error_class"] == "RecursionError"
        for f in failures
    )
    # consume drained the buffer
    assert consume_capture_failures() == []


def test_build_run_summary_normalizes_0_to_1_baseline() -> None:
    """When the harness passes a 0-1 fraction (legacy path), summary
    renders as 0-100 percent."""
    from genie_space_optimizer.optimization.run_output_bundle import (
        build_run_summary,
    )
    out = build_run_summary(
        baseline={"overall_accuracy": 0.8947},
        terminal_state={
            "status": "thresholds_met",
            "should_continue": False,
        },
        iteration_count=1,
        accuracy_delta_pp=10.5,
    )
    assert out["baseline"]["overall_accuracy"] == 89.5
    assert out["accuracy_delta_pp"] == 10.5


def test_build_run_summary_preserves_0_to_100_baseline() -> None:
    """When the harness passes 0-100 percent (current path), summary
    must NOT scale it again."""
    from genie_space_optimizer.optimization.run_output_bundle import (
        build_run_summary,
    )
    out = build_run_summary(
        baseline={"overall_accuracy": 89.47},
        terminal_state={
            "status": "thresholds_met",
            "should_continue": False,
        },
        iteration_count=1,
        accuracy_delta_pp=10.5,
    )
    assert out["baseline"]["overall_accuracy"] == 89.5
    assert out["accuracy_delta_pp"] == 10.5
