"""Plan 12 — GSO_PLAN11_DISPATCH_DECISION_V1 marker shape."""
import json


def _parse(line: str) -> tuple[str, dict]:
    name, _, payload = line.partition(" ")
    return name, json.loads(payload)


def test_dispatch_decision_entered_marker():
    from genie_space_optimizer.optimization.run_analysis_contract import (
        plan11_dispatch_decision_marker,
    )
    line = plan11_dispatch_decision_marker(
        optimization_run_id="run_x",
        iteration=2,
        namespace="hard",
        outcome="entered",
        skip_reason="",
        failing_qids_count=3,
        rca_evidence_typed_present=True,
    )
    name, payload = _parse(line)
    assert name == "GSO_PLAN11_DISPATCH_DECISION_V1"
    assert payload["outcome"] == "entered"
    assert payload["skip_reason"] == ""
    assert payload["failing_qids_count"] == 3


def test_dispatch_decision_skipped_marker_has_typed_reason():
    from genie_space_optimizer.optimization.run_analysis_contract import (
        VALID_PLAN11_SKIP_REASONS,
        plan11_dispatch_decision_marker,
    )
    assert "flag_disabled" in VALID_PLAN11_SKIP_REASONS
    assert "no_failing_qids" in VALID_PLAN11_SKIP_REASONS
    assert "build_failing_qids_empty" in VALID_PLAN11_SKIP_REASONS
    assert "stage1_llm_declined" in VALID_PLAN11_SKIP_REASONS
    assert "stage2_llm_declined" in VALID_PLAN11_SKIP_REASONS

    line = plan11_dispatch_decision_marker(
        optimization_run_id="run_x",
        iteration=2,
        namespace="hard",
        outcome="skipped",
        skip_reason="flag_disabled",
        failing_qids_count=3,
        rca_evidence_typed_present=False,
    )
    _, payload = _parse(line)
    assert payload["outcome"] == "skipped"
    assert payload["skip_reason"] == "flag_disabled"


def test_dispatch_decision_skip_reason_must_be_typed():
    """skip_reason MUST come from a closed vocabulary; free-form rejected."""
    from genie_space_optimizer.optimization.run_analysis_contract import (
        plan11_dispatch_decision_marker,
    )

    try:
        plan11_dispatch_decision_marker(
            optimization_run_id="run_x",
            iteration=2,
            namespace="hard",
            outcome="skipped",
            skip_reason="something_random",
            failing_qids_count=3,
            rca_evidence_typed_present=False,
        )
    except ValueError as exc:
        assert "skip_reason" in str(exc)
    else:
        raise AssertionError(
            "Expected ValueError for unknown skip_reason"
        )
