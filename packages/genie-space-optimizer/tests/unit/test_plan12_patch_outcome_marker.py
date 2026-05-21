"""Plan 12 — GSO_PATCH_OUTCOME_V1 marker shape."""
import json


def _parse(line: str) -> tuple[str, dict]:
    name, _, payload = line.partition(" ")
    return name, json.loads(payload)


def test_marker_applied_outcome():
    from genie_space_optimizer.optimization.run_analysis_contract import (
        patch_outcome_marker,
    )
    line = patch_outcome_marker(
        optimization_run_id="run_x",
        iteration=2,
        ag_id="H001",
        cluster_id="C001",
        intent_id="intent_001",
        outcome_kind="applied",
        terminal_reason="",
        applied_patch_id="ap_001",
    )
    name, payload = _parse(line)
    assert name == "GSO_PATCH_OUTCOME_V1"
    assert payload["outcome_kind"] == "applied"
    assert payload["applied_patch_id"] == "ap_001"


def test_marker_blast_radius_rejected_with_narrow():
    from genie_space_optimizer.optimization.run_analysis_contract import (
        patch_outcome_marker,
    )
    line = patch_outcome_marker(
        optimization_run_id="run_x",
        iteration=2,
        ag_id="H001",
        cluster_id="C001",
        intent_id="intent_001",
        outcome_kind="blast_radius_rejected",
        terminal_reason="blast_radius_rejected",
        collateral_qids=["gs_003", "gs_005"],
        narrow_replacement_attempted=True,
        narrow_outcome="exhausted",
    )
    _, payload = _parse(line)
    assert payload["narrow_replacement_attempted"] is True
    assert payload["narrow_outcome"] == "exhausted"
    assert payload["collateral_qids"] == ["gs_003", "gs_005"]


def test_marker_rejects_unknown_outcome_kind():
    from genie_space_optimizer.optimization.run_analysis_contract import (
        patch_outcome_marker,
    )
    try:
        patch_outcome_marker(
            optimization_run_id="run_x",
            iteration=2,
            ag_id="H001",
            cluster_id="C001",
            intent_id="intent_001",
            outcome_kind="bogus_value",
            terminal_reason="",
        )
    except ValueError as exc:
        assert "outcome_kind" in str(exc)
    else:
        raise AssertionError("Expected ValueError")
