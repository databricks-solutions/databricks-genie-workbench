"""P-E1 Task 3 — narrow_skipped_no_original_patch_type record factory."""
from __future__ import annotations


def test_record_factory_populates_canonical_fields():
    from genie_space_optimizer.optimization.decision_emitters import (
        narrow_skipped_no_original_patch_type_record,
    )
    from genie_space_optimizer.optimization.rca_decision_trace import (
        ReasonCode, DecisionType, DecisionOutcome,
    )

    rec = narrow_skipped_no_original_patch_type_record(
        run_id="r1",
        iteration=2,
        ag_id="AG_X",
        cluster_id="H004",
        root_cause="missing_filter",
    )
    assert rec.reason_code == ReasonCode.NARROW_SKIPPED_NO_ORIGINAL_PATCH_TYPE
    assert rec.decision_type == DecisionType.PROPOSAL_GENERATED
    assert rec.outcome == DecisionOutcome.UNRESOLVED
    assert rec.ag_id == "AG_X"
    assert rec.cluster_id == "H004"
    assert rec.gate == "proposal_generation"
    assert "ag:AG_X" in rec.evidence_refs
    assert "cluster:H004" in rec.evidence_refs


def test_record_factory_to_dict_round_trip():
    from genie_space_optimizer.optimization.decision_emitters import (
        narrow_skipped_no_original_patch_type_record,
    )
    rec = narrow_skipped_no_original_patch_type_record(
        run_id="r1", iteration=0, ag_id="", cluster_id="",
        root_cause="",
    )
    d = rec.to_dict()
    assert d["reason_code"] == "narrow_skipped_no_original_patch_type"


def test_marker_string_shape_is_versioned_json():
    import json
    from genie_space_optimizer.common.mlflow_markers import (
        narrow_skipped_no_original_patch_type_marker,
    )
    s = narrow_skipped_no_original_patch_type_marker(
        run_id="r1",
        iteration=2,
        ag_id="AG_X",
        cluster_id="H004",
        root_cause="missing_filter",
    )
    assert s.startswith("GSO_NARROW_SKIPPED_NO_ORIGINAL_PATCH_TYPE_V1 ")
    payload = json.loads(s.split(" ", 1)[1])
    assert payload == {
        "run_id": "r1",
        "iteration": 2,
        "ag_id": "AG_X",
        "cluster_id": "H004",
        "root_cause": "missing_filter",
    }
