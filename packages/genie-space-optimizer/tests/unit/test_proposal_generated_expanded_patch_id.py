"""F-4 — proposal_generated records emit expanded_patch_id, not bare.

Run 833969815458299 emitted five proposal_generated records on iter 1
all carrying bare proposal_id="P001"/"P002"/"P003"/"P001"/"P001"
across L1/L5/L6 — three different patches collapsed to lane[P001] in
N1's lane-key derivation, producing illegal-transition violations.
P2 (Cycle 3) normalized signatures and gate-decision records to use
expanded_patch_id, but proposal_generated records still emitted bare
proposal_id values. Pin the contract: expanded_patch_id wins,
metrics.parent_proposal_id preserves the bare id for legitimate
parent-grouping.
"""
from __future__ import annotations


def test_proposal_generated_record_uses_expanded_patch_id_when_available() -> None:
    """When a proposal carries expanded_patch_id, the record's
    proposal_id field reflects it (e.g. 'L1:P001#1'), not bare 'P001'.
    """
    from genie_space_optimizer.optimization.decision_emitters import (
        proposal_generated_records,
    )
    proposals = [{
        "proposal_id": "P001",
        "expanded_patch_id": "L1:P001#1",
        "patch_type": "update_column_description",
        "lever": 1,
        "target_qids": ["gs_026"],
        "cluster_id": "AG1",
    }]
    recs = proposal_generated_records(
        run_id="run-x",
        iteration=1,
        ag_id="AG1",
        proposals=proposals,
        rca_id_by_cluster={},
        cluster_root_cause_by_id={},
    )
    assert len(recs) == 1
    rec = recs[0]
    assert rec.proposal_id == "L1:P001#1"
    assert rec.proposal_ids == ("L1:P001#1",)
    assert (rec.metrics or {}).get("parent_proposal_id") == "P001"


def test_proposal_generated_record_falls_back_to_bare_when_no_expansion() -> None:
    """Backward compat: when no expanded_patch_id is present, the
    record uses the bare proposal_id (legacy behavior); the bare id
    also lands on metrics.parent_proposal_id."""
    from genie_space_optimizer.optimization.decision_emitters import (
        proposal_generated_records,
    )
    proposals = [{
        "proposal_id": "P002",
        "patch_type": "update_description",
        "target_qids": ["gs_001"],
        "cluster_id": "AG1",
    }]
    recs = proposal_generated_records(
        run_id="run-x",
        iteration=1,
        ag_id="AG1",
        proposals=proposals,
        rca_id_by_cluster={},
        cluster_root_cause_by_id={},
    )
    assert len(recs) == 1
    rec = recs[0]
    assert rec.proposal_id == "P002"
    assert rec.proposal_ids == ("P002",)
    assert (rec.metrics or {}).get("parent_proposal_id") == "P002"


def test_proposal_generated_record_distinguishes_p001_across_levers() -> None:
    """The defect repro: three patches all named 'P001' across L1/L5/L6
    must produce three distinct expanded_patch_id values when emitted.
    """
    from genie_space_optimizer.optimization.decision_emitters import (
        proposal_generated_records,
    )
    proposals = [
        {
            "proposal_id": "P001",
            "expanded_patch_id": "L1:P001#1",
            "lever": 1,
            "target_qids": ["gs_001"],
            "cluster_id": "AG1",
        },
        {
            "proposal_id": "P001",
            "expanded_patch_id": "P001#1",
            "lever": 5,
            "target_qids": ["gs_001"],
            "cluster_id": "AG1",
        },
        {
            "proposal_id": "P001",
            "expanded_patch_id": "L6:P001#4",
            "lever": 6,
            "target_qids": ["gs_001"],
            "cluster_id": "AG1",
        },
    ]
    recs = proposal_generated_records(
        run_id="x",
        iteration=1,
        ag_id="AG1",
        proposals=proposals,
        rca_id_by_cluster={},
        cluster_root_cause_by_id={},
    )
    assert len(recs) == 3
    ids = tuple(r.proposal_id for r in recs)
    assert ids == ("L1:P001#1", "P001#1", "L6:P001#4")
    parent_ids = tuple((r.metrics or {}).get("parent_proposal_id") for r in recs)
    assert parent_ids == ("P001", "P001", "P001")
