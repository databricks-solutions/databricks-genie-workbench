"""Optimizer Control-Plane Hardening Plan — Task B.

When every RCA-grounded proposal in an AG is dropped by upstream
gates, the harness must halt the AG with reason
``no_causal_applyable_patch`` instead of falling back to non-causal
proposals. The helper ``_filter_to_causal_applyable_proposals`` is
the unit-testable surface; the harness wraps it behind
``GSO_NO_CAUSAL_APPLYABLE_HALT``.
"""

from genie_space_optimizer.optimization.harness import (
    _filter_to_causal_applyable_proposals,
)


def test_returns_proposals_with_matching_rca_id():
    ag = {"id": "AG_H003", "rca_id": "RCA_TOP10_LOGIC"}
    proposals = [
        {"proposal_id": "P001", "rca_id": "RCA_TOP10_LOGIC",
         "patch_type": "add_sql_snippet_expression"},
        {"proposal_id": "P002", "rca_id": None,
         "patch_type": "add_join_spec"},
        {"proposal_id": "P003", "rca_id": "RCA_TOP10_LOGIC",
         "patch_type": "add_join_spec"},
    ]
    causal, has_any_rca_matched = _filter_to_causal_applyable_proposals(
        ag=ag, proposals=proposals,
    )
    assert [p["proposal_id"] for p in causal] == ["P001", "P003"]
    assert has_any_rca_matched is True


def test_returns_empty_with_signal_when_all_dropped():
    ag = {"id": "AG_H003", "rca_id": "RCA_TOP10_LOGIC"}
    proposals = [
        {"proposal_id": "P002", "rca_id": None,
         "patch_type": "add_join_spec"},
        {"proposal_id": "P004", "rca_id": "RCA_OTHER",
         "patch_type": "add_join_spec"},
    ]
    causal, has_any_rca_matched = _filter_to_causal_applyable_proposals(
        ag=ag, proposals=proposals,
    )
    assert causal == []
    assert has_any_rca_matched is False


def test_no_rca_id_on_ag_returns_all_proposals_unchanged():
    ag = {"id": "AG_DIAGNOSTIC", "rca_id": None}
    proposals = [
        {"proposal_id": "P001", "rca_id": None,
         "patch_type": "add_join_spec"},
    ]
    causal, has_any_rca_matched = _filter_to_causal_applyable_proposals(
        ag=ag, proposals=proposals,
    )
    assert causal == proposals
    assert has_any_rca_matched is False
