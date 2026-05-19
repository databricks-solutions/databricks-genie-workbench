"""Plan 8 Task 3 — when the Plan-5 synthesizer declines for an L6
cluster, _generate_lever6_proposal falls back to the legacy body."""
from __future__ import annotations

from unittest.mock import MagicMock, patch


def test_l6_generator_falls_back_when_intent_dispatch_returns_none():
    from genie_space_optimizer.optimization.optimizer import (
        _generate_lever6_proposal,
    )
    legacy_return = {"snippet_type": "filter", "sql": "where x > 0",
                     "target": "fact_sales"}
    with patch(
        "genie_space_optimizer.optimization.lever6_intent_dispatch."
        "dispatch_lever_6_with_intent",
        return_value=None,
    ), patch(
        "genie_space_optimizer.optimization.optimizer."
        "_generate_lever6_proposal_legacy_body",
        return_value=legacy_return,
    ) as mock_legacy:
        out = _generate_lever6_proposal(
            cluster={"cluster_id": "H001", "root_cause": "x"},
            metadata_snapshot={},
            rca_evidence_typed={"q1": MagicMock()},
            llm_cluster=MagicMock(),
            ag_id="AG_X", iteration=1,
        )
    assert out == legacy_return
    assert mock_legacy.called


def test_l6_generator_skips_intent_dispatch_when_typed_inputs_absent():
    """When rca_evidence_typed / llm_cluster are not supplied (e.g.
    holistic legacy callers), the wrapper goes directly to the legacy
    body without invoking the intent dispatch."""
    from genie_space_optimizer.optimization.optimizer import (
        _generate_lever6_proposal,
    )
    legacy_return = {"snippet_type": "expression", "sql": "x+y",
                     "target": "fact_sales"}
    with patch(
        "genie_space_optimizer.optimization.lever6_intent_dispatch."
        "dispatch_lever_6_with_intent",
    ) as mock_dispatch, patch(
        "genie_space_optimizer.optimization.optimizer."
        "_generate_lever6_proposal_legacy_body",
        return_value=legacy_return,
    ) as mock_legacy:
        out = _generate_lever6_proposal(
            cluster={"cluster_id": "H001"}, metadata_snapshot={},
            # No rca_evidence_typed / llm_cluster / ag_id supplied.
        )
    assert out == legacy_return
    assert not mock_dispatch.called
    assert mock_legacy.called
