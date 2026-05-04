"""Coverage-gap diagnostic AG triggers RCA regeneration (Cycle 5 T3).

When ``rca_cards_present[c]=False`` for a hard cluster, the existing
``materialize_diagnostic_ag`` produces an AG with empty ``rca_id``
that the rca_groundedness gate drops as ``rca_ungrounded``. T3
splits the AG kind so the harness can route the no-parent-RCA case
to a regeneration step instead of generating empty proposals.
"""
from __future__ import annotations

import os
from unittest.mock import patch


def test_flag_helper_default_off() -> None:
    from genie_space_optimizer.common.config import (
        diagnostic_ag_rca_regen_enabled,
    )
    with patch.dict(os.environ, {}, clear=True):
        assert diagnostic_ag_rca_regen_enabled() is False


def test_flag_helper_on_when_env_set() -> None:
    from genie_space_optimizer.common.config import (
        diagnostic_ag_rca_regen_enabled,
    )
    with patch.dict(
        os.environ, {"GSO_DIAGNOSTIC_AG_RCA_REGEN": "1"}, clear=True,
    ):
        assert diagnostic_ag_rca_regen_enabled() is True


def test_materialize_diagnostic_ag_marks_ungrounded_when_no_parent_rca() -> None:
    """When the parent cluster has no ``rca_id``, the AG carries
    ``ag_kind='diagnostic_no_parent_rca'`` so the harness can route it
    to regeneration instead of generating empty proposals."""
    from genie_space_optimizer.optimization.stages.action_groups import (
        materialize_diagnostic_ag,
    )
    cluster = {"id": "H001", "qids": ("gs_026",)}
    ag = materialize_diagnostic_ag(
        cluster=cluster, rca_id_by_cluster={},  # empty: no parent RCA
    )
    assert ag["ag_kind"] == "diagnostic_no_parent_rca"
    assert ag["rca_id"] == ""
    assert ag["needs_rca_regeneration"] is True


def test_materialize_diagnostic_ag_inherits_when_parent_rca_present() -> None:
    """Backward-compat: existing AG-1-F path (parent RCA exists) still
    produces ``ag_kind='diagnostic'`` and ``needs_rca_regeneration=False``."""
    from genie_space_optimizer.optimization.stages.action_groups import (
        materialize_diagnostic_ag,
    )
    cluster = {"id": "H001", "qids": ("gs_026",)}
    ag = materialize_diagnostic_ag(
        cluster=cluster, rca_id_by_cluster={"H001": "rca_x"},
    )
    assert ag["ag_kind"] == "diagnostic"
    assert ag["rca_id"] == "rca_x"
    assert ag.get("needs_rca_regeneration", False) is False


def test_rca_regeneration_reason_codes_exist() -> None:
    from genie_space_optimizer.optimization.rca_decision_trace import (
        ReasonCode,
    )
    assert ReasonCode.RCA_REGENERATION_TRIGGERED.value == "rca_regeneration_triggered"
    assert ReasonCode.RCA_REGENERATION_EXHAUSTED.value == "rca_regeneration_exhausted"


def test_rca_regeneration_triggered_emitter() -> None:
    from genie_space_optimizer.optimization.decision_emitters import (
        rca_regeneration_triggered_record,
    )
    from genie_space_optimizer.optimization.rca_decision_trace import (
        DecisionType, DecisionOutcome, ReasonCode,
    )
    rec = rca_regeneration_triggered_record(
        run_id="run-x", iteration=2,
        cluster_id="H001", target_qids=("gs_026",),
    )
    assert rec.decision_type == DecisionType.RCA_FORMED
    assert rec.outcome == DecisionOutcome.INFO
    assert rec.reason_code == ReasonCode.RCA_REGENERATION_TRIGGERED


def test_rca_regeneration_exhausted_emitter() -> None:
    from genie_space_optimizer.optimization.decision_emitters import (
        rca_regeneration_exhausted_record,
    )
    from genie_space_optimizer.optimization.rca_decision_trace import (
        DecisionOutcome, ReasonCode,
    )
    rec = rca_regeneration_exhausted_record(
        run_id="run-x", iteration=2,
        cluster_id="H001", attempted_evidence_sources=("clusterer", "asi"),
    )
    assert rec.outcome == DecisionOutcome.UNRESOLVED
    assert rec.reason_code == ReasonCode.RCA_REGENERATION_EXHAUSTED
    assert rec.metrics["attempted_evidence_sources"] == ["clusterer", "asi"]
