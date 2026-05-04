"""Optimizer Control-Plane Hardening Plan — Tasks D + F.

Task D: when ``GSO_RCA_AWARE_PATCH_CAP`` is on, every proposal in F5
inherits its parent AG's ``rca_id`` from the cluster RCA map so
``select_causal_patch_cap``'s ``causal_attribution_tier`` ranks them
correctly instead of falling back to insertion order.

Task F: ``materialize_diagnostic_ag`` constructs a diagnostic AG for
a hard cluster carrying the cluster's ``rca_id``. Combined with Task
D, every proposal generated for that AG inherits the same rca_id.
"""

from dataclasses import dataclass

from genie_space_optimizer.optimization.stages.action_groups import (
    materialize_diagnostic_ag,
)
from genie_space_optimizer.optimization.stages.proposals import (
    ProposalsInput,
    generate,
)


@dataclass
class _Ctx:
    run_id: str = "r"
    iteration: int = 1

    @staticmethod
    def decision_emit(*_a, **_k):
        return None

    @staticmethod
    def journey_emit(*_a, **_k):
        return None


def test_proposals_inherit_rca_id_from_parent_cluster(monkeypatch):
    monkeypatch.setenv("GSO_RCA_AWARE_PATCH_CAP", "1")
    inp = ProposalsInput(
        proposals_by_ag={
            "AG_H003": (
                {
                    "proposal_id": "P001",
                    "patch_type": "add_sql_snippet_expression",
                    "primary_cluster_id": "H003",
                },
                {
                    "proposal_id": "P002",
                    "patch_type": "add_join_spec",
                    "primary_cluster_id": "H003",
                },
            )
        },
        rca_id_by_cluster={"H003": "RCA_TOP10_LOGIC"},
        cluster_root_cause_by_id={"H003": "wrong_top_n_logic"},
    )
    slate = generate(_Ctx(), inp)
    stamped = slate.proposals_by_ag["AG_H003"]
    assert stamped[0]["rca_id"] == "RCA_TOP10_LOGIC"
    assert stamped[1]["rca_id"] == "RCA_TOP10_LOGIC"


def test_explicit_disable_does_not_stamp(monkeypatch):
    """Flag was flipped default-on for cycle-9 deploy; setting the
    env-var to ``0`` is the disable path that skips the stamping."""
    monkeypatch.setenv("GSO_RCA_AWARE_PATCH_CAP", "0")
    inp = ProposalsInput(
        proposals_by_ag={
            "AG_H003": (
                {"proposal_id": "P001", "primary_cluster_id": "H003"},
            )
        },
        rca_id_by_cluster={"H003": "RCA_TOP10_LOGIC"},
    )
    slate = generate(_Ctx(), inp)
    stamped = slate.proposals_by_ag["AG_H003"]
    assert "rca_id" not in stamped[0] or not stamped[0].get("rca_id")


def test_diagnostic_ag_carries_cluster_rca():
    """Task F — a diagnostic AG materialized for a hard cluster must
    inherit the cluster's rca_id so its proposals are not dropped by
    the rca_groundedness gate."""
    cluster = {"id": "H003", "qids": ("gs_009",)}
    ag = materialize_diagnostic_ag(
        cluster=cluster,
        rca_id_by_cluster={"H003": "RCA_TOP10_LOGIC"},
    )
    assert ag["rca_id"] == "RCA_TOP10_LOGIC"
    assert ag["id"] == "AG_COVERAGE_H003"
    assert ag["target_qids"] == ("gs_009",)
