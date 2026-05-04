"""Optimizer Control-Plane Hardening Plan — Task C.

Bucket-driven AG selection: when ``GSO_BUCKET_DRIVEN_AG_SELECTION`` is
on, ``stages.action_groups.select`` consumes prior-iteration failure
buckets to (a) drop ``MODEL_CEILING`` qids from AG target sets,
(b) mark all-``EVIDENCE_GAP`` AGs with ``ag_kind="evidence_gathering"``.

Note: ``ActionGroupsInput`` takes ``action_groups`` (post-strategist),
not ``clusters`` — the bucket policy filters and tags those AGs.
"""

from dataclasses import dataclass

from genie_space_optimizer.optimization.failure_bucketing import (
    FailureBucket,
)
from genie_space_optimizer.optimization.stages.action_groups import (
    ActionGroupsInput,
    select,
)


@dataclass
class _Ctx:
    run_id: str = "r"
    iteration: int = 2

    @staticmethod
    def decision_emit(*_a, **_k):
        return None

    @staticmethod
    def journey_emit(*_a, **_k):
        return None


def test_model_ceiling_qid_dropped_from_targets(monkeypatch):
    monkeypatch.setenv("GSO_BUCKET_DRIVEN_AG_SELECTION", "1")
    inp = ActionGroupsInput(
        action_groups=(
            {
                "id": "AG_H001",
                "ag_id": "AG_H001",
                "target_qids": ("gs_029",),
                "affected_questions": ("gs_029",),
                "source_cluster_ids": ("H001",),
            },
            {
                "id": "AG_H002",
                "ag_id": "AG_H002",
                "target_qids": ("gs_009",),
                "affected_questions": ("gs_009",),
                "source_cluster_ids": ("H002",),
            },
        ),
        prior_buckets_by_qid={
            "gs_029": FailureBucket.MODEL_CEILING,
            "gs_009": FailureBucket.GATE_OR_CAP_GAP,
        },
    )
    slate = select(_Ctx(), inp)
    selected_qids = {
        q for ag in slate.ags for q in ag.get("target_qids", ())
    }
    assert "gs_009" in selected_qids
    assert "gs_029" not in selected_qids


def test_evidence_gap_marks_evidence_gathering_ag(monkeypatch):
    monkeypatch.setenv("GSO_BUCKET_DRIVEN_AG_SELECTION", "1")
    inp = ActionGroupsInput(
        action_groups=(
            {
                "id": "AG_H003",
                "ag_id": "AG_H003",
                "target_qids": ("gs_024",),
                "affected_questions": ("gs_024",),
                "source_cluster_ids": ("H003",),
            },
        ),
        prior_buckets_by_qid={"gs_024": FailureBucket.EVIDENCE_GAP},
    )
    slate = select(_Ctx(), inp)
    assert any(
        ag.get("ag_kind") == "evidence_gathering" for ag in slate.ags
    )


def test_explicit_disable_preserves_legacy_behavior(monkeypatch):
    """Flag was flipped default-on for cycle-9 deploy; setting the
    env-var to ``0`` is the disable path that preserves the legacy
    behaviour."""
    monkeypatch.setenv("GSO_BUCKET_DRIVEN_AG_SELECTION", "0")
    inp = ActionGroupsInput(
        action_groups=(
            {
                "id": "AG_H001",
                "ag_id": "AG_H001",
                "target_qids": ("gs_029",),
                "affected_questions": ("gs_029",),
                "source_cluster_ids": ("H001",),
            },
        ),
        prior_buckets_by_qid={"gs_029": FailureBucket.MODEL_CEILING},
    )
    slate = select(_Ctx(), inp)
    # MODEL_CEILING qid not removed when flag off.
    assert slate.ags
    assert "gs_029" in slate.ags[0].get("target_qids", ())
