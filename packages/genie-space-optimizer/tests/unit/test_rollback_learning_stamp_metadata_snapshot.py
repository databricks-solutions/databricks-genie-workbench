"""Plan 7 Task 10 — stamp_hypotheses_on_metadata_snapshot helper."""
from __future__ import annotations

from genie_space_optimizer.optimization.repair_intent import (
    PatchType,
    RepairShape,
)
from genie_space_optimizer.optimization.rollback_hypothesis_typed import (
    NextAttemptHypothesis,
)
from genie_space_optimizer.optimization.rollback_learning import (
    stamp_hypotheses_on_metadata_snapshot,
)


def _hypothesis(cluster_id: str, confidence: str = "high") -> NextAttemptHypothesis:
    return NextAttemptHypothesis(
        rolled_back_intent_id=f"i_{cluster_id}",
        cluster_id=cluster_id,
        ag_id="AG3",
        iteration=2,
        why_failed="x",
        failure_mode="x",
        revised_repair_shape=RepairShape.TOP_N_BY_METRIC,
        revised_patch_type=PatchType.ADD_EXAMPLE_SQL,
        revised_blame_set=("sales.fact_sales.revenue",),
        additional_evidence_needed=(),
        forbidden_signatures=(),
        confidence=confidence,  # type: ignore[arg-type]
    )


def test_stamp_creates_key_when_absent() -> None:
    metadata_snapshot: dict = {}
    hypotheses = {"H001": _hypothesis("H001")}
    stamp_hypotheses_on_metadata_snapshot(metadata_snapshot, hypotheses)
    assert "_last_attempt_hypothesis_by_cluster" in metadata_snapshot
    by_cluster = metadata_snapshot["_last_attempt_hypothesis_by_cluster"]
    assert "H001" in by_cluster


def test_stamp_value_is_json_dict_not_dataclass() -> None:
    metadata_snapshot: dict = {}
    stamp_hypotheses_on_metadata_snapshot(
        metadata_snapshot, {"H001": _hypothesis("H001")},
    )
    val = metadata_snapshot["_last_attempt_hypothesis_by_cluster"]["H001"]
    assert isinstance(val, dict)
    assert val["cluster_id"] == "H001"
    assert val["confidence"] == "high"
    assert val["revised_repair_shape"] == "top_n_by_metric"


def test_stamp_merges_into_existing_key() -> None:
    prior = {"H002": {"cluster_id": "H002", "confidence": "low"}}
    metadata_snapshot: dict = {
        "_last_attempt_hypothesis_by_cluster": dict(prior),
    }
    stamp_hypotheses_on_metadata_snapshot(
        metadata_snapshot, {"H001": _hypothesis("H001")},
    )
    by = metadata_snapshot["_last_attempt_hypothesis_by_cluster"]
    assert set(by.keys()) == {"H001", "H002"}
    assert by["H002"] == prior["H002"]
    assert by["H001"]["confidence"] == "high"


def test_stamp_overwrites_per_cluster_when_new_hypothesis_for_same_cluster() -> None:
    metadata_snapshot: dict = {
        "_last_attempt_hypothesis_by_cluster": {
            "H001": {"cluster_id": "H001", "confidence": "low"},
        },
    }
    stamp_hypotheses_on_metadata_snapshot(
        metadata_snapshot, {"H001": _hypothesis("H001", confidence="high")},
    )
    assert (
        metadata_snapshot["_last_attempt_hypothesis_by_cluster"]["H001"][
            "confidence"
        ]
        == "high"
    )


def test_stamp_is_a_noop_when_hypotheses_empty() -> None:
    metadata_snapshot: dict = {}
    stamp_hypotheses_on_metadata_snapshot(metadata_snapshot, {})
    assert "_last_attempt_hypothesis_by_cluster" not in metadata_snapshot


def test_stamp_does_not_disturb_other_metadata_snapshot_keys() -> None:
    metadata_snapshot: dict = {
        "_failure_clusters": [{"cluster_id": "H001"}],
        "_data_profile": {"x": 1},
        "_asset_semantics": {},
    }
    stamp_hypotheses_on_metadata_snapshot(
        metadata_snapshot, {"H001": _hypothesis("H001")},
    )
    assert metadata_snapshot["_failure_clusters"] == [{"cluster_id": "H001"}]
    assert metadata_snapshot["_data_profile"] == {"x": 1}
    assert metadata_snapshot["_asset_semantics"] == {}
    assert "_last_attempt_hypothesis_by_cluster" in metadata_snapshot
