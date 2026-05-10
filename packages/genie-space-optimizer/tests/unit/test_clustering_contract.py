"""C15-P2.2: ClusteringInput / ClusterFindings typed contract tests.

The existing clustering stage preserves its production-shape classes
(ClusteringInput with eval_result_for_clustering / metadata_snapshot,
ClusterFindings with clusters / soft_clusters / rejected_cluster_alternatives)
and adds JsonRoundTrip as a mixin so boundary-fixture replay can serialize /
deserialize stage I/O.

Naming note: test_stage_io_class_declarations.py pins:
  INPUT_CLASS  = ClusteringInput   (not a simplified dict-only form)
  OUTPUT_CLASS = ClusterFindings   (not ClusteringOutput)
These tests conform to those pinned names.
"""

from __future__ import annotations

import pytest

from genie_space_optimizer.optimization.stages.clustering import (
    ClusteringInput,
    ClusterFindings,
)
from genie_space_optimizer.optimization.stages._json_io import JsonRoundTrip


def test_input_mixes_jsonroundtrip() -> None:
    assert issubclass(ClusteringInput, JsonRoundTrip)


def test_output_mixes_jsonroundtrip() -> None:
    assert issubclass(ClusterFindings, JsonRoundTrip)


def test_input_to_json_round_trips_fields() -> None:
    """ClusteringInput.to_json() / from_json() preserves all fields."""
    inp = ClusteringInput(
        eval_result_for_clustering={"rows": []},
        metadata_snapshot={"space_id": "abc"},
        soft_eval_result=None,
        held_out_qids=("gs_001",),
        qid_state={"gs_001": "active"},
    )
    payload = inp.to_json()
    assert payload["metadata_snapshot"]["space_id"] == "abc"
    restored = ClusteringInput.from_json(payload)
    assert restored.held_out_qids == ("gs_001",)
    assert restored.metadata_snapshot["space_id"] == "abc"


def test_output_to_json_round_trips_fields() -> None:
    """ClusterFindings.to_json() / from_json() preserves tuple fields."""
    cluster_a = {"cluster_id": "H001", "failure_type": "wrong_join_spec"}
    cluster_b = {"cluster_id": "H002", "failure_type": "missing_filter"}
    out = ClusterFindings(
        clusters=(cluster_a,),
        soft_clusters=(cluster_b,),
        rejected_cluster_alternatives=(),
    )
    payload = out.to_json()
    restored = ClusterFindings.from_json(payload)
    assert len(restored.clusters) == 1
    assert restored.clusters[0]["cluster_id"] == "H001"
    assert restored.soft_clusters[0]["failure_type"] == "missing_filter"
    assert restored.rejected_cluster_alternatives == ()
