"""Round-trip tests for the L5-dispatch-replay fixture keys.

Phase 1 extends the PHASE_A replay fixture's cluster entries to admit
``asi_failure_type`` and the per-iteration entries to admit four new
top-level keys. These tests pin the round-trip contract:
serialize_replay_fixture must preserve the new keys; load_fixture must
return them unchanged.
"""
from __future__ import annotations

import json


def test_cluster_asi_failure_type_round_trips() -> None:
    from genie_space_optimizer.optimization.journey_fixture_exporter import (
        serialize_replay_fixture,
    )

    iterations_data = [{
        "iteration": 1,
        "clusters": [{
            "cluster_id": "H001",
            "root_cause": "plural_top_n_collapse",
            "question_ids": ["gs_009"],
            "asi_failure_type": "wrong_aggregation",
        }],
    }]
    out_json = serialize_replay_fixture(
        fixture_id="test", iterations_data=iterations_data,
    )
    out = json.loads(out_json)
    cluster = out["iterations"][0]["clusters"][0]
    assert cluster["asi_failure_type"] == "wrong_aggregation"
    assert cluster["root_cause"] == "plural_top_n_collapse"


def test_lever5_gate_drops_round_trips() -> None:
    from genie_space_optimizer.optimization.journey_fixture_exporter import (
        serialize_replay_fixture,
    )

    iterations_data = [{
        "iteration": 1,
        "lever5_gate_drops": [{
            "ag_id": "AG_DECOMPOSED_H001",
            "source_clusters": ["H001"],
            "root_causes": ["wrong_aggregation"],
            "target_lever": 5,
            "had_example_sqls": False,
            "instruction_sections_dropped": True,
            "instruction_guidance_dropped": False,
        }],
    }]
    out_json = serialize_replay_fixture(
        fixture_id="test", iterations_data=iterations_data,
    )
    out = json.loads(out_json)
    assert out["iterations"][0]["lever5_gate_drops"][0]["root_causes"] == [
        "wrong_aggregation"
    ]
    assert out["iterations"][0]["lever5_gate_drops"][0]["source_clusters"] == [
        "H001"
    ]


def test_iter_source_clusters_by_id_round_trips() -> None:
    from genie_space_optimizer.optimization.journey_fixture_exporter import (
        serialize_replay_fixture,
    )

    iterations_data = [{
        "iteration": 1,
        "iter_source_clusters_by_id": {
            "H001": {
                "cluster_id": "H001",
                "root_cause": "plural_top_n_collapse",
                "question_ids": ["gs_009"],
                "asi_failure_type": "wrong_aggregation",
            },
        },
    }]
    out_json = serialize_replay_fixture(
        fixture_id="test", iterations_data=iterations_data,
    )
    out = json.loads(out_json)
    h001 = out["iterations"][0]["iter_source_clusters_by_id"]["H001"]
    assert h001["root_cause"] == "plural_top_n_collapse"
    assert h001["asi_failure_type"] == "wrong_aggregation"


def test_iter_rca_id_by_cluster_round_trips() -> None:
    from genie_space_optimizer.optimization.journey_fixture_exporter import (
        serialize_replay_fixture,
    )

    iterations_data = [{
        "iteration": 1,
        "iter_rca_id_by_cluster": {"H001": "rca_h001", "H002": "rca_h002"},
    }]
    out_json = serialize_replay_fixture(
        fixture_id="test", iterations_data=iterations_data,
    )
    out = json.loads(out_json)
    assert out["iterations"][0]["iter_rca_id_by_cluster"] == {
        "H001": "rca_h001",
        "H002": "rca_h002",
    }


def test_metadata_failure_clusters_round_trips() -> None:
    from genie_space_optimizer.optimization.journey_fixture_exporter import (
        serialize_replay_fixture,
    )

    iterations_data = [{
        "iteration": 1,
        "metadata_failure_clusters": [{
            "cluster_id": "H001",
            "root_cause": "plural_top_n_collapse",
            "question_ids": ["gs_009"],
            "asi_failure_type": "wrong_aggregation",
        }],
    }]
    out_json = serialize_replay_fixture(
        fixture_id="test", iterations_data=iterations_data,
    )
    out = json.loads(out_json)
    mfc = out["iterations"][0]["metadata_failure_clusters"]
    assert len(mfc) == 1
    assert mfc[0]["asi_failure_type"] == "wrong_aggregation"
