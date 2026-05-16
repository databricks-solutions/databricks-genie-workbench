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
