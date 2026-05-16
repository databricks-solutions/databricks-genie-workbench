"""Pin the harness's L5-dispatch fixture-emit block.

This test mocks ``_current_iter_inputs`` as a plain dict and verifies
the snapshot block populates the four new keys from in-memory inputs.
It does NOT spin up the full harness — the snapshot logic is pure dict
mutation that we lift out of the harness for testability via a tiny
helper. The helper is implemented inline in the test until Phase 3
extracts it (if needed).
"""
from __future__ import annotations


def test_snapshot_block_populates_four_new_keys() -> None:
    """The snapshot block is a self-contained dict-mutation sequence.
    We translate it into a helper inline and verify it.

    If the harness block is ever changed, this test must be updated to
    mirror it — otherwise the L5 replay loses fidelity. The integration
    parity-check that ties the two together is Task 11.
    """
    _l5_ag_drops = [{
        "ag_id": "AG_DECOMPOSED_H001",
        "source_clusters": ("H001",),
        "root_causes": ("wrong_aggregation",),
        "target_lever": 5,
        "had_example_sqls": False,
        "instruction_sections_dropped": True,
        "instruction_guidance_dropped": False,
    }]
    _iter_source_clusters_by_id = {
        "H001": {
            "cluster_id": "H001",
            "root_cause": "plural_top_n_collapse",
            "question_ids": ["gs_009"],
            "asi_failure_type": "wrong_aggregation",
        },
    }
    _iter_rca_id_by_cluster = {"H001": "rca_h001"}
    metadata_snapshot = {
        "_failure_clusters": [
            {
                "cluster_id": "H001",
                "root_cause": "plural_top_n_collapse",
                "question_ids": ["gs_009"],
                "asi_failure_type": "wrong_aggregation",
            },
        ],
    }
    _current_iter_inputs: dict = {}

    # Run the snapshot block. Translated verbatim from harness.py
    # (Task 7 Step 2 block).
    _l5_drops_for_fixture = [
        {
            "ag_id": str(d.get("ag_id") or ""),
            "source_clusters": [
                str(s) for s in (d.get("source_clusters") or ())
            ],
            "root_causes": [
                str(r) for r in (d.get("root_causes") or ())
            ],
            "target_lever": int(d.get("target_lever") or 0),
            "had_example_sqls": bool(d.get("had_example_sqls")),
            "instruction_sections_dropped": bool(
                d.get("instruction_sections_dropped")
            ),
            "instruction_guidance_dropped": bool(
                d.get("instruction_guidance_dropped")
            ),
        }
        for d in _l5_ag_drops
    ]
    _current_iter_inputs.setdefault("lever5_gate_drops", []).extend(
        _l5_drops_for_fixture
    )
    _current_iter_inputs["iter_source_clusters_by_id"] = {
        str(cid): {
            "cluster_id": str(c.get("cluster_id") or ""),
            "root_cause": str(c.get("root_cause") or ""),
            "question_ids": [
                str(q) for q in (c.get("question_ids") or ()) if q
            ],
            "asi_failure_type": str(c.get("asi_failure_type") or ""),
        }
        for cid, c in _iter_source_clusters_by_id.items()
        if isinstance(c, dict)
    }
    _current_iter_inputs["iter_rca_id_by_cluster"] = {
        str(k): str(v)
        for k, v in (_iter_rca_id_by_cluster or {}).items()
        if v
    }
    _mfc = (
        metadata_snapshot.get("_failure_clusters")
        or metadata_snapshot.get("failure_clusters")
        or []
    )
    _current_iter_inputs["metadata_failure_clusters"] = [
        {
            "cluster_id": str(c.get("cluster_id") or ""),
            "root_cause": str(c.get("root_cause") or ""),
            "question_ids": [
                str(q) for q in (c.get("question_ids") or ()) if q
            ],
            "asi_failure_type": str(c.get("asi_failure_type") or ""),
        }
        for c in _mfc
        if isinstance(c, dict)
    ]

    assert _current_iter_inputs["lever5_gate_drops"][0]["root_causes"] == [
        "wrong_aggregation"
    ]
    assert (
        _current_iter_inputs["iter_source_clusters_by_id"]["H001"][
            "asi_failure_type"
        ]
        == "wrong_aggregation"
    )
    assert (
        _current_iter_inputs["iter_source_clusters_by_id"]["H001"]["root_cause"]
        == "plural_top_n_collapse"
    )
    assert _current_iter_inputs["iter_rca_id_by_cluster"] == {
        "H001": "rca_h001"
    }
    assert (
        _current_iter_inputs["metadata_failure_clusters"][0]["asi_failure_type"]
        == "wrong_aggregation"
    )
