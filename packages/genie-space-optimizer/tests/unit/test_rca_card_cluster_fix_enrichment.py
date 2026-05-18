"""Phase 4a — unit tests for the build_rca_card wrapper enrichment.

When ``rca_card_cluster_fix_enrichment_enabled`` is ON, the wrapper
folds ``cluster["asi_counterfactual_fixes"]`` (cluster-level plural
aggregate) into every per-qid ``counterfactual_fix`` before calling
``build_card``. This closes the singular/plural impedance mismatch
between judge ASI emission and the deterministic builder.
"""
from __future__ import annotations

from unittest.mock import patch

from genie_space_optimizer.optimization.rca import build_rca_card


_GS_009_CLUSTER_FIXES = [
    "Result mismatch: expected 10 rows, got 16 rows.",
    (
        "Use ROW_NUMBER() instead of RANK() for route_rank to ensure exactly "
        "10 rows are returned, or use LIMIT 10 after filtering fare_rank = 1"
    ),
    "Use LIMIT 10 to avoid ties producing more than 10 rows.",
]


def _capture_asi_passed_to_build_card():
    captured: dict = {}

    def _stub_build_card(**kwargs):
        captured.update(kwargs)
        return None, "ungrounded_term", None

    return captured, _stub_build_card


def test_wrapper_folds_cluster_fixes_when_per_qid_is_empty() -> None:
    captured, stub = _capture_asi_passed_to_build_card()
    cluster = {
        "cluster_id": "H001",
        "asi_counterfactual_fixes": _GS_009_CLUSTER_FIXES,
    }
    asi_by_qid = {
        "airline_gs_009": {
            "failure_type": "unknown",
            "blame_set": [],
            "counterfactual_fix": "",
        },
    }
    with patch(
        "genie_space_optimizer.optimization.rca_card_builder.build_card",
        side_effect=stub,
    ):
        build_rca_card(
            cluster_id="H001",
            qids=("airline_gs_009",),
            asi_metadata=asi_by_qid,
            cluster=cluster,
        )
    enriched = captured["asi_by_qid"]["airline_gs_009"]["counterfactual_fix"]
    assert "ROW_NUMBER" in enriched
    assert "LIMIT 10" in enriched


def test_wrapper_folds_cluster_fixes_when_per_qid_is_boilerplate() -> None:
    captured, stub = _capture_asi_passed_to_build_card()
    cluster = {
        "cluster_id": "H001",
        "asi_counterfactual_fixes": _GS_009_CLUSTER_FIXES,
    }
    asi_by_qid = {
        "airline_gs_009": {
            "failure_type": "unknown",
            "blame_set": [],
            "counterfactual_fix": (
                "Result mismatch: expected 10 rows, got 16 rows."
            ),
        },
    }
    with patch(
        "genie_space_optimizer.optimization.rca_card_builder.build_card",
        side_effect=stub,
    ):
        build_rca_card(
            cluster_id="H001",
            qids=("airline_gs_009",),
            asi_metadata=asi_by_qid,
            cluster=cluster,
        )
    enriched = captured["asi_by_qid"]["airline_gs_009"]["counterfactual_fix"]
    assert "ROW_NUMBER" in enriched


def test_wrapper_preserves_substantive_per_qid_fix() -> None:
    captured, stub = _capture_asi_passed_to_build_card()
    cluster = {
        "cluster_id": "H001",
        "asi_counterfactual_fixes": _GS_009_CLUSTER_FIXES,
    }
    asi_by_qid = {
        "airline_gs_009": {
            "failure_type": "unknown",
            "blame_set": [],
            # Substantive per-qid fix — should NOT be overwritten.
            "counterfactual_fix": (
                "The Genie Space should add a metric view dimension."
            ),
        },
    }
    with patch(
        "genie_space_optimizer.optimization.rca_card_builder.build_card",
        side_effect=stub,
    ):
        build_rca_card(
            cluster_id="H001",
            qids=("airline_gs_009",),
            asi_metadata=asi_by_qid,
            cluster=cluster,
        )
    enriched = captured["asi_by_qid"]["airline_gs_009"]["counterfactual_fix"]
    # Per-qid prose is preserved; cluster prose is appended.
    assert "metric view dimension" in enriched
    assert "ROW_NUMBER" in enriched


def test_wrapper_is_noop_when_cluster_has_no_fixes() -> None:
    captured, stub = _capture_asi_passed_to_build_card()
    cluster = {"cluster_id": "H001"}
    asi_by_qid = {
        "airline_gs_009": {
            "failure_type": "unknown",
            "blame_set": [],
            "counterfactual_fix": "original prose",
        },
    }
    with patch(
        "genie_space_optimizer.optimization.rca_card_builder.build_card",
        side_effect=stub,
    ):
        build_rca_card(
            cluster_id="H001",
            qids=("airline_gs_009",),
            asi_metadata=asi_by_qid,
            cluster=cluster,
        )
    assert (
        captured["asi_by_qid"]["airline_gs_009"]["counterfactual_fix"]
        == "original prose"
    )
