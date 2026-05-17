"""Phase 6.3 — the Lever 5 structural gate must fire when EITHER
``asi_failure_type`` OR ``root_cause`` is a SQL-shape RCA.

Regression evidence: Run B (ab65fefe) iter 3 had H002 with
``asi_failure_type='other'`` AND ``root_cause='plural_top_n_collapse'``.
The short-circuit ``_rc = (asi_failure_type or root_cause or "")``
read ``'other'``, missed SQL-shape, and the gate did not fire —
leading to zero structural synthesis attempts for that cluster.
"""
from __future__ import annotations

from genie_space_optimizer.optimization.optimizer import (
    _ag_structural_root_causes_for_clusters,
)


def test_gate_fires_on_root_cause_when_asi_failure_type_is_other():
    """The critical Run-B regression case."""
    clusters = [
        {
            "cluster_id": "gs_013",
            "asi_failure_type": "other",
            "root_cause": "plural_top_n_collapse",
        },
    ]
    rcs = _ag_structural_root_causes_for_clusters(
        source_clusters=["gs_013"],
        clusters_by_id={c["cluster_id"]: c for c in clusters},
    )
    assert rcs == {"plural_top_n_collapse"}


def test_gate_fires_on_asi_failure_type_when_root_cause_is_empty():
    """Legacy case — preserved from pre-Phase-6 behavior."""
    clusters = [
        {
            "cluster_id": "gs_024",
            "asi_failure_type": "wrong_aggregation",
            "root_cause": "",
        },
    ]
    rcs = _ag_structural_root_causes_for_clusters(
        source_clusters=["gs_024"],
        clusters_by_id={c["cluster_id"]: c for c in clusters},
    )
    assert rcs == {"wrong_aggregation"}


def test_gate_does_not_fire_when_neither_label_is_sql_shape():
    """Both labels are non-SQL-shape → gate does not fire."""
    clusters = [
        {
            "cluster_id": "gs_100",
            "asi_failure_type": "other",
            "root_cause": "ambiguous_question",
        },
    ]
    rcs = _ag_structural_root_causes_for_clusters(
        source_clusters=["gs_100"],
        clusters_by_id={c["cluster_id"]: c for c in clusters},
    )
    assert rcs == set()


def test_gate_fires_on_both_labels_when_both_are_sql_shape():
    """Both labels are SQL-shape → both added."""
    clusters = [
        {
            "cluster_id": "gs_050",
            "asi_failure_type": "missing_filter",
            "root_cause": "wrong_aggregation",
        },
    ]
    rcs = _ag_structural_root_causes_for_clusters(
        source_clusters=["gs_050"],
        clusters_by_id={c["cluster_id"]: c for c in clusters},
    )
    assert rcs == {"missing_filter", "wrong_aggregation"}


def test_gate_handles_missing_cluster_id_gracefully():
    rcs = _ag_structural_root_causes_for_clusters(
        source_clusters=["gs_missing"],
        clusters_by_id={},
    )
    assert rcs == set()


def test_gate_handles_non_dict_cluster_gracefully():
    rcs = _ag_structural_root_causes_for_clusters(
        source_clusters=["gs_007"],
        clusters_by_id={"gs_007": "not_a_dict"},  # type: ignore[dict-item]
    )
    assert rcs == set()
