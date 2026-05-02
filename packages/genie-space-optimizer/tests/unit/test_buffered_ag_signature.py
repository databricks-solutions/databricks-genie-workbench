"""Track D — diagnostic and buffered AGs must be keyed by a stable
signature derived from cluster_signature and qid set, not by the
human-facing H00N cluster label which re-numbers every iteration.
"""
from __future__ import annotations


def test_compute_ag_stable_signature_reads_cluster_signature_set() -> None:
    """The signature must be stable across iterations — derived from
    cluster_signature, qid set, and root-cause family, not cluster_id.
    """
    from genie_space_optimizer.optimization.control_plane import (
        compute_ag_stable_signature,
    )

    ag = {
        "id": "AG_COVERAGE_H001",
        "source_cluster_ids": ["H001"],
        "affected_questions": ["q1", "q2"],
        "lever_directives": {"5": {"root_cause": "plural_top_n_collapse"}},
    }
    clusters = [
        {
            "cluster_id": "H001",
            "cluster_signature": "plural_top_n_collapse|cat.sch.fact|cy_sales",
            "question_ids": ["q1", "q2"],
            "root_cause": "plural_top_n_collapse",
        },
    ]

    sig = compute_ag_stable_signature(ag, clusters)
    assert isinstance(sig, tuple), "signature must be hashable"
    # The signature MUST contain the cluster_signature, not the H00N label.
    flat = "|".join(str(x) for part in sig for x in (part if isinstance(part, tuple) else (part,)))
    assert "plural_top_n_collapse|cat.sch.fact|cy_sales" in flat, (
        f"signature missing cluster_signature; got {sig!r}"
    )
    assert "H001" not in flat, (
        f"signature should not include unstable H00N labels; got {sig!r}"
    )


def test_signature_stable_across_iterations_when_cluster_id_renumbers() -> None:
    """Iter-1 produces H001 for cluster_signature S1; iter-2 produces
    H002 for the same cluster_signature. The two AGs derived from S1
    must hash to the same signature so revalidation accepts the buffered
    AG against iter-2's renumbered cluster.
    """
    from genie_space_optimizer.optimization.control_plane import (
        compute_ag_stable_signature,
    )

    ag_iter1 = {
        "id": "AG_COVERAGE_H001",
        "source_cluster_ids": ["H001"],
        "affected_questions": ["q1"],
        "lever_directives": {"5": {"root_cause": "missing_filter"}},
    }
    clusters_iter1 = [{
        "cluster_id": "H001",
        "cluster_signature": "missing_filter|cat.sch.fact|year",
        "question_ids": ["q1"],
        "root_cause": "missing_filter",
    }]

    ag_iter2 = {
        "id": "AG_COVERAGE_H002",
        "source_cluster_ids": ["H002"],
        "affected_questions": ["q1"],
        "lever_directives": {"5": {"root_cause": "missing_filter"}},
    }
    clusters_iter2 = [{
        "cluster_id": "H002",
        "cluster_signature": "missing_filter|cat.sch.fact|year",
        "question_ids": ["q1"],
        "root_cause": "missing_filter",
    }]

    assert (
        compute_ag_stable_signature(ag_iter1, clusters_iter1)
        == compute_ag_stable_signature(ag_iter2, clusters_iter2)
    ), "signature drifted across iterations despite identical semantics"
