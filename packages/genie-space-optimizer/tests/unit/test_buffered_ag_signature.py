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


def test_signature_match_accepts_buffered_ag_when_cluster_id_changed() -> None:
    """End-to-end smoke: an AG buffered with signature S, when revalidated
    against a cluster set where cluster_id has re-numbered but
    cluster_signature is still S, must be accepted as reusable.
    """
    from genie_space_optimizer.optimization.control_plane import (
        compute_ag_stable_signature,
    )

    buffered_ag = {
        "id": "AG_PRIMARY_H001",
        "source_cluster_ids": ["H001"],
        "affected_questions": ["q1"],
        "lever_directives": {"6": {"root_cause": "missing_filter"}},
    }
    clusters_at_buffering = [{
        "cluster_id": "H001",
        "cluster_signature": "missing_filter|cat.sch.fact|year",
        "question_ids": ["q1"],
        "root_cause": "missing_filter",
    }]
    buffered_ag["_stable_signature"] = compute_ag_stable_signature(
        buffered_ag, clusters_at_buffering
    )

    # Iter-2: the same cluster signature now lives at H002.
    iter2_clusters = [{
        "cluster_id": "H002",
        "cluster_signature": "missing_filter|cat.sch.fact|year",
        "question_ids": ["q1"],
        "root_cause": "missing_filter",
    }]
    live_signatures = {c["cluster_signature"] for c in iter2_clusters}

    candidate_sig_set = set(buffered_ag["_stable_signature"][0])
    assert candidate_sig_set & live_signatures, (
        "buffered AG signature should match iter-2 live signatures even "
        "though cluster_id renumbered from H001 to H002"
    )


def test_signature_drift_rejects_buffered_ag_when_cluster_resolved() -> None:
    """An AG buffered against cluster_signature S, when iter-2's clusters
    no longer carry S (because the underlying problem was fixed), must
    fail revalidation. The harness will drop and audit it.
    """
    from genie_space_optimizer.optimization.control_plane import (
        compute_ag_stable_signature,
    )

    buffered_ag = {
        "id": "AG_PRIMARY_H001",
        "source_cluster_ids": ["H001"],
        "affected_questions": ["q1"],
        "lever_directives": {"6": {"root_cause": "missing_filter"}},
    }
    clusters_at_buffering = [{
        "cluster_id": "H001",
        "cluster_signature": "missing_filter|cat.sch.fact|year",
        "question_ids": ["q1"],
        "root_cause": "missing_filter",
    }]
    buffered_ag["_stable_signature"] = compute_ag_stable_signature(
        buffered_ag, clusters_at_buffering
    )

    # Iter-2: signature S no longer appears anywhere — cluster resolved.
    iter2_clusters = [{
        "cluster_id": "H001",
        "cluster_signature": "wrong_aggregation|cat.sch.fact|year",
        "question_ids": ["q2"],
        "root_cause": "wrong_aggregation",
    }]
    live_signatures = {c["cluster_signature"] for c in iter2_clusters}

    candidate_sig_set = set(buffered_ag["_stable_signature"][0])
    assert not (candidate_sig_set & live_signatures), (
        "buffered AG must NOT match iter-2 live signatures when the "
        "underlying cluster_signature drifted"
    )


def test_diagnostic_queue_drain_initializes_src_ids_before_signature_branch() -> None:
    """Regression: diagnostic AGs stamped with _stable_signature (Track D)
    must not crash the queue-drain loop with UnboundLocalError on _src_ids.
    The print at "USING DIAGNOSTIC AG FROM COVERAGE GAP" references _src_ids
    in both the signature path and the legacy id-fallback path, so
    initialization must happen before the if/else fork.
    """
    import inspect

    from genie_space_optimizer.optimization import harness

    src = inspect.getsource(harness)
    drain_idx = src.find(
        "while diagnostic_action_queue and _diag_preempt is None:"
    )
    assert drain_idx >= 0, "diagnostic queue drain block not found"
    print_idx = src.find("USING DIAGNOSTIC AG FROM COVERAGE GAP", drain_idx)
    assert print_idx >= 0, "diagnostic-AG print site not found"

    block = src[drain_idx:print_idx]
    assignment_idx = block.find("_src_ids")
    branch_idx = block.find("if _candidate_sig_set:")
    assert assignment_idx >= 0 and branch_idx >= 0
    assert assignment_idx < branch_idx, (
        "_src_ids must be initialized BEFORE the signature/id-fallback "
        "branch — otherwise the signature path leaves it unbound and the "
        '"USING DIAGNOSTIC AG" print raises UnboundLocalError.'
    )
