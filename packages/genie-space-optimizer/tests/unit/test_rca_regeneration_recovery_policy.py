"""Plan P-D — orchestrator unit tests for
``rca.regenerate_rca_if_policy_permits`` (single cluster) and
``rca.regenerate_rca_for_clusters`` (batch).
"""

from __future__ import annotations


def _cluster(
    cluster_id="H001",
    signature="sig-h001",
    rca_card=False,
    rca_id="rca-stage2",
    base_qids=("airline_gs_009",),
    root_cause="wrong_aggregation",
):
    return {
        "cluster_id": cluster_id,
        "cluster_signature": signature,
        "rca_card": rca_card,
        "rca_id": rca_id,
        "base_question_ids": list(base_qids),
        "question_ids": list(base_qids),
        "affected_questions": list(base_qids),
        "root_cause": root_cause,
    }


def _evidence(qids=("airline_gs_009",)):
    return {
        "_failure_buckets": {q: {"bucket": "wrong_aggregation"} for q in qids},
        "_asi_metadata": {},
    }


def _policy_default():
    from genie_space_optimizer.optimization.rca_execution import (
        RcaRegenerationPolicy,
    )

    return RcaRegenerationPolicy.default()


def _cache():
    from genie_space_optimizer.optimization.rca_execution import (
        RcaRegenerationCache,
    )

    return RcaRegenerationCache()


def test_skipped_when_cluster_already_grounded():
    from genie_space_optimizer.optimization.rca import (
        regenerate_rca_if_policy_permits,
    )

    c = _cluster(rca_card={"sections": [{}]})
    records = regenerate_rca_if_policy_permits(
        cluster=c,
        findings=[],
        evidence_snapshot=_evidence(),
        cache=_cache(),
        policy=_policy_default(),
        run_id="r",
        iteration=1,
        attempt_driver=lambda **_: {"rca_id": "should-not-be-called"},
    )
    assert records == []


def test_classified_then_refused_for_missing_target_qids():
    from genie_space_optimizer.optimization.rca import (
        regenerate_rca_if_policy_permits,
    )

    c = _cluster(base_qids=())
    records = regenerate_rca_if_policy_permits(
        cluster=c,
        findings=[],
        evidence_snapshot=_evidence(qids=()),
        cache=_cache(),
        policy=_policy_default(),
        run_id="r",
        iteration=1,
        attempt_driver=lambda **_: {"rca_id": "should-not-be-called"},
    )
    reason_codes = [r["reason_code"] for r in records]
    assert reason_codes == ["rca_classified_ungrounded"]
    assert records[0]["metrics"]["ungrounded_reason"] == "missing_target_qids"
    assert records[0]["metrics"]["policy_max_attempts"] == 0


def test_classified_then_refused_for_no_evidence_available():
    from genie_space_optimizer.optimization.rca import (
        regenerate_rca_if_policy_permits,
    )

    c = _cluster()
    drove: list[str] = []

    def driver(**kwargs):
        drove.append(kwargs.get("cluster", {}).get("cluster_id", ""))
        return {"rca_id": "irrelevant"}

    records = regenerate_rca_if_policy_permits(
        cluster=c,
        findings=[],
        evidence_snapshot={"_failure_buckets": {}, "_asi_metadata": {}},
        cache=_cache(),
        policy=_policy_default(),
        run_id="r",
        iteration=1,
        attempt_driver=driver,
    )
    assert drove == []
    reason_codes = [r["reason_code"] for r in records]
    assert reason_codes == ["rca_classified_ungrounded"]
    assert records[0]["metrics"]["ungrounded_reason"] == "no_evidence_available"


def test_classified_then_triggered_then_succeeded_for_no_findings():
    from genie_space_optimizer.optimization.rca import (
        regenerate_rca_if_policy_permits,
    )

    c = _cluster()
    cache = _cache()
    records = regenerate_rca_if_policy_permits(
        cluster=c,
        findings=[],
        evidence_snapshot=_evidence(),
        cache=cache,
        policy=_policy_default(),
        run_id="r",
        iteration=1,
        attempt_driver=lambda **_: {
            "rca_id": "rca-recovered",
            "attempted_sources": ("failure_buckets",),
        },
    )
    assert [r["reason_code"] for r in records] == [
        "rca_classified_ungrounded",
        "rca_regeneration_triggered",
        "rca_regeneration_succeeded",
    ]
    assert c["rca_card"] == {"rca_id": "rca-recovered"}
    succeeded = records[-1]
    assert succeeded["metrics"]["ungrounded_reason"] == "no_findings"
    assert succeeded["metrics"]["attempt_number"] == 1


def test_classified_then_triggered_then_exhausted_when_driver_fails():
    from genie_space_optimizer.optimization.rca import (
        regenerate_rca_if_policy_permits,
    )

    c = _cluster()
    cache = _cache()
    records = regenerate_rca_if_policy_permits(
        cluster=c,
        findings=[],
        evidence_snapshot=_evidence(),
        cache=cache,
        policy=_policy_default(),
        run_id="r",
        iteration=1,
        attempt_driver=lambda **_: {
            "rca_id": "",
            "attempted_sources": ("failure_buckets", "asi"),
        },
    )
    # NO_FINDINGS allows 1 attempt — first failure is exhaustion.
    assert [r["reason_code"] for r in records] == [
        "rca_classified_ungrounded",
        "rca_regeneration_triggered",
        "rca_regeneration_exhausted",
    ]
    assert c["rca_card"] is False


def test_short_circuits_on_cached_success_across_iterations():
    from genie_space_optimizer.optimization.rca import (
        regenerate_rca_if_policy_permits,
    )

    c = _cluster()
    cache = _cache()
    success_driver = lambda **_: {
        "rca_id": "rca-iter1",
        "attempted_sources": ("failure_buckets",),
    }
    explode_driver = lambda **_: (_ for _ in ()).throw(
        AssertionError("driver must not be called on cached success")
    )

    iter1 = regenerate_rca_if_policy_permits(
        cluster=c,
        findings=[],
        evidence_snapshot=_evidence(),
        cache=cache,
        policy=_policy_default(),
        run_id="r",
        iteration=1,
        attempt_driver=success_driver,
    )
    # Reset rca_card to simulate the cluster getting reclustered with
    # the same signature on iter 2 (or the in-place stamp not surviving
    # between iterations) — the cache should still short-circuit.
    c["rca_card"] = False
    iter2 = regenerate_rca_if_policy_permits(
        cluster=c,
        findings=[],
        evidence_snapshot=_evidence(),
        cache=cache,
        policy=_policy_default(),
        run_id="r",
        iteration=2,
        attempt_driver=explode_driver,
    )
    assert [r["reason_code"] for r in iter1[-1:]] == ["rca_regeneration_succeeded"]
    # iter2 short-circuits via cache: classified + succeeded (no triggered).
    assert [r["reason_code"] for r in iter2] == [
        "rca_classified_ungrounded",
        "rca_regeneration_succeeded",
    ]
    assert c["rca_card"] == {"rca_id": "rca-iter1"}


def test_independent_budgets_per_typed_reason():
    """A cluster classified NO_FINDINGS on iter 1 and reclassified
    NO_TERM_OVERLAP on iter 2 gets a fresh budget for the second
    typed reason — proves the (signature, reason) cache key."""
    from genie_space_optimizer.optimization.rca import (
        regenerate_rca_if_policy_permits,
    )

    c = _cluster()
    cache = _cache()
    fail_driver = lambda **_: {
        "rca_id": "",
        "attempted_sources": ("failure_buckets",),
    }

    iter1 = regenerate_rca_if_policy_permits(
        cluster=c,
        findings=[],
        evidence_snapshot=_evidence(),
        cache=cache,
        policy=_policy_default(),
        run_id="r",
        iteration=1,
        attempt_driver=fail_driver,
    )
    # Iter 2: same cluster signature, different classifier output
    # (NO_TERM_OVERLAP because findings now overlap qids without
    # term overlap).
    finding_no_terms = {
        "rca_id": "f1",
        "target_qids": ("airline_gs_009",),
        "grounding_terms": (),
        "blame_set": (),
    }
    iter2 = regenerate_rca_if_policy_permits(
        cluster=c,
        findings=[finding_no_terms],
        evidence_snapshot=_evidence(),
        cache=cache,
        policy=_policy_default(),
        run_id="r",
        iteration=2,
        attempt_driver=fail_driver,
    )

    iter1_reasons = {
        (r.get("metrics") or {}).get("ungrounded_reason") for r in iter1
    }
    iter2_reasons = {
        (r.get("metrics") or {}).get("ungrounded_reason") for r in iter2
    }
    assert "no_findings" in iter1_reasons
    assert "no_term_overlap" in iter2_reasons
    # Iter 2 should have triggered a fresh attempt (not short-circuit
    # on iter 1's NO_FINDINGS exhaustion).
    assert "rca_regeneration_triggered" in [r["reason_code"] for r in iter2]


def test_batch_wrapper_processes_only_ungrounded():
    from genie_space_optimizer.optimization.rca import (
        regenerate_rca_for_clusters,
    )

    grounded = _cluster(
        cluster_id="H_OK", signature="sig-ok",
        rca_card={"sections": [{}]},
    )
    ungrounded = _cluster(cluster_id="H_BAD", signature="sig-bad")
    drove: list[str] = []

    def driver(*, cluster, **_):
        drove.append(cluster["cluster_id"])
        return {"rca_id": "rca-x", "attempted_sources": ("failure_buckets",)}

    records = regenerate_rca_for_clusters(
        clusters=[grounded, ungrounded],
        findings_by_cluster_id={"H_BAD": []},
        evidence_snapshot=_evidence(),
        cache=_cache(),
        policy=_policy_default(),
        run_id="r",
        iteration=1,
        attempt_driver=driver,
    )
    assert drove == ["H_BAD"]
    cluster_ids = {r["cluster_id"] for r in records}
    assert cluster_ids == {"H_BAD"}
