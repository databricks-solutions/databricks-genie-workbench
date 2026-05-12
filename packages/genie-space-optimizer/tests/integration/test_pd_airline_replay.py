"""Plan P-D Task 9 — airline-trial regression (run 31ecd96f).

Reproduces the iter-1 condition: two target clusters arrive
``rca_ungrounded`` from Stage 2 with parent ``rca_id`` set but
falsy ``rca_card`` and evidence buckets present (so the typed
classifier returns ``NO_FINDINGS``). Pre-P-D, after Defect Plan 1
G1 lands, both clusters short-circuit with ``cluster_blocked_no_rca``
and the run terminates STALLED. Post-P-D, the policy classifies
NO_FINDINGS, permits one regen attempt, and (when the simulated
driver succeeds) the clusters proceed to AG-emit.
"""

from __future__ import annotations


def _airline_clusters():
    return [
        {
            "cluster_id": "H001",
            "cluster_signature": "sig-h001-airline-iter1",
            "rca_id": "rca-stage2-h001",
            "rca_card": False,
            "base_question_ids": [
                "airline_ticketing_and_fare_analysis_gs_009",
            ],
            "question_ids": [
                "airline_ticketing_and_fare_analysis_gs_009",
            ],
            "affected_questions": [
                "airline_ticketing_and_fare_analysis_gs_009",
            ],
            "root_cause": "wrong_aggregation",
        },
        {
            "cluster_id": "H002",
            "cluster_signature": "sig-h002-airline-iter1",
            "rca_id": "rca-stage2-h002",
            "rca_card": False,
            "base_question_ids": [
                "airline_ticketing_and_fare_analysis_gs_024",
            ],
            "question_ids": [
                "airline_ticketing_and_fare_analysis_gs_024",
            ],
            "affected_questions": [
                "airline_ticketing_and_fare_analysis_gs_024",
            ],
            "root_cause": "missing_filter",
        },
    ]


def _evidence_for_airline():
    return {
        "_failure_buckets": {
            "airline_ticketing_and_fare_analysis_gs_009": {"bucket": "wrong_aggregation"},
            "airline_ticketing_and_fare_analysis_gs_024": {"bucket": "missing_filter"},
        },
        "_asi_metadata": {},
    }


def test_pd_airline_iter1_classifies_no_findings_and_recovers(monkeypatch):
    monkeypatch.setenv("GSO_RCA_REGEN_RECOVERY_POLICY", "1")
    from genie_space_optimizer.optimization.harness import (
        run_rca_recovery_for_iteration,
    )

    clusters = _airline_clusters()
    holder: dict = {}

    successful_driver = lambda *, cluster, **_: {
        "rca_id": f"rca-recovered-{cluster['cluster_id']}",
        "attempted_sources": ("failure_buckets",),
    }

    records = run_rca_recovery_for_iteration(
        clusters=clusters,
        findings_by_cluster_id={"H001": [], "H002": []},
        evidence_snapshot=_evidence_for_airline(),
        cache_holder=holder,
        run_id="airline-replay",
        iteration=1,
        attempt_driver=successful_driver,
    )

    assert clusters[0]["rca_card"] == {"rca_id": "rca-recovered-H001"}
    assert clusters[1]["rca_card"] == {"rca_id": "rca-recovered-H002"}

    classified = [r for r in records if r["reason_code"] == "rca_classified_ungrounded"]
    succeeded = [r for r in records if r["reason_code"] == "rca_regeneration_succeeded"]
    assert {c["cluster_id"] for c in classified} == {"H001", "H002"}
    assert {s["cluster_id"] for s in succeeded} == {"H001", "H002"}
    for c in classified:
        assert c["metrics"]["ungrounded_reason"] == "no_findings"
        assert c["metrics"]["policy_max_attempts"] == 1


def test_pd_airline_no_evidence_classifies_as_non_retryable(monkeypatch):
    """Variant of the airline condition where the evidence pipeline
    produced no buckets for the cluster qids. Policy refuses the
    regen — no driver call, falls through to G1 short-circuit."""
    monkeypatch.setenv("GSO_RCA_REGEN_RECOVERY_POLICY", "1")
    from genie_space_optimizer.optimization.harness import (
        run_rca_recovery_for_iteration,
    )

    clusters = _airline_clusters()
    holder: dict = {}
    drove: list[str] = []

    def driver(*, cluster, **_):
        drove.append(cluster["cluster_id"])
        return {"rca_id": "should-not-fire"}

    records = run_rca_recovery_for_iteration(
        clusters=clusters,
        findings_by_cluster_id={"H001": [], "H002": []},
        evidence_snapshot={"_failure_buckets": {}, "_asi_metadata": {}},
        cache_holder=holder,
        run_id="airline-no-evidence",
        iteration=1,
        attempt_driver=driver,
    )

    assert drove == []
    assert all(c["rca_card"] is False for c in clusters)
    classified = [r for r in records if r["reason_code"] == "rca_classified_ungrounded"]
    assert len(classified) == 2
    for c in classified:
        assert c["metrics"]["ungrounded_reason"] == "no_evidence_available"
        assert c["metrics"]["policy_max_attempts"] == 0


def test_pd_airline_failing_driver_caps_at_no_findings_default(monkeypatch):
    """Driver fails on the single permitted attempt. Iter 1 → triggered+exhausted.
    Iter 2 → classified+exhausted (cache short-circuit)."""
    monkeypatch.setenv("GSO_RCA_REGEN_RECOVERY_POLICY", "1")
    from genie_space_optimizer.optimization.harness import (
        run_rca_recovery_for_iteration,
    )

    clusters = _airline_clusters()
    holder: dict = {}
    failing_driver = lambda **_: {
        "rca_id": "",
        "attempted_sources": ("failure_buckets", "asi"),
    }

    iter1 = run_rca_recovery_for_iteration(
        clusters=clusters,
        findings_by_cluster_id={"H001": [], "H002": []},
        evidence_snapshot=_evidence_for_airline(),
        cache_holder=holder,
        run_id="r",
        iteration=1,
        attempt_driver=failing_driver,
    )
    iter2 = run_rca_recovery_for_iteration(
        clusters=clusters,
        findings_by_cluster_id={"H001": [], "H002": []},
        evidence_snapshot=_evidence_for_airline(),
        cache_holder=holder,
        run_id="r",
        iteration=2,
        attempt_driver=failing_driver,
    )

    assert all(c["rca_card"] is False for c in clusters)
    iter1_codes = {r["reason_code"] for r in iter1}
    iter2_codes = {r["reason_code"] for r in iter2}
    assert "rca_regeneration_triggered" in iter1_codes
    assert "rca_regeneration_exhausted" in iter1_codes
    # Iter 2: cache says exhausted → classified + exhausted (no triggered).
    assert "rca_regeneration_triggered" not in iter2_codes
    assert "rca_regeneration_exhausted" in iter2_codes


def test_pd_airline_master_flag_off_is_byte_stable(monkeypatch):
    monkeypatch.setenv("GSO_RCA_REGEN_RECOVERY_POLICY", "0")
    from genie_space_optimizer.optimization.harness import (
        run_rca_recovery_for_iteration,
    )

    clusters = _airline_clusters()
    holder: dict = {}
    records = run_rca_recovery_for_iteration(
        clusters=clusters,
        findings_by_cluster_id={"H001": [], "H002": []},
        evidence_snapshot=_evidence_for_airline(),
        cache_holder=holder,
        run_id="r",
        iteration=1,
        attempt_driver=lambda **_: {"rca_id": "would-have-succeeded"},
    )

    assert records == []
    assert all(c["rca_card"] is False for c in clusters)
    assert "rca_regen_cache" not in holder
    assert "rca_regen_policy" not in holder
