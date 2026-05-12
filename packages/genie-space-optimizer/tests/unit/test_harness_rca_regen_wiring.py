"""Plan P-D — unit tests for the harness extracted helper
``run_rca_recovery_for_iteration``."""

from __future__ import annotations


def _cluster(cid="H001", sig="sig-h001", rca_card=False, qids=("q1",)):
    return {
        "cluster_id": cid,
        "cluster_signature": sig,
        "rca_card": rca_card,
        "rca_id": "rca-stage2",
        "base_question_ids": list(qids),
        "question_ids": list(qids),
        "affected_questions": list(qids),
        "root_cause": "wrong_aggregation",
    }


def _evidence_with_buckets(qids=("q1",)):
    return {
        "_failure_buckets": {q: {"bucket": "wrong_aggregation"} for q in qids},
        "_asi_metadata": {},
    }


def test_recovery_no_op_when_master_flag_off(monkeypatch):
    monkeypatch.setenv("GSO_RCA_REGEN_RECOVERY_POLICY", "0")
    from genie_space_optimizer.optimization.harness import (
        run_rca_recovery_for_iteration,
    )

    clusters = [_cluster()]
    cache_holder: dict = {}
    records = run_rca_recovery_for_iteration(
        clusters=clusters,
        findings_by_cluster_id={"H001": []},
        evidence_snapshot=_evidence_with_buckets(),
        cache_holder=cache_holder,
        run_id="r",
        iteration=1,
        attempt_driver=lambda **_: {"rca_id": "would-succeed"},
    )

    assert records == []
    assert clusters[0]["rca_card"] is False
    assert "rca_regen_cache" not in cache_holder
    assert "rca_regen_policy" not in cache_holder


def test_recovery_instantiates_cache_and_policy_on_first_call(monkeypatch):
    monkeypatch.setenv("GSO_RCA_REGEN_RECOVERY_POLICY", "1")
    from genie_space_optimizer.optimization.harness import (
        run_rca_recovery_for_iteration,
    )
    from genie_space_optimizer.optimization.rca_execution import (
        RcaRegenerationCache, RcaRegenerationPolicy,
    )

    clusters = [_cluster()]
    cache_holder: dict = {}
    run_rca_recovery_for_iteration(
        clusters=clusters,
        findings_by_cluster_id={"H001": []},
        evidence_snapshot=_evidence_with_buckets(),
        cache_holder=cache_holder,
        run_id="r",
        iteration=1,
        attempt_driver=lambda **_: {
            "rca_id": "rca-1",
            "attempted_sources": ("failure_buckets",),
        },
    )
    assert isinstance(cache_holder["rca_regen_cache"], RcaRegenerationCache)
    assert isinstance(cache_holder["rca_regen_policy"], RcaRegenerationPolicy)


def test_recovery_reuses_cache_and_policy_across_iterations(monkeypatch):
    monkeypatch.setenv("GSO_RCA_REGEN_RECOVERY_POLICY", "1")
    from genie_space_optimizer.optimization.harness import (
        run_rca_recovery_for_iteration,
    )

    clusters = [_cluster()]
    cache_holder: dict = {}
    fail = lambda **_: {"rca_id": "", "attempted_sources": ("failure_buckets",)}

    run_rca_recovery_for_iteration(
        clusters=clusters,
        findings_by_cluster_id={"H001": []},
        evidence_snapshot=_evidence_with_buckets(),
        cache_holder=cache_holder,
        run_id="r",
        iteration=1,
        attempt_driver=fail,
    )
    cache_after_iter1 = cache_holder["rca_regen_cache"]
    policy_after_iter1 = cache_holder["rca_regen_policy"]

    run_rca_recovery_for_iteration(
        clusters=clusters,
        findings_by_cluster_id={"H001": []},
        evidence_snapshot=_evidence_with_buckets(),
        cache_holder=cache_holder,
        run_id="r",
        iteration=2,
        attempt_driver=fail,
    )
    assert cache_holder["rca_regen_cache"] is cache_after_iter1
    assert cache_holder["rca_regen_policy"] is policy_after_iter1


def test_recovery_swallows_orchestrator_exceptions(monkeypatch):
    monkeypatch.setenv("GSO_RCA_REGEN_RECOVERY_POLICY", "1")
    from genie_space_optimizer.optimization import harness as _h
    from genie_space_optimizer.optimization.harness import (
        run_rca_recovery_for_iteration,
    )

    def _raise(**_):
        raise RuntimeError("simulated regen helper crash")

    monkeypatch.setattr(_h, "_regenerate_rca_for_cluster", _raise)

    clusters = [_cluster()]
    cache_holder: dict = {}
    records = run_rca_recovery_for_iteration(
        clusters=clusters,
        findings_by_cluster_id={"H001": []},
        evidence_snapshot=_evidence_with_buckets(),
        cache_holder=cache_holder,
        run_id="r",
        iteration=1,
    )
    # Orchestrator catches the driver exception and converts it to
    # an empty outcome → records still include classified + triggered
    # + exhausted (NO_FINDINGS, default cap 1, single failed attempt).
    assert clusters[0]["rca_card"] is False
    reason_codes = [r.get("reason_code") for r in records]
    assert "rca_classified_ungrounded" in reason_codes
    assert "rca_regeneration_triggered" in reason_codes


def test_recovery_applies_env_overrides_to_policy(monkeypatch):
    monkeypatch.setenv("GSO_RCA_REGEN_RECOVERY_POLICY", "1")
    monkeypatch.setenv("GSO_RCA_REGEN_MAX_ATTEMPTS_NO_FINDINGS", "0")
    from genie_space_optimizer.optimization.harness import (
        run_rca_recovery_for_iteration,
    )

    clusters = [_cluster()]
    cache_holder: dict = {}
    drove: list[str] = []

    def driver(*, cluster, **_):
        drove.append(cluster["cluster_id"])
        return {"rca_id": "rca-x", "attempted_sources": ()}

    records = run_rca_recovery_for_iteration(
        clusters=clusters,
        findings_by_cluster_id={"H001": []},
        evidence_snapshot=_evidence_with_buckets(),
        cache_holder=cache_holder,
        run_id="r",
        iteration=1,
        attempt_driver=driver,
    )
    # Override forced NO_FINDINGS to cap 0 → policy refuses → no driver call.
    assert drove == []
    reason_codes = [r["reason_code"] for r in records]
    assert reason_codes == ["rca_classified_ungrounded"]
