"""WU-2 — RCA regeneration retry tests."""
from __future__ import annotations

from genie_space_optimizer.optimization.rca_regen_retry import (
    RcaRegenRetryResult,
    RcaRegenAttempt,
    retry_rca_regeneration_for_blocked,
)


def test_rca_regen_retry_result_default_empty():
    r = RcaRegenRetryResult(attempts=(), regenerated_cluster_ids=())
    assert r.attempts == ()
    assert r.regenerated_cluster_ids == ()


def test_rca_regen_attempt_carries_cluster_id_and_outcome():
    a = RcaRegenAttempt(
        cluster_id="H001",
        rca_id="rca-1",
        attempted_sources=("failure_buckets", "asi"),
        succeeded=True,
    )
    assert a.cluster_id == "H001"
    assert a.succeeded is True
    assert a.attempted_sources == ("failure_buckets", "asi")


# ── retry helper ─────────────────────────────────────────────────────


def _fake_regenerator_factory(cards_by_cluster):
    """Return a callable mimicking harness._regenerate_rca_for_cluster.

    ``cards_by_cluster`` maps cluster_id -> dict to install on
    ``cluster["rca_card"]`` (or ``None`` to leave the cluster
    ungrounded).
    """
    def _regenerate(*, spark, run_id, cluster, metadata_snapshot,
                    soft_clusters=None):
        cid = str(
            cluster.get("primary_cluster_id")
            or cluster.get("cluster_id")
            or ""
        )
        card = cards_by_cluster.get(cid)
        if card is None:
            return {"rca_id": "", "attempted_sources": ("failure_buckets", "asi")}
        cluster["rca_card"] = card
        return {"rca_id": str(card.get("rca_id") or ""),
                "attempted_sources": ("failure_buckets",)}
    return _regenerate


def test_retry_succeeds_when_regenerator_produces_card():
    clusters = [
        {"cluster_id": "H001", "primary_cluster_id": "H001",
         "target_qids": ["gs_009"], "rca_card": None},
        {"cluster_id": "H002", "primary_cluster_id": "H002",
         "target_qids": ["gs_024"], "rca_card": None},
    ]
    regenerator = _fake_regenerator_factory({
        "H001": {"rca_id": "rca-1", "root_cause": "shape"},
        "H002": None,
    })
    result = retry_rca_regeneration_for_blocked(
        clusters=clusters,
        blocked_cluster_ids=("H001", "H002"),
        spark=None,
        run_id="run-1",
        metadata_snapshot={},
        regenerator=regenerator,
    )
    assert result.regenerated_cluster_ids == ("H001",)
    attempts = {a.cluster_id: a for a in result.attempts}
    assert attempts["H001"].succeeded is True
    assert attempts["H001"].rca_id == "rca-1"
    assert attempts["H002"].succeeded is False
    assert attempts["H002"].rca_id == ""
    assert clusters[0]["rca_card"]["rca_id"] == "rca-1"
    assert clusters[1]["rca_card"] is None


def test_retry_no_op_when_blocked_set_empty():
    clusters = [{"cluster_id": "H001", "rca_card": {"rca_id": "x"}}]
    regenerator = _fake_regenerator_factory({})
    result = retry_rca_regeneration_for_blocked(
        clusters=clusters,
        blocked_cluster_ids=(),
        spark=None,
        run_id="run-1",
        metadata_snapshot={},
        regenerator=regenerator,
    )
    assert result.attempts == ()
    assert result.regenerated_cluster_ids == ()


def test_retry_skips_clusters_not_in_blocked_set():
    clusters = [
        {"cluster_id": "H001", "primary_cluster_id": "H001",
         "rca_card": None},
        {"cluster_id": "H002", "primary_cluster_id": "H002",
         "rca_card": None},
    ]
    regenerator = _fake_regenerator_factory({
        "H001": {"rca_id": "rca-1"},
        "H002": {"rca_id": "rca-2"},
    })
    result = retry_rca_regeneration_for_blocked(
        clusters=clusters,
        blocked_cluster_ids=("H001",),
        spark=None,
        run_id="run-1",
        metadata_snapshot={},
        regenerator=regenerator,
    )
    assert result.regenerated_cluster_ids == ("H001",)
    assert clusters[1]["rca_card"] is None  # H002 was not in blocked set


def test_retry_handles_regenerator_exception_as_failed_attempt():
    clusters = [
        {"cluster_id": "H001", "primary_cluster_id": "H001",
         "rca_card": None}
    ]

    def _raises(**_kwargs):
        raise RuntimeError("regen failed")
    result = retry_rca_regeneration_for_blocked(
        clusters=clusters,
        blocked_cluster_ids=("H001",),
        spark=None,
        run_id="run-1",
        metadata_snapshot={},
        regenerator=_raises,
    )
    assert result.regenerated_cluster_ids == ()
    assert result.attempts[0].cluster_id == "H001"
    assert result.attempts[0].succeeded is False
    assert clusters[0]["rca_card"] is None


# ── Task 8: flag + verdict record + marker ───────────────────────────


import importlib


def test_rca_regen_retry_default_on(monkeypatch):
    monkeypatch.delenv("GSO_RCA_REGEN_RETRY", raising=False)
    from genie_space_optimizer.common import config
    importlib.reload(config)
    assert config.rca_regen_retry_enabled() is True


def test_rca_regen_retry_off_when_env_zero(monkeypatch):
    monkeypatch.setenv("GSO_RCA_REGEN_RETRY", "0")
    from genie_space_optimizer.common import config
    importlib.reload(config)
    assert config.rca_regen_retry_enabled() is False


def test_rca_regen_retry_verdict_record_emits_one_per_cluster():
    from genie_space_optimizer.optimization.decision_emitters import (
        rca_regen_retry_verdict_record,
    )
    from genie_space_optimizer.optimization.rca_decision_trace import (
        DecisionType,
    )
    attempt = RcaRegenAttempt(
        cluster_id="H001",
        rca_id="rca-1",
        attempted_sources=("failure_buckets",),
        succeeded=True,
    )
    rec = rca_regen_retry_verdict_record(
        run_id="run-1",
        iteration=2,
        attempt=attempt,
    )
    d = rec.to_dict()
    assert d["decision_type"] == DecisionType.RCA_REGEN_RETRY_VERDICT.value
    assert d["cluster_id"] == "H001"
    assert d["rca_id"] == "rca-1"
    assert d["metrics"]["succeeded"] is True
    assert d["metrics"]["attempted_sources"] == ["failure_buckets"]


def test_rca_regen_retry_verdict_marker_emits_marker_line():
    from genie_space_optimizer.optimization.run_analysis_contract import (
        rca_regen_retry_verdict_marker,
    )
    line = rca_regen_retry_verdict_marker(
        optimization_run_id="run-1",
        iteration=2,
        cluster_id="H001",
        rca_id="rca-1",
        succeeded=True,
        attempted_sources=("failure_buckets",),
    )
    assert line.startswith("GSO_RCA_REGEN_RETRY_VERDICT_V1 ")
    import json
    payload = json.loads(
        line[len("GSO_RCA_REGEN_RETRY_VERDICT_V1 "):]
    )
    assert payload["cluster_id"] == "H001"
    assert payload["succeeded"] is True
    assert payload["attempted_sources"] == ["failure_buckets"]
