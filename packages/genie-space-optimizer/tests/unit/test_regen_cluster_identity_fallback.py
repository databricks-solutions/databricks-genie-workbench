"""WU-1 — cluster-identity fallback in _regenerate_rca_for_cluster."""
from __future__ import annotations

import os
from unittest.mock import patch


def test_flag_default_on() -> None:
    from genie_space_optimizer.common.config import (
        rca_regen_cluster_identity_fallback_enabled,
    )
    # Default-on per WU-1 contract.
    assert rca_regen_cluster_identity_fallback_enabled() is True


def test_flag_off_when_explicit_zero(monkeypatch) -> None:
    from genie_space_optimizer.common.config import (
        rca_regen_cluster_identity_fallback_enabled,
    )
    monkeypatch.setenv("GSO_RCA_REGEN_CLUSTER_IDENTITY_FALLBACK", "0")
    assert rca_regen_cluster_identity_fallback_enabled() is False


def test_regen_reads_cluster_id_when_primary_absent(monkeypatch) -> None:
    """Repro: failure cluster carries cluster_id, not primary_cluster_id.
    Pre-WU-1 the helper read primary_cluster_id and got ""."""
    from genie_space_optimizer.optimization.harness import (
        _regenerate_rca_for_cluster,
    )

    captured = {}

    def fake_build(*, cluster_id, qids, **_kwargs):
        captured["cluster_id"] = cluster_id
        captured["qids"] = qids
        return {"rca_id": "stub-card"}

    monkeypatch.setattr(
        "genie_space_optimizer.optimization.rca.build_rca_card", fake_build
    )

    failure_cluster = {
        "cluster_id": "H001",
        "question_ids": ("gs_009", "gs_029"),
    }

    out = _regenerate_rca_for_cluster(
        spark=None,
        run_id="run_x",
        cluster=failure_cluster,
        metadata_snapshot={"_failure_buckets": {"sig": ["gs_009"]}},
    )

    assert out["rca_id"] == "stub-card"
    assert captured["cluster_id"] == "H001"
    assert captured["qids"] == ("gs_009", "gs_029")


def test_regen_prefers_primary_when_both_present(monkeypatch) -> None:
    """When BOTH primary_cluster_id and cluster_id are set, the helper
    still prefers primary_cluster_id (matches the action-group shape
    used downstream)."""
    from genie_space_optimizer.optimization.harness import (
        _regenerate_rca_for_cluster,
    )

    captured = {}

    def fake_build(*, cluster_id, qids, **_kwargs):
        captured["cluster_id"] = cluster_id
        captured["qids"] = qids
        return {"rca_id": "stub-card"}

    monkeypatch.setattr(
        "genie_space_optimizer.optimization.rca.build_rca_card", fake_build
    )

    ag_shape_cluster = {
        "primary_cluster_id": "AG_H001_DECOMP",
        "cluster_id": "H001",
        "target_qids": ("gs_009",),
        "question_ids": ("gs_009", "gs_029"),
    }

    out = _regenerate_rca_for_cluster(
        spark=None,
        run_id="run_x",
        cluster=ag_shape_cluster,
        metadata_snapshot={},
    )

    assert out["rca_id"] == "stub-card"
    assert captured["cluster_id"] == "AG_H001_DECOMP"
    assert captured["qids"] == ("gs_009",)


def test_regen_returns_empty_when_both_identities_missing(monkeypatch) -> None:
    """When cluster carries neither identity, regen returns empty."""
    from genie_space_optimizer.optimization.harness import (
        _regenerate_rca_for_cluster,
    )

    monkeypatch.setattr(
        "genie_space_optimizer.optimization.rca.build_rca_card",
        lambda **_k: {"rca_id": ""},
    )

    out = _regenerate_rca_for_cluster(
        spark=None,
        run_id="run_x",
        cluster={},
        metadata_snapshot={},
    )

    assert out["rca_id"] == ""
    assert "failure_buckets" in out["attempted_sources"]
    assert "asi" in out["attempted_sources"]


def test_regen_flag_off_preserves_legacy_reads(monkeypatch) -> None:
    """With the flag OFF, the helper reads primary_cluster_id /
    target_qids only — legacy byte-stable behavior."""
    from genie_space_optimizer.optimization.harness import (
        _regenerate_rca_for_cluster,
    )

    captured = {}

    def fake_build(*, cluster_id, qids, **_kwargs):
        captured["cluster_id"] = cluster_id
        captured["qids"] = qids
        return {"rca_id": "stub-card"}

    monkeypatch.setattr(
        "genie_space_optimizer.optimization.rca.build_rca_card", fake_build
    )
    monkeypatch.setenv("GSO_RCA_REGEN_CLUSTER_IDENTITY_FALLBACK", "0")

    failure_cluster = {
        "cluster_id": "H001",
        "question_ids": ("gs_009",),
    }

    _ = _regenerate_rca_for_cluster(
        spark=None,
        run_id="run_x",
        cluster=failure_cluster,
        metadata_snapshot={},
    )

    # With flag OFF, falls back to legacy reads which find "" / ().
    assert captured["cluster_id"] == ""
    assert captured["qids"] == ()
