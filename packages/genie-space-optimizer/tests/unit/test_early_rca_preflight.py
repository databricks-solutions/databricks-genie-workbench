"""WU-3 — early RCA preflight at lever_per_iter_setup."""
from __future__ import annotations

import os
from unittest.mock import patch


# ── Task 7: flag ─────────────────────────────────────────────────────


def test_flag_default_on() -> None:
    """WU-3 ships default-ON for the production rollout. The
    preflight runs unless an operator explicitly disables it via
    GSO_EARLY_RCA_PREFLIGHT=0 in app.yaml (rollback path)."""
    from genie_space_optimizer.common.config import (
        early_rca_preflight_enabled,
    )
    with patch.dict(os.environ, {}, clear=False):
        os.environ.pop("GSO_EARLY_RCA_PREFLIGHT", None)
        assert early_rca_preflight_enabled() is True


def test_flag_off_when_explicit_zero() -> None:
    """Rollback path — operators can force the pre-WU-3 control flow
    back on by setting GSO_EARLY_RCA_PREFLIGHT=0 in app.yaml."""
    from genie_space_optimizer.common.config import (
        early_rca_preflight_enabled,
    )
    for val in ("0", "false", "no", "off"):
        with patch.dict(os.environ, {"GSO_EARLY_RCA_PREFLIGHT": val}):
            assert early_rca_preflight_enabled() is False, val


def test_flag_on_when_explicit_one() -> None:
    from genie_space_optimizer.common.config import (
        early_rca_preflight_enabled,
    )
    with patch.dict(os.environ, {"GSO_EARLY_RCA_PREFLIGHT": "1"}):
        assert early_rca_preflight_enabled() is True


def test_flag_on_when_truthy_string() -> None:
    """_flag_default_on returns True for the default and for any
    non-falsy value; verify truthy strings stay ON as before."""
    from genie_space_optimizer.common.config import (
        early_rca_preflight_enabled,
    )
    for val in ("true", "yes", "on"):
        with patch.dict(os.environ, {"GSO_EARLY_RCA_PREFLIGHT": val}):
            assert early_rca_preflight_enabled() is True, val


# ── Task 8: preflight module ─────────────────────────────────────────


def test_preflight_module_exposes_run_function() -> None:
    """Module is importable and exposes ``run_early_rca_preflight``."""
    from genie_space_optimizer.optimization.early_rca_preflight import (
        run_early_rca_preflight,
    )
    assert callable(run_early_rca_preflight)


def test_preflight_proceeds_when_all_clusters_grounded() -> None:
    from genie_space_optimizer.optimization.early_rca_preflight import (
        run_early_rca_preflight,
    )
    from genie_space_optimizer.optimization.slate_consumption import (
        SlateAction,
    )

    ag = {"id": "AG_1", "source_cluster_ids": ["H001"]}
    clusters = [
        {"cluster_id": "H001", "rca_card": {"rca_id": "card-001"}},
    ]

    decision, records = run_early_rca_preflight(
        ag=ag,
        clusters=clusters,
        spark=None,
        run_id="run_x",
        iteration=1,
        metadata_snapshot={},
        regenerator=lambda **_: {"rca_id": "should-not-be-called"},
    )

    assert decision.action == SlateAction.PROCEED
    assert records == ()


def test_preflight_passes_live_cluster_to_regenerator() -> None:
    """CRITICAL contract — regenerator receives the LIVE cluster
    object (not a copy) so build_rca_card's in-place mutations
    survive."""
    from genie_space_optimizer.optimization.early_rca_preflight import (
        run_early_rca_preflight,
    )

    ag = {"id": "AG_1", "source_cluster_ids": ["H001"]}
    cluster: dict = {"cluster_id": "H001"}
    clusters = [cluster]
    seen_cluster: list[object] = []
    seen_metadata: list[object] = []
    metadata_snapshot: dict = {"_existing_key": "preserve"}

    def regen(*, cluster, metadata_snapshot, **_kwargs):
        seen_cluster.append(cluster)
        seen_metadata.append(metadata_snapshot)
        metadata_snapshot.setdefault("_rca_card_store", {})["card-001"] = {
            "card_id": "card-001",
            "intended_patch_shape": "structural",
            "root_cause": "rank without partition",
        }
        cluster["rca_card_id"] = "card-001"
        cluster["rca_card"] = {"rca_id": "card-001"}
        return {"rca_id": "card-001", "attempted_sources": ("asi",)}

    run_early_rca_preflight(
        ag=ag,
        clusters=clusters,
        spark=None,
        run_id="run_x",
        iteration=1,
        metadata_snapshot=metadata_snapshot,
        regenerator=regen,
    )

    assert seen_cluster[0] is cluster, (
        "regenerator received a copy of cluster; build_rca_card's "
        "in-place mutations would be lost"
    )
    assert seen_metadata[0] is metadata_snapshot, (
        "regenerator received a copy of metadata_snapshot; "
        "build_rca_card's _rca_card_store stash would be lost"
    )
    assert cluster["rca_card_id"] == "card-001"
    assert cluster["rca_card"] == {"rca_id": "card-001"}
    assert metadata_snapshot["_rca_card_store"]["card-001"][
        "intended_patch_shape"
    ] == "structural"


def test_preflight_skip_when_regen_fails() -> None:
    from genie_space_optimizer.optimization.early_rca_preflight import (
        run_early_rca_preflight,
    )
    from genie_space_optimizer.optimization.slate_consumption import (
        SlateAction,
    )

    ag = {"id": "AG_1", "source_cluster_ids": ["H001"]}
    cluster = {"cluster_id": "H001"}
    clusters = [cluster]

    decision, records = run_early_rca_preflight(
        ag=ag,
        clusters=clusters,
        spark=None,
        run_id="run_x",
        iteration=1,
        metadata_snapshot={},
        regenerator=lambda **_: {"rca_id": "", "attempted_sources": ("asi",)},
    )

    assert decision.action == SlateAction.SKIP_AG
    assert decision.reason == "cluster_blocked_no_rca"
    assert decision.denied_ag_id == "AG_1"
    blocked_records = [
        r for r in records if r.get("reason") == "cluster_blocked_no_rca"
    ]
    assert len(blocked_records) == 1
    assert blocked_records[0].get("cluster_id") == "H001"


def test_preflight_uses_decide_slate_action_for_verdict() -> None:
    """The preflight delegates the verdict to the existing
    decide_slate_action. This makes the previously-dead function
    reachable on the anchor failure shape."""
    from unittest.mock import patch as _patch

    from genie_space_optimizer.optimization.slate_consumption import (
        SlateAction,
    )

    ag = {"id": "AG_1", "source_cluster_ids": ["H001"]}
    cluster = {"cluster_id": "H001"}
    clusters = [cluster]

    with _patch(
        "genie_space_optimizer.optimization.early_rca_preflight"
        "._decide_slate_action",
        wraps=__import__(
            "genie_space_optimizer.optimization.slate_consumption",
            fromlist=["decide_slate_action"],
        ).decide_slate_action,
    ) as spy:
        from genie_space_optimizer.optimization.early_rca_preflight import (
            run_early_rca_preflight,
        )

        decision, _ = run_early_rca_preflight(
            ag=ag,
            clusters=clusters,
            spark=None,
            run_id="run_x",
            iteration=1,
            metadata_snapshot={},
            regenerator=lambda **_: {"rca_id": ""},
        )

    assert spy.called, "preflight must call decide_slate_action"
    call_kwargs = spy.call_args.kwargs
    assert call_kwargs["ag"] is ag
    assert "H001" in tuple(call_kwargs["blocked_cluster_ids"])
    assert decision.action == SlateAction.SKIP_AG
    assert decision.reason == "cluster_blocked_no_rca"


def test_preflight_handles_action_group_patches_shape() -> None:
    """When ag.source_cluster_ids is absent but ag.patches[*].cluster_id
    is present, preflight uses patches."""
    from genie_space_optimizer.optimization.early_rca_preflight import (
        run_early_rca_preflight,
    )
    from genie_space_optimizer.optimization.slate_consumption import (
        SlateAction,
    )

    ag = {
        "id": "AG_1",
        "patches": [{"cluster_id": "H001"}, {"cluster_id": "H002"}],
    }
    clusters = [
        {"cluster_id": "H001", "rca_card": {"rca_id": "card-001"}},
        {"cluster_id": "H002", "rca_card": {"rca_id": "card-002"}},
    ]

    decision, _ = run_early_rca_preflight(
        ag=ag,
        clusters=clusters,
        spark=None,
        run_id="run_x",
        iteration=1,
        metadata_snapshot={},
        regenerator=lambda **_: {"rca_id": ""},
    )

    assert decision.action == SlateAction.PROCEED


def test_preflight_uses_cluster_identity_fallback() -> None:
    """Matches WU-1 contract — clusters that expose cluster_id (not
    primary_cluster_id) still resolve correctly."""
    from genie_space_optimizer.optimization.early_rca_preflight import (
        run_early_rca_preflight,
    )
    from genie_space_optimizer.optimization.slate_consumption import (
        SlateAction,
    )

    ag = {"id": "AG_1", "source_cluster_ids": ["H001"]}
    clusters = [
        {"cluster_id": "H001", "rca_card": {"rca_id": "card-001"}},
    ]

    decision, _ = run_early_rca_preflight(
        ag=ag,
        clusters=clusters,
        spark=None,
        run_id="run_x",
        iteration=1,
        metadata_snapshot={},
        regenerator=lambda **_: {"rca_id": ""},
    )

    assert decision.action == SlateAction.PROCEED


# ── Task 9: harness wiring assertion ─────────────────────────────────


def test_harness_imports_preflight_module() -> None:
    """WU-3 wiring contract: harness.py must import
    ``run_early_rca_preflight`` and check the gating flag."""
    from genie_space_optimizer.optimization import harness

    src = open(harness.__file__).read()
    assert "run_early_rca_preflight" in src, (
        "harness.py must wire run_early_rca_preflight at "
        "lever_per_iter_setup for WU-3 to work"
    )
    assert "early_rca_preflight_enabled" in src, (
        "harness.py must check the GSO_EARLY_RCA_PREFLIGHT flag"
    )
