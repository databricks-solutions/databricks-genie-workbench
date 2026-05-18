"""WU-3.5 — full RCA card resolver for downstream intent reads."""
from __future__ import annotations

import os
from unittest.mock import patch


def test_flag_default_on() -> None:
    """WU-3.5 ships default-ON because it is pure read-path
    normalization. The resolver cannot make things worse than the
    current getattr(thin_dict, attr, "") that always reads empty."""
    from genie_space_optimizer.common.config import (
        full_rca_card_resolver_enabled,
    )
    assert full_rca_card_resolver_enabled() is True


def test_flag_off_when_explicit_zero() -> None:
    from genie_space_optimizer.common.config import (
        full_rca_card_resolver_enabled,
    )
    with patch.dict(os.environ, {"GSO_FULL_RCA_CARD_RESOLVER": "0"}):
        assert full_rca_card_resolver_enabled() is False


def test_resolver_module_exposes_function() -> None:
    """Module is importable and exposes the resolver function."""
    from genie_space_optimizer.optimization.rca_card_resolver import (
        resolve_full_rca_card,
    )
    assert callable(resolve_full_rca_card)


def test_resolver_returns_full_card_from_store() -> None:
    """The headline contract — cluster has thin dict, metadata_snapshot
    has full card in _rca_card_store, resolver returns the full card."""
    from genie_space_optimizer.optimization.rca_card_resolver import (
        resolve_full_rca_card,
    )

    class _FakeCard:
        card_id = "card-001"
        intended_patch_shape = "structural"
        root_cause = "rank without partition"

    cluster = {"cluster_id": "H001", "rca_card": {"rca_id": "card-001"}}
    metadata_snapshot = {"_rca_card_store": {"card-001": _FakeCard()}}

    card = resolve_full_rca_card(cluster, metadata_snapshot)
    assert card is not None
    assert card.intended_patch_shape == "structural"
    assert card.root_cause == "rank without partition"
    assert card.card_id == "card-001"


def test_resolver_returns_cluster_card_when_already_full() -> None:
    """If the cluster carries the full card object already (legacy
    pre-Plan-4a paths), return it as-is without consulting the store."""
    from genie_space_optimizer.optimization.rca_card_resolver import (
        resolve_full_rca_card,
    )

    class _FakeCard:
        card_id = "legacy-card"
        intended_patch_shape = "instructional"
        root_cause = "ambiguous_column_reference"

    legacy_card = _FakeCard()
    cluster = {"cluster_id": "H001", "rca_card": legacy_card}
    metadata_snapshot: dict = {}

    card = resolve_full_rca_card(cluster, metadata_snapshot)
    assert card is legacy_card


def test_resolver_uses_cluster_rca_card_id_as_fallback() -> None:
    """If cluster["rca_card"] is missing/empty but cluster["rca_card_id"]
    is set, resolver uses the id field."""
    from genie_space_optimizer.optimization.rca_card_resolver import (
        resolve_full_rca_card,
    )

    class _FakeCard:
        card_id = "card-002"
        intended_patch_shape = "structural"
        root_cause = "grain_mismatch"

    cluster = {"cluster_id": "H002", "rca_card_id": "card-002"}
    metadata_snapshot = {"_rca_card_store": {"card-002": _FakeCard()}}

    card = resolve_full_rca_card(cluster, metadata_snapshot)
    assert card is not None
    assert card.intended_patch_shape == "structural"


def test_resolver_returns_none_when_no_card_anywhere() -> None:
    """No card on cluster, no store, no id → None."""
    from genie_space_optimizer.optimization.rca_card_resolver import (
        resolve_full_rca_card,
    )

    cluster = {"cluster_id": "H003"}
    metadata_snapshot: dict = {}
    assert resolve_full_rca_card(cluster, metadata_snapshot) is None


def test_resolver_returns_none_when_store_missing_id() -> None:
    """Cluster has rca_id but the store doesn't have it (stale or
    builder failure). Resolver must not raise; returns None."""
    from genie_space_optimizer.optimization.rca_card_resolver import (
        resolve_full_rca_card,
    )

    cluster = {"cluster_id": "H004", "rca_card": {"rca_id": "card-missing"}}
    metadata_snapshot = {"_rca_card_store": {"card-other": object()}}
    assert resolve_full_rca_card(cluster, metadata_snapshot) is None


def test_resolver_is_pure_no_mutation() -> None:
    """Resolver MUST NOT mutate the cluster or metadata_snapshot."""
    from genie_space_optimizer.optimization.rca_card_resolver import (
        resolve_full_rca_card,
    )

    class _FakeCard:
        card_id = "card-001"
        intended_patch_shape = "structural"
        root_cause = "x"

    cluster = {"cluster_id": "H001", "rca_card": {"rca_id": "card-001"}}
    metadata_snapshot = {"_rca_card_store": {"card-001": _FakeCard()}}
    cluster_before = dict(cluster)
    snapshot_keys_before = set(metadata_snapshot.keys())

    resolve_full_rca_card(cluster, metadata_snapshot)

    assert cluster == cluster_before
    assert set(metadata_snapshot.keys()) == snapshot_keys_before


def test_resolver_noop_when_flag_off(monkeypatch) -> None:
    """With the flag OFF, resolver returns None unconditionally so
    callers fall back to the legacy getattr path (byte-stable)."""
    from genie_space_optimizer.optimization.rca_card_resolver import (
        resolve_full_rca_card,
    )

    monkeypatch.setenv("GSO_FULL_RCA_CARD_RESOLVER", "0")

    class _FakeCard:
        card_id = "card-001"
        intended_patch_shape = "structural"

    cluster = {"cluster_id": "H001", "rca_card": {"rca_id": "card-001"}}
    metadata_snapshot = {"_rca_card_store": {"card-001": _FakeCard()}}

    assert resolve_full_rca_card(cluster, metadata_snapshot) is None


# ── Task 9c wiring assertions ────────────────────────────────────────


def test_harness_uses_resolver_at_best_of_n_trigger() -> None:
    """WU-3.5 wiring contract — harness.py imports the resolver."""
    from genie_space_optimizer.optimization import harness

    src = open(harness.__file__).read()
    assert "resolve_full_rca_card" in src, (
        "harness.py must wire resolve_full_rca_card at the Best-of-N "
        "trigger and the structural-repair-gate consumer for WU-3.5 "
        "to fire"
    )


def test_harness_no_longer_uses_getattr_for_intended_patch_shape() -> None:
    """The legacy getattr-on-thin-dict pattern must not remain as the
    SOLE intent read at the Best-of-N trigger."""
    from genie_space_optimizer.optimization import harness

    src = open(harness.__file__).read()
    bon_idx = src.find("_bon_card = ")
    if bon_idx < 0:
        return
    window = src[bon_idx : bon_idx + 1200]
    assert (
        "resolve_full_rca_card" in window
        or "_bon_intent_shape" not in window
    ), (
        "Best-of-N trigger reads intent without consulting the "
        "resolver — Task 9c wiring incomplete"
    )
