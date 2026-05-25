"""Trial 17 Step 7 — flag-gated softening of ``pick_archetype`` as a
control-flow gate inside ``_derive_asset_slice_from_afs``.

Pins (Trial 17.1 default-on semantics):

- Default (env unset OR set to a non-off value): the helper falls back to
  the ``simple_enumerate`` safety-net archetype and returns a populated
  slice so the cluster proceeds to LLM-led synthesis.
- Explicit opt-out (``GSO_TRIAL17_LEVER_LED_SYNTHESIS=0`` and friends):
  legacy behaviour — the helper returns ``None`` when ``pick_archetype``
  returns ``None``.
"""
from __future__ import annotations

import os
from unittest.mock import patch as mock_patch

import pytest

from genie_space_optimizer.optimization import cluster_driven_synthesis as cds


@pytest.fixture
def afs_with_unknown_failure_type() -> dict:
    # blame_set names a *table* identifier so
    # ``_resolve_asset_by_identifier`` returns a populated asset; the
    # softening branch we're testing fires when ``pick_archetype``
    # itself returns None for the failure_type.
    return {
        "cluster_id": "H_X",
        "failure_type": "",
        "blame_set": ["main.demo.t"],
    }


@pytest.fixture
def snapshot() -> dict:
    return {
        "data_sources": {
            "tables": [
                {
                    "identifier": "main.demo.t",
                    "columns": [
                        {"name": "col", "data_type": "double"},
                    ],
                },
            ],
        },
    }


@pytest.mark.parametrize("off_value", ["0", "false", "no", "off", "FALSE", "Off"])
def test_explicit_opt_out_keeps_legacy_decline_on_missing_archetype(
    afs_with_unknown_failure_type, snapshot, monkeypatch, off_value,
):
    monkeypatch.setenv("GSO_TRIAL17_LEVER_LED_SYNTHESIS", off_value)
    with mock_patch.object(
        cds, "pick_archetype", return_value=None,
    ):
        result = cds._derive_asset_slice_from_afs(
            afs_with_unknown_failure_type, snapshot,
        )
    assert result is None, (
        f"explicit opt-out ({off_value!r}) must restore legacy NO_ARCHETYPE "
        "decline"
    )


@pytest.mark.parametrize(
    "env_setup",
    [
        ("unset", None),
        ("empty", ""),
        ("explicit_on", "1"),
        ("explicit_true", "true"),
    ],
)
def test_default_on_falls_back_to_safety_net_archetype(
    afs_with_unknown_failure_type, snapshot, monkeypatch, env_setup,
):
    label, value = env_setup
    if value is None:
        monkeypatch.delenv("GSO_TRIAL17_LEVER_LED_SYNTHESIS", raising=False)
    else:
        monkeypatch.setenv("GSO_TRIAL17_LEVER_LED_SYNTHESIS", value)
    with mock_patch.object(
        cds, "pick_archetype", return_value=None,
    ):
        result = cds._derive_asset_slice_from_afs(
            afs_with_unknown_failure_type, snapshot,
        )
    assert result is not None, (
        f"under env={label!r} (default-on semantics), the cluster must "
        "proceed instead of declining via NO_ARCHETYPE_OR_SLICE"
    )
    _slice, archetype = result
    assert getattr(archetype, "name", "") == "simple_enumerate"


def test_fallback_helper_picks_simple_enumerate():
    arch = cds._fallback_menu_archetype()
    assert arch is not None
    assert arch.name == "simple_enumerate"
