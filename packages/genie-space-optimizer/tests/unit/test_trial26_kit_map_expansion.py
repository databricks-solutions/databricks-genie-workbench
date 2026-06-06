"""Trial 26 W26.2 — kit-map + mechanism-routing expansion.

Pins:

* When ``GSO_TRIAL26_KIT_MAP_EXPANDED`` is ON (default):
  - ``RCA_KIND_TO_FIXING_MECHANISMS`` answers for ``wrong_aggregation``
    and ``wrong_column`` (the live airline RCA distribution that
    Trial 24's kit gate could not see).
  - ``_TRIAL24_KIT_FOR_RCA`` answers for ``wrong_aggregation`` and
    ``wrong_column`` with matched companion levers.
  - ``plural_top_n_collapse`` normalises to
    ``top_n_cardinality_collapse`` (alias).
  - ``example_sql_is_insufficient_for`` and
    ``recommended_mechanisms_for_rca`` recognise the expanded keys.

* When the flag is OFF, the maps return their pre-Trial-26 shape and
  the expanded keys produce empty / ``None`` (byte-stable rollback).
"""
from __future__ import annotations

import pytest

from genie_space_optimizer.optimization.patch_mechanism import PatchMechanism


# ---------------------------------------------------------------------------
# RCA_KIND_TO_FIXING_MECHANISMS expansion
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "rca_kind, expected_mechanisms",
    [
        (
            "wrong_aggregation",
            frozenset(
                {PatchMechanism.SQL_SNIPPET, PatchMechanism.METADATA_DESCRIPTION}
            ),
        ),
        (
            "wrong_column",
            frozenset(
                {PatchMechanism.METADATA_DESCRIPTION, PatchMechanism.INSTRUCTION_TEXT}
            ),
        ),
    ],
)
def test_recommended_mechanisms_covers_expanded_keys_when_flag_on(
    monkeypatch, rca_kind, expected_mechanisms
):
    monkeypatch.delenv("GSO_TRIAL26_KIT_GATE_REACHABLE", raising=False)
    monkeypatch.delenv("GSO_TRIAL26_KIT_MAP_EXPANDED", raising=False)

    from genie_space_optimizer.optimization.rca_mechanism_routing import (
        example_sql_is_insufficient_for,
        recommended_mechanisms_for_rca,
    )

    assert example_sql_is_insufficient_for(rca_kind) is True
    mechanisms = set(recommended_mechanisms_for_rca(rca_kind))
    assert mechanisms == {m.value for m in expected_mechanisms}


@pytest.mark.parametrize("rca_kind", ["wrong_aggregation", "wrong_column"])
def test_recommended_mechanisms_empty_when_flag_off(monkeypatch, rca_kind):
    monkeypatch.setenv("GSO_TRIAL26_KIT_MAP_EXPANDED", "0")

    from genie_space_optimizer.optimization.rca_mechanism_routing import (
        example_sql_is_insufficient_for,
        recommended_mechanisms_for_rca,
    )

    assert example_sql_is_insufficient_for(rca_kind) is False
    assert recommended_mechanisms_for_rca(rca_kind) == ()


def test_expansion_master_off_forces_subflag_off(monkeypatch):
    """Master rollback also disables W26.2 even with sub-flag ON."""
    monkeypatch.setenv("GSO_TRIAL26_KIT_GATE_REACHABLE", "0")
    monkeypatch.setenv("GSO_TRIAL26_KIT_MAP_EXPANDED", "1")

    from genie_space_optimizer.optimization.rca_mechanism_routing import (
        recommended_mechanisms_for_rca,
    )

    assert recommended_mechanisms_for_rca("wrong_aggregation") == ()


# ---------------------------------------------------------------------------
# plural_top_n_collapse alias
# ---------------------------------------------------------------------------


def test_plural_top_n_collapse_aliases_to_top_n_cardinality_when_flag_on(
    monkeypatch,
):
    monkeypatch.delenv("GSO_TRIAL26_KIT_GATE_REACHABLE", raising=False)
    monkeypatch.delenv("GSO_TRIAL26_KIT_MAP_EXPANDED", raising=False)

    from genie_space_optimizer.optimization.rca_mechanism_routing import (
        _normalize_rca_kind,
        example_sql_is_insufficient_for,
        recommended_mechanisms_for_rca,
    )

    assert _normalize_rca_kind("plural_top_n_collapse") == "top_n_cardinality_collapse"
    assert example_sql_is_insufficient_for("plural_top_n_collapse") is True
    assert set(recommended_mechanisms_for_rca("plural_top_n_collapse")) == {
        PatchMechanism.SQL_SNIPPET.value,
        PatchMechanism.METADATA_DESCRIPTION.value,
    }


def test_plural_top_n_collapse_passthrough_when_flag_off(monkeypatch):
    monkeypatch.setenv("GSO_TRIAL26_KIT_MAP_EXPANDED", "0")

    from genie_space_optimizer.optimization.rca_mechanism_routing import (
        _normalize_rca_kind,
        example_sql_is_insufficient_for,
    )

    assert _normalize_rca_kind("plural_top_n_collapse") == "plural_top_n_collapse"
    assert example_sql_is_insufficient_for("plural_top_n_collapse") is False


# ---------------------------------------------------------------------------
# _TRIAL24_KIT_FOR_RCA expansion (the actual Trial 24 gate)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "rca_kind, expected_levers",
    [
        ("wrong_aggregation", frozenset({"lever-6", "lever-1"})),
        ("wrong_column", frozenset({"lever-1", "lever-5a"})),
    ],
)
def test_kit_for_rca_companions_covers_expanded_keys_when_flag_on(
    monkeypatch, rca_kind, expected_levers
):
    monkeypatch.delenv("GSO_TRIAL26_KIT_GATE_REACHABLE", raising=False)
    monkeypatch.delenv("GSO_TRIAL26_KIT_MAP_EXPANDED", raising=False)
    # Trial 24 master must stay ON so the legacy gate is active too.
    monkeypatch.delenv("GSO_TRIAL24_KIT_AT_SOURCE", raising=False)

    from genie_space_optimizer.optimization.stages.action_groups import (
        _kit_for_rca_companions,
    )

    assert _kit_for_rca_companions(rca_kind) == expected_levers


@pytest.mark.parametrize(
    "rca_kind",
    ["wrong_aggregation", "wrong_column"],
)
def test_kit_for_rca_companions_returns_none_when_w26_2_flag_off(
    monkeypatch, rca_kind
):
    monkeypatch.setenv("GSO_TRIAL26_KIT_MAP_EXPANDED", "0")
    monkeypatch.delenv("GSO_TRIAL24_KIT_AT_SOURCE", raising=False)

    from genie_space_optimizer.optimization.stages.action_groups import (
        _kit_for_rca_companions,
    )

    assert _kit_for_rca_companions(rca_kind) is None


def test_plural_top_n_collapse_resolves_to_top_n_kit_when_flag_on(monkeypatch):
    """Alias routes to ``top_n_cardinality_collapse``'s existing kit."""
    monkeypatch.delenv("GSO_TRIAL26_KIT_GATE_REACHABLE", raising=False)
    monkeypatch.delenv("GSO_TRIAL26_KIT_MAP_EXPANDED", raising=False)
    monkeypatch.delenv("GSO_TRIAL24_KIT_AT_SOURCE", raising=False)

    from genie_space_optimizer.optimization.stages.action_groups import (
        _kit_for_rca_companions,
    )

    expected = frozenset({"lever-6", "lever-1"})  # Trial 24 top_n kit
    assert _kit_for_rca_companions("plural_top_n_collapse") == expected


# ---------------------------------------------------------------------------
# Pre-Trial-26 keys unchanged
# ---------------------------------------------------------------------------


def test_trial24_keys_still_resolve_when_w26_2_off(monkeypatch):
    """W26.2 OFF must leave the Trial 24 keys exactly as they were."""
    monkeypatch.setenv("GSO_TRIAL26_KIT_MAP_EXPANDED", "0")
    monkeypatch.delenv("GSO_TRIAL24_KIT_AT_SOURCE", raising=False)
    monkeypatch.delenv("GSO_TRIAL24_FILTER_REMOVAL_SOLO", raising=False)

    from genie_space_optimizer.optimization.stages.action_groups import (
        _kit_for_rca_companions,
    )

    assert _kit_for_rca_companions("top_n_cardinality_collapse") == frozenset(
        {"lever-6", "lever-1"}
    )


def test_trial24_keys_still_resolve_when_w26_2_on(monkeypatch):
    """W26.2 ON must NOT regress the existing Trial 24 entries."""
    monkeypatch.delenv("GSO_TRIAL26_KIT_GATE_REACHABLE", raising=False)
    monkeypatch.delenv("GSO_TRIAL26_KIT_MAP_EXPANDED", raising=False)
    monkeypatch.delenv("GSO_TRIAL24_KIT_AT_SOURCE", raising=False)
    monkeypatch.delenv("GSO_TRIAL24_FILTER_REMOVAL_SOLO", raising=False)

    from genie_space_optimizer.optimization.stages.action_groups import (
        _kit_for_rca_companions,
    )

    assert _kit_for_rca_companions("top_n_cardinality_collapse") == frozenset(
        {"lever-6", "lever-1"}
    )


def test_unknown_kind_still_returns_none(monkeypatch):
    """A key not in any map (legacy or W26.2-expanded) still returns None."""
    monkeypatch.delenv("GSO_TRIAL26_KIT_GATE_REACHABLE", raising=False)
    monkeypatch.delenv("GSO_TRIAL26_KIT_MAP_EXPANDED", raising=False)

    from genie_space_optimizer.optimization.stages.action_groups import (
        _kit_for_rca_companions,
    )

    assert _kit_for_rca_companions("some_unknown_rca_kind_2026") is None


# ---------------------------------------------------------------------------
# Marker emission for observability
# ---------------------------------------------------------------------------


def test_expanded_key_emits_w26_kit_marker(monkeypatch, capsys):
    """Hitting an expanded entry must emit the
    ``GSO_TRIAL26_KIT_MAP_EXPANDED_V1`` marker so postmortems can prove
    the new coverage is reachable.
    """
    monkeypatch.delenv("GSO_TRIAL26_KIT_GATE_REACHABLE", raising=False)
    monkeypatch.delenv("GSO_TRIAL26_KIT_MAP_EXPANDED", raising=False)
    monkeypatch.delenv("GSO_TRIAL24_KIT_AT_SOURCE", raising=False)

    from genie_space_optimizer.optimization.stages.action_groups import (
        _kit_for_rca_companions,
    )

    _kit_for_rca_companions("wrong_aggregation")
    out = capsys.readouterr().out

    assert "GSO_TRIAL26_KIT_MAP_EXPANDED_V1" in out, (
        f"missing marker in:\n{out}"
    )


def test_legacy_trial24_key_does_not_emit_w26_kit_marker(monkeypatch, capsys):
    """The marker is for W26.2-expanded keys ONLY, never for legacy
    Trial 24 keys (otherwise postmortem counts mean nothing).
    """
    monkeypatch.delenv("GSO_TRIAL26_KIT_GATE_REACHABLE", raising=False)
    monkeypatch.delenv("GSO_TRIAL26_KIT_MAP_EXPANDED", raising=False)
    monkeypatch.delenv("GSO_TRIAL24_KIT_AT_SOURCE", raising=False)
    monkeypatch.delenv("GSO_TRIAL24_FILTER_REMOVAL_SOLO", raising=False)

    from genie_space_optimizer.optimization.stages.action_groups import (
        _kit_for_rca_companions,
    )

    _kit_for_rca_companions("top_n_cardinality_collapse")
    out = capsys.readouterr().out

    assert "GSO_TRIAL26_KIT_MAP_EXPANDED_V1" not in out
