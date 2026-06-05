"""Phase 2 #10 — mechanism-binding pure selectors + flags.

Covers ``mechanism_binding.coverage_survivor_indices`` /
``rca_route_survivor_indices`` (pure index selectors) and the
default-ON binding flags in ``mechanism_binding_flags``.
"""
from __future__ import annotations

import pytest

from genie_space_optimizer.optimization.mechanism_binding import (
    coverage_survivor_indices,
    rca_route_survivor_indices,
)
from genie_space_optimizer.optimization.mechanism_binding_flags import (
    mechanism_coverage_binding_enabled,
    rca_mechanism_route_binding_enabled,
)


# ── coverage_survivor_indices ─────────────────────────────────────────

def test_coverage_observe_only_keeps_all_when_disabled():
    survivors, dropped = coverage_survivor_indices(
        ["covered", "uncovered", "override"],
        binding_enabled=False,
    )
    assert survivors == (0, 1, 2)
    assert dropped == ()


def test_coverage_binding_drops_uncovered():
    survivors, dropped = coverage_survivor_indices(
        ["covered", "uncovered", "override"],
        binding_enabled=True,
    )
    assert survivors == (0, 2)
    assert dropped == (1,)


def test_coverage_binding_never_empties_slate():
    """Slate-safety: all-uncovered keeps all (no flatline)."""
    survivors, dropped = coverage_survivor_indices(
        ["uncovered", "uncovered"],
        binding_enabled=True,
    )
    assert survivors == (0, 1)
    assert dropped == ()


def test_coverage_binding_none_outcome_not_droppable():
    """``None`` (no mechanism mapped) is not subject to the check."""
    survivors, dropped = coverage_survivor_indices(
        [None, "uncovered", "covered"],
        binding_enabled=True,
    )
    assert survivors == (0, 2)
    assert dropped == (1,)


def test_coverage_binding_empty_list():
    survivors, dropped = coverage_survivor_indices([], binding_enabled=True)
    assert survivors == ()
    assert dropped == ()


# ── rca_route_survivor_indices ────────────────────────────────────────

def test_rca_route_observe_only_keeps_all_when_disabled():
    survivors, dropped = rca_route_survivor_indices(
        [True, False],
        binding_enabled=False,
    )
    assert survivors == (0, 1)
    assert dropped == ()


def test_rca_route_binding_drops_defaulted():
    survivors, dropped = rca_route_survivor_indices(
        [True, False, True],
        binding_enabled=True,
    )
    assert survivors == (1,)
    assert dropped == (0, 2)


def test_rca_route_binding_never_empties_slate():
    survivors, dropped = rca_route_survivor_indices(
        [True, True],
        binding_enabled=True,
    )
    assert survivors == (0, 1)
    assert dropped == ()


def test_rca_route_binding_no_defaulted():
    survivors, dropped = rca_route_survivor_indices(
        [False, False],
        binding_enabled=True,
    )
    assert survivors == (0, 1)
    assert dropped == ()


# ── flags default ON ──────────────────────────────────────────────────

def test_coverage_flag_default_on(monkeypatch):
    monkeypatch.delenv("GSO_MECHANISM_COVERAGE_BINDING", raising=False)
    assert mechanism_coverage_binding_enabled() is True


def test_coverage_flag_explicit_on_values(monkeypatch):
    for val in ("1", "true", "yes", "on", "", "maybe"):
        monkeypatch.setenv("GSO_MECHANISM_COVERAGE_BINDING", val)
        assert mechanism_coverage_binding_enabled() is True


def test_coverage_flag_opt_out(monkeypatch):
    for val in ("0", "false", "no", "off"):
        monkeypatch.setenv("GSO_MECHANISM_COVERAGE_BINDING", val)
        assert mechanism_coverage_binding_enabled() is False


def test_rca_route_flag_default_on(monkeypatch):
    monkeypatch.delenv("GSO_RCA_MECHANISM_ROUTE_BINDING", raising=False)
    assert rca_mechanism_route_binding_enabled() is True


def test_rca_route_flag_opt_out(monkeypatch):
    monkeypatch.setenv("GSO_RCA_MECHANISM_ROUTE_BINDING", "0")
    assert rca_mechanism_route_binding_enabled() is False
    monkeypatch.setenv("GSO_RCA_MECHANISM_ROUTE_BINDING", "off")
    assert rca_mechanism_route_binding_enabled() is False
