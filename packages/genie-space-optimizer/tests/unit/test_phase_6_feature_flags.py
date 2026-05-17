"""Phase 6 — feature flag accessors for control-plane authoritativeness."""
from __future__ import annotations

import pytest

from genie_space_optimizer.common.config import (
    abort_run_authoritative_enabled,
    forbidden_set_terminal_signature_axis_enabled,
    forced_synthesis_unconditional_enabled,
)


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    for k in (
        "GSO_FORBIDDEN_SET_TERMINAL_SIGNATURE_AXIS_ENABLED",
        "GSO_ABORT_RUN_AUTHORITATIVE_ENABLED",
        "GSO_FORCED_SYNTHESIS_UNCONDITIONAL_ENABLED",
    ):
        monkeypatch.delenv(k, raising=False)


def test_forbidden_set_terminal_signature_axis_defaults_on():
    assert forbidden_set_terminal_signature_axis_enabled() is True


def test_forbidden_set_terminal_signature_axis_off_when_zero(monkeypatch):
    monkeypatch.setenv(
        "GSO_FORBIDDEN_SET_TERMINAL_SIGNATURE_AXIS_ENABLED", "0",
    )
    assert forbidden_set_terminal_signature_axis_enabled() is False


def test_abort_run_authoritative_defaults_on():
    assert abort_run_authoritative_enabled() is True


def test_abort_run_authoritative_off_when_zero(monkeypatch):
    monkeypatch.setenv("GSO_ABORT_RUN_AUTHORITATIVE_ENABLED", "0")
    assert abort_run_authoritative_enabled() is False


def test_forced_synthesis_unconditional_defaults_on():
    assert forced_synthesis_unconditional_enabled() is True


def test_forced_synthesis_unconditional_off_when_zero(monkeypatch):
    monkeypatch.setenv("GSO_FORCED_SYNTHESIS_UNCONDITIONAL_ENABLED", "0")
    assert forced_synthesis_unconditional_enabled() is False
