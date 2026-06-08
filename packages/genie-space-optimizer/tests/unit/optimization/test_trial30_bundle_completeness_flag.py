"""Trial 30 W30.3 — bundle-completeness sub-flag gating.

Mirrors the existing Trial 30 sub-flag semantics: default ON when the
master is ON, forced OFF when the master is OFF (byte-stable rollback),
and independently opt-outable via its own env var.
"""
import importlib

import genie_space_optimizer.optimization.trial30_flags as flags


def _reload():
    return importlib.reload(flags)


def test_default_on_when_master_on(monkeypatch):
    monkeypatch.delenv("GSO_TRIAL30_ENFORCED_SWITCH", raising=False)
    monkeypatch.delenv("GSO_TRIAL30_BUNDLE_COMPLETENESS", raising=False)
    f = _reload()
    assert f.trial30_bundle_completeness_enabled() is True


def test_forced_off_when_master_off(monkeypatch):
    monkeypatch.setenv("GSO_TRIAL30_ENFORCED_SWITCH", "0")
    monkeypatch.delenv("GSO_TRIAL30_BUNDLE_COMPLETENESS", raising=False)
    f = _reload()
    # Master kill-switch forces every sub-flag OFF regardless of its own var.
    assert f.trial30_bundle_completeness_enabled() is False


def test_independent_opt_out(monkeypatch):
    monkeypatch.delenv("GSO_TRIAL30_ENFORCED_SWITCH", raising=False)
    monkeypatch.setenv("GSO_TRIAL30_BUNDLE_COMPLETENESS", "off")
    f = _reload()
    assert f.trial30_bundle_completeness_enabled() is False
