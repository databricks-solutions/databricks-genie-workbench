import importlib

import genie_space_optimizer.optimization.trial30_flags as flags


def _reload(monkeypatch, env: dict[str, str]):
    for k in (
        "GSO_TRIAL30_ENFORCED_SWITCH",
        "GSO_TRIAL30_INERT_HARVEST_WIRE",
        "GSO_TRIAL30_ENFORCE_GUARD",
    ):
        monkeypatch.delenv(k, raising=False)
    for k, v in env.items():
        monkeypatch.setenv(k, v)
    return importlib.reload(flags)


def test_master_default_on(monkeypatch):
    m = _reload(monkeypatch, {})
    assert m.trial30_enforced_switch_enabled() is True


def test_master_opt_out(monkeypatch):
    m = _reload(monkeypatch, {"GSO_TRIAL30_ENFORCED_SWITCH": "0"})
    assert m.trial30_enforced_switch_enabled() is False


def test_subflags_default_on_when_master_on(monkeypatch):
    m = _reload(monkeypatch, {})
    assert m.trial30_inert_harvest_wire_enabled() is True
    assert m.trial30_enforce_guard_enabled() is True


def test_subflags_forced_off_when_master_off(monkeypatch):
    m = _reload(
        monkeypatch,
        {
            "GSO_TRIAL30_ENFORCED_SWITCH": "off",
            "GSO_TRIAL30_INERT_HARVEST_WIRE": "1",
            "GSO_TRIAL30_ENFORCE_GUARD": "1",
        },
    )
    assert m.trial30_inert_harvest_wire_enabled() is False
    assert m.trial30_enforce_guard_enabled() is False


def test_guard_subflag_independent_opt_out(monkeypatch):
    m = _reload(monkeypatch, {"GSO_TRIAL30_ENFORCE_GUARD": "false"})
    assert m.trial30_inert_harvest_wire_enabled() is True
    assert m.trial30_enforce_guard_enabled() is False
