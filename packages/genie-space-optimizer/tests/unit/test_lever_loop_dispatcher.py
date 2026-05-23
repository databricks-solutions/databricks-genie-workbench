"""SM Cutover Phase 4 — lever-loop dispatcher tests.

The dispatcher is the one-flag rollback contract for the SM-first
trial. ``GSO_USE_LEGACY_LEVER_LOOP=true`` must route to
``_run_lever_loop_legacy``; anything else must route to
``_run_lever_loop_sm_first``.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest

from genie_space_optimizer.optimization import harness as _h


def _call_dispatcher() -> object:
    """Call the dispatcher with sentinel arguments.

    All call-site work is patched away — we only assert which body the
    dispatcher selected.
    """
    return _h._run_lever_loop(
        w=object(),
        spark=object(),
        run_id="run-0",
        space_id="space-0",
        domain="ad-hoc",
        benchmarks=[],
        exp_name="exp",
        prev_scores={},
        prev_accuracy=0.0,
        prev_model_id="model-0",
        config={},
        catalog="cat",
        schema="sch",
    )


def test_legacy_flag_true_routes_to_legacy(monkeypatch: pytest.MonkeyPatch) -> None:
    """With ``GSO_USE_LEGACY_LEVER_LOOP=true`` the legacy body runs."""
    monkeypatch.setenv("GSO_USE_LEGACY_LEVER_LOOP", "true")

    legacy_called = {"value": 0}
    sm_called = {"value": 0}

    def _fake_legacy(**_kwargs: object) -> dict:
        legacy_called["value"] += 1
        return {"levers_attempted": [], "levers_accepted": [],
                "levers_rolled_back": [], "iteration_counter": 0,
                "accuracy": 0.0, "model_id": ""}

    def _fake_sm_first(**_kwargs: object) -> dict:
        sm_called["value"] += 1
        return {}

    with patch.object(_h, "_run_lever_loop_legacy", _fake_legacy), \
         patch.object(_h, "_run_lever_loop_sm_first", _fake_sm_first):
        _call_dispatcher()

    assert legacy_called["value"] == 1, "legacy body must be called"
    assert sm_called["value"] == 0, "SM-first body must not be called"


@pytest.mark.parametrize("value", ["false", "0", "no", "off", "", "  "])
def test_legacy_flag_off_routes_to_sm_first(
    monkeypatch: pytest.MonkeyPatch, value: str,
) -> None:
    """Any non-true flag value routes to the SM-first body."""
    monkeypatch.setenv("GSO_USE_LEGACY_LEVER_LOOP", value)

    legacy_called = {"value": 0}
    sm_called = {"value": 0}

    def _fake_legacy(**_kwargs: object) -> dict:
        legacy_called["value"] += 1
        return {}

    def _fake_sm_first(**_kwargs: object) -> dict:
        sm_called["value"] += 1
        return {"levers_attempted": [], "levers_accepted": [],
                "levers_rolled_back": [], "iteration_counter": 0,
                "accuracy": 0.0, "model_id": ""}

    with patch.object(_h, "_run_lever_loop_legacy", _fake_legacy), \
         patch.object(_h, "_run_lever_loop_sm_first", _fake_sm_first):
        _call_dispatcher()

    assert sm_called["value"] == 1, f"SM-first body must run for value={value!r}"
    assert legacy_called["value"] == 0, (
        f"legacy must not run for value={value!r}"
    )


def test_legacy_flag_unset_defaults_to_legacy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """If the env var is unset, the dispatcher defaults to legacy.

    This is the safety default during the Phase 4 PR — the trial-prep
    PR flips it to SM-first once Phase 6 / Phase 7 confirm the path.
    """
    monkeypatch.delenv("GSO_USE_LEGACY_LEVER_LOOP", raising=False)

    legacy_called = {"value": 0}
    sm_called = {"value": 0}

    def _fake_legacy(**_kwargs: object) -> dict:
        legacy_called["value"] += 1
        return {}

    def _fake_sm_first(**_kwargs: object) -> dict:
        sm_called["value"] += 1
        return {}

    with patch.object(_h, "_run_lever_loop_legacy", _fake_legacy), \
         patch.object(_h, "_run_lever_loop_sm_first", _fake_sm_first):
        _call_dispatcher()

    assert legacy_called["value"] == 1
    assert sm_called["value"] == 0


def test_sm_first_stub_delegates_to_legacy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Phase 4 SM-first body is a stub that falls through to legacy.

    The follow-up PR will replace the stub with the ~250 LOC SM-driven
    loop, at which point this test changes to assert SM-first owns the
    iteration end-to-end.
    """
    legacy_called = {"value": 0}

    def _fake_legacy(**_kwargs: object) -> dict:
        legacy_called["value"] += 1
        return {"sentinel": "legacy"}

    with patch.object(_h, "_run_lever_loop_legacy", _fake_legacy):
        result = _h._run_lever_loop_sm_first(
            w=object(), spark=object(), run_id="r", space_id="s",
            domain="d", benchmarks=[], exp_name="e", prev_scores={},
            prev_accuracy=0.0, prev_model_id="m", config={},
            catalog="c", schema="sch",
        )

    assert legacy_called["value"] == 1
    assert result == {"sentinel": "legacy"}
