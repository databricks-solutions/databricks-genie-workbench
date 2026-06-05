"""Trial 19 A6 — ``fallback_no_new_strategy`` marker.

Pins ``regenerate_action_groups_with_signatures`` emitting
``GSO_FALLBACK_NO_NEW_STRATEGY_V1`` when:

* ``inner_regenerate`` returns empty, AND
* The expanded forbidden set has at least one prior terminal or
  insufficient signature.

Flag-gated: ``trial19_enforce_insufficient_enabled()`` must be ON.
"""
from __future__ import annotations

import json

import pytest

from genie_space_optimizer.optimization.stages.action_groups import (
    regenerate_action_groups_with_signatures,
)


def _make_terminal_sig(label: str = "sig-1"):
    class _Sig:
        terminal_reason = "ag_collision_with_forbidden_set"

        def __repr__(self):
            return f"<TerminalSig {label}>"

        def __hash__(self):
            return hash(label)

        def __eq__(self, other):
            return getattr(other, "__class__", None) is _Sig

    return _Sig()


def test_fallback_marker_emitted_when_empty_and_signatures_present(
    capsys, monkeypatch,
):
    monkeypatch.setenv("GSO_TRIAL19_ENFORCE", "1")
    monkeypatch.setenv("GSO_TRIAL19_ENFORCE_INSUFFICIENT", "1")

    def _empty_regen(**kwargs):
        return []

    regenerate_action_groups_with_signatures(
        prior_clusters=[],
        prior_terminal_signatures=[_make_terminal_sig()],
        existing_forbidden_set=set(),
        inner_regenerate=_empty_regen,
        insufficient_repair_signatures=(
            "lever-5:add_example_sql:insufficient:rca=x:behavior=unchanged",
        ),
    )

    captured = capsys.readouterr().out
    assert "GSO_FALLBACK_NO_NEW_STRATEGY_V1" in captured, (
        f"expected marker in stdout; got {captured!r}"
    )

    marker_line = [
        ln for ln in captured.splitlines()
        if ln.startswith("GSO_FALLBACK_NO_NEW_STRATEGY_V1 ")
    ][0]
    payload = json.loads(marker_line.split(" ", 1)[1])
    assert payload["insufficient_signatures_count"] == 1
    assert payload["prior_terminal_signatures_count"] == 1
    assert payload["expanded_forbidden_count"] >= 1


def test_fallback_marker_not_emitted_when_regen_returns_non_empty(
    capsys, monkeypatch,
):
    monkeypatch.setenv("GSO_TRIAL19_ENFORCE", "1")
    monkeypatch.setenv("GSO_TRIAL19_ENFORCE_INSUFFICIENT", "1")

    def _non_empty_regen(**kwargs):
        return [{"id": "ag1"}]

    regenerate_action_groups_with_signatures(
        prior_clusters=[],
        prior_terminal_signatures=[_make_terminal_sig()],
        existing_forbidden_set=set(),
        inner_regenerate=_non_empty_regen,
        insufficient_repair_signatures=("sig-1",),
    )

    out = capsys.readouterr().out
    assert "GSO_FALLBACK_NO_NEW_STRATEGY_V1" not in out


def test_fallback_marker_not_emitted_without_signatures(
    capsys, monkeypatch,
):
    """An empty inner_regenerate result with NO prior signatures is
    just ``no_action_group_emitted`` — a different terminal cause —
    and must NOT emit the A6 marker."""
    monkeypatch.setenv("GSO_TRIAL19_ENFORCE", "1")
    monkeypatch.setenv("GSO_TRIAL19_ENFORCE_INSUFFICIENT", "1")

    def _empty_regen(**kwargs):
        return []

    regenerate_action_groups_with_signatures(
        prior_clusters=[],
        prior_terminal_signatures=[],
        existing_forbidden_set=set(),
        inner_regenerate=_empty_regen,
        insufficient_repair_signatures=(),
    )

    out = capsys.readouterr().out
    assert "GSO_FALLBACK_NO_NEW_STRATEGY_V1" not in out


def test_fallback_marker_not_emitted_when_flag_off(capsys, monkeypatch):
    monkeypatch.setenv("GSO_TRIAL19_ENFORCE_INSUFFICIENT", "0")

    def _empty_regen(**kwargs):
        return []

    regenerate_action_groups_with_signatures(
        prior_clusters=[],
        prior_terminal_signatures=[_make_terminal_sig()],
        existing_forbidden_set=set(),
        inner_regenerate=_empty_regen,
        insufficient_repair_signatures=("sig-1",),
    )

    out = capsys.readouterr().out
    assert "GSO_FALLBACK_NO_NEW_STRATEGY_V1" not in out, (
        "Flag-off must not emit the A6 marker"
    )


def test_insufficient_signatures_unioned_into_forbidden_set():
    """Verify the wrapper unions insufficient signatures into the
    forbidden_set passed to ``inner_regenerate``."""
    captured_forbidden = {}

    def _regen(**kwargs):
        captured_forbidden["set"] = kwargs.get("forbidden_set")
        captured_forbidden["insufficient_kwarg"] = kwargs.get(
            "insufficient_repair_signatures",
        )
        return [{"id": "ag1"}]

    regenerate_action_groups_with_signatures(
        prior_clusters=[],
        prior_terminal_signatures=[],
        existing_forbidden_set={"existing-sig"},
        inner_regenerate=_regen,
        insufficient_repair_signatures=("insuf-1", "insuf-2"),
    )

    assert "existing-sig" in captured_forbidden["set"]
    assert "insuf-1" in captured_forbidden["set"]
    assert "insuf-2" in captured_forbidden["set"]
    # And the raw sequence flows through to inner regen.
    assert captured_forbidden["insufficient_kwarg"] == ("insuf-1", "insuf-2")
