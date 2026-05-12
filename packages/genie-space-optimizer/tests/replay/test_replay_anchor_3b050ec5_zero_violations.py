"""Cycle 17 T4 — 3b050ec5 anchor regression fixture.

The 3b050ec5 anchor (``runid_analysis/3b050ec5-4032-457f-a785-2d1a3942a097``)
is the canonical evidence run for Cycle 17. Postmortem F9 documents
25 illegal trunk transitions on local replay:
  - 10 × ``clustered → already_passing`` for qids gs_001, gs_013 across
    5 iterations.
  - 15 × ``clustered → soft_signal`` for qids gs_012, gs_021, gs_026
    across 5 iterations.

Cycle 17 closes both classes:
  - T1 state-machine extensions (unconditional) legalize
    ``clustered → already_passing``. Flag-off path clears 10/25.
  - T2 producer fix (GSO_JOURNEY_PRODUCER_STRICT, default-off) suppresses
    the redundant ``soft_signal`` emit for hard-clustered qids.
    Flag-on path clears the remaining 15/25.

This test asserts both regimes so a future regression in either
surface fails loudly. **Forward dependency**: every Cycle 18+ that
touches the decision-record stream, journey events, or the canonical
render path must re-run this fixture and assert byte-stability.
"""
from __future__ import annotations

import importlib
import json
import pathlib
from collections import Counter

import pytest

from genie_space_optimizer.optimization.lever_loop_replay import run_replay

FIXTURE_PATH = (
    pathlib.Path(__file__).parent
    / "fixtures"
    / "run_3b050ec5_7now.json"
)


@pytest.fixture(scope="module")
def fixture() -> dict:
    if not FIXTURE_PATH.exists():
        pytest.skip(f"3b050ec5 anchor fixture missing at {FIXTURE_PATH}")
    return json.loads(FIXTURE_PATH.read_text())


def _transition_counter(violations) -> Counter:
    out: Counter = Counter()
    for v in violations:
        if v.kind != "illegal_transition":
            continue
        det = v.detail.split(": ", 1)[-1] if ":" in v.detail else v.detail
        out[det] += 1
    return out


def test_anchor_fixture_loads(fixture):
    assert fixture["fixture_id"] == (
        "airline_real_v1_run_3b050ec5-4032-457f-a785-2d1a3942a097"
    )
    assert len(fixture.get("iterations") or []) == 5


def test_anchor_flag_off_clears_clustered_to_already_passing(
    fixture, monkeypatch,
):
    """Defect Plan 3 — explicit legacy off-branch: with
    ``GSO_JOURNEY_PRODUCER_STRICT=0`` set explicitly, T1's
    unconditional state-machine extension still eliminates the
    10 × ``clustered → already_passing`` violations; the 15 ×
    ``clustered → soft_signal`` violations remain (T2 inactive).

    The branch is pinned via explicit ``setenv=0`` because the
    default is now ON (Defect Plan 3, 2026-05-12). Pre-Defect-3 this
    test used ``monkeypatch.delenv`` which produced the same flag-
    off semantics by accident-of-default; now it must be explicit
    to remain a true legacy regression.
    """
    monkeypatch.setenv("GSO_JOURNEY_PRODUCER_STRICT", "0")
    from genie_space_optimizer.common import config

    importlib.reload(config)

    result = run_replay(fixture)
    counts = _transition_counter(result.validation.violations)
    assert counts.get("clustered -> already_passing", 0) == 0, (
        f"T1 extension #1 must legalize clustered → already_passing; "
        f"got {counts!r}"
    )
    assert counts.get("clustered -> soft_signal", 0) == 15, (
        f"explicit-off legacy branch should retain clustered → soft_signal × 15; "
        f"got {counts!r}"
    )
    assert sum(counts.values()) == 15, (
        f"explicit-off total expected 15; got {sum(counts.values())} "
        f"(transitions={counts!r})"
    )


def test_anchor_flag_on_clears_all_25_violations(fixture, monkeypatch):
    """With flag-on, T2 producer fix suppresses the redundant
    ``soft_signal`` emit. Combined with T1's unconditional extension,
    all 25 illegal trunk transitions clear. ``is_valid`` is True.
    """
    monkeypatch.setenv("GSO_JOURNEY_PRODUCER_STRICT", "1")
    from genie_space_optimizer.common import config

    importlib.reload(config)

    result = run_replay(fixture)
    counts = _transition_counter(result.validation.violations)
    assert counts == Counter(), (
        f"T1+T2 flag-on should clear all illegal trunk transitions; "
        f"got {counts!r}"
    )
    illegal = [
        v for v in result.validation.violations
        if v.kind == "illegal_transition"
    ]
    assert illegal == [], (
        f"expected zero illegal_transition violations; got "
        f"{[(v.question_id, v.detail) for v in illegal[:5]]}"
    )
    assert result.validation.is_valid, (
        "replay validation must be valid under T1+T2 flag-on"
    )
