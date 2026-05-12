"""Defect Plan 3 (2026-05-12) — ccf1d60d anchor regression fixture.

The ccf1d60d 7Now consolidating-trial run
(``runid_analysis/ccf1d60d-d686-467b-bafa-1640131b4393``) exhibited
5 illegal trunk transitions on local replay, all
``clustered → soft_signal`` for ``7now_delivery_analytics_space_gs_021``
across 5 iterations. Postmortem F7:

    Violation details: `trunk: clustered -> soft_signal` repeated
    for `7now_delivery_analytics_space_gs_021`.

Defect 3 closes this by flipping ``journey_producer_strict_enabled()``
to default-ON. The producer mutual-exclusion logic from Cycle 17 T2
is already shipped and tested; the flip is the entire delta.

This test pins the closeout across both regimes:

    setenv=0  → legacy: 5 cluster→soft violations still observed
    delenv    → DEFAULT POST-FLIP: 0 violations, is_valid=True
    setenv=1  → explicit: 0 violations, is_valid=True

If a future change to the producer reintroduces the dual-emit, the
``delenv`` and ``setenv=1`` cases fail loudly with the actual
violation count. **Forward dependency**: every cycle that touches
``_replay_iteration``, ``emit_cluster_membership_events``, or
``_LEGAL_NEXT`` must re-run this fixture and confirm byte-stability.
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
    / "run_ccf1d60d_7now.json"
)


@pytest.fixture(scope="module")
def fixture() -> dict:
    if not FIXTURE_PATH.exists():
        pytest.skip(f"ccf1d60d anchor fixture missing at {FIXTURE_PATH}")
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
    iters = fixture.get("iterations") or []
    assert len(iters) == 5, f"expected 5 iterations; got {len(iters)}"


def test_anchor_setenv_zero_preserves_legacy_violations(
    fixture, monkeypatch,
):
    """Legacy regression branch: with ``GSO_JOURNEY_PRODUCER_STRICT=0``
    explicitly set, the producer keeps the redundant ``soft_signal``
    emit and the contract sees 5 × ``clustered → soft_signal``
    violations. Locks the legacy regime regardless of what the
    flag's default is."""
    monkeypatch.setenv("GSO_JOURNEY_PRODUCER_STRICT", "0")
    from genie_space_optimizer.common import config

    importlib.reload(config)

    result = run_replay(fixture)
    counts = _transition_counter(result.validation.violations)
    assert counts.get("clustered -> soft_signal", 0) == 5, (
        f"legacy off-branch should retain 5 × clustered → soft_signal; "
        f"got {counts!r}"
    )
    assert not result.validation.is_valid, (
        "legacy off-branch replay must remain invalid (gs_021 leg open)"
    )


def test_anchor_delenv_uses_default_and_clears_all_violations(
    fixture, monkeypatch,
):
    """**Post-Defect-3 default**: with the env var unset, the
    flag accessor returns True (``_flag_default_on``). The producer
    suppresses the redundant ``soft_signal`` emit for gs_021 across
    all 5 iterations. Replay validation passes.

    Pre-Defect-3 this assertion fails because the default was OFF.
    The failure mode is exactly the 5 × ``clustered → soft_signal``
    violations the legacy-branch test pins.
    """
    monkeypatch.delenv("GSO_JOURNEY_PRODUCER_STRICT", raising=False)
    from genie_space_optimizer.common import config

    importlib.reload(config)

    result = run_replay(fixture)
    counts = _transition_counter(result.validation.violations)
    assert counts.get("clustered -> soft_signal", 0) == 0, (
        f"default-on path must clear clustered → soft_signal; "
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
        "replay validation must be valid under default-on producer strict"
    )


def test_anchor_setenv_one_explicit_on_clears_all_violations(
    fixture, monkeypatch,
):
    """Defense-in-depth: explicit ``=1`` matches the delenv-default
    behaviour. Catches a future regression where the default-on path
    diverges from the explicit-on path (e.g. a typo in
    ``_flag_default_on``).
    """
    monkeypatch.setenv("GSO_JOURNEY_PRODUCER_STRICT", "1")
    from genie_space_optimizer.common import config

    importlib.reload(config)

    result = run_replay(fixture)
    counts = _transition_counter(result.validation.violations)
    assert counts == Counter(), (
        f"explicit-on path must clear all illegal trunk transitions; "
        f"got {counts!r}"
    )
    assert result.validation.is_valid
