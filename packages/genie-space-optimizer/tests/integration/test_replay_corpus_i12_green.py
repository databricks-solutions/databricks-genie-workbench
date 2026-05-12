"""Risk 2 — corpus regression: under the Defect-3 strict producer
(``GSO_JOURNEY_PRODUCER_STRICT=1``, default-on), every in-tree replay
fixture must run through ``lever_loop_replay.run_replay`` with zero
``clustered -> soft_signal`` violations.

This is the gating evidence for RCO-2b's strict-mode merge-gate
flip: once Task 2 lands and I12 is wired into HIGH-tier, any
fixture that fails this assertion would cause RCO-2b to block its
own canary trial.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest


# This test file lives at packages/genie-space-optimizer/tests/integration/.
# The fixture roots live under packages/genie-space-optimizer/tests/.
TESTS_ROOT = Path(__file__).resolve().parents[1]
FIXTURE_GLOBS = (
    "replay/fixtures/*.json",
    "integration/fixtures/**/replay_fixture.json",
)


def _discover_fixtures() -> list[Path]:
    fixtures: list[Path] = []
    for pattern in FIXTURE_GLOBS:
        fixtures.extend(TESTS_ROOT.glob(pattern))
    return sorted(set(fixtures))


_DISCOVERED = _discover_fixtures()


@pytest.mark.skipif(
    not _DISCOVERED,
    reason="no replay fixtures discovered in expected locations",
)
@pytest.mark.parametrize("fixture_path", _DISCOVERED, ids=lambda p: p.name)
def test_replay_fixture_has_no_clustered_to_soft_signal_violations(
    fixture_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("GSO_JOURNEY_PRODUCER_STRICT", "1")
    from genie_space_optimizer.optimization.lever_loop_replay import run_replay

    payload = json.loads(fixture_path.read_text())
    result = run_replay(payload)

    offending = [
        v for v in (result.validation.violations or ())
        if "clustered" in (str(getattr(v, "detail", v))).lower()
        and "soft_signal" in (str(getattr(v, "detail", v))).lower()
    ]
    assert not offending, (
        f"fixture {fixture_path.name} still emits "
        f"clustered → soft_signal violations under strict producer: "
        f"{offending!r}. Either re-record the fixture against the "
        f"current Defect-3 producer or quarantine it under "
        f"tests/replay/fixtures/legacy_pre_defect3/."
    )
