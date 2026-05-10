"""Shared replay helpers for boundary-fixture integration tests.

Used by test_chunk_a_replay.py and test_chunk_d_replay.py (Chunk D uses
its own stage-specific assertions for richer failure messages; this helper
is the shared infrastructure for stages whose fixtures exist).
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Iterable

import pytest

from genie_space_optimizer.optimization.stages import get_stage
from genie_space_optimizer.optimization.stages._json_io import JsonRoundTrip


FIXTURES_ROOT = Path(__file__).resolve().parent / "fixtures"


def anchor_dirs() -> list[Path]:
    if not FIXTURES_ROOT.exists():
        return []
    return [p for p in FIXTURES_ROOT.iterdir()
            if p.is_dir() and p.name not in {"__pycache__"}]


def cases_for_chunk(stage_keys: Iterable[str]) -> list:
    """Return parametrize cases for (anchor, stage_key) pairs where
    both input.json and expected_output.json exist under the fixture dir.

    Returns an empty list if no fixtures exist for any stage_key — the
    parametrized test will then be collected but immediately skipped
    (pytest deselects empty parametrize sets by default). Use
    ``pytest.mark.skipif(len(cases) == 0, ...)`` at the call site if
    explicit skip messaging is desired.
    """
    cases = []
    for anchor in anchor_dirs():
        for stage_key in stage_keys:
            stage_dir = anchor / stage_key
            if (
                (stage_dir / "input.json").exists()
                and (stage_dir / "expected_output.json").exists()
            ):
                cases.append(
                    pytest.param(
                        anchor.name,
                        stage_key,
                        id=f"{anchor.name}::{stage_key}",
                    )
                )
    return cases


class DummyCtx:
    """Minimal context object for replay tests.

    Satisfies the ctx.run_id / ctx.iteration / ctx.decision_emit /
    ctx.journey_emit interface that stage execute() functions may call.
    """

    def __init__(self, run_id: str, iteration: int) -> None:
        self.run_id = run_id
        self.iteration = iteration
        self.space_id = "anchor"
        self.domain = "anchor"
        self.catalog = "x"
        self.schema = "x"
        self.apply_mode = "dry_run"
        self._journey: list[dict] = []
        self._decisions: list = []

    def journey_emit(self, **kw) -> None:  # noqa: ANN003
        self._journey.append(kw)

    def decision_emit(self, rec) -> None:  # noqa: ANN001
        self._decisions.append(rec)


def assert_replay_matches(anchor: str, stage_key: str) -> None:
    """Load fixture → execute stage → compare to expected output key-by-key.

    This is the generic replay assertion. Stages with richer comparison
    logic (like acceptance_decision checking outcomes_by_ag per-AG) should
    use their own assertion in the stage-specific test file.
    """
    stage = get_stage(stage_key)
    in_payload = json.loads(
        (FIXTURES_ROOT / anchor / stage_key / "input.json").read_text()
    )
    expected = json.loads(
        (FIXTURES_ROOT / anchor / stage_key / "expected_output.json").read_text()
    )
    inp = stage.input_class.from_json(in_payload)
    ctx = DummyCtx(
        run_id=f"replay-{anchor}",
        iteration=int(in_payload.get("iteration", 1)),
    )
    actual_obj = stage.execute(ctx, inp)
    actual = (
        actual_obj.to_json()
        if isinstance(actual_obj, JsonRoundTrip)
        else actual_obj
    )

    if not isinstance(actual, dict) or not isinstance(expected, dict):
        assert actual == expected, (
            f"{anchor}::{stage_key}: output is not a dict; "
            f"actual type={type(actual).__name__}"
        )
        return

    actual_keys = set(actual.keys())
    expected_keys = set(expected.keys())
    assert actual_keys == expected_keys, (
        f"key drift on {anchor}::{stage_key}: "
        f"missing={expected_keys - actual_keys}, "
        f"extra={actual_keys - expected_keys}"
    )
    drifts = []
    for k in sorted(expected_keys):
        if actual[k] != expected[k]:
            drifts.append((k, expected[k], actual[k]))
    assert not drifts, (
        f"{anchor}::{stage_key} drifted on:\n"
        + "\n".join(
            f"  {k}: expected={e!r} actual={a!r}" for k, e, a in drifts[:5]
        )
    )
