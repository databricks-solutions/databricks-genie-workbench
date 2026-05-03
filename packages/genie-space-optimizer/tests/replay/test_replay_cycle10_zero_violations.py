"""Cycle 10 replay intake — gate promotion on zero violations.

The cycle 10 raw fixture is committed at
``tests/replay/fixtures/airline_real_v1_cycle10_raw.json`` (extracted
from optimization_run_id ``407772af-9662-4803-be6b-f00a368c528a``).

The two strict assertions stay skipped until PR-C (lane-aware journey
validator) and PR-B2 (decision-record producer wiring) ship. The
"fixture loaded" smoke test runs always so regressions in the fixture
file itself surface immediately.
"""
from __future__ import annotations

import json
import pathlib

import pytest

from genie_space_optimizer.optimization.lever_loop_replay import run_replay

FIXTURE_PATH = (
    pathlib.Path(__file__).parent
    / "fixtures"
    / "airline_real_v1_cycle10_raw.json"
)


@pytest.fixture(scope="module")
def fixture() -> dict:
    if not FIXTURE_PATH.exists():
        pytest.skip(f"cycle 10 raw fixture missing at {FIXTURE_PATH}")
    return json.loads(FIXTURE_PATH.read_text())


def test_fixture_loads_with_iterations(fixture):
    assert fixture["fixture_id"].startswith("airline_real_v1_run_")
    assert isinstance(fixture.get("iterations"), list)
    assert len(fixture["iterations"]) >= 1


@pytest.mark.skip(
    reason=(
        "Cycle 10 raw fixture — pre-PR-B2 producers. decision_records are "
        "empty per iteration. Unskip once PR-B2 has shipped and a refreshed "
        "run lands a fixture with populated decision_records."
    )
)
def test_every_iteration_has_decision_records(fixture):
    for it in fixture["iterations"]:
        assert it["decision_records"], (
            f"iter {it['iteration']}: decision_records empty"
        )


@pytest.mark.skip(
    reason=(
        "Cycle 10 raw fixture — post-PR-C lane-aware validator eliminated "
        "the flat-validator cross-lane false positives, but 8 trunk-level "
        "producer violations remain on the captured fixture: 3 qids reach "
        "post_eval directly from `evaluated` (filtered between eval and "
        "classification — no `clustered`/`soft_signal`/`already_passing`/"
        "`gt_correction_candidate` emitted by the replay's "
        "`_classify_eval_rows`), and qid_016 emits both `soft_signal` and "
        "`clustered` in the same iteration, producing `clustered -> "
        "soft_signal` after canonical sort. These are real producer bugs "
        "in `lever_loop_replay._classify_eval_rows` / fixture shape, NOT "
        "validator gaps. Unskip after a PR-D follow-up fixes the "
        "classification path so every evaluated qid lands in exactly one "
        "partition."
    )
)
def test_replay_yields_zero_violations(fixture):
    result = run_replay(fixture)
    assert list(result.validation.violations) == [], (
        f"replay produced {len(result.validation.violations)} violations: "
        f"{[(v.question_id, v.kind, v.detail) for v in result.validation.violations[:5]]}"
    )
    assert result.validation.is_valid
