"""RCO-4b Phase C Task 6 — production-shape fixture tests.

Each fixture directory under ``fixtures/rco4b/p0_gate/`` contains an
``input.json`` carrying the ``p0_gate_input`` and an optional
``post_eval_input``, and an ``expected_output.json`` carrying the
two pure-helper outputs. The test runs both helpers and asserts the
combined output matches.
"""
from __future__ import annotations

import json
import pathlib

import pytest

from genie_space_optimizer.optimization.stages.eval_gates import (
    decide_p0_gate_post_eval,
    decide_p0_gate_should_run,
)
from genie_space_optimizer.optimization.stages.gate_types import (
    P0GateInput,
)


FIXTURE_ROOT = (
    pathlib.Path(__file__).parent / "fixtures" / "rco4b" / "p0_gate"
)


def _list_fixture_dirs() -> list[pathlib.Path]:
    return sorted(p for p in FIXTURE_ROOT.iterdir() if p.is_dir())


def _outcome_to_dict(o) -> dict:
    return {
        "should_run": o.should_run,
        "skip_reason": o.skip_reason,
        "passed": o.passed,
        "failure_count": o.failure_count,
        "rollback_reason": o.rollback_reason,
    }


@pytest.mark.parametrize(
    "fixture_dir",
    _list_fixture_dirs(),
    ids=lambda p: p.name,
)
def test_p0_gate_fixtures(fixture_dir: pathlib.Path) -> None:
    inp = json.loads((fixture_dir / "input.json").read_text(encoding="utf-8"))
    expected = json.loads(
        (fixture_dir / "expected_output.json").read_text(encoding="utf-8")
    )

    p0i = P0GateInput(
        ag_id=inp["p0_gate_input"]["ag_id"],
        run_id=inp["p0_gate_input"]["run_id"],
        iteration=int(inp["p0_gate_input"]["iteration"]),
        p0_benchmark_count=int(inp["p0_gate_input"]["p0_benchmark_count"]),
        legacy_gates_enabled=bool(inp["p0_gate_input"]["legacy_gates_enabled"]),
    )

    pre = decide_p0_gate_should_run(p0i)
    assert _outcome_to_dict(pre) == expected["pre_eval_outcome"]

    if pre.should_run:
        post_inp = inp["post_eval_input"]
        post = decide_p0_gate_post_eval(
            p0i,
            p0_failures_count=int(post_inp["p0_failures_count"]),
        )
        assert _outcome_to_dict(post) == expected["post_eval_outcome"]
    else:
        assert expected["post_eval_outcome"] is None
