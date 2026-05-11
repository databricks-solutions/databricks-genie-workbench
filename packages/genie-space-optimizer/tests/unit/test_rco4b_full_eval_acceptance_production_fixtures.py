"""RCO-4b Phase E Task 6 — production-shape fixture tests.

Each fixture pair drives ``decide_full_eval_acceptance`` and asserts
the output matches the recorded expected outcome.
"""
from __future__ import annotations

import json
import pathlib

import pytest

from genie_space_optimizer.optimization.stages.eval_gates import (
    decide_full_eval_acceptance,
)
from genie_space_optimizer.optimization.stages.gate_types import (
    FullEvalAcceptanceInput,
)


FIXTURE_ROOT = (
    pathlib.Path(__file__).parent
    / "fixtures"
    / "rco4b"
    / "full_eval_acceptance"
)


def _list_fixture_dirs() -> list[pathlib.Path]:
    return sorted(p for p in FIXTURE_ROOT.iterdir() if p.is_dir())


def _outcome_to_dict(o) -> dict:
    return {
        "accepted": o.accepted,
        "branch": o.branch,
        "reason_code": o.reason_code,
        "rollback_reason": o.rollback_reason,
        "regression_count": o.regression_count,
        "verdict_audit_metrics": dict(o.verdict_audit_metrics),
        "rollback_audit_metrics": (
            dict(o.rollback_audit_metrics)
            if o.rollback_audit_metrics else None
        ),
        "accept_audit_metrics": (
            dict(o.accept_audit_metrics)
            if o.accept_audit_metrics else None
        ),
    }


@pytest.mark.parametrize(
    "fixture_dir",
    _list_fixture_dirs(),
    ids=lambda p: p.name,
)
def test_full_eval_acceptance_fixtures(fixture_dir: pathlib.Path) -> None:
    inp_payload = json.loads(
        (fixture_dir / "input.json").read_text(encoding="utf-8")
    )
    expected = json.loads(
        (fixture_dir / "expected_output.json").read_text(encoding="utf-8")
    )

    fe = inp_payload["full_eval_input"]
    inp = FullEvalAcceptanceInput(
        ag_id=fe["ag_id"],
        iteration=int(fe["iteration"]),
        strict_decision_accepted=bool(fe["strict_decision_accepted"]),
        strict_decision_reason_code=str(fe["strict_decision_reason_code"]),
        strict_decision_delta_pp=float(fe["strict_decision_delta_pp"]),
        strict_decision_post_arbiter_candidate=float(
            fe["strict_decision_post_arbiter_candidate"]
        ),
        strict_decision_post_arbiter_baseline=float(
            fe["strict_decision_post_arbiter_baseline"]
        ),
        strict_decision_min_gain_pp=float(fe["strict_decision_min_gain_pp"]),
        pre_arbiter_candidate=float(fe["pre_arbiter_candidate"]),
        pre_arbiter_baseline=float(fe["pre_arbiter_baseline"]),
        control_plane_reason_code=str(fe["control_plane_reason_code"]),
        diagnostic_regression_judges=tuple(fe["diagnostic_regression_judges"]),
        regressions=tuple(fe["regressions"]),
    )

    out = decide_full_eval_acceptance(inp)
    assert _outcome_to_dict(out) == expected["outcome"]
