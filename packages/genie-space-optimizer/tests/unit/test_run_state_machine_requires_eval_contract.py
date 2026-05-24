"""Trial 15 — structural invariant: the SM lane must receive an
evaluator boundary contract (``eval_kwargs`` or a workbench
``extras['post_apply_eval']`` stub) at call time.

Why this test exists:
    The dc89d1a9 + 98ec8950 postmortems showed every APPLIED state
    dying at ``evaluated_gate`` with
    ``TypeError: run_evaluation() argument after ** must be a mapping,
    not NoneType``. Root cause:
    ``run_state_machine_iteration_and_persist`` was being called from
    the harness without ``stage_ctx`` / ``eval_kwargs`` / ``eval_qids``,
    and the SM ``TransformerContext`` defaults propagated all the way
    to ``evaluate_post_patch``. The fail-fast invariant added in
    optimizer.py converts that silent-at-call-time / explode-8-frames-
    deep defect into a build-time ``ValueError`` at the seam.

The three cases cover the full state space of the invariant:
    1. Missing both -> ``ValueError("SM_EVALUATOR_CONTRACT_MISSING:...")``
    2. eval_kwargs hydrated -> succeeds (production path).
    3. extras['post_apply_eval'] stub present -> succeeds (workbench escape hatch).
"""
from __future__ import annotations

import os

import pytest


# These tests intentionally exercise the strict invariant. The
# package-level conftest sets
# ``GSO_SM_TEST_ALLOW_MISSING_EVAL_CONTRACT=1`` so pre-Trial-15 tests
# keep passing; clear it for this file so each case observes the
# production-equivalent behavior.
@pytest.fixture(autouse=True)
def _disable_test_escape_hatch(monkeypatch):
    monkeypatch.delenv(
        "GSO_SM_TEST_ALLOW_MISSING_EVAL_CONTRACT", raising=False,
    )
    yield


def test_missing_eval_kwargs_and_no_stub_raises_value_error(tmp_path):
    """Case 1 — the SM_EVALUATOR_CONTRACT_MISSING canary.

    Pre-Trial-15 callsites that forgot to pass ``eval_kwargs`` from
    the harness must fail at the seam, not at ``evaluated_gate`` 8
    transformers deep with a TypeError.
    """
    from genie_space_optimizer.optimization import optimizer as opt_mod

    with pytest.raises(ValueError) as exc_info:
        opt_mod.run_state_machine_iteration_and_persist(
            eval_rows=(),
            iteration=1,
            run_id="trial15-d1-missing",
            run_root=tmp_path,
            workspace_client=None,
            forbidden_signatures=(),
        )
    msg = str(exc_info.value)
    assert "SM_EVALUATOR_CONTRACT_MISSING" in msg, (
        f"Expected the canary 'SM_EVALUATOR_CONTRACT_MISSING' in the "
        f"ValueError message so postmortems can grep for it; got: {msg!r}"
    )


def test_eval_kwargs_hydrated_passes_invariant(tmp_path):
    """Case 2 — production path. The harness builds
    ``RunEvaluationKwargs`` and passes it as ``eval_kwargs``.

    With ``eval_rows=()`` the SM still returns the empty tuple early
    (so we do not need a live applier / Genie space), but the
    invariant must NOT fire.
    """
    from genie_space_optimizer.optimization import optimizer as opt_mod

    eval_kwargs = {
        "space_id": "space-001",
        "experiment_name": "/Shared/test",
        "iteration": 1,
        "benchmarks": [],
        "domain": "test",
        "model_id": None,
        "eval_scope": "full",
        "predict_fn": lambda *a, **k: None,
        "scorers": [],
    }

    result = opt_mod.run_state_machine_iteration_and_persist(
        eval_rows=(),
        iteration=1,
        run_id="trial15-d1-hydrated",
        run_root=tmp_path,
        workspace_client=None,
        forbidden_signatures=(),
        eval_kwargs=eval_kwargs,
    )
    assert result == (), (
        "Empty eval_rows must short-circuit to an empty tuple; the "
        "invariant should have passed and the SM should not have run."
    )


def test_post_apply_eval_stub_passes_invariant(tmp_path):
    """Case 3 — workbench escape hatch.

    Tests / devtools that provide ``extras['post_apply_eval']`` may
    pass ``eval_kwargs=None`` and still satisfy the invariant. This
    mirrors how ``local_runner.py`` exercises the gate without an
    MLflow / Genie backend.
    """
    from genie_space_optimizer.optimization import optimizer as opt_mod

    def _stub(*, state, ctx):
        return (0.0, "", "")

    result = opt_mod.run_state_machine_iteration_and_persist(
        eval_rows=(),
        iteration=1,
        run_id="trial15-d1-stub",
        run_root=tmp_path,
        workspace_client=None,
        forbidden_signatures=(),
        extras={"post_apply_eval": _stub},
    )
    assert result == ()
