"""PR 34 — Post-enrichment gate parity.

Locks the contract that
:func:`genie_space_optimizer.optimization.harness._resolve_effective_starting_point`
returns the *current* evaluated state of the Genie Space when gating the
lever loop. The in-process orchestration previously gated on the stale
baseline ``thresholds_met``, which made post-enrichment regressions
silently converge as ``baseline_meets_thresholds`` and skip the lever
loop. The notebook task path (``jobs/run_lever_loop.py``) already prefers
the post-enrichment values when present; these tests bring the in-process
helper into parity.
"""

from __future__ import annotations

from genie_space_optimizer.optimization.harness import (
    _resolve_effective_starting_point,
)


def _baseline_kwargs(**overrides):
    base = {
        "baseline_scores": {"schema_accuracy": 100.0, "result_correctness": 100.0},
        "baseline_accuracy": 100.0,
        "baseline_thresholds_met": True,
        "baseline_model_id": "baseline-model",
    }
    base.update(overrides)
    return base


def test_post_enrichment_regression_overrides_baseline_gate():
    """Baseline meets thresholds, enrichment regresses → resolved state
    must be the post-enrichment values so the lever-loop gate sees
    ``thresholds_met=False`` and the loop is entered.
    """
    resolved = _resolve_effective_starting_point(
        **_baseline_kwargs(),
        enrichment_out={
            "enrichment_skipped": False,
            "enrichment_model_id": "enriched-model",
            "post_enrichment_accuracy": 92.31,
            "post_enrichment_scores": {"schema_accuracy": 92.3},
            "post_enrichment_thresholds_met": False,
            "post_enrichment_model_id": "enriched-model",
        },
    )

    assert resolved["accuracy"] == 92.31
    assert resolved["scores"] == {"schema_accuracy": 92.3}
    assert resolved["thresholds_met"] is False
    assert resolved["model_id"] == "enriched-model"
    assert resolved["source"] == "enrichment.post_enrichment_accuracy"


def test_post_enrichment_passes_thresholds_carries_enriched_state():
    """Baseline fails thresholds but enrichment recovers → resolved state
    must use post-enrichment values and ``thresholds_met=True`` so the
    loop can short-circuit on the *current* state, not the stale
    baseline.
    """
    resolved = _resolve_effective_starting_point(
        **_baseline_kwargs(
            baseline_scores={"schema_accuracy": 80.0},
            baseline_accuracy=80.0,
            baseline_thresholds_met=False,
        ),
        enrichment_out={
            "enrichment_skipped": False,
            "enrichment_model_id": "enriched-model",
            "post_enrichment_accuracy": 96.5,
            "post_enrichment_scores": {"schema_accuracy": 96.5},
            "post_enrichment_thresholds_met": True,
            "post_enrichment_model_id": "enriched-model",
        },
    )

    assert resolved["accuracy"] == 96.5
    assert resolved["thresholds_met"] is True
    assert resolved["model_id"] == "enriched-model"
    assert resolved["source"] == "enrichment.post_enrichment_accuracy"


def test_enrichment_skipped_keeps_baseline_state():
    """When enrichment is skipped (``enrichment_skipped=True``) the
    resolved state must remain on baseline so the gate behaves exactly
    as if enrichment had never run.
    """
    resolved = _resolve_effective_starting_point(
        **_baseline_kwargs(),
        enrichment_out={
            "enrichment_skipped": True,
            "enrichment_model_id": "baseline-model",
            "post_enrichment_accuracy": None,
            "post_enrichment_scores": {},
            "post_enrichment_thresholds_met": False,
        },
    )

    assert resolved["accuracy"] == 100.0
    assert resolved["scores"] == {"schema_accuracy": 100.0, "result_correctness": 100.0}
    assert resolved["thresholds_met"] is True
    assert resolved["model_id"] == "baseline-model"
    assert resolved["source"] == "baseline_eval"


def test_enrichment_applied_but_post_eval_missing_uses_baseline_with_diagnostic():
    """Enrichment ran but post-enrichment eval did not produce an accuracy
    (failure / skip). Resolved state stays on baseline but the source
    must be the diagnostic value so log readers can see the run silently
    used baseline numbers despite enrichment having mutated the space.
    """
    resolved = _resolve_effective_starting_point(
        **_baseline_kwargs(),
        enrichment_out={
            "enrichment_skipped": False,
            "enrichment_model_id": "enriched-model",
            "post_enrichment_accuracy": None,
            "post_enrichment_scores": {},
            "post_enrichment_thresholds_met": False,
        },
    )

    assert resolved["accuracy"] == 100.0
    assert resolved["thresholds_met"] is True
    assert resolved["model_id"] == "baseline-model"
    assert resolved["source"] == "baseline_eval_post_enrichment_missing"


def test_enrichment_out_none_keeps_baseline_state():
    """``enrichment_out=None`` (e.g. enrichment raised) must behave the
    same as ``enrichment_skipped=True``: keep baseline state.
    """
    resolved = _resolve_effective_starting_point(
        **_baseline_kwargs(),
        enrichment_out=None,
    )

    assert resolved["accuracy"] == 100.0
    assert resolved["thresholds_met"] is True
    assert resolved["model_id"] == "baseline-model"
    assert resolved["source"] == "baseline_eval"


def test_post_enrichment_model_id_falls_back_to_enrichment_model_id():
    """When ``post_enrichment_model_id`` is not separately set, the
    resolved ``model_id`` must fall back to ``enrichment_model_id`` —
    that is the model the post-enrichment eval was actually run on.
    """
    resolved = _resolve_effective_starting_point(
        **_baseline_kwargs(),
        enrichment_out={
            "enrichment_skipped": False,
            "enrichment_model_id": "enriched-model",
            "post_enrichment_accuracy": 92.31,
            "post_enrichment_scores": {"schema_accuracy": 92.3},
            "post_enrichment_thresholds_met": False,
        },
    )

    assert resolved["model_id"] == "enriched-model"


def test_regression_baseline_perfect_post_enrichment_regresses_enters_lever_loop():
    """Regression for the production divergence (run ``9db57dad``):

    - Baseline eval reported 100% accuracy and ``thresholds_met=True``.
    - Proactive enrichment applied changes (MV detection / join repairs)
      and post-enrichment eval regressed to 92.31% with thresholds NOT
      met.
    - In-process ``optimize_genie_space`` previously gated on the stale
      baseline ``thresholds_met=True`` and converged as
      ``baseline_meets_thresholds`` — the lever loop was skipped despite
      a regression that the loop is designed to repair.

    With the gate fix the resolved state must reflect the *current*
    evaluated state, so the orchestration falls into the
    ``not thresholds_met`` branch and enters the lever loop.
    """
    resolved = _resolve_effective_starting_point(
        baseline_scores={
            "schema_accuracy": 100.0,
            "result_correctness": 100.0,
        },
        baseline_accuracy=100.0,
        baseline_thresholds_met=True,
        baseline_model_id="baseline-9db57dad",
        enrichment_out={
            "enrichment_skipped": False,
            "enrichment_model_id": "enriched-9db57dad",
            "post_enrichment_accuracy": 92.31,
            "post_enrichment_scores": {
                "schema_accuracy": 92.3,
                "result_correctness": 92.3,
            },
            "post_enrichment_thresholds_met": False,
            "post_enrichment_model_id": "enriched-9db57dad",
        },
    )

    assert resolved["thresholds_met"] is False, (
        "post-enrichment regression must clear thresholds_met so the "
        "in-process gate routes to the lever loop"
    )
    assert resolved["accuracy"] == 92.31
    assert resolved["model_id"] == "enriched-9db57dad", (
        "lever loop must iterate from the post-enrichment model, not "
        "the stale baseline model"
    )
    assert resolved["source"] == "enrichment.post_enrichment_accuracy"

    would_skip_lever_loop = bool(resolved["thresholds_met"])
    assert would_skip_lever_loop is False, (
        "regression: the in-process path used to skip the lever loop "
        "here — the gate must now enter it"
    )


def test_returned_scores_are_an_independent_copy():
    """Mutating the returned ``scores`` must not mutate the input dicts —
    the gate is allowed to add provenance keys without leaking them
    back into the post-enrichment publishing surface.
    """
    pe_scores = {"schema_accuracy": 92.3}
    resolved = _resolve_effective_starting_point(
        **_baseline_kwargs(),
        enrichment_out={
            "enrichment_skipped": False,
            "enrichment_model_id": "enriched-model",
            "post_enrichment_accuracy": 92.31,
            "post_enrichment_scores": pe_scores,
            "post_enrichment_thresholds_met": False,
            "post_enrichment_model_id": "enriched-model",
        },
    )

    resolved["scores"]["__probe__"] = 1.0
    assert "__probe__" not in pe_scores
