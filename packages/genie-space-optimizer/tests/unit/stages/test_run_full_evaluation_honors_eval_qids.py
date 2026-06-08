"""Trial 16 RC1 — ``_run_full_evaluation`` must honour
``EvaluationInput.eval_qids`` and scope the benchmark slice it passes
to ``evaluation.run_evaluation`` accordingly.

Why this test exists:
    Production postmortems 575892594490176 and 319530250904653 both
    timed out at ~120 minutes because every applied patch triggered a
    full 23-question post-apply evaluation (~12 min each, 7+ patches
    applied → ~84 minutes of redundant work). The root cause:
    ``stages/evaluation.py:_run_full_evaluation`` accepts an
    ``EvaluationInput`` carrying ``eval_qids`` but ignores it when
    calling the underlying ``run_evaluation`` — the full benchmarks
    list arrives unchanged via ``**eval_kwargs``.

    The downstream code at ``evaluated_gate._run_post_apply_eval``
    relies on ``inp.eval_qids`` only to classify the *returned* rows,
    not to scope the work. Fixing the scope-of-work means the
    post-apply path goes from O(n) full-eval to O(1) targeted-eval —
    a 23× speedup on the postmortem space, which collapses the
    timeout into normal runtime.

The fix is the smallest possible: before splatting ``eval_kwargs``,
filter ``benchmarks`` to those whose ``question_id`` is in
``inp.eval_qids``. No new policy, no batched scheduler, no remaining-
time guard — just stop ignoring a parameter that's already plumbed.
"""
from __future__ import annotations

from unittest.mock import patch

import pytest


def _bench(qid: str) -> dict:
    """Minimal benchmark row matching ``RunEvaluationKwargs``'s shape."""
    return {"question_id": qid, "question_text": f"Q{qid}?"}


# Trial 16 RC1 — Chunk 2 wired ``inp.eval_qids`` through to a
# benchmark slice inside ``_run_full_evaluation``. Up to Trial 15
# the helper splatted the full benchmarks list regardless of
# ``eval_qids``, so each applied patch triggered a 23-question
# post-apply eval (~12 min) and the optimizer blew its 120 min
# budget after 7 patches. The slice + ``GSO_POST_APPLY_EVAL_SLICED_V1``
# marker keep the contract: empty / missing ``eval_qids`` is the
# no-scope path used by the baseline-once call; a non-empty list
# filters the benchmarks to that set before forwarding to
# ``run_evaluation``.
def test_run_full_evaluation_filters_benchmarks_by_eval_qids() -> None:
    """When ``inp.eval_qids`` is a single qid, ``run_evaluation`` must
    receive a benchmarks slice of exactly that one row.

    The test captures the kwargs that arrive at the underlying
    ``run_evaluation`` so we assert on the *length* of the slice, not
    on a side effect inside the evaluator. The fix replaces the
    ``return _eval_primitives.run_evaluation(**eval_kwargs)`` call
    site with a ``benchmarks = [b for b in ... if b['question_id'] in
    requested]`` filter applied to a copy of ``eval_kwargs`` before
    the splat.
    """
    from genie_space_optimizer.optimization.stages.evaluation import (
        EvaluationInput,
        _run_full_evaluation,
    )

    inp = EvaluationInput(
        space_state={},
        eval_qids=("gs_026",),
        run_role="iteration_eval",
        iteration_label="iter_001",
        scope="full",
    )
    eval_kwargs: dict = {
        "benchmarks": [
            _bench("gs_001"),
            _bench("gs_009"),
            _bench("gs_013"),
            _bench("gs_024"),
            _bench("gs_026"),
            _bench("gs_031"),
        ],
        "space_id": "space-abc",
    }
    recorded: dict = {}

    def _fake_run_evaluation(**kwargs):
        recorded["benchmarks"] = list(kwargs.get("benchmarks") or [])
        return {"rows": [], "scores": {}}

    with patch(
        "genie_space_optimizer.optimization.evaluation.run_evaluation",
        side_effect=_fake_run_evaluation,
    ):
        _run_full_evaluation(inp, eval_kwargs)

    qids_passed = [str(b.get("question_id") or "") for b in recorded["benchmarks"]]
    assert qids_passed == ["gs_026"], (
        f"Expected the benchmark slice to be scoped to "
        f"inp.eval_qids=('gs_026',); instead got benchmarks "
        f"qids={qids_passed!r} (length {len(qids_passed)}). The "
        f"production failure is that the full 23-row list arrives "
        f"unchanged, so every post-apply eval costs ~12 minutes "
        f"instead of ~30 seconds."
    )


def test_run_full_evaluation_filters_benchmarks_by_eval_qid_set() -> None:
    """When ``inp.eval_qids`` names multiple qids, the slice must
    contain *exactly* those rows (order-insensitive)."""
    from genie_space_optimizer.optimization.stages.evaluation import (
        EvaluationInput,
        _run_full_evaluation,
    )

    inp = EvaluationInput(
        space_state={},
        eval_qids=("gs_009", "gs_026"),
        run_role="iteration_eval",
        iteration_label="iter_002",
        scope="full",
    )
    eval_kwargs: dict = {
        "benchmarks": [
            _bench("gs_001"),
            _bench("gs_009"),
            _bench("gs_013"),
            _bench("gs_026"),
        ],
    }
    recorded: dict = {}

    def _fake_run_evaluation(**kwargs):
        recorded["benchmarks"] = list(kwargs.get("benchmarks") or [])
        return {"rows": []}

    with patch(
        "genie_space_optimizer.optimization.evaluation.run_evaluation",
        side_effect=_fake_run_evaluation,
    ):
        _run_full_evaluation(inp, eval_kwargs)

    qids_passed = sorted(
        str(b.get("question_id") or "")
        for b in recorded["benchmarks"]
    )
    assert qids_passed == ["gs_009", "gs_026"], (
        f"Expected the benchmark slice to be scoped to inp.eval_qids "
        f"(gs_009, gs_026); got {qids_passed!r}."
    )


# Post-Trial-16 production-replay regression — postmortems
# 127751814861356 and 813949510175466 both show the slice firing with
# ``requested_qids=['<full canonical qid>']`` and ``benchmarks_count=0``.
# Root cause: ``b.get("question_id")`` is a strict top-level accessor,
# but production benchmark rows carry the canonical qid under nested
# carriers (``inputs.question_id``, ``inputs/question_id`` slash form,
# ``request.kwargs.question_id``, ``metadata.question_id``, or only
# ``id``). Trial 16 RC2 already taught the OUTPUT row-match path in
# ``evaluated_gate`` to use ``extract_question_id``; the slice INPUT
# filter must use the same canonical extractor for the contract to
# hold end-to-end.
def test_run_full_evaluation_slice_uses_canonical_qid_extraction_nested_inputs() -> None:
    """A benchmark whose canonical qid lives under nested ``inputs.question_id``
    must survive the slice when its qid is in ``inp.eval_qids``.

    Reproduces postmortem 127751814861356: marker fired
    ``requested_qids=['airline_ticketing_and_fare_analysis_gs_009']
    benchmarks_count=0`` even though the harness loaded 23 benchmark
    rows, because the rows carried the qid under
    ``inputs.question_id`` (MLflow ``genai.datasets`` nested shape)
    rather than top-level.
    """
    from genie_space_optimizer.optimization.stages.evaluation import (
        EvaluationInput,
        _run_full_evaluation,
    )

    inp = EvaluationInput(
        space_state={},
        eval_qids=("airline_ticketing_and_fare_analysis_gs_009",),
        run_role="iteration_eval",
        iteration_label="iter_001",
        scope="full",
    )
    eval_kwargs: dict = {
        "benchmarks": [
            {"inputs": {"question_id": "airline_ticketing_and_fare_analysis_gs_001"}},
            {"inputs": {"question_id": "airline_ticketing_and_fare_analysis_gs_009"}},
            {"inputs": {"question_id": "airline_ticketing_and_fare_analysis_gs_013"}},
        ],
    }
    recorded: dict = {}

    def _fake_run_evaluation(**kwargs):
        recorded["benchmarks"] = list(kwargs.get("benchmarks") or [])
        return {"rows": []}

    with patch(
        "genie_space_optimizer.optimization.evaluation.run_evaluation",
        side_effect=_fake_run_evaluation,
    ):
        _run_full_evaluation(inp, eval_kwargs)

    assert len(recorded["benchmarks"]) == 1, (
        f"Expected the slice to retain the one benchmark whose nested "
        f"inputs.question_id matches the requested qid; instead the "
        f"slice produced {len(recorded['benchmarks'])} benchmarks. "
        f"This is the production POST_APPLY_EVAL_SLICED_ZERO_BENCHMARKS "
        f"bug — strict b.get('question_id') misses MLflow nested shapes."
    )


def test_run_full_evaluation_slice_uses_canonical_qid_extraction_flat_slash() -> None:
    """A benchmark with the MLflow flat-slash carrier ``inputs/question_id``
    must survive the slice when its qid is in ``inp.eval_qids``."""
    from genie_space_optimizer.optimization.stages.evaluation import (
        EvaluationInput,
        _run_full_evaluation,
    )

    inp = EvaluationInput(
        space_state={},
        eval_qids=("7now_delivery_analytics_space_gs_026",),
        run_role="iteration_eval",
        iteration_label="iter_001",
        scope="full",
    )
    eval_kwargs: dict = {
        "benchmarks": [
            {"inputs/question_id": "7now_delivery_analytics_space_gs_013"},
            {"inputs/question_id": "7now_delivery_analytics_space_gs_026"},
        ],
    }
    recorded: dict = {}

    def _fake_run_evaluation(**kwargs):
        recorded["benchmarks"] = list(kwargs.get("benchmarks") or [])
        return {"rows": []}

    with patch(
        "genie_space_optimizer.optimization.evaluation.run_evaluation",
        side_effect=_fake_run_evaluation,
    ):
        _run_full_evaluation(inp, eval_kwargs)

    assert len(recorded["benchmarks"]) == 1, (
        f"Expected slice to retain the row with inputs/question_id="
        f"'7now_delivery_analytics_space_gs_026'; got "
        f"{len(recorded['benchmarks'])} benchmarks. Reproduces postmortem "
        f"813949510175466 POST_APPLY_EVAL_SLICED_ZERO_BENCHMARKS."
    )


def test_run_full_evaluation_slice_uses_canonical_qid_extraction_id_only() -> None:
    """A benchmark with the canonical qid in ``id`` (no top-level
    ``question_id``) must survive the slice when its qid is in
    ``inp.eval_qids``. The benchmarks loader aliases ``question_id``
    → ``id`` but not the other way, so production datasets that use
    ``id`` as the canonical column produce rows without ``question_id``.
    """
    from genie_space_optimizer.optimization.stages.evaluation import (
        EvaluationInput,
        _run_full_evaluation,
    )

    inp = EvaluationInput(
        space_state={},
        eval_qids=("gs_026",),
        run_role="iteration_eval",
        iteration_label="iter_001",
        scope="full",
    )
    eval_kwargs: dict = {
        "benchmarks": [
            {"id": "gs_009"},
            {"id": "gs_026"},
        ],
    }
    recorded: dict = {}

    def _fake_run_evaluation(**kwargs):
        recorded["benchmarks"] = list(kwargs.get("benchmarks") or [])
        return {"rows": []}

    with patch(
        "genie_space_optimizer.optimization.evaluation.run_evaluation",
        side_effect=_fake_run_evaluation,
    ):
        _run_full_evaluation(inp, eval_kwargs)

    assert len(recorded["benchmarks"]) == 1, (
        f"Expected slice to retain the row whose canonical qid lives "
        f"in 'id' (not 'question_id'); got {len(recorded['benchmarks'])} "
        f"benchmarks. The harness normalizer aliases question_id→id but "
        f"not the reverse, so id-only rows go missing from the slice."
    )


def test_run_full_evaluation_raises_typed_error_when_slice_empty_for_requested_qid() -> None:
    """Defense-in-depth — if ``inp.eval_qids`` is non-empty but no
    benchmark row matches via the canonical extractor, fail with a
    typed ``post_apply_eval_empty_slice_for_requested_qid`` error
    BEFORE invoking ``run_evaluation``.

    Both postmortems (127751814861356 and 813949510175466) explicitly
    recommended this invariant: "Refuse to apply when the requested
    hard QID cannot map to exactly one benchmark row." Surfacing this
    as a typed exception lets ``evaluated_gate`` convert it to a
    structured terminal whose ``forbidden_signature`` teaches the next
    iteration's strategist (instead of silently running an empty
    eval and producing ``no_post_apply_row_for_qid``).
    """
    from genie_space_optimizer.optimization.stages.evaluation import (
        EvaluationInput,
        _run_full_evaluation,
    )

    inp = EvaluationInput(
        space_state={},
        eval_qids=("gs_999_missing",),
        run_role="iteration_eval",
        iteration_label="iter_001",
        scope="full",
    )
    eval_kwargs: dict = {
        "benchmarks": [
            {"question_id": "gs_001"},
            {"question_id": "gs_009"},
        ],
    }

    def _fake_run_evaluation(**kwargs):
        # Should never be reached — the typed error must fire first.
        raise AssertionError(
            "run_evaluation was invoked despite an empty post-apply "
            "slice; defense-in-depth invariant did not fire."
        )

    with patch(
        "genie_space_optimizer.optimization.evaluation.run_evaluation",
        side_effect=_fake_run_evaluation,
    ):
        with pytest.raises(Exception, match="post_apply_eval_empty_slice_for_requested_qid"):
            _run_full_evaluation(inp, eval_kwargs)


# ── Trial 31 W31.4 — empty-slice invariant excludes already-correct /
# no-benchmark QIDs ───────────────────────────────────────────────────
#
# W30.4(c) cleared the legacy namespace mismatch, but a distinct
# ``post_apply_eval_empty_slice_for_requested_qid`` then fired for an
# already-correct QID (``gs_024``) that legitimately has NO benchmark
# row. Because W31.3 now FAILS the lever_loop task on an
# ``OPTIMIZER_INVARIANT_VIOLATION``, that spurious empty-slice would
# fail the whole run. The fix: the invariant only fires for a
# *benchmark-expected* (live-hard) requested QID. A requested set that
# is entirely already-correct / no-benchmark is a benign skip.
#
# The discriminator is a typed ``EvaluationInput.enforce_benchmark_
# presence`` boolean — default ``True`` preserves the Trial 16.1
# fail-fast for genuine hard QIDs; the SM gate sets it ``False`` when
# ``state.qid`` is not a live-hard repair target.


def test_trial31_w314_benign_skip_when_qid_not_benchmark_expected() -> None:
    """An already-correct / no-benchmark requested QID must NOT raise
    the empty-slice invariant — it is a benign skip, not an
    OPTIMIZER_INVARIANT_VIOLATION."""
    from genie_space_optimizer.optimization.stages.evaluation import (
        EvaluationInput,
        _run_full_evaluation,
    )

    inp = EvaluationInput(
        space_state={},
        eval_qids=("gs_024",),  # already-correct, no benchmark row
        run_role="iteration_eval",
        iteration_label="iter_001",
        scope="full",
        enforce_benchmark_presence=False,
    )
    eval_kwargs: dict = {
        "benchmarks": [{"question_id": "gs_001"}, {"question_id": "gs_009"}],
    }

    def _fake_run_evaluation(**kwargs):
        raise AssertionError(
            "run_evaluation must NOT be invoked on a benign empty slice; "
            "the W31.4 skip should return early."
        )

    with patch(
        "genie_space_optimizer.optimization.evaluation.run_evaluation",
        side_effect=_fake_run_evaluation,
    ):
        out = _run_full_evaluation(inp, eval_kwargs)

    assert out == {"rows": []}, (
        "a benign empty slice (no benchmark-expected QID) must return an "
        f"empty eval result, not {out!r}, and must not raise"
    )


def test_trial31_w314_still_raises_for_benchmark_expected_missing() -> None:
    """A genuine hard QID (``enforce_benchmark_presence=True``, the
    default) whose benchmark row is absent MUST still raise the typed
    invariant — W31.4 narrows the fail-fast, it does not disable it."""
    from genie_space_optimizer.optimization.stages.evaluation import (
        EvaluationInput,
        _run_full_evaluation,
    )

    inp = EvaluationInput(
        space_state={},
        eval_qids=("gs_013_hard_missing_benchmark",),
        run_role="iteration_eval",
        iteration_label="iter_001",
        scope="full",
        # default enforce_benchmark_presence=True
    )
    eval_kwargs: dict = {
        "benchmarks": [{"question_id": "gs_001"}, {"question_id": "gs_009"}],
    }

    with patch(
        "genie_space_optimizer.optimization.evaluation.run_evaluation",
        side_effect=AssertionError("must not reach run_evaluation"),
    ):
        with pytest.raises(
            Exception, match="post_apply_eval_empty_slice_for_requested_qid"
        ):
            _run_full_evaluation(inp, eval_kwargs)


def test_run_full_evaluation_passes_through_when_eval_qids_empty() -> None:
    """Backward-compat: empty ``eval_qids`` means "no scoping" — the
    full benchmarks list must reach ``run_evaluation`` unchanged.

    This case is NOT xfail because the current code already does this
    (it ignores the parameter, which happens to be the desired
    behaviour when the parameter is empty). The fix in Chunk 2 must
    preserve this property.
    """
    from genie_space_optimizer.optimization.stages.evaluation import (
        EvaluationInput,
        _run_full_evaluation,
    )

    inp = EvaluationInput(
        space_state={},
        eval_qids=(),  # empty: no scoping
        run_role="baseline",
        iteration_label="iter_000",
        scope="full",
    )
    eval_kwargs: dict = {
        "benchmarks": [_bench("gs_001"), _bench("gs_009")],
    }
    recorded: dict = {}

    def _fake_run_evaluation(**kwargs):
        recorded["benchmarks"] = list(kwargs.get("benchmarks") or [])
        return {"rows": []}

    with patch(
        "genie_space_optimizer.optimization.evaluation.run_evaluation",
        side_effect=_fake_run_evaluation,
    ):
        _run_full_evaluation(inp, eval_kwargs)

    assert [b["question_id"] for b in recorded["benchmarks"]] == [
        "gs_001",
        "gs_009",
    ]
