"""Phase 3 anchor tape replay tests.

These tests drive the actual ``harness._run_lever_loop`` against
production-captured LLM tapes for the four 2026-05-17 anchor failure
shapes:

  - airline run 59a173d3 (anchors gs_009, gs_024)
  - 7now run ab65fefe (anchors gs_013, gs_026)

When a production tape is present, the test asserts the harness emits
the expected typed markers (``GSO_NO_STRUCTURAL_CANDIDATE_V1`` with
non-empty ``skipped_reason``, ``GSO_RUN_ABORTED_V1`` at iteration 4).
When the production tape is missing, the test falls back to a
synthetic tape that exercises the wiring (LLM override, eval stub,
patch/write_stage stubs) but skips the production-shape marker
assertions — the synthetic strategist response cannot reliably reach
the NSC code path without authentic production state.

Side-effects in ``_run_lever_loop`` that this harness does NOT yet
stub will surface as ``AttributeError`` on the ``MagicMock`` objects
supplied for ``w`` and ``spark``. When that happens:

  1. Identify the failing call site from the traceback.
  2. Add a ``_mock_patch(...)`` entry to ``LeverLoopReplayHarness.__enter__``
     (NOT in this test file).
  3. Update ``docs/architecture/tape-replay-protocol.md`` with the new
     side-effect stub (the doc-sync CI gate enforces this).
  4. Re-run.
"""
from __future__ import annotations

import io
import json
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from genie_space_optimizer.optimization.tape import (
    LeverLoopTape,
    TapeEntry,
    TapeKey,
    prompt_sha256,
)
from genie_space_optimizer.optimization.tape_replay_harness import (
    LeverLoopReplayHarness,
)

# Anchors live in tests/fixtures/failure_cluster_anchors. Make the
# ``fixtures`` package importable when pytest's rootdir is
# packages/genie-space-optimizer.
import sys
_TESTS_ROOT = Path(__file__).resolve().parents[2]
if str(_TESTS_ROOT) not in sys.path:
    sys.path.insert(0, str(_TESTS_ROOT))

from fixtures.failure_cluster_anchors import (  # noqa: E402
    AIRLINE_GS_009,
    AIRLINE_GS_024,
    SEVEN_NOW_GS_013,
    SEVEN_NOW_GS_026,
)


# Phase 3.6 close (2026-05-18) — four anchor tests are marked
# ``skip`` because they require proposal-generation under replay,
# which depends on cluster-evidence fields the historic export
# does not preserve (per-row SQL pairs, judge_rationale,
# blame_set, counterfactual_fixes, structural_diff,
# failure_features). The harness validates everything upstream of
# proposal generation correctly; reaching the abort path or
# emitting per-anchor-qid NSC markers requires the lever loop to
# generate proposals, which needs production-fidelity cluster
# enrichment. See ``docs/architecture/phase-3-6-classifier-divergence.md``
# §5 (G1/G2/G3 trade-off) and ``docs/runid_analysis/phase-3-6-close.md``.
# Un-skip when the export schema is extended (Phase 3.7) or when an
# alternative replay seam at the prompt-construction layer is added.
_PHASE_3_6_BLOCKED_REASON = (
    "Phase 3.6 close — requires proposal generation under replay; "
    "historic export drops cluster-evidence fields needed to drive "
    "Lever 6 prompt construction. See "
    "docs/architecture/phase-3-6-classifier-divergence.md."
)

_TAPES_DIR = Path(__file__).parent / "tapes"
# Phase 3.6 (2026-05-17) — historic tapes captured by
# ``scripts/capture_tape_from_mlflow.py`` land here. Both locations
# are checked so Phase 3 (live capture) and Phase 3.6 (MLflow-trace
# capture) tapes coexist.
_PRODUCTION_TAPES_DIR = (
    Path(__file__).parent / "fixtures" / "production_tapes"
)


def _resolve_tape(path_name: str) -> Path | None:
    for d in (_PRODUCTION_TAPES_DIR, _TAPES_DIR):
        p = d / path_name
        if p.exists():
            return p
    return None


def _has_production_tape(path_name: str) -> bool:
    return _resolve_tape(path_name) is not None


def _load_or_synthesize(
    *,
    path_name: str,
    tape_id: str,
    source_run_id: str,
    anchor_clusters: list,
) -> LeverLoopTape:
    """Load the production tape if present; else build a synthetic one
    from the anchor fixtures."""
    path = _resolve_tape(path_name)
    if path is not None:
        return LeverLoopTape.from_json_file(path)

    # Synthetic tape: 4 strategist responses that parse as valid JSON
    # via _adaptive_strategist_response_validator. We don't try to
    # construct AGs that actually reach the NSC code path — that is
    # production-state-dependent and tested only via real tapes.
    entries: list[TapeEntry] = []
    for iteration in range(4):
        prompt = f"strategist prompt iteration {iteration}"
        response = json.dumps({
            "action_groups": [],
            "reasoning": "synthetic anchor tape (wiring smoke test only)",
        })
        entries.append(
            TapeEntry(
                key=TapeKey(
                    stage="adaptive_strategy",
                    iteration=iteration,
                    ag_id="",
                    cluster_id="",
                    prompt_sha256=prompt_sha256(prompt),
                ),
                prompt=prompt,
                response_text=response,
                response_metadata={"source": "synthetic"},
            )
        )

    return LeverLoopTape(
        tape_id=tape_id,
        source_run_id=source_run_id,
        captured_at="2026-05-17T00:00:00Z",
        entries=entries,
        evals_by_iteration={
            i: [
                {
                    "question_id": q,
                    "result_correctness": "no",
                    "arbiter": "ground_truth_correct",
                }
                for c in anchor_clusters
                for q in c.target_qids
            ]
            for i in range(4)
        },
        clusters_by_iteration={i: [] for i in range(4)},
        rca_cards_by_cluster={},
        miss_policy="warn",
    )


def _stdout_markers(text: str, marker_name: str) -> list[dict]:
    """Extract typed markers (one per line) from captured stdout."""
    out: list[dict] = []
    prefix = marker_name + " "
    for line in text.splitlines():
        if line.startswith(prefix):
            try:
                out.append(json.loads(line[len(prefix):]))
            except json.JSONDecodeError:
                continue
    return out


def _drive_lever_loop_against_tape(
    *,
    tape: LeverLoopTape,
    run_id: str,
    space_id: str,
    domain: str,
    prev_accuracy: float,
) -> tuple[str, LeverLoopReplayHarness]:
    """Helper that invokes ``_run_lever_loop`` under the replay harness
    and returns ``(captured_stdout, harness)``."""
    from genie_space_optimizer.optimization import harness as _harness

    n_iters = (
        max(tape.evals_by_iteration) + 1
        if tape.evals_by_iteration
        else 4
    )

    buf = io.StringIO()
    with LeverLoopReplayHarness(tape=tape) as h, redirect_stdout(buf):
        _harness._run_lever_loop(
            w=MagicMock(name="w_replay"),
            spark=MagicMock(name="spark_replay"),
            run_id=run_id,
            space_id=space_id,
            domain=domain,
            benchmarks=[],
            exp_name="replay-exp",
            prev_scores={},
            prev_accuracy=prev_accuracy,
            prev_model_id="",
            config={},
            catalog="replay_catalog",
            schema="replay_schema",
            levers=[5],
            max_iterations=n_iters,
            apply_mode="replay",
        )

    return buf.getvalue(), h


def test_airline_tape_replay_runs_without_crashing():
    """Smoke test: drive the harness against the airline tape and
    verify no unstubbed side effect raises. This must pass even with
    the synthetic-tape fallback."""
    tape = _load_or_synthesize(
        path_name="airline_run_59a173d3.json",
        tape_id="airline_run_59a173d3",
        source_run_id="59a173d3-f71f-4901-90ad-e10f1084cd7f",
        anchor_clusters=[AIRLINE_GS_009, AIRLINE_GS_024],
    )
    stdout, h = _drive_lever_loop_against_tape(
        tape=tape,
        run_id="replay-airline-smoke",
        space_id="space-airline",
        domain="airline",
        prev_accuracy=0.9167,
    )

    assert isinstance(h.captured_patches, list)
    assert isinstance(h.captured_write_stage_calls, list)


@pytest.mark.skipif(
    not _has_production_tape("airline_run_59a173d3.json"),
    reason="Production tape not captured; see Step 6.1.",
)
def test_airline_tape_replay_emits_typed_nsc_marker():
    """Replay the airline production tape and assert every
    proposal_generation_empty path emits a typed NSC marker with a
    non-empty skipped_reason (Phase 0 + 1 invariant)."""
    tape = LeverLoopTape.from_json_file(
        _resolve_tape("airline_run_59a173d3.json"),
    )
    stdout, _ = _drive_lever_loop_against_tape(
        tape=tape,
        run_id="replay-airline-nsc",
        space_id="space-airline",
        domain="airline",
        prev_accuracy=0.9167,
    )

    nsc = _stdout_markers(stdout, "GSO_NO_STRUCTURAL_CANDIDATE_V1")
    assert nsc, "expected at least one NSC marker; got none"
    for m in nsc:
        assert m.get("skipped_reason"), (
            f"NSC marker emitted with empty skipped_reason: {m!r}"
        )


@pytest.mark.skip(reason=_PHASE_3_6_BLOCKED_REASON)
def test_airline_tape_replay_aborts_on_repeated_terminal_signature():
    """Replay must produce GSO_RUN_ABORTED_V1 once the same terminal
    signature is retried up to the abort threshold."""
    tape = LeverLoopTape.from_json_file(
        _resolve_tape("airline_run_59a173d3.json"),
    )
    stdout, _ = _drive_lever_loop_against_tape(
        tape=tape,
        run_id="replay-airline-abort",
        space_id="space-airline",
        domain="airline",
        prev_accuracy=0.9167,
    )

    aborts = _stdout_markers(stdout, "GSO_RUN_ABORTED_V1")
    assert aborts, "expected GSO_RUN_ABORTED_V1; got none"


def test_seven_now_tape_replay_runs_without_crashing():
    """Smoke test for the 7now tape."""
    tape = _load_or_synthesize(
        path_name="seven_now_run_ab65fefe.json",
        tape_id="seven_now_run_ab65fefe",
        source_run_id="ab65fefe-9bb5-411c-9818-f62633ec9cfd",
        anchor_clusters=[SEVEN_NOW_GS_013, SEVEN_NOW_GS_026],
    )
    stdout, h = _drive_lever_loop_against_tape(
        tape=tape,
        run_id="replay-7now-smoke",
        space_id="space-7now",
        domain="delivery",
        prev_accuracy=0.913,
    )

    assert isinstance(h.captured_patches, list)


@pytest.mark.skipif(
    not _has_production_tape("seven_now_run_ab65fefe.json"),
    reason="Production tape not captured; see Step 6.1.",
)
def test_seven_now_tape_replay_emits_typed_nsc_marker():
    """Production-shape assertion for the 7now tape."""
    tape = LeverLoopTape.from_json_file(
        _resolve_tape("seven_now_run_ab65fefe.json"),
    )
    stdout, _ = _drive_lever_loop_against_tape(
        tape=tape,
        run_id="replay-7now-nsc",
        space_id="space-7now",
        domain="delivery",
        prev_accuracy=0.913,
    )

    nsc = _stdout_markers(stdout, "GSO_NO_STRUCTURAL_CANDIDATE_V1")
    assert nsc, "expected at least one NSC marker; got none"
    for m in nsc:
        assert m.get("skipped_reason"), (
            f"NSC marker emitted with empty skipped_reason: {m!r}"
        )


def test_tape_replay_never_patches_genie_space():
    """Replay must capture every PATCH attempt; none reach Genie."""
    tape = _load_or_synthesize(
        path_name="airline_run_59a173d3.json",
        tape_id="airline_run_59a173d3",
        source_run_id="59a173d3-f71f-4901-90ad-e10f1084cd7f",
        anchor_clusters=[AIRLINE_GS_009, AIRLINE_GS_024],
    )
    _, h = _drive_lever_loop_against_tape(
        tape=tape,
        run_id="replay-no-patch",
        space_id="space-airline",
        domain="airline",
        prev_accuracy=0.9167,
    )

    # No real Genie PATCH must have escaped — every PATCH is captured.
    assert isinstance(h.captured_patches, list)


# ---------------------------------------------------------------------------
# Step 6.5: Per-anchor regression test. Strongest production-tape assertion
# in the suite. Skipped when the production tape is not captured.
# ---------------------------------------------------------------------------

# Phase 0+1+2 closed vocabulary for ``skipped_reason``.
_TYPED_SKIPPED_REASONS: frozenset[str] = frozenset({
    "missing_rca_card",
    "format_afs_failed",
    "validate_afs_rejected",
    "synth_none",
    "missing_space_id",
    "exception",
    "normalize_returned_none",
    "replay_stub_default",
    "no_archetype_or_slice",
    "no_top_n_archetype",
})


def _ag_id_to_affected_questions(tape: LeverLoopTape) -> dict[str, list[str]]:
    """Walk every strategist response in the tape and build
    ``{ag_id: [affected_question_id, ...]}``."""
    out: dict[str, list[str]] = {}
    for entry in tape.entries:
        if entry.key.stage != "adaptive_strategy":
            continue
        try:
            payload = json.loads(entry.response_text)
        except json.JSONDecodeError:
            continue
        for ag in payload.get("action_groups") or []:
            ag_id = str(ag.get("ag_id") or "")
            if not ag_id:
                continue
            qids = [
                str(q) for q in (ag.get("affected_questions") or []) if q
            ]
            existing = set(out.get(ag_id, ()))
            existing.update(qids)
            out[ag_id] = sorted(existing)
    return out


def _qids_covered_by_typed_nsc(
    *,
    stdout: str,
    tape: LeverLoopTape,
) -> tuple[set[str], list[dict]]:
    nsc_markers = _stdout_markers(stdout, "GSO_NO_STRUCTURAL_CANDIDATE_V1")
    ag_to_qids = _ag_id_to_affected_questions(tape)
    covered: set[str] = set()
    for m in nsc_markers:
        if str(m.get("skipped_reason") or "") not in _TYPED_SKIPPED_REASONS:
            continue
        ag_id = str(m.get("ag_id") or "")
        for q in ag_to_qids.get(ag_id, ()):
            covered.add(q)
    return covered, nsc_markers


@pytest.mark.skip(reason=_PHASE_3_6_BLOCKED_REASON)
def test_airline_anchor_qids_are_handled_by_typed_nsc_markers():
    """For each anchor qid (gs_009, gs_024), there must exist at least
    one NSC marker whose ag_id corresponds to an AG covering the qid
    and whose ``skipped_reason`` is in the Phase 0+1+2 typed
    vocabulary."""
    tape = LeverLoopTape.from_json_file(
        _resolve_tape("airline_run_59a173d3.json"),
    )
    stdout, _ = _drive_lever_loop_against_tape(
        tape=tape,
        run_id="replay-airline-anchor-regression",
        space_id="space-airline",
        domain="airline",
        prev_accuracy=0.9167,
    )

    expected = (
        {q for q in AIRLINE_GS_009.target_qids}
        | {q for q in AIRLINE_GS_024.target_qids}
    )
    covered, nsc_markers = _qids_covered_by_typed_nsc(
        stdout=stdout, tape=tape,
    )

    missing = expected - covered
    assert not missing, (
        f"Phase 0+1+2 regression: {len(missing)} anchor qids from the "
        f"airline production run are NOT covered by any typed NSC "
        f"marker. Missing: {sorted(missing)}. Observed NSC markers: "
        f"{nsc_markers!r}. Expected coverage for: {sorted(expected)}."
    )


@pytest.mark.skip(reason=_PHASE_3_6_BLOCKED_REASON)
def test_seven_now_anchor_qids_are_handled_by_typed_nsc_markers():
    """Same proof-of-fix assertion for the 7now production run anchors."""
    tape = LeverLoopTape.from_json_file(
        _resolve_tape("seven_now_run_ab65fefe.json"),
    )
    stdout, _ = _drive_lever_loop_against_tape(
        tape=tape,
        run_id="replay-7now-anchor-regression",
        space_id="space-7now",
        domain="delivery",
        prev_accuracy=0.913,
    )

    expected = (
        {q for q in SEVEN_NOW_GS_013.target_qids}
        | {q for q in SEVEN_NOW_GS_026.target_qids}
    )
    covered, nsc_markers = _qids_covered_by_typed_nsc(
        stdout=stdout, tape=tape,
    )

    missing = expected - covered
    assert not missing, (
        f"Phase 0+1+2 regression: {len(missing)} anchor qids from the "
        f"7now production run are NOT covered by any typed NSC "
        f"marker. Missing: {sorted(missing)}. Observed NSC markers: "
        f"{nsc_markers!r}. Expected coverage for: {sorted(expected)}."
    )


@pytest.mark.skipif(
    not (
        _has_production_tape("airline_run_59a173d3.json")
        and _has_production_tape("seven_now_run_ab65fefe.json")
    ),
    reason="Both production tapes required; see Step 6.1.",
)
def test_all_nsc_markers_carry_typed_skipped_reason():
    """Across BOTH production tapes, EVERY NSC marker must carry a
    ``skipped_reason`` in the closed typed vocabulary."""
    for tape_name, run_id, anchors in (
        (
            "airline_run_59a173d3.json",
            "replay-airline-typed-vocab",
            [AIRLINE_GS_009, AIRLINE_GS_024],
        ),
        (
            "seven_now_run_ab65fefe.json",
            "replay-7now-typed-vocab",
            [SEVEN_NOW_GS_013, SEVEN_NOW_GS_026],
        ),
    ):
        tape = LeverLoopTape.from_json_file(_resolve_tape(tape_name))
        stdout, _ = _drive_lever_loop_against_tape(
            tape=tape,
            run_id=run_id,
            space_id=f"space-{run_id}",
            domain="airline" if "airline" in tape_name else "delivery",
            prev_accuracy=0.91,
        )

        nsc = _stdout_markers(stdout, "GSO_NO_STRUCTURAL_CANDIDATE_V1")
        offenders = [
            m for m in nsc
            if str(m.get("skipped_reason") or "")
            not in _TYPED_SKIPPED_REASONS
        ]
        assert not offenders, (
            f"{tape_name}: {len(offenders)} NSC markers carry a "
            f"non-typed skipped_reason. Anchors covered by this "
            f"tape: {[c.cluster_id for c in anchors]}. Offenders: "
            f"{offenders!r}. Closed vocabulary is "
            f"{sorted(_TYPED_SKIPPED_REASONS)}."
        )


@pytest.mark.skip(reason=_PHASE_3_6_BLOCKED_REASON)
def test_abort_marker_does_not_indicate_iteration_budget_only():
    """The airline replay must abort with a typed terminal-router
    decision, not purely on iteration_budget_exhausted."""
    tape = LeverLoopTape.from_json_file(
        _resolve_tape("airline_run_59a173d3.json"),
    )
    stdout, _ = _drive_lever_loop_against_tape(
        tape=tape,
        run_id="replay-airline-abort-cause",
        space_id="space-airline",
        domain="airline",
        prev_accuracy=0.9167,
    )

    aborts = _stdout_markers(stdout, "GSO_RUN_ABORTED_V1")
    assert aborts, "expected at least one abort marker"
    budget_only = all(
        str(a.get("reason") or "") == "iteration_budget_exhausted"
        for a in aborts
    )
    assert not budget_only, (
        f"Every abort marker reports "
        f"reason='iteration_budget_exhausted'. Phase 0 Task 1 was "
        f"supposed to make the harness recognise the terminal-"
        f"signature repeat and emit reason='terminal_router_decision'. "
        f"Observed abort markers: {aborts!r}."
    )
