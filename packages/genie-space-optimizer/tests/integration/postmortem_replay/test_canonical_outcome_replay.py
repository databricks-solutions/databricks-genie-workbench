"""Canonical CandidateOutcome postmortem-replay merge gate (Phase 3 #14).

The e943 / d139 postmortems showed a clean full-eval win that was
authoritative for *nothing*: the run outcome, the candidate ledger row,
the scoreboard/run_summary delta, and the post-win terminal reason each
re-derived the story from disjoint inputs and disagreed. Phase 0 made a
single :class:`CandidateOutcome` record the source of truth for all of
them.

This gate replays four production-derived scenarios through the live
canonical projection helpers and asserts every projection surface agrees
with the authoritative full-eval result:

  1. flat kept-insufficient (d139 315989263572380) — no accept; the run
     honestly reports OPTIMIZER_TRIED_INSUFFICIENT_GAIN, no ledger
     overrides, zero delta, not a clean win.
  2. invalid snippet ID (d139) — a freshly minted ID is 32-char and
     Genie-valid; a 16-char ID can NEVER be stamped.
  3. target-debt acceptance — an accepted aggregate gain with a target
     QID still hard projects OPTIMIZER_AGGREGATE_GAIN_TARGET_DEBT and is
     NOT a clean deployable win, but the ledger/delta still carry the
     real gain.
  4. clean full-eval success (e943 257106447313472) — projects
     OPTIMIZER_IMPROVED, the ledger/delta carry +12.5pp, the win is
     clean/deployable, and the post-win no-work iteration is relabelled
     to an explicit reason (never a misleading 'unknown' abort).

The gate is BLOCKING in ci-merge-gate.yml — a change is not done until
all four scenarios show agreeing projections.
"""
from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

import pytest

from genie_space_optimizer.optimization.candidate_outcome import (
    POST_WIN_NO_WORK_TERMINAL_REASON,
    CandidateOutcome,
    relabel_post_win_terminal_reason,
)
from genie_space_optimizer.optimization.run_output_bundle import (
    build_run_summary,
    build_scoreboard,
)

FIXTURE = (
    Path(__file__).parent / "fixtures" / "run_canonical_outcome_replay.json"
)
_HEX32 = re.compile(r"^[0-9a-f]{32}$")


def _load() -> dict[str, Any]:
    return json.loads(FIXTURE.read_text())


@pytest.fixture(scope="module")
def fx() -> dict[str, Any]:
    return _load()


def _scoreboard_delta(candidate: CandidateOutcome | None) -> float:
    """Project the scoreboard delta from the canonical record exactly as
    the harness does — canonical_delta_pp overrides the legacy compute."""
    canonical = candidate.delta_pp if candidate is not None else None
    sb = build_scoreboard(
        iter_record_counts=[1],
        iter_violation_counts=[0],
        no_records_iterations=[],
        levers_attempted={},
        levers_accepted={},
        levers_rolled_back={},
        best_accuracy=(
            candidate.candidate_accuracy if candidate is not None else 87.5
        ),
        baseline_accuracy=(
            candidate.baseline_accuracy if candidate is not None else 87.5
        ),
        iteration_count=1,
        canonical_delta_pp=canonical,
    )
    return float(sb["accuracy_delta_pp"] or 0.0)


def _run_summary_delta(candidate: CandidateOutcome | None) -> float:
    canonical = candidate.delta_pp if candidate is not None else None
    rs = build_run_summary(
        baseline={"overall_accuracy": 87.5},
        terminal_state={},
        iteration_count=1,
        accuracy_delta_pp=0.0,
        canonical_delta_pp=canonical,
    )
    return float(rs["accuracy_delta_pp"] or 0.0)


# ── Scenario 1: flat kept-insufficient ────────────────────────────────

def test_flat_kept_insufficient_all_projections_agree(fx) -> None:
    sc = fx["flat_kept_insufficient"]
    exp = sc["expected"]
    candidate = None  # no accept landed

    run_outcome = CandidateOutcome.project_run_outcome(
        sc["trajectory_outcome"], candidate
    )
    assert run_outcome == exp["run_outcome"], (
        "flat kept-insufficient: no accept → the honest trajectory "
        f"outcome must pass through; got {run_outcome}"
    )
    # No candidate → no ledger overrides for the iteration.
    overrides = (
        candidate.ledger_overrides_for_iteration(sc["iteration"])
        if candidate is not None
        else {}
    )
    assert overrides == exp["ledger_overrides"]
    assert _scoreboard_delta(candidate) == pytest.approx(
        exp["scoreboard_delta_pp"]
    )
    assert _run_summary_delta(candidate) == pytest.approx(
        exp["scoreboard_delta_pp"]
    )


# ── Scenario 2: invalid snippet ID ────────────────────────────────────

def test_invalid_snippet_id_cannot_be_stamped(fx) -> None:
    from genie_space_optimizer.optimization.producer_snippet_validator import (
        _mint_snippet_id,
        _stamped_id_is_genie_valid,
    )

    sc = fx["invalid_snippet_id"]
    exp = sc["expected"]

    minted = _mint_snippet_id(sc["intent_id"], sc["normalized_sql"])
    assert len(minted) == exp["minted_id_len"]
    assert _HEX32.match(minted), f"minted id not 32-char hex: {minted!r}"

    # A correctly-minted ID is Genie-valid (both the top-level
    # ``snippet_id`` and the nested ``sql_snippet.id`` are validated).
    ok, _ = _stamped_id_is_genie_valid(
        {"snippet_id": minted, "sql_snippet": {"id": minted}}
    )
    assert ok is exp["minted_id_genie_valid"]

    # The 16-char malformed ID from the postmortem is rejected.
    bad = sc["malformed_id_16char"]
    bad_ok, _ = _stamped_id_is_genie_valid(
        {"snippet_id": bad, "sql_snippet": {"id": bad}}
    )
    assert bad_ok is exp["malformed_id_stamped"]


# ── Scenario 3: target-debt acceptance ────────────────────────────────

def test_target_debt_acceptance_all_projections_agree(fx) -> None:
    sc = fx["target_debt_acceptance"]
    exp = sc["expected"]
    candidate = CandidateOutcome.from_full_eval_payload(
        sc["full_eval_payload"],
        selected_proposal_id=sc["selected_proposal_id"],
        acceptance_tier=sc["acceptance_tier"],
        patches_applied=sc["patches_applied"],
        levers=sc["levers"],
    )

    run_outcome = CandidateOutcome.project_run_outcome(
        sc["trajectory_outcome"], candidate
    )
    assert run_outcome == exp["run_outcome"], (
        "target-debt accept: an accepted gain with a target still hard "
        f"must project AGGREGATE_GAIN_TARGET_DEBT; got {run_outcome}"
    )
    assert candidate.has_target_debt is True
    assert candidate.is_clean_win is False
    assert candidate.is_clean_win is exp["deploy_clean_win"]

    overrides = candidate.ledger_overrides_for_iteration(
        sc["full_eval_payload"]["iteration"]
    )
    assert overrides == exp["ledger_overrides"]
    assert _scoreboard_delta(candidate) == pytest.approx(
        exp["scoreboard_delta_pp"]
    )
    assert _run_summary_delta(candidate) == pytest.approx(
        exp["scoreboard_delta_pp"]
    )


# ── Scenario 4: clean full-eval success ───────────────────────────────

def test_clean_full_eval_success_all_projections_agree(fx) -> None:
    sc = fx["clean_full_eval_success"]
    exp = sc["expected"]
    candidate = CandidateOutcome.from_full_eval_payload(
        sc["full_eval_payload"],
        selected_proposal_id=sc["selected_proposal_id"],
        acceptance_tier=sc["acceptance_tier"],
        patches_applied=sc["patches_applied"],
        levers=sc["levers"],
    )

    # 1. Run outcome — the clean accept dominates the stale trajectory.
    run_outcome = CandidateOutcome.project_run_outcome(
        sc["trajectory_outcome"], candidate
    )
    assert run_outcome == exp["run_outcome"], (
        "clean win: a clean full-eval accept must dominate the per-QID "
        f"kept_insufficient trajectory; got {run_outcome}"
    )

    # 2. Clean / deployable.
    assert candidate.is_clean_win is exp["deploy_clean_win"]
    assert candidate.has_target_debt is False

    # 3. Ledger row carries the real win, not the 0.0/reject_loss defaults.
    overrides = candidate.ledger_overrides_for_iteration(
        sc["full_eval_payload"]["iteration"]
    )
    assert overrides == exp["ledger_overrides"]

    # 4. Scoreboard + run_summary deltas agree with the canonical delta.
    assert _scoreboard_delta(candidate) == pytest.approx(
        exp["scoreboard_delta_pp"]
    )
    assert _run_summary_delta(candidate) == pytest.approx(
        exp["scoreboard_delta_pp"]
    )

    # 5. Post-win no-work iteration is relabelled, not a misleading abort.
    relabelled = relabel_post_win_terminal_reason(
        candidate, sc["post_win_terminal_reason_raw"]
    )
    assert relabelled == exp["post_win_terminal_reason"]
    assert relabelled == POST_WIN_NO_WORK_TERMINAL_REASON


def test_invariant_violation_is_never_masked_by_a_clean_win(fx) -> None:
    """Cross-cutting guard: even an accepted clean candidate must NOT
    paper over a genuine SM1 invariant violation in the trajectory
    classifier — that breakage stays visible."""
    sc = fx["clean_full_eval_success"]
    candidate = CandidateOutcome.from_full_eval_payload(
        sc["full_eval_payload"],
        selected_proposal_id=sc["selected_proposal_id"],
        acceptance_tier=sc["acceptance_tier"],
        patches_applied=sc["patches_applied"],
        levers=sc["levers"],
    )
    projected = CandidateOutcome.project_run_outcome(
        "OPTIMIZER_INVARIANT_VIOLATION", candidate
    )
    assert projected == "OPTIMIZER_INVARIANT_VIOLATION"
