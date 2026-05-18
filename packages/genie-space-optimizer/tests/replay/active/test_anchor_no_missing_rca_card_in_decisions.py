"""WU-B — replay-fixture regression smoke tests.

Walks pre-captured fixtures with the REAL run_replay() API and
asserts the anchor qids' journey state is well-typed and free of
broken-state signatures. This is regression-only: it catches "we
broke an existing fixture's journey shape" — it does NOT validate
new harness wiring (the Phase 4.5 spike showed full re-execution
costs ~1.5k LoC of fixture wiring and was abandoned).

For new-wiring validation, use
``scripts/verify_anchor_chain_invariants.py`` against a real run's
postmortem + transcript.

Per-fixture ground state at WU-B-Task-6 land time (captured against
the real run_replay API):
  * 7now (pre_bugc_fix):
      validation.is_valid=True, missing_qids=0, violations=0;
      anchor terminal states: gs_013=already_passing,
                              gs_026=rolled_back_no_progress.
  * airline (pre_bugb_fix):
      validation.is_valid=True, missing_qids=0, violations=0;
      anchor terminal states: gs_009=terminal_unactionable,
                              gs_024=rolled_back_no_progress.

If a future commit changes these fixtures or the journey-replay
walker, this test catches the drift.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest


FIXTURES_DIR = (
    Path(__file__).resolve().parents[2] / "replay" / "fixtures"
)

# Canonical pre-bug fixtures captured during prior fix cycles.
AIRLINE_FIXTURE = FIXTURES_DIR / "run_40405156883710_airline_pre_bugb_fix.json"
SEVEN_NOW_FIXTURE = FIXTURES_DIR / "run_476499410793687_7now_pre_bugc_fix.json"

# Anchor qid suffixes (space-independent).
ANCHOR_QIDS_AIRLINE = ("gs_009", "gs_024")
ANCHOR_QIDS_SEVEN_NOW = ("gs_013", "gs_026")


def _qid_suffix(qid: str) -> str:
    """Strip space prefix → 'gs_NNN'."""
    parts = (qid or "").split("_")
    for i in range(len(parts) - 1):
        if parts[i] == "gs" and parts[i + 1].isdigit():
            return f"gs_{parts[i + 1]}"
    return qid or ""


def _load_fixture(path: Path) -> dict:
    if not path.exists():
        pytest.skip(f"Fixture not present at {path}")
    with path.open() as fh:
        return json.load(fh)


@pytest.mark.parametrize(
    "fixture_path,anchor_qid_suffixes",
    [
        (AIRLINE_FIXTURE, ANCHOR_QIDS_AIRLINE),
        (SEVEN_NOW_FIXTURE, ANCHOR_QIDS_SEVEN_NOW),
    ],
)
def test_anchor_qids_are_not_missing_from_validation(
    fixture_path: Path, anchor_qid_suffixes: tuple[str, ...]
) -> None:
    """Every anchor qid the fixture touches must NOT appear in
    ``validation.missing_qids``. Missing qids are the silent
    fall-through that the WU-4 contract forbids — a question whose
    journey ended without a terminal state."""
    from genie_space_optimizer.optimization.lever_loop_replay import (
        run_replay,
    )

    fixture = _load_fixture(fixture_path)
    result = run_replay(fixture)

    anchor_set = set(anchor_qid_suffixes)
    missing_anchors = [
        q for q in result.validation.missing_qids
        if _qid_suffix(q) in anchor_set
    ]
    assert not missing_anchors, (
        f"Regression in {fixture_path.name}: anchor qids ended "
        f"without a terminal state (in validation.missing_qids): "
        f"{missing_anchors}"
    )


@pytest.mark.parametrize(
    "fixture_path,anchor_qid_suffixes",
    [
        (AIRLINE_FIXTURE, ANCHOR_QIDS_AIRLINE),
        (SEVEN_NOW_FIXTURE, ANCHOR_QIDS_SEVEN_NOW),
    ],
)
def test_anchor_terminal_state_is_typed_journey_terminal_state(
    fixture_path: Path, anchor_qid_suffixes: tuple[str, ...]
) -> None:
    """Every anchor qid that the run touched must reach a typed
    JourneyTerminalState (anything in the enum). A qid that ends
    with a string fall-through is the silent failure mode that the
    WU-4 contract forbids."""
    from genie_space_optimizer.optimization.lever_loop_replay import (
        run_replay,
    )
    from genie_space_optimizer.optimization.question_journey_contract import (
        JourneyTerminalState,
    )

    fixture = _load_fixture(fixture_path)
    result = run_replay(fixture)
    terminals = result.validation.terminal_state_by_qid

    anchor_set = set(anchor_qid_suffixes)
    untyped: list[str] = []
    found: list[str] = []
    for qid, state in terminals.items():
        if _qid_suffix(qid) not in anchor_set:
            continue
        found.append(_qid_suffix(qid))
        if not isinstance(state, JourneyTerminalState):
            untyped.append(f"{qid}={state!r} (type={type(state).__name__})")

    assert not untyped, (
        f"Regression in {fixture_path.name}: anchor qids reached "
        f"a non-typed terminal state: {untyped}"
    )
    # Sanity: at least one anchor should be present in the fixture.
    assert found, (
        f"Regression in {fixture_path.name}: none of the canonical "
        f"anchor qids {sorted(anchor_set)} were found in "
        f"terminal_state_by_qid. The fixture may have been "
        f"recaptured against a different qid set."
    )


@pytest.mark.parametrize(
    "fixture_path,anchor_qid_suffixes",
    [
        (AIRLINE_FIXTURE, ANCHOR_QIDS_AIRLINE),
        (SEVEN_NOW_FIXTURE, ANCHOR_QIDS_SEVEN_NOW),
    ],
)
def test_no_anchor_decision_record_carries_cluster_blocked_no_rca(
    fixture_path: Path, anchor_qid_suffixes: tuple[str, ...]
) -> None:
    """If the fixture carries any CLUSTER_BLOCKED_NO_RCA decision
    record (Cycle-5-T3+ shape), none of them must target an anchor
    qid. Today's pre-bug fixtures predate that decision type so the
    test is a no-op vs. real assertions; when fixtures are
    re-captured against modern decision types, this test activates
    automatically and catches any regression that re-emits the
    broken-state signature for an anchor."""
    from genie_space_optimizer.optimization.lever_loop_replay import (
        run_replay,
    )
    from genie_space_optimizer.optimization.rca_decision_trace import (
        DecisionType,
    )

    fixture = _load_fixture(fixture_path)
    result = run_replay(fixture)

    anchor_set = set(anchor_qid_suffixes)
    offenders: list[str] = []
    for rec in result.decision_records:
        if rec.decision_type != DecisionType.CLUSTER_BLOCKED_NO_RCA:
            continue
        candidate_qids: list[str] = []
        if rec.question_id:
            candidate_qids.append(rec.question_id)
        candidate_qids.extend(rec.target_qids)
        candidate_qids.extend(rec.affected_qids)
        for q in candidate_qids:
            if _qid_suffix(q) in anchor_set:
                offenders.append(
                    f"CLUSTER_BLOCKED_NO_RCA on qid={q} "
                    f"(suffix={_qid_suffix(q)}, "
                    f"cluster_id={rec.cluster_id!r}, "
                    f"iter={rec.iteration})"
                )
    assert not offenders, (
        f"Regression in {fixture_path.name}: decision records "
        f"re-emit CLUSTER_BLOCKED_NO_RCA for anchor qids: "
        f"{offenders}"
    )


@pytest.mark.parametrize(
    "fixture_path",
    [AIRLINE_FIXTURE, SEVEN_NOW_FIXTURE],
)
def test_fixture_journey_validation_passes(fixture_path: Path) -> None:
    """The journey-replay walker's full validation report must
    pass. This is a coarse regression sentinel: if a future commit
    breaks ``_replay_iteration`` or ``_LEGAL_NEXT`` in a way that
    fires illegal-transition violations on these fixtures, the
    test catches it before the broken walker reaches a live run."""
    from genie_space_optimizer.optimization.lever_loop_replay import (
        run_replay,
    )

    fixture = _load_fixture(fixture_path)
    result = run_replay(fixture)
    assert result.validation.is_valid, (
        f"Regression in {fixture_path.name}: validation.is_valid "
        f"is False. missing_qids={list(result.validation.missing_qids)}, "
        f"violations={[v.kind for v in result.validation.violations]}"
    )
