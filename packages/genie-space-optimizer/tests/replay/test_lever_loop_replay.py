"""End-to-end replay test: byte-stable canonical ledger + zero violations."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

FIXTURE_PATH = (
    Path(__file__).parent / "fixtures" / "airline_5cluster.json"
)


def _load_fixture() -> dict:
    return json.loads(FIXTURE_PATH.read_text())


def test_airline_5cluster_replay_validation_is_clean() -> None:
    from genie_space_optimizer.optimization.lever_loop_replay import (
        run_replay,
    )

    result = run_replay(_load_fixture())
    if not result.validation.is_valid:
        pytest.fail(
            "Replay produced validation violations:\n"
            + "\n".join(
                f"  qid={v.question_id} kind={v.kind} detail={v.detail}"
                for v in result.validation.violations
            )
            + f"\nmissing_qids={list(result.validation.missing_qids)}"
        )


def test_airline_5cluster_replay_canonical_ledger_is_byte_stable() -> None:
    from genie_space_optimizer.optimization.lever_loop_replay import (
        run_replay,
    )

    fixture = _load_fixture()
    expected = fixture.get("expected_canonical_journey")
    if not expected:
        pytest.skip(
            "expected_canonical_journey not yet recorded; run "
            "scripts/record_replay_baseline.py to seed it."
        )
    result = run_replay(fixture)
    assert result.canonical_json == expected, (
        "Canonical journey drift detected. If this drift was intentional, "
        "rerun the baseline recorder script and commit the new fixture."
    )


def test_airline_5cluster_replay_completes_in_under_thirty_seconds() -> None:
    import time
    from genie_space_optimizer.optimization.lever_loop_replay import (
        run_replay,
    )

    fixture = _load_fixture()
    started = time.perf_counter()
    run_replay(fixture)
    elapsed = time.perf_counter() - started
    assert elapsed < 30.0, f"Replay took {elapsed:.2f}s (>30s budget)."


# -----------------------------------------------------------------------------
# Per-iteration validation tests for `run_replay`.
#
# Pin that ``run_replay`` invokes ``validate_question_journeys`` once per
# iteration (mirroring the harness production contract at
# ``harness.py:16039-16056``) so cross-iteration ``X -> X`` self-transitions on
# the same qid are not reported as illegal_transition violations.
#
# See `docs/2026-05-02-run-replay-per-iteration-fix-plan.md` for the full
# diagnosis (cycle 7's airline_real_v1.json produced 328 violations under the
# old single-call validator, ~320 of which were cross-iteration noise).
# -----------------------------------------------------------------------------

FIXTURES = Path(__file__).parent / "fixtures"


def _load(name: str) -> dict:
    return json.loads((FIXTURES / name).read_text())


def test_run_replay_two_iter_clean_fixture_validates_cleanly() -> None:
    """A 2-iter fixture where each iteration is independently legal must report
    zero violations. Reproducer for cycle 7's cross-iteration noise."""
    from genie_space_optimizer.optimization.lever_loop_replay import run_replay

    result = run_replay(_load("synthetic_two_iter_clean.json"))

    assert result.validation.is_valid, (
        "Expected zero violations for a 2-iter fixture where each iteration "
        "is independently legal. Violations:\n"
        + "\n".join(
            f"  qid={v.question_id} kind={v.kind} detail={v.detail}"
            for v in result.validation.violations
        )
    )
    assert result.validation.violations == []
    assert result.validation.missing_qids == ()


def test_run_replay_intra_iter_violation_is_caught_and_attributed() -> None:
    """A qid that goes evaluated -> post_eval (no classification stage) is an
    illegal transition. The fix must keep catching this; it must NOT be silenced
    by per-iteration scoping."""
    from genie_space_optimizer.optimization.lever_loop_replay import run_replay

    result = run_replay(_load("synthetic_two_iter_one_intra_violation.json"))

    assert not result.validation.is_valid
    illegal = [
        v for v in result.validation.violations if v.kind == "illegal_transition"
    ]
    assert len(illegal) == 1, (
        f"Expected 1 illegal_transition (syn_q2 evaluated -> post_eval), got "
        f"{len(illegal)}: {[(v.question_id, v.detail) for v in illegal]}"
    )
    assert illegal[0].question_id == "syn_q2"
    # PR-C: detail is prefixed with the chain label ("trunk: " for trunk
    # transitions, "lane[<pid>]: " for proposal-lane transitions) so
    # postmortem readers can locate the offending chain.
    assert illegal[0].detail == "trunk: evaluated -> post_eval"


def test_run_replay_single_iter_5cluster_fixture_still_validates_cleanly() -> None:
    """Regression: airline_5cluster.json (1 iteration, the original test
    fixture the validator was designed against) must keep validating cleanly
    after the per-iteration refactor."""
    from genie_space_optimizer.optimization.lever_loop_replay import run_replay

    result = run_replay(_load("airline_5cluster.json"))

    assert result.validation.is_valid, (
        "5cluster regression: validation must remain clean. Violations:\n"
        + "\n".join(
            f"  qid={v.question_id} kind={v.kind} detail={v.detail}"
            for v in result.validation.violations
        )
    )


def test_run_replay_airline_real_v1_within_burndown_budget() -> None:
    """The current canonical airline fixture must validate with no more than
    BURNDOWN_BUDGET violations.

    Tighten the budget in this test each time a real intra-iteration violation
    is fixed in the harness/exporter. When the budget reaches 0, Phase A
    burn-down has closed hard against the airline corpus.

    See `docs/2026-05-02-phase-a-burndown-log.md` for the per-cycle history
    and `docs/2026-05-02-run-replay-per-iteration-fix-plan.md` Phase 5 for
    the per-cycle intake runbook.
    """
    from genie_space_optimizer.optimization.lever_loop_replay import run_replay

    # Updated by Phase 5 Task 14 Step 6 each time a fresh cycle lands.
    # Never increase this number without explicit triage in the burn-down log.
    #
    # Cycle 8 (2026-05-02): hard-closed at 0. The replay-engine fix at
    # lever_loop_replay.py:80-86 (commit abd0716) eliminated the dominant
    # `soft_signal -> already_passing` double-emit pattern, and Cycle 8's
    # harness-side improvements zeroed the remaining harness emit-gap
    # patterns. See docs/2026-05-02-phase-a-burndown-log.md cycle 8 row.
    BURNDOWN_BUDGET = 0

    fx = json.loads((FIXTURES / "airline_real_v1.json").read_text())
    result = run_replay(fx)
    summary = [
        (v.question_id, v.kind, v.detail)
        for v in result.validation.violations[:5]
    ]
    assert len(result.validation.violations) <= BURNDOWN_BUDGET, (
        f"airline_real_v1 produced {len(result.validation.violations)} "
        f"violations (budget={BURNDOWN_BUDGET}). First 5: {summary}"
    )


def test_run_replay_demotes_already_passing_when_qid_in_soft_cluster() -> None:
    """A qid that is row-level `already_passing` (rc=yes, arbiter=both_correct)
    AND listed in `soft_clusters[*].question_ids` must NOT receive both an
    `already_passing` and a `soft_signal` event in the same iteration. The
    explicit fixture-soft-promotion at `lever_loop_replay.py:74-82` declares
    the cluster's classification authoritative, so the qid must be demoted
    out of every other row-level partition it might also belong to.

    Reproducer for the Cycle 8 dominant violation pattern:
    `soft_signal -> already_passing` × 9 overlap qids × 5 iterations = 45.
    All 45 vanish after this fix; that is the entire Cycle 8 burn-down.
    """
    from genie_space_optimizer.optimization.lever_loop_replay import run_replay

    fixture_path = (
        Path(__file__).parent
        / "fixtures"
        / "synthetic_already_passing_in_soft_cluster.json"
    )
    fixture = json.loads(fixture_path.read_text())

    result = run_replay(fixture)

    # The whole 3-qid fixture must validate cleanly.
    assert result.validation.is_valid, (
        f"expected clean validation, got "
        f"{len(result.validation.violations)} violations: "
        f"{[(v.question_id, v.kind, v.detail) for v in result.validation.violations]}"
    )

    # Direct stage-set checks per qid (semantic, not order-dependent).
    stages_by_qid: dict[str, set[str]] = {}
    for ev in result.events:
        stages_by_qid.setdefault(ev.question_id, set()).add(ev.stage)

    overlap_stages = stages_by_qid.get("syn_overlap_q", set())
    assert "soft_signal" in overlap_stages, (
        "syn_overlap_q is in soft_clusters[*] so it MUST receive a "
        f"soft_signal event; got stages={overlap_stages}"
    )
    assert "already_passing" not in overlap_stages, (
        "syn_overlap_q is row-level already_passing AND in a soft_cluster; "
        "the fixture-soft-promotion makes the cluster authoritative, so "
        "already_passing must be demoted. "
        f"got stages={overlap_stages}"
    )

    # Pure already_passing (not in any soft_cluster) is unchanged.
    pure_passing_stages = stages_by_qid.get("syn_pure_passing_q", set())
    assert "already_passing" in pure_passing_stages, (
        f"syn_pure_passing_q must keep already_passing; got "
        f"stages={pure_passing_stages}"
    )
    assert "soft_signal" not in pure_passing_stages

    # Pure soft (not row-level already_passing) is unchanged.
    pure_soft_stages = stages_by_qid.get("syn_pure_soft_q", set())
    assert "soft_signal" in pure_soft_stages
    assert "already_passing" not in pure_soft_stages


def test_run_replay_recognizes_skipped_ag_outcomes() -> None:
    """Cycle 8 introduced two new AG outcomes (skipped_no_applied_patches,
    skipped_dead_on_arrival). The replay engine must recognize them
    explicitly: emit a terminal `rolled_back` event ONLY for qids that
    reached `applied`, and emit nothing for qids that never reached
    `applied`. In both cases the resulting journey must be valid.
    """
    from genie_space_optimizer.optimization.lever_loop_replay import run_replay

    fixture_path = (
        Path(__file__).parent
        / "fixtures"
        / "synthetic_skipped_ag_outcomes.json"
    )
    fixture = json.loads(fixture_path.read_text())

    result = run_replay(fixture)

    # All three sub-cases must validate cleanly.
    assert result.validation.is_valid, (
        f"expected clean validation, got "
        f"{len(result.validation.violations)} violations: "
        f"{[(v.question_id, v.kind) for v in result.validation.violations]}"
    )

    # Sub-case 1 + 2 (target_qids:[]): no terminal AG event for those qids.
    rolled_back_qids = {
        e.question_id for e in result.events if e.stage == "rolled_back"
    }
    no_apply_qids = {"syn_q1", "syn_q2", "syn_q3", "syn_q4"}
    assert rolled_back_qids.isdisjoint(no_apply_qids), (
        f"qids whose AG had no applied patches must NOT receive a "
        f"terminal AG event; got rolled_back for "
        f"{rolled_back_qids & no_apply_qids}"
    )

    # Sub-case 3: applied qid gets a terminal rolled_back.
    assert "syn_q5" in rolled_back_qids, (
        "qid that reached `applied` under skipped_dead_on_arrival must "
        "receive a terminal `rolled_back` event so the journey legally "
        "ends `applied -> rolled_back -> post_eval`"
    )
    # syn_q6 is in affected_questions but NOT in target_qids, so no `applied`
    # event for it → no terminal rolled_back for it either.
    assert "syn_q6" not in rolled_back_qids, (
        "qid that is in affected_questions but never reached `applied` "
        "must NOT receive a terminal AG event"
    )


def test_airline_real_v1_replay_canonical_ledger_is_byte_stable() -> None:
    """The canonical journey ledger persisted as `expected_canonical_journey`
    in airline_real_v1.json must reproduce byte-for-byte from a fresh
    `run_replay` call. If this drifts, either the producer (replay engine)
    changed legitimately — re-record with `scripts/record_replay_baseline.py`
    — or there's a regression."""
    from genie_space_optimizer.optimization.lever_loop_replay import run_replay

    fixture = _load("airline_real_v1.json")
    expected = fixture.get("expected_canonical_journey")
    if not expected:
        pytest.skip(
            "expected_canonical_journey not yet recorded; run "
            "scripts/record_replay_baseline.py to seed it."
        )
    result = run_replay(fixture)
    assert result.canonical_json == expected, (
        "Canonical journey drift detected. If this drift was intentional, "
        "rerun the baseline recorder script and commit the new fixture."
    )


def test_airline_real_v1_replay_completes_in_under_thirty_seconds() -> None:
    """Replay must stay fast enough to be a developer inner-loop tool. The
    full 5-iteration airline fixture takes well under a second locally; a
    30s budget catches accidental quadratic regressions in the replay
    engine without being flaky in CI."""
    import time
    from genie_space_optimizer.optimization.lever_loop_replay import run_replay

    fixture = _load("airline_real_v1.json")
    started = time.perf_counter()
    run_replay(fixture)
    elapsed = time.perf_counter() - started
    assert elapsed < 30.0, f"Replay took {elapsed:.2f}s (>30s budget)."


def test_run_replay_exposes_decision_trace_outputs() -> None:
    from genie_space_optimizer.optimization.lever_loop_replay import run_replay

    fixture = {
        "fixture_id": "decision_replay_v1",
        "iterations": [
            {
                "iteration": 1,
                "eval_rows": [
                    {
                        "question_id": "q1",
                        "result_correctness": "no",
                        "arbiter": "ground_truth_correct",
                    }
                ],
                "clusters": [
                    {
                        "cluster_id": "H001",
                        "root_cause": "missing_filter",
                        "question_ids": ["q1"],
                    }
                ],
                "soft_clusters": [],
                "strategist_response": {
                    "action_groups": [
                        {
                            "id": "AG1",
                            "affected_questions": ["q1"],
                            "patches": [
                                {
                                    "proposal_id": "P001",
                                    "patch_type": "add_sql_snippet_filter",
                                    "target_qids": ["q1"],
                                    "cluster_id": "H001",
                                }
                            ],
                        }
                    ]
                },
                "ag_outcomes": {"AG1": "accepted"},
                "post_eval_passing_qids": ["q1"],
                "decision_records": [
                    {
                        "run_id": "fixture",
                        "iteration": 1,
                        "decision_type": "proposal_generated",
                        "outcome": "accepted",
                        "reason_code": "proposal_emitted",
                        "question_id": "q1",
                        "rca_id": "rca_q1_missing_filter",
                        "root_cause": "missing_filter",
                        "ag_id": "AG1",
                        "proposal_id": "P001",
                        "evidence_refs": ["eval:q1", "cluster:H001"],
                        "target_qids": ["q1"],
                        "expected_effect": "The proposed filter should make q1 pass.",
                        "observed_effect": "q1 passed post-eval.",
                        "regression_qids": [],
                        "next_action": "Keep patch if no regressions are observed.",
                        "affected_qids": ["q1"],
                    }
                ],
            }
        ],
    }

    result = run_replay(fixture)

    assert result.decision_records[0].proposal_id == "P001"
    assert "proposal_generated" in result.canonical_decision_json
    assert "OPERATOR TRANSCRIPT  iteration=1" in result.operator_transcript
    assert result.decision_validation == []


def test_airline_real_v1_replay_decision_trace_is_byte_stable() -> None:
    """Phase B Task 8 — pin the canonical decision-trace JSON for the
    real-run airline fixture once it carries decision_records. Skips
    until the next real cycle refreshes the fixture (the run that
    produces this fixture is also the first one running with the
    Phase B harness wiring from Task 7)."""
    from genie_space_optimizer.optimization.lever_loop_replay import run_replay

    fixture = _load("airline_real_v1.json")
    expected = fixture.get("expected_canonical_decisions")
    if not expected:
        pytest.skip(
            "expected_canonical_decisions not yet recorded; seed it after "
            "Phase B decision_records are present in airline_real_v1.json."
        )
    result = run_replay(fixture)
    assert result.canonical_decision_json == expected


def test_airline_real_v1_operator_transcript_is_byte_stable() -> None:
    """Phase B Task 8 — pin the rendered operator transcript so any
    drift in render_operator_transcript fails CI."""
    from genie_space_optimizer.optimization.lever_loop_replay import run_replay

    fixture = _load("airline_real_v1.json")
    expected = fixture.get("expected_operator_transcript")
    if not expected:
        pytest.skip(
            "expected_operator_transcript not yet recorded; seed it after "
            "Phase B decision_records are present in airline_real_v1.json."
        )
    result = run_replay(fixture)
    assert result.operator_transcript == expected


def test_synthetic_multi_alt_proposals_one_qid_does_not_double_emit() -> None:
    """Cycle 10 RC-4 regression: an AG with N alternative proposals all
    targeting the same qid must produce exactly one ``proposed`` and one
    ``applied`` event for that qid (per-qid per-AG dedup at
    ``lever_loop_replay.py``). Without the fix, the replay engine emits N
    ``proposed`` and N ``applied`` events for the same qid in the same iter,
    producing ``proposed -> proposed`` and ``applied -> applied`` self-
    transition violations against the journey contract.
    """
    from collections import Counter

    from genie_space_optimizer.optimization.lever_loop_replay import run_replay

    fixture = _load("synthetic_multi_alt_proposals_one_qid.json")
    result = run_replay(fixture)

    proposed_for_q1 = [
        e for e in result.events
        if e.stage == "proposed" and e.question_id == "Q1"
    ]
    applied_for_q1 = [
        e for e in result.events
        if e.stage == "applied" and e.question_id == "Q1"
    ]
    assert len(proposed_for_q1) == 1, (
        f"expected exactly 1 'proposed' event for Q1 across the AG's 13 "
        f"alternative patches, got {len(proposed_for_q1)}"
    )
    assert len(applied_for_q1) == 1, (
        f"expected exactly 1 'applied' event for Q1 across the AG's 13 "
        f"alternative patches, got {len(applied_for_q1)}"
    )
    assert result.validation.is_valid, (
        "expected zero violations, got "
        f"{Counter(v.detail for v in result.validation.violations)}"
    )
