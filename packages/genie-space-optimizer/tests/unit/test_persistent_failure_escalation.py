"""Tests for Task 8: persistent-failure escalation.

After a cluster signature accumulates two CONTENT_REGRESSION rollbacks
within a run, the optimizer should stop proposing patches for it and
park the case for human review. These tests pin:

* Threshold semantics (default 2; configurable per call).
* Only ``CONTENT_REGRESSION`` rollbacks count — infra / schema
  failures don't.
* Accepted entries don't count.
* ``already_escalated_signatures`` makes the helper idempotent: a
  re-run on the same buffer doesn't re-escalate.
* Each affected qid produces its own Delta row so the reviewer queue
  is keyed at the question level.
* Evidence carries the rollback reasons, iterations, levers tried,
  and AG ids.
"""

from __future__ import annotations

from genie_space_optimizer.optimization.persistent_failure_escalation import (
    HUMAN_REQUIRED_CONTENT_ROLLBACK_THRESHOLD,
    HumanRequiredCase,
    case_to_delta_row,
    compute_human_required_escalations,
)


def _make_entry(
    *,
    sig: str = "sig_h001",
    accepted: bool = False,
    rollback_class: str = "content_regression",
    iteration: int = 1,
    affected: list[str] | None = None,
    levers: list[int] | None = None,
    rollback_reason: str = "full_eval: post_arbiter_guardrail",
    root_cause: str = "wrong_table",
    ag_id: str = "AG1",
) -> dict:
    return {
        "iteration": iteration,
        "ag_id": ag_id,
        "accepted": accepted,
        "rollback_class": rollback_class,
        "rollback_reason": rollback_reason,
        "source_cluster_signatures": [sig] if sig else [],
        "root_cause": root_cause,
        # Differentiate ``None`` (caller wants the default) from ``[]``
        # (caller wants explicitly empty).
        "affected_question_ids": (
            ["q1", "q2"] if affected is None else affected
        ),
        "levers": levers or [1],
    }


# ── Threshold semantics ──────────────────────────────────────


def test_default_threshold_is_two():
    assert HUMAN_REQUIRED_CONTENT_ROLLBACK_THRESHOLD == 2


def test_single_rollback_does_not_escalate():
    buffer = [_make_entry(iteration=1)]

    cases, newly = compute_human_required_escalations(buffer, run_id="r")

    assert cases == []
    assert newly == set()


def test_two_rollbacks_on_same_signature_escalate():
    buffer = [
        _make_entry(sig="H001", iteration=1, ag_id="AG1"),
        _make_entry(sig="H001", iteration=2, ag_id="AG2", levers=[5]),
    ]

    cases, newly = compute_human_required_escalations(buffer, run_id="r")

    assert "H001" in newly
    # 2 affected qids → 2 cases, both attempt_count=2
    assert len(cases) == 2
    assert all(c.attempt_count == 2 for c in cases)
    assert all(c.last_iteration == 2 for c in cases)
    assert all(c.reason_code == "persistent_content_rollback" for c in cases)


def test_threshold_can_be_raised_per_call():
    buffer = [
        _make_entry(sig="H001", iteration=1),
        _make_entry(sig="H001", iteration=2),
    ]

    cases, newly = compute_human_required_escalations(
        buffer, run_id="r", threshold=3,
    )

    assert cases == []
    assert newly == set()


# ── Class filtering ──────────────────────────────────────────


def test_infra_rollbacks_do_not_count():
    buffer = [
        _make_entry(sig="H001", iteration=1, rollback_class="infra_failure"),
        _make_entry(sig="H001", iteration=2, rollback_class="infra_failure"),
    ]

    cases, newly = compute_human_required_escalations(buffer, run_id="r")

    assert cases == []
    assert newly == set()


def test_schema_failures_do_not_count():
    buffer = [
        _make_entry(sig="H001", iteration=1, rollback_class="schema_failure"),
        _make_entry(sig="H001", iteration=2, rollback_class="schema_failure"),
    ]

    cases, newly = compute_human_required_escalations(buffer, run_id="r")

    assert cases == []


def test_accepted_entries_do_not_count():
    buffer = [
        _make_entry(sig="H001", iteration=1, accepted=True),
        _make_entry(sig="H001", iteration=2, accepted=True),
    ]

    cases, newly = compute_human_required_escalations(buffer, run_id="r")

    assert cases == []


def test_mixed_classes_only_counts_content_regressions():
    buffer = [
        _make_entry(sig="H001", iteration=1),
        _make_entry(sig="H001", iteration=2, rollback_class="infra_failure"),
        _make_entry(sig="H001", iteration=3),
    ]

    cases, newly = compute_human_required_escalations(buffer, run_id="r")

    # 2 of 3 are content rollbacks → escalates
    assert "H001" in newly
    assert all(c.attempt_count == 2 for c in cases)


# ── Idempotency ──────────────────────────────────────────────


def test_already_escalated_signatures_are_skipped():
    buffer = [
        _make_entry(sig="H001", iteration=1),
        _make_entry(sig="H001", iteration=2),
    ]

    cases, newly = compute_human_required_escalations(
        buffer, run_id="r", already_escalated_signatures={"H001"},
    )

    assert cases == []
    assert newly == set()


def test_partial_already_escalated_only_emits_new_signatures():
    buffer = [
        _make_entry(sig="H001", iteration=1),
        _make_entry(sig="H001", iteration=2),
        _make_entry(sig="H002", iteration=1),
        _make_entry(sig="H002", iteration=2),
    ]

    cases, newly = compute_human_required_escalations(
        buffer, run_id="r", already_escalated_signatures={"H001"},
    )

    assert newly == {"H002"}
    assert {c.cluster_signature for c in cases} == {"H002"}


# ── Multi-signature handling ─────────────────────────────────


def test_each_signature_evaluated_independently():
    buffer = [
        _make_entry(sig="H001", iteration=1),
        _make_entry(sig="H001", iteration=2),
        _make_entry(sig="H002", iteration=1),  # only one rollback for H002
    ]

    cases, newly = compute_human_required_escalations(buffer, run_id="r")

    assert newly == {"H001"}
    assert all(c.cluster_signature == "H001" for c in cases)


def test_one_row_per_affected_qid():
    buffer = [
        _make_entry(sig="H001", iteration=1, affected=["q1", "q2", "q3"]),
        _make_entry(sig="H001", iteration=2, affected=["q1", "q4"]),
    ]

    cases, _ = compute_human_required_escalations(buffer, run_id="r")

    qids = {c.question_id for c in cases}
    assert qids == {"q1", "q2", "q3", "q4"}


def test_signature_with_no_affected_qids_emits_sentinel_row():
    buffer = [
        _make_entry(sig="H001", iteration=1, affected=[]),
        _make_entry(sig="H001", iteration=2, affected=[]),
    ]

    cases, _ = compute_human_required_escalations(buffer, run_id="r")

    # Single sentinel row with empty qid so the signature is queryable.
    assert len(cases) == 1
    assert cases[0].question_id == ""


# ── Evidence shape ─────────────────────────────────────────


def test_evidence_carries_typed_rollback_history():
    buffer = [
        _make_entry(
            sig="H001", iteration=1, ag_id="AG1", levers=[2],
            rollback_reason="full_eval: post_arbiter_guardrail",
        ),
        _make_entry(
            sig="H001", iteration=3, ag_id="AG3", levers=[5, 6],
            rollback_reason="full_eval: per_question_regression",
        ),
    ]

    cases, _ = compute_human_required_escalations(buffer, run_id="r")

    ev = cases[0].evidence
    assert ev["iterations"] == [1, 3]
    assert sorted(ev["levers_tried"]) == [2, 5, 6]
    assert "post_arbiter_guardrail" in ev["rollback_reasons"][0]
    assert "per_question_regression" in ev["rollback_reasons"][1]
    assert ev["ag_ids"] == ["AG1", "AG3"]


# ── Conversion helper ────────────────────────────────────


def test_case_to_delta_row_shape():
    case = HumanRequiredCase(
        run_id="r",
        cluster_signature="H001",
        question_id="q1",
        root_cause="wrong_table",
        attempt_count=2,
        last_iteration=2,
        reason_code="persistent_content_rollback",
        evidence={"iterations": [1, 2]},
    )

    row = case_to_delta_row(case)

    assert row["run_id"] == "r"
    assert row["cluster_signature"] == "H001"
    assert row["question_id"] == "q1"
    assert row["attempt_count"] == 2
    assert row["last_iteration"] == 2
    assert row["reason_code"] == "persistent_content_rollback"
    # evidence_json carries the dict; the state writer JSON-serializes.
    assert row["evidence_json"] == {"iterations": [1, 2]}


# ── Defensive paths ─────────────────────────────────────


def test_empty_buffer_is_safe():
    cases, newly = compute_human_required_escalations([], run_id="r")

    assert cases == []
    assert newly == set()


def test_handles_malformed_entries():
    buffer = [None, "not a dict", {}, _make_entry(sig="H001", iteration=1)]

    cases, _ = compute_human_required_escalations(buffer, run_id="r")

    # Only one valid content rollback under threshold → no escalation
    assert cases == []


def test_iteration_sort_handles_string_iteration_field():
    buffer = [
        {**_make_entry(sig="H001", iteration=2)},
        {**_make_entry(sig="H001"), "iteration": "1"},
    ]

    cases, _ = compute_human_required_escalations(buffer, run_id="r")

    assert all(c.attempt_count == 2 for c in cases)
    # last_iteration prefers the larger numeric value
    assert all(c.last_iteration == 2 for c in cases)
