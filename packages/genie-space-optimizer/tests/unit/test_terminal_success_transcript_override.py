"""Plan N2 — terminal-success transcript override.

Inspiration: run 2afb0be2-88b6-4832-99aa-c7e78fbc90f7 retry attempt
993610879088298 iteration 2. ``AG2`` was accepted, ``gs_009`` flipped
to passing, accuracy hit 100%, but the operator transcript still
rendered ``RCA Cards With Evidence: outcome=unresolved
reason=rca_ungrounded`` for cluster ``H001`` and ``Next Suggested
Action: Generate proposals for H001.``

These tests pin the render-time override that suppresses pre-
acceptance UNRESOLVED records once the matching ACCEPTANCE_DECIDED
lands in the same iteration. The trace is unchanged; only the
projection is overridden.
"""
from __future__ import annotations


def _trace_with_terminal_success():
    """Build a minimal OptimizationTrace where AG2 closes cluster
    H001 and gs_009 flips to RESOLVED in iteration 2.
    """
    from genie_space_optimizer.optimization.rca_decision_trace import (
        DecisionOutcome,
        DecisionRecord,
        DecisionType,
        OptimizationTrace,
        ReasonCode,
    )

    records = (
        # Hard failure surfaces in iter 2.
        DecisionRecord(
            run_id="2afb0be2",
            iteration=2,
            decision_type=DecisionType.EVAL_CLASSIFIED,
            outcome=DecisionOutcome.UNRESOLVED,
            reason_code=ReasonCode.HARD_FAILURE,
            question_id="gs_009",
            target_qids=("gs_009",),
        ),
        # Cluster H001 selected and target_qids stamped on the record.
        DecisionRecord(
            run_id="2afb0be2",
            iteration=2,
            decision_type=DecisionType.CLUSTER_SELECTED,
            outcome=DecisionOutcome.UNRESOLVED,
            reason_code=ReasonCode.CLUSTERED,
            cluster_id="H001",
            target_qids=("gs_009",),
            next_action="Generate proposals for H001.",
        ),
        # Pre-acceptance UNRESOLVED RCA card — the stale row that
        # leaks into the transcript today.
        DecisionRecord(
            run_id="2afb0be2",
            iteration=2,
            decision_type=DecisionType.RCA_FORMED,
            outcome=DecisionOutcome.UNRESOLVED,
            reason_code=ReasonCode.RCA_UNGROUNDED,
            cluster_id="H001",
            target_qids=("gs_009",),
            next_action="Re-run RCA prompt for H001.",
        ),
        # AG2 emitted from H001.
        DecisionRecord(
            run_id="2afb0be2",
            iteration=2,
            decision_type=DecisionType.STRATEGIST_AG_EMITTED,
            outcome=DecisionOutcome.INFO,
            reason_code=ReasonCode.STRATEGIST_SELECTED,
            ag_id="AG2",
            cluster_id="H001",
            source_cluster_ids=("H001",),
            target_qids=("gs_009",),
        ),
        # Acceptance lands, no regression debt.
        DecisionRecord(
            run_id="2afb0be2",
            iteration=2,
            decision_type=DecisionType.ACCEPTANCE_DECIDED,
            outcome=DecisionOutcome.ACCEPTED,
            reason_code=ReasonCode.NONE,
            ag_id="AG2",
            cluster_id="H001",
            target_qids=("gs_009",),
            metrics={
                "passing_to_hard_regressed_qids": [],
                "soft_to_hard_regressed_qids": [],
                "unknown_to_hard_regressed_qids": [],
            },
        ),
        # gs_009 flips to passing.
        DecisionRecord(
            run_id="2afb0be2",
            iteration=2,
            decision_type=DecisionType.QID_RESOLUTION,
            outcome=DecisionOutcome.RESOLVED,
            reason_code=ReasonCode.POST_EVAL_FAIL_TO_PASS,
            question_id="gs_009",
            cluster_id="H001",
        ),
    )
    return OptimizationTrace(decision_records=records)


def _next_action_body(transcript: str) -> str:
    """Return the body of the ``Next Suggested Action`` section.

    Section bodies in the operator transcript run from the section
    heading to the next pipe-only divider line. Used to scope
    assertions about Next Action without false-positives on
    record-line ``next=...`` fields elsewhere.
    """
    lines = transcript.splitlines()
    try:
        idx = next(
            i for i, line in enumerate(lines)
            if line.strip().endswith("Next Suggested Action")
        )
    except StopIteration:
        return ""
    body: list[str] = []
    for line in lines[idx + 1:]:
        if line.startswith("+"):  # final separator bar
            break
        body.append(line)
    return "\n".join(body)


def test_resolved_cluster_does_not_drive_next_action() -> None:
    """``Next Suggested Action`` must NOT pick a next_action from a
    record whose target cluster is now resolved. Today's section
    body surfaces ``Re-run RCA prompt for H001.``; after the
    override, the section body emits ``(no open next action)``.

    (The full RCA-card line still keeps its ``next=...`` field for
    audit; the override applies a marker, not a record rewrite.)
    """
    from genie_space_optimizer.optimization.rca_decision_trace import (
        render_operator_transcript,
    )

    trace = _trace_with_terminal_success()
    transcript = render_operator_transcript(trace=trace, iteration=2)
    nsa_body = _next_action_body(transcript)

    assert "Re-run RCA prompt for H001." not in nsa_body, (
        "Next Suggested Action body must drop next_actions whose "
        "cluster reached terminal success; got body:\n" + nsa_body
    )
    assert "Generate proposals for H001." not in nsa_body, (
        "CLUSTER_SELECTED next_action for a resolved cluster must "
        "also be filtered from Next Action body; got body:\n"
        + nsa_body
    )
    assert "(no open next action)" in nsa_body, (
        "with H001 resolved and no other open next_action, the "
        "section body must collapse to '(no open next action)'; "
        "got body:\n" + nsa_body
    )


def test_resolved_cluster_rca_card_shows_resolved_marker() -> None:
    """``RCA Cards With Evidence`` lines for the resolved cluster
    must be annotated ``[RESOLVED BY AG2 ✓]`` so the auditor sees
    that the pre-acceptance UNRESOLVED is stale-by-design.
    """
    from genie_space_optimizer.optimization.rca_decision_trace import (
        render_operator_transcript,
    )

    trace = _trace_with_terminal_success()
    transcript = render_operator_transcript(trace=trace, iteration=2)

    rca_lines = [
        line for line in transcript.splitlines()
        if "cluster=H001" in line
        and ("rca_ungrounded" in line or "clustered" in line)
    ]
    assert rca_lines, (
        "expected at least one cluster=H001 RCA-card line in the "
        "transcript:\n" + transcript
    )
    assert all(
        "[RESOLVED BY AG2" in line for line in rca_lines
    ), (
        "every cluster=H001 RCA-card line must carry the "
        "[RESOLVED BY AG2] annotation; got:\n"
        + "\n".join(rca_lines)
    )


def test_terminal_success_section_lists_each_resolved_cluster() -> None:
    """A new ``Terminal Success`` section heading must exist with
    one line naming the cluster, the AG that closed it, and the
    target QIDs that flipped.
    """
    from genie_space_optimizer.optimization.rca_decision_trace import (
        render_operator_transcript,
    )

    trace = _trace_with_terminal_success()
    transcript = render_operator_transcript(trace=trace, iteration=2)

    assert "Terminal Success" in transcript, (
        "Terminal Success section heading must appear in every "
        "iteration block; transcript:\n" + transcript
    )
    success_lines = [
        line for line in transcript.splitlines()
        if "cluster=H001" in line
        and "ag=AG2" in line
        and "gs_009" in line
        and ("resolved=" in line or "RESOLVED" in line)
    ]
    assert success_lines, (
        "Terminal Success section must list cluster=H001 ag=AG2 "
        "resolved=[gs_009]; transcript:\n" + transcript
    )


def test_partial_resolution_only_annotates_resolved_cluster() -> None:
    """Multi-cluster AG with partial resolution: cluster H001 is
    resolved but cluster H002's qid is still unresolved. The override
    annotates only H001; H002 keeps its UNRESOLVED rendering and can
    still drive Next Action.
    """
    from genie_space_optimizer.optimization.rca_decision_trace import (
        DecisionOutcome,
        DecisionRecord,
        DecisionType,
        OptimizationTrace,
        ReasonCode,
        render_operator_transcript,
    )

    records = (
        DecisionRecord(
            run_id="r",
            iteration=2,
            decision_type=DecisionType.CLUSTER_SELECTED,
            outcome=DecisionOutcome.UNRESOLVED,
            reason_code=ReasonCode.CLUSTERED,
            cluster_id="H001",
            target_qids=("gs_009",),
        ),
        DecisionRecord(
            run_id="r",
            iteration=2,
            decision_type=DecisionType.CLUSTER_SELECTED,
            outcome=DecisionOutcome.UNRESOLVED,
            reason_code=ReasonCode.CLUSTERED,
            cluster_id="H002",
            target_qids=("gs_021",),
            next_action="Generate proposals for H002.",
        ),
        DecisionRecord(
            run_id="r",
            iteration=2,
            decision_type=DecisionType.STRATEGIST_AG_EMITTED,
            outcome=DecisionOutcome.INFO,
            reason_code=ReasonCode.STRATEGIST_SELECTED,
            ag_id="AG2",
            cluster_id="H001",
            source_cluster_ids=("H001", "H002"),
            target_qids=("gs_009", "gs_021"),
        ),
        DecisionRecord(
            run_id="r",
            iteration=2,
            decision_type=DecisionType.ACCEPTANCE_DECIDED,
            outcome=DecisionOutcome.ACCEPTED,
            reason_code=ReasonCode.NONE,
            ag_id="AG2",
            target_qids=("gs_009", "gs_021"),
            metrics={},
        ),
        DecisionRecord(
            run_id="r",
            iteration=2,
            decision_type=DecisionType.QID_RESOLUTION,
            outcome=DecisionOutcome.RESOLVED,
            reason_code=ReasonCode.POST_EVAL_FAIL_TO_PASS,
            question_id="gs_009",
            cluster_id="H001",
        ),
        # gs_021 NOT resolved; UNRESOLVED record present.
        DecisionRecord(
            run_id="r",
            iteration=2,
            decision_type=DecisionType.QID_RESOLUTION,
            outcome=DecisionOutcome.UNRESOLVED,
            reason_code=ReasonCode.HARD_FAILURE,
            question_id="gs_021",
            cluster_id="H002",
        ),
    )

    trace = OptimizationTrace(decision_records=records)
    transcript = render_operator_transcript(trace=trace, iteration=2)

    # H001 is annotated; H002 is not.
    h001_lines = [
        line for line in transcript.splitlines()
        if "cluster=H001" in line
    ]
    h002_lines = [
        line for line in transcript.splitlines()
        if "cluster=H002" in line
    ]
    assert h001_lines and any("[RESOLVED BY AG2" in l for l in h001_lines), (
        "cluster=H001 must be annotated; got:\n"
        + "\n".join(h001_lines)
    )
    assert h002_lines and not any("[RESOLVED BY" in l for l in h002_lines), (
        "cluster=H002 must NOT be annotated (still unresolved); got:\n"
        + "\n".join(h002_lines)
    )
    # H002's next_action should still surface.
    assert "Generate proposals for H002." in transcript, (
        "H002's next_action must still drive Next Action when only "
        "H001 reached terminal success; transcript:\n" + transcript
    )
