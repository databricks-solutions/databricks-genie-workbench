"""Phase C Task 6 — orphan_rca_records producer.

Pins:

* One ``STRATEGIST_AG_EMITTED`` + ``UNRESOLVED`` + ``RCA_UNGROUNDED``
  record per finding whose qids are not covered by any AG's
  ``affected_questions``.
* The record carries the finding's ``rca_id`` (it IS known — the
  validator's ``rca_id`` requirement passes without exemption).
* Findings covered by at least one AG produce no record.
* Empty findings → empty list.
* Empty action_groups + non-empty findings → one record per finding.

Plan: ``docs/2026-05-03-phase-c-rca-loop-contract-and-residuals-plan.md`` Task 6.
"""
from __future__ import annotations


def _f(rca_id: str, qids: tuple[str, ...], root_cause: str = "missing_filter") -> object:
    class _F:
        pass
    f = _F()
    f.rca_id = rca_id
    f.target_qids = qids
    f.root_cause = root_cause
    return f


def test_finding_covered_by_ag_produces_no_record() -> None:
    from genie_space_optimizer.optimization.decision_emitters import (
        orphan_rca_records,
    )

    findings = [_f("rca_a", ("q1",))]
    action_groups = [{"id": "AG1", "affected_questions": ["q1"]}]

    assert orphan_rca_records(
        run_id="r", iteration=1,
        findings=findings, action_groups=action_groups,
    ) == []


def test_uncovered_finding_produces_one_record() -> None:
    from genie_space_optimizer.optimization.decision_emitters import (
        orphan_rca_records,
    )
    from genie_space_optimizer.optimization.rca_decision_trace import (
        DecisionOutcome,
        DecisionType,
        ReasonCode,
    )

    findings = [_f("rca_orphan", ("q_orphan",))]
    action_groups = [{"id": "AG1", "affected_questions": ["q1"]}]

    records = orphan_rca_records(
        run_id="r", iteration=1,
        findings=findings, action_groups=action_groups,
    )
    assert len(records) == 1
    rec = records[0]
    assert rec.decision_type == DecisionType.STRATEGIST_AG_EMITTED
    assert rec.outcome == DecisionOutcome.UNRESOLVED
    assert rec.reason_code == ReasonCode.RCA_UNGROUNDED
    assert rec.rca_id == "rca_orphan"
    assert rec.target_qids == ("q_orphan",)
    assert rec.affected_qids == ("q_orphan",)
    assert rec.root_cause == "missing_filter"
    assert rec.ag_id == ""


def test_empty_inputs_yield_empty_list() -> None:
    from genie_space_optimizer.optimization.decision_emitters import (
        orphan_rca_records,
    )

    assert orphan_rca_records(
        run_id="r", iteration=1, findings=[], action_groups=[],
    ) == []


def test_no_ags_emitted_at_all_each_finding_orphaned() -> None:
    from genie_space_optimizer.optimization.decision_emitters import (
        orphan_rca_records,
    )

    findings = [_f("rca_a", ("q1",)), _f("rca_b", ("q2",))]
    records = orphan_rca_records(
        run_id="r", iteration=1, findings=findings, action_groups=[],
    )
    assert len(records) == 2
    assert {r.rca_id for r in records} == {"rca_a", "rca_b"}


def test_validator_passes_for_orphan_records() -> None:
    """Orphan records carry rca_id but no ag_id; the validator does
    NOT enforce ag_id non-empty for STRATEGIST_AG_EMITTED, so the
    record passes without needing any exemption."""
    from genie_space_optimizer.optimization.decision_emitters import (
        orphan_rca_records,
    )
    from genie_space_optimizer.optimization.rca_decision_trace import (
        validate_decisions_against_journey,
    )

    findings = [_f("rca_orphan", ("q_orphan",))]
    records = orphan_rca_records(
        run_id="r", iteration=1, findings=findings, action_groups=[],
    )
    violations = validate_decisions_against_journey(
        records=list(records), events=[],
    )
    rca_id_violations = [v for v in violations if "has no rca_id" in v]
    assert rca_id_violations == []
