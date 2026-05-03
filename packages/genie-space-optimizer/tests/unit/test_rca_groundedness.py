"""Phase C Task 4 — unified RCA-groundedness gate.

Pins the gate's contract for AG, proposal, and patch shapes:

* AG with non-empty ``affected_questions`` whose qids overlap a
  finding's ``target_qids`` → accepted with that finding's id.
* AG without ``affected_questions`` → MISSING_TARGET_QIDS.
* Proposal with ``target_qids`` outside any finding → NO_CAUSAL_TARGET.
* Proposal that overlaps a finding but whose grounding terms touch
  *none* of the finding's blame_set / counterfactual surface →
  RCA_UNGROUNDED.

Plan: ``docs/2026-05-03-phase-c-rca-loop-contract-and-residuals-plan.md`` Task 4.
"""
from __future__ import annotations


def _finding(rca_id: str, target_qids: tuple[str, ...], terms: tuple[str, ...] = ()) -> object:
    """Lightweight stand-in: any object with the right attributes works
    because the gate uses ``getattr`` defensively."""
    class _F:
        pass
    f = _F()
    f.rca_id = rca_id
    f.target_qids = target_qids
    f.grounding_terms = terms
    f.blame_set = terms  # the gate accepts either field
    return f


def test_ag_with_overlapping_target_qids_is_accepted() -> None:
    from genie_space_optimizer.optimization.rca_decision_trace import ReasonCode
    from genie_space_optimizer.optimization.rca_groundedness import is_rca_grounded

    ag = {"id": "AG1", "affected_questions": ["q1", "q2"]}
    findings = [_finding("rca_a", ("q1", "q2", "q3"))]

    verdict = is_rca_grounded(ag, findings, target_kind="ag")

    assert verdict.accepted is True
    assert verdict.reason_code == ReasonCode.RCA_GROUNDED
    assert verdict.finding_id == "rca_a"


def test_ag_with_no_affected_questions_is_missing_target_qids() -> None:
    from genie_space_optimizer.optimization.rca_decision_trace import ReasonCode
    from genie_space_optimizer.optimization.rca_groundedness import is_rca_grounded

    ag = {"id": "AG_BROKEN", "affected_questions": []}
    findings = [_finding("rca_a", ("q1",))]

    verdict = is_rca_grounded(ag, findings, target_kind="ag")

    assert verdict.accepted is False
    assert verdict.reason_code == ReasonCode.MISSING_TARGET_QIDS
    assert verdict.finding_id == ""


def test_proposal_outside_all_findings_is_no_causal_target() -> None:
    from genie_space_optimizer.optimization.rca_decision_trace import ReasonCode
    from genie_space_optimizer.optimization.rca_groundedness import is_rca_grounded

    proposal = {
        "proposal_id": "P_FAR",
        "target_qids": ["q_unrelated"],
        "lever_directives": {},
    }
    findings = [_finding("rca_a", ("q1", "q2"))]

    verdict = is_rca_grounded(proposal, findings, target_kind="proposal")

    assert verdict.accepted is False
    assert verdict.reason_code == ReasonCode.NO_CAUSAL_TARGET
    assert verdict.finding_id == ""


def test_proposal_overlapping_qids_but_no_term_overlap_is_rca_ungrounded() -> None:
    from genie_space_optimizer.optimization.rca_decision_trace import ReasonCode
    from genie_space_optimizer.optimization.rca_groundedness import is_rca_grounded

    proposal = {
        "proposal_id": "P_NOOP",
        "target_qids": ["q1"],
        # Grounding terms point at totally different surface area than
        # the finding's blame set / counterfactual fixes.
        "target": "main.silver.unrelated_table",
        "intent": "Add a tooltip to a header cell.",
    }
    findings = [_finding(
        "rca_a",
        target_qids=("q1",),
        terms=("main.gold.order_summary", "active", "filter"),
    )]

    verdict = is_rca_grounded(proposal, findings, target_kind="proposal")

    assert verdict.accepted is False
    assert verdict.reason_code == ReasonCode.RCA_UNGROUNDED
    assert verdict.finding_id == ""


def test_patch_routes_to_blast_radius_helper_unchanged() -> None:
    """The unified gate's patch shape is a thin pass-through to the
    existing patch-level grounding helpers; the new module does not
    re-implement what blast-radius / patch-cap already cover. This
    test only pins that the patch path returns RCA_GROUNDED for a
    well-formed patch and that the verdict contract matches the
    AG/proposal shapes."""
    from genie_space_optimizer.optimization.rca_decision_trace import ReasonCode
    from genie_space_optimizer.optimization.rca_groundedness import is_rca_grounded

    patch = {
        "proposal_id": "P001",
        "patch_type": "add_sql_snippet_filter",
        "target_qids": ["q1"],
        "target": "main.gold.order_summary",
    }
    findings = [_finding(
        "rca_a",
        target_qids=("q1",),
        terms=("main.gold.order_summary", "active", "filter"),
    )]

    verdict = is_rca_grounded(patch, findings, target_kind="patch")

    assert verdict.accepted is True
    assert verdict.reason_code == ReasonCode.RCA_GROUNDED
    assert verdict.finding_id == "rca_a"


def test_no_findings_at_all_is_rca_ungrounded_for_any_target() -> None:
    """When the iteration produced zero RCA findings, every shape is
    ungrounded — the loop has nothing to ground against. The reason
    code must be RCA_UNGROUNDED, not NO_CAUSAL_TARGET (the latter
    means "we have findings but none cover this target")."""
    from genie_space_optimizer.optimization.rca_decision_trace import ReasonCode
    from genie_space_optimizer.optimization.rca_groundedness import is_rca_grounded

    ag = {"id": "AG1", "affected_questions": ["q1"]}
    proposal = {"proposal_id": "P1", "target_qids": ["q1"]}
    patch = {"proposal_id": "P1", "target_qids": ["q1"]}

    for shape, kind in [(ag, "ag"), (proposal, "proposal"), (patch, "patch")]:
        verdict = is_rca_grounded(shape, [], target_kind=kind)
        assert verdict.accepted is False
        assert verdict.reason_code == ReasonCode.RCA_UNGROUNDED
        assert verdict.finding_id == ""
