"""Trial 19 A1 — admission gate hard-rejects insufficient-signature repeats."""
from dataclasses import dataclass, field

import pytest

from genie_space_optimizer.optimization.admission_gate import (
    ADMITTED,
    ADMITTED_WITH_REINFORCEMENT,
    REJECTED_INSUFFICIENT_REPEAT,
    AdmissionEvaluation,
    evaluate_admission,
)


@dataclass
class _StubProposal:
    selected_lever: str
    patch_type: str
    target_qids: tuple[str, ...] = ()
    bundle_id: str = ""
    rca_kind: str = ""


def _flag_on(monkeypatch) -> None:
    monkeypatch.setenv("GSO_TRIAL19_ENFORCE", "1")
    monkeypatch.setenv("GSO_TRIAL19_ENFORCE_INSUFFICIENT", "1")


def _flag_off(monkeypatch) -> None:
    monkeypatch.setenv("GSO_TRIAL19_ENFORCE_INSUFFICIENT", "0")


def test_admits_when_no_insufficient_signatures(monkeypatch) -> None:
    _flag_on(monkeypatch)
    proposals = [
        _StubProposal(
            selected_lever="lever-5",
            patch_type="add_example_sql",
            target_qids=("gs_009",),
        ),
    ]
    result = evaluate_admission(
        proposals,
        insufficient_signatures=(),
        rca_kind_label_by_qid={"gs_009": "top_n_cardinality_collapse"},
    )
    assert isinstance(result, AdmissionEvaluation)
    assert result.admitted_set is True
    assert result.rejected == 0
    assert result.verdicts[0].decision == ADMITTED


def test_rejects_sole_primary_repeat(monkeypatch) -> None:
    _flag_on(monkeypatch)
    signature = (
        "lever-5:add_example_sql:insufficient"
        ":rca=top_n_cardinality_collapse:behavior=unchanged"
    )
    proposals = [
        _StubProposal(
            selected_lever="lever-5",
            patch_type="add_example_sql",
            target_qids=("gs_009",),
        ),
    ]
    result = evaluate_admission(
        proposals,
        insufficient_signatures=(signature,),
        rca_kind_label_by_qid={"gs_009": "top_n_cardinality_collapse"},
    )
    assert result.rejected == 1
    assert result.rejected_set is True
    verdict = result.verdicts[0]
    assert verdict.decision == REJECTED_INSUFFICIENT_REPEAT
    assert verdict.matched_signature == signature
    assert (
        "sole-primary" in verdict.reason.lower()
        or "insufficient" in verdict.reason.lower()
    )
    assert "insufficient" in result.typed_feedback.lower()


def test_admits_with_reinforcement_bundle(monkeypatch) -> None:
    """Bundled proposals (different lever, shared bundle_id) admitted."""
    _flag_on(monkeypatch)
    signature = (
        "lever-5:add_example_sql:insufficient"
        ":rca=top_n_cardinality_collapse:behavior=unchanged"
    )
    proposals = [
        _StubProposal(
            selected_lever="lever-5",
            patch_type="add_example_sql",
            target_qids=("gs_009",),
            bundle_id="bundle-42",
        ),
        _StubProposal(
            selected_lever="lever-6",
            patch_type="add_sql_snippet_filter",
            target_qids=("gs_009",),
            bundle_id="bundle-42",
        ),
    ]
    result = evaluate_admission(
        proposals,
        insufficient_signatures=(signature,),
        rca_kind_label_by_qid={"gs_009": "top_n_cardinality_collapse"},
    )
    assert result.rejected == 0
    assert result.admitted_set is True
    assert result.verdicts[0].decision == ADMITTED_WITH_REINFORCEMENT


def test_admits_with_distinct_patch_type_bundle(monkeypatch) -> None:
    _flag_on(monkeypatch)
    signature = (
        "lever-5:add_example_sql:insufficient"
        ":rca=top_n_cardinality_collapse:behavior=unchanged"
    )
    proposals = [
        _StubProposal(
            selected_lever="lever-5",
            patch_type="add_example_sql",
            target_qids=("gs_009",),
            bundle_id="b1",
        ),
        _StubProposal(
            selected_lever="lever-5",
            patch_type="add_sql_snippet_filter",
            target_qids=("gs_009",),
            bundle_id="b1",
        ),
    ]
    result = evaluate_admission(
        proposals,
        insufficient_signatures=(signature,),
        rca_kind_label_by_qid={"gs_009": "top_n_cardinality_collapse"},
    )
    assert result.rejected == 0
    assert result.verdicts[0].decision == ADMITTED_WITH_REINFORCEMENT


def test_unbundled_companion_does_not_count_as_reinforcement(monkeypatch) -> None:
    """Two proposals without a shared bundle_id don't form a bundle."""
    _flag_on(monkeypatch)
    signature = (
        "lever-5:add_example_sql:insufficient"
        ":rca=top_n_cardinality_collapse:behavior=unchanged"
    )
    proposals = [
        _StubProposal(
            selected_lever="lever-5",
            patch_type="add_example_sql",
            target_qids=("gs_009",),
        ),
        _StubProposal(
            selected_lever="lever-6",
            patch_type="add_sql_snippet_filter",
            target_qids=("gs_009",),
        ),
    ]
    result = evaluate_admission(
        proposals,
        insufficient_signatures=(signature,),
        rca_kind_label_by_qid={"gs_009": "top_n_cardinality_collapse"},
    )
    assert result.rejected == 1
    assert result.verdicts[0].decision == REJECTED_INSUFFICIENT_REPEAT


def test_pivoted_proposal_admitted(monkeypatch) -> None:
    """A different lever / patch_type evades the matched signature."""
    _flag_on(monkeypatch)
    signature = (
        "lever-5:add_example_sql:insufficient"
        ":rca=top_n_cardinality_collapse:behavior=unchanged"
    )
    proposals = [
        _StubProposal(
            selected_lever="lever-6",
            patch_type="add_sql_snippet_filter",
            target_qids=("gs_009",),
        ),
    ]
    result = evaluate_admission(
        proposals,
        insufficient_signatures=(signature,),
        rca_kind_label_by_qid={"gs_009": "top_n_cardinality_collapse"},
    )
    assert result.rejected == 0
    assert result.verdicts[0].decision == ADMITTED


def test_behavior_suffix_is_ignored_in_match(monkeypatch) -> None:
    """The trailing ``behavior=`` segment must not affect the match."""
    _flag_on(monkeypatch)
    sig_unchanged = (
        "lever-5:add_example_sql:insufficient"
        ":rca=top_n_cardinality_collapse:behavior=unchanged"
    )
    sig_partial = (
        "lever-5:add_example_sql:insufficient"
        ":rca=top_n_cardinality_collapse:behavior=partial"
    )
    proposals = [
        _StubProposal(
            selected_lever="lever-5",
            patch_type="add_example_sql",
            target_qids=("gs_009",),
        ),
    ]
    for sig in (sig_unchanged, sig_partial):
        result = evaluate_admission(
            proposals,
            insufficient_signatures=(sig,),
            rca_kind_label_by_qid={"gs_009": "top_n_cardinality_collapse"},
        )
        assert result.rejected == 1


def test_flag_off_admits_everything(monkeypatch) -> None:
    """Pre-Trial-19 behavior preserved when the sub-flag is OFF."""
    _flag_off(monkeypatch)
    signature = (
        "lever-5:add_example_sql:insufficient"
        ":rca=top_n_cardinality_collapse:behavior=unchanged"
    )
    proposals = [
        _StubProposal(
            selected_lever="lever-5",
            patch_type="add_example_sql",
            target_qids=("gs_009",),
        ),
    ]
    result = evaluate_admission(
        proposals,
        insufficient_signatures=(signature,),
        rca_kind_label_by_qid={"gs_009": "top_n_cardinality_collapse"},
    )
    assert result.rejected == 0
    assert result.verdicts[0].decision == ADMITTED


def test_empty_proposal_list_returns_empty_evaluation(monkeypatch) -> None:
    _flag_on(monkeypatch)
    result = evaluate_admission(
        [],
        insufficient_signatures=("lever-5:add_example_sql:insufficient:rca=x:behavior=unchanged",),
    )
    assert result.verdicts == ()
    assert result.rejected == 0
    assert result.admitted_set is True


def test_falls_back_to_rca_kind_attribute_when_label_missing(monkeypatch) -> None:
    """When ``rca_kind_label_by_qid`` lacks the qid, use proposal.rca_kind."""
    _flag_on(monkeypatch)
    signature = (
        "lever-5:add_example_sql:insufficient"
        ":rca=top_n_cardinality_collapse:behavior=unchanged"
    )
    proposals = [
        _StubProposal(
            selected_lever="lever-5",
            patch_type="add_example_sql",
            target_qids=("gs_009",),
            rca_kind="top_n_cardinality_collapse",
        ),
    ]
    result = evaluate_admission(
        proposals,
        insufficient_signatures=(signature,),
    )
    assert result.rejected == 1
