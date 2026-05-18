"""Phase 1.5 — NSC marker and record constructors must refuse when
both skipped_reason and attempted_archetypes are empty.

The synthesizer always knows something. Constructing the marker
with both empty is the exact failure pattern seen in two live
runs (airline 59a173d3, 7now ab65fefe). After Phase 0.2 wired the
fields through, the construction sites should never be empty —
this test makes "should never" a hard guarantee.
"""

import pytest

from genie_space_optimizer.optimization.decision_emitters import (
    no_structural_candidate_record,
)
from genie_space_optimizer.optimization.run_analysis_contract import (
    no_structural_candidate_marker,
)


def test_no_structural_candidate_marker_refuses_double_empty():
    with pytest.raises(ValueError) as exc_info:
        no_structural_candidate_marker(
            ag_id="AG_DECOMPOSED_H001",
            iteration=1,
            attempted_archetypes=(),
            skipped_reason="",
        )
    assert "skipped_reason" in str(exc_info.value)
    assert "attempted_archetypes" in str(exc_info.value)


def test_no_structural_candidate_marker_accepts_skipped_reason_alone():
    line = no_structural_candidate_marker(
        ag_id="AG_DECOMPOSED_H001",
        iteration=1,
        attempted_archetypes=(),
        skipped_reason="no_archetype_or_slice",
    )
    assert "no_archetype_or_slice" in line


def test_no_structural_candidate_marker_accepts_attempted_archetypes_alone():
    """If the synthesizer attempted archetypes but gates rejected all
    of them, skipped_reason may legitimately be empty AT THIS LAYER
    (the gate-specific reason lives in a different record). The
    typed contract therefore accepts attempted_archetypes alone."""
    line = no_structural_candidate_marker(
        ag_id="AG_DECOMPOSED_H001",
        iteration=1,
        attempted_archetypes=("plural_top_n",),
        skipped_reason="",
    )
    assert "plural_top_n" in line


def test_no_structural_candidate_record_refuses_double_empty():
    with pytest.raises(ValueError):
        no_structural_candidate_record(
            run_id="test-run",
            iteration=1,
            ag_id="AG_DECOMPOSED_H001",
            cluster_id="H001",
            rca_id="",
            root_cause="wrong_filter_condition",
            target_qids=("7now_delivery_analytics_space_gs_013",),
            attempted_archetypes=(),
            skipped_reason="",
        )


def test_no_structural_candidate_record_accepts_skipped_reason_alone():
    rec = no_structural_candidate_record(
        run_id="test-run",
        iteration=1,
        ag_id="AG_DECOMPOSED_H001",
        cluster_id="H001",
        rca_id="",
        root_cause="wrong_filter_condition",
        target_qids=("7now_delivery_analytics_space_gs_013",),
        attempted_archetypes=(),
        skipped_reason="missing_rca_card",
    )
    assert rec.metrics["skipped_reason"] == "missing_rca_card"
