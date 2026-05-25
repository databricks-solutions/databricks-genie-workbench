"""Workbench V1.5 — funnel report surfaces evaluated_gate / acceptance_gate verdicts.

The report has been the primary debugging interface for trial postmortems;
operators must see at-a-glance whether the new gates fired and how.
"""
from __future__ import annotations

import pytest

from local_lever_workbench.funnel_report import _count_markers


@pytest.mark.workbench
def test_funnel_report_counts_evaluated_gate_accepted_and_rejected() -> None:
    stdout = """
some other line
GSO_GATE_REASONING_V1 gate=evaluated_gate qid=q1 verdict=accepted reason=ok predicate_inputs={}
another line
GSO_GATE_REASONING_V1 gate=evaluated_gate qid=q2 verdict=rejected reason=foo predicate_inputs={}
GSO_GATE_REASONING_V1 gate=acceptance_gate qid=q1 verdict=rejected reason=target_unchanged predicate_inputs={}
"""
    counts = _count_markers(stdout)
    assert counts["GSO_GATE_REASONING_V1 gate=evaluated_gate verdict=accepted"] == 1
    assert counts["GSO_GATE_REASONING_V1 gate=evaluated_gate verdict=rejected"] == 1
    assert counts["GSO_GATE_REASONING_V1 gate=acceptance_gate verdict=rejected"] == 1
    assert counts["GSO_GATE_REASONING_V1 gate=acceptance_gate verdict=accepted"] == 0
