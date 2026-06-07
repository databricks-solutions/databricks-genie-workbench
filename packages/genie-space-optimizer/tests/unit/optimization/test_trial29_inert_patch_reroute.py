"""Trial 29 W29.1 — kit-forced inert-patch re-route acceptance lane.

Phase 2 (this file's first cohort): the AcceptanceDecisionRecord
literal accepts ``"kit_forced_inert_reroute"`` and carries a
``rejected_mechanism`` field. Pure type test; no acceptance-gate
behaviour yet (Phase 4 cohort appended below).
"""
from __future__ import annotations

from dataclasses import asdict

from genie_space_optimizer.optimization.state_machine.records import (
    AcceptanceDecisionRecord,
)


def test_record_accepts_kit_forced_inert_reroute_literal():
    record = AcceptanceDecisionRecord(
        decision="kit_forced_inert_reroute",
        arbiter_reason="kit_forced_inert_reroute:behavior=unchanged",
        target_fixed=False,
        collateral_regressions=(),
        insufficient_repair_signature="",
        behavioral_diff="unchanged",
        rejected_mechanism="add_sql_snippet_filter",
    )
    assert record.decision == "kit_forced_inert_reroute"
    assert record.rejected_mechanism == "add_sql_snippet_filter"


def test_record_rejected_mechanism_defaults_to_empty():
    record = AcceptanceDecisionRecord(
        decision="accepted",
        arbiter_reason="ok",
        target_fixed=True,
        collateral_regressions=(),
    )
    assert record.rejected_mechanism == ""


def test_record_serialises_rejected_mechanism():
    record = AcceptanceDecisionRecord(
        decision="kit_forced_inert_reroute",
        arbiter_reason="kit_forced_inert_reroute:behavior=unchanged",
        target_fixed=False,
        collateral_regressions=(),
        insufficient_repair_signature=(
            "add_sql_snippet_filter:filter:insufficient:"
            "rca=wrong_aggregation:behavior=unchanged"
        ),
        behavioral_diff="unchanged",
        rejected_mechanism="add_sql_snippet_filter",
    )
    payload = asdict(record)
    assert payload["decision"] == "kit_forced_inert_reroute"
    assert payload["rejected_mechanism"] == "add_sql_snippet_filter"
