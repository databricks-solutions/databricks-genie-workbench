"""Trial 30 W30.1 — end-to-end replay closing the W29.4 PARTIAL gap.

W29 made the inert mechanism *visible* to the LLM (harvest -> prompt), but
W29.4's live verification was PARTIAL: with awareness alone, the LLM still
re-emitted the mechanism the acceptance gate had already proven inert. W30.1
adds the deterministic enforcement the loop was missing.

This replay drives the same 7now-shaped payload
(qid=gs_026, rca=plural_top_n_collapse -> top_n_cardinality_collapse,
behavioral_diff=unchanged) through the real surface:

  1. acceptance_gate -> kit_forced_inert_reroute decision (iter1)
  2. harvest_sm_inert_mechanism_history -> InertMechanismHistory tuple
  3. enforced_switch_survivors (iter2 slate: re-emit + structural fallback)
       -> the re-emit is DROPPED, the fallback survives  <-- the W30 delta
  4. a re-emit with NO fallback in slate is KEPT + flagged no_fallback
       (never zero out a QID)

Step 3 is the behavior W29 could not produce. Asserting it here proves the
W29 harvest channel and the W30 enforcement guard compose end-to-end on the
canonical failing QID.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from unittest.mock import patch

import pytest

from genie_space_optimizer.optimization.enforced_mechanism_switch import (
    enforced_switch_survivors,
)
from genie_space_optimizer.optimization.inert_mechanism_history import (
    harvest_sm_inert_mechanism_history,
)
from genie_space_optimizer.optimization.patch_mechanism import PatchMechanism
from genie_space_optimizer.optimization.state_machine.funnel import (
    FunnelStage,
)
from genie_space_optimizer.optimization.state_machine.records import (
    AppliedRecord,
    ClusterMembershipRecord,
    DiagnosisRecord,
    EvaluatedRecord,
    HardQidSeenRecord,
    ProposalAttempt,
    StageTransition,
)
from genie_space_optimizer.optimization.state_machine.state import (
    build_initial_state,
)
from genie_space_optimizer.optimization.state_machine.transformers.acceptance_gate import (  # noqa: E501
    acceptance_gate,
)
from genie_space_optimizer.optimization.state_machine.verdict import (
    TransformerContext,
    ValidationContext,
)


@pytest.fixture(autouse=True)
def _trial_flags_on(monkeypatch):
    """Default ALL Trial 29 + Trial 30 prereq flags ON."""
    for env in (
        "GSO_TRIAL18_ACCEPTANCE_OVERHAUL",
        "GSO_TRIAL24_KIT_AT_SOURCE",
        "GSO_TRIAL26_KIT_MAP_EXPANDED",
        "GSO_TRIAL26_KIT_GATE_REACHABLE",
        "GSO_TRIAL29_BEHAVIOR_DELTA",
        "GSO_TRIAL29_INERT_REROUTE",
        "GSO_TRIAL30_ENFORCED_SWITCH",
        "GSO_TRIAL30_INERT_HARVEST_WIRE",
        "GSO_TRIAL30_ENFORCE_GUARD",
    ):
        monkeypatch.delenv(env, raising=False)
    yield


@dataclass
class _View:
    """RepairProposal-shaped view the guard consumes (mirrors the
    _Trial30ProposalView adapter the synthesis stage builds)."""

    intent_id: str
    qid: str
    rca_kind: str
    patch_type: Any


def _state_at_evaluated(*, qid: str, rca_kind: str, mechanism: str):
    s = build_initial_state(
        qid=qid,
        iteration=1,
        seen=HardQidSeenRecord(
            "r", "row_is_hard_failure", 0.0, "SELECT 1", "x", 1,
        ),
    )
    for from_s, to_s, kw in (
        (FunnelStage.HARD_QID_SEEN, FunnelStage.DIAGNOSED,
         {"diagnosed": DiagnosisRecord(
             "plan11_stage1", rca_kind, "s", "f", "e", "high", "r")}),
        (FunnelStage.DIAGNOSED, FunnelStage.CLUSTERED,
         {"clustered": ClusterMembershipRecord(
             "H1", "AG", (qid,), 6, "k")}),
        (FunnelStage.CLUSTERED, FunnelStage.PROPOSED,
         {"proposals": (ProposalAttempt(
             0, "i", mechanism,
             FunnelStage.APPLIED, "applied", "ok"),)}),
        (FunnelStage.PROPOSED, FunnelStage.NORMALIZED, {}),
        (FunnelStage.NORMALIZED, FunnelStage.APPLYABLE, {}),
        (FunnelStage.APPLYABLE, FunnelStage.APPLIED,
         {"applied": AppliedRecord(1, "c", 0, ("i",))}),
        (FunnelStage.APPLIED, FunnelStage.EVALUATED,
         {"evaluated": EvaluatedRecord(
             0.0, 0.0, "SELECT 1", "SELECT 1", "rp",
             behavioral_diff="unchanged")}),
    ):
        s = s.advance(
            to_s,
            StageTransition(from_s, to_s, 1, "t", "validation_gate"),
            **kw,
        )
    return s


def _run_gate(state):
    with patch(
        "genie_space_optimizer.optimization.state_machine.transformers."
        "acceptance_gate._assess_collateral",
        return_value=(),
    ):
        return acceptance_gate.transform(
            state,
            TransformerContext(1, "r", ValidationContext(1, "r", {})),
        )


def test_w30_enforced_switch_drops_reemit_with_fallback():
    """The W29.4 closure: gate -> harvest -> ENFORCE. The mechanism the
    gate proved inert in iter1 is hard-dropped from the iter2 slate when a
    structural fallback survives — the deterministic action W29 lacked."""
    # ── Iter1: kit-forced SQL_SNIPPET patch goes inert -> reroute decision.
    s1 = _run_gate(_state_at_evaluated(
        qid="gs_026",
        rca_kind="plural_top_n_collapse",  # -> top_n_cardinality_collapse
        mechanism="add_sql_snippet_filter",
    ))
    assert s1.accepted is not None
    assert s1.accepted.decision == "kit_forced_inert_reroute"

    history = harvest_sm_inert_mechanism_history(
        [s1.accepted],
        qid_rca_pairs=[("gs_026", "plural_top_n_collapse")],
    )
    assert len(history) == 1

    # Canonical rca_kind the synthesis guard keys on (action_groups
    # normaliser collapses plural_top_n_collapse -> top_n_cardinality_collapse).
    canonical_rca = history[0].rca_kind

    # ── Iter2 slate: the LLM re-emits the inert SQL_SNIPPET (the W29.4
    # failure mode) AND a structural METADATA_DESCRIPTION fallback is also
    # present in the same QID's slate.
    slate = [
        _View("i_reemit", "gs_026", canonical_rca, "add_sql_snippet_filter"),
        _View("i_fallback", "gs_026", canonical_rca, "add_column_description"),
    ]
    outcome = enforced_switch_survivors(slate, history)

    survivor_ids = {getattr(p, "intent_id") for p in outcome.survivors}
    dropped_ids = {getattr(p, "intent_id") for p in outcome.dropped}
    assert dropped_ids == {"i_reemit"}, "inert re-emit must be dropped"
    assert "i_fallback" in survivor_ids, "structural fallback must survive"
    assert outcome.no_fallback_qids == []
    # The drop carries a typed enforced-switch reason for observability.
    assert "GSO_TRIAL30_ENFORCED_SWITCH_V1" in (
        outcome.dropped_reasons["i_reemit"]
    )


def test_w30_keeps_reemit_when_no_fallback_in_slate():
    """Guard never zeroes out a QID: a re-emit with no surviving fallback
    is kept and the QID is flagged no_fallback."""
    s1 = _run_gate(_state_at_evaluated(
        qid="gs_026",
        rca_kind="plural_top_n_collapse",
        mechanism="add_sql_snippet_filter",
    ))
    history = harvest_sm_inert_mechanism_history(
        [s1.accepted],
        qid_rca_pairs=[("gs_026", "plural_top_n_collapse")],
    )
    canonical_rca = history[0].rca_kind

    # Slate contains ONLY the inert re-emit; no structural fallback.
    slate = [
        _View("i_reemit", "gs_026", canonical_rca, "add_sql_snippet_filter"),
    ]
    outcome = enforced_switch_survivors(slate, history)

    assert [getattr(p, "intent_id") for p in outcome.survivors] == ["i_reemit"]
    assert outcome.dropped == []
    assert outcome.no_fallback_qids == ["gs_026"]


def test_harvested_mechanism_is_sql_snippet():
    """Sanity: the gate's rejected_mechanism for the 7now snippet patch
    normalises to SQL_SNIPPET, so the guard's PatchMechanism comparison
    (not lever-id strings) is what actually gates the drop."""
    s1 = _run_gate(_state_at_evaluated(
        qid="gs_026",
        rca_kind="plural_top_n_collapse",
        mechanism="add_sql_snippet_filter",
    ))
    history = harvest_sm_inert_mechanism_history(
        [s1.accepted],
        qid_rca_pairs=[("gs_026", "plural_top_n_collapse")],
    )
    from genie_space_optimizer.optimization.rca_mechanism_routing import (
        mechanisms_for_rejected_levers,
    )
    mechs = mechanisms_for_rejected_levers(history[0].rejected_mechanisms)
    assert PatchMechanism.SQL_SNIPPET in mechs
