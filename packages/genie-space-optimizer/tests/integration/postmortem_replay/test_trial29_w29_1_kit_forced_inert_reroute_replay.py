"""Trial 29 W29.1 — end-to-end replay across the W29.1 surface.

Drives a 7now-shaped (qid=gs_026, rca=plural_top_n_collapse,
behavioral_diff=unchanged) payload through:

  1. acceptance_gate -> kit_forced_inert_reroute decision
  2. harvest_sm_inert_mechanism_history -> InertMechanismHistory tuple
  3. Trial29InertPatchDiagnostic persistence + load round-trip
  4. render_inert_mechanism_history_section -> Stage 3 prompt section
  5. extend_sm_inert_mechanism_history -> cumulative across iter1 + iter2

Asserts every hop and confirms that the rejected mechanism from iter1
appears in the prompt the LLM would consume in iter2 with explicit
AVOID instruction, and that the cumulative history dedupes correctly
across mechanisms.
"""
from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest

from genie_space_optimizer.optimization.inert_mechanism_history import (
    InertMechanismHistory,
    extend_sm_inert_mechanism_history,
    harvest_sm_inert_mechanism_history,
)
from genie_space_optimizer.optimization.inert_patch_diagnostic import (
    Trial29InertPatchDiagnostic,
    load_inert_patch_diagnostics,
    persist_inert_patch_diagnostic,
)
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
from genie_space_optimizer.optimization.state_machine.transformers.acceptance_gate import (
    acceptance_gate,
)
from genie_space_optimizer.optimization.state_machine.verdict import (
    TransformerContext,
    ValidationContext,
)
from genie_space_optimizer.optimization.stages.synthesize import (
    render_inert_mechanism_history_section,
)


@pytest.fixture(autouse=True)
def _trial_flags_on(monkeypatch):
    """Default ALL Trial 29 prereq flags ON."""
    for env in (
        "GSO_TRIAL18_ACCEPTANCE_OVERHAUL",
        "GSO_TRIAL24_KIT_AT_SOURCE",
        "GSO_TRIAL26_KIT_MAP_EXPANDED",
        "GSO_TRIAL26_KIT_GATE_REACHABLE",
        "GSO_TRIAL29_BEHAVIOR_DELTA",
        "GSO_TRIAL29_INERT_REROUTE",
    ):
        monkeypatch.delenv(env, raising=False)
    yield


def _state_at_evaluated(
    *,
    qid: str,
    rca_kind: str,
    mechanism: str,
):
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


def test_kit_forced_inert_reroute_end_to_end(tmp_path: Path):
    """The full W29.1 surface: gate -> harvest -> persist -> prompt
    -> extend. Two iterations on the same QID; the cumulative
    history accumulates BOTH rejected mechanisms; the prompt for the
    third iteration would instruct the LLM to avoid both.
    """
    # ── Iteration 1: kit-forced patch with add_sql_snippet_filter (inert).
    state_iter1 = _state_at_evaluated(
        qid="gs_026",
        rca_kind="plural_top_n_collapse",  # Trial 26 kit alias for top_n_cardinality_collapse
        mechanism="add_sql_snippet_filter",
    )
    s1 = _run_gate(state_iter1)
    assert s1.accepted is not None
    assert s1.accepted.decision == "kit_forced_inert_reroute"
    assert s1.accepted.rejected_mechanism != ""

    history_iter1 = harvest_sm_inert_mechanism_history(
        [s1.accepted],
        qid_rca_pairs=[("gs_026", "plural_top_n_collapse")],
    )
    assert len(history_iter1) == 1
    assert history_iter1[0].qid == "gs_026"
    iter1_rejected = history_iter1[0].rejected_mechanisms[0]

    # ── Persist iter1 diagnostic.
    bundle_dir = tmp_path / "postmortem_bundle"
    diagnostic_iter1 = Trial29InertPatchDiagnostic(
        qid="gs_026",
        rca_kind="plural_top_n_collapse",
        rejected_mechanism=iter1_rejected,
        patch_json={"mechanism": iter1_rejected},
        pre_arbiter_score=0.0,
        post_arbiter_score=0.0,
        behavioral_diff="unchanged",
        signature=s1.accepted.insufficient_repair_signature,
        iteration=1,
        trial="trial29",
    )
    persist_inert_patch_diagnostic(diagnostic_iter1, bundle_dir=bundle_dir)

    # ── Render the Stage 3 prompt section iter2 would consume.
    prompt_section = render_inert_mechanism_history_section(history_iter1)
    assert "gs_026" in prompt_section
    assert "plural_top_n_collapse" in prompt_section
    assert iter1_rejected in prompt_section
    assert (
        "avoid" in prompt_section.lower()
        or "do not" in prompt_section.lower()
        or "must not" in prompt_section.lower()
    )

    # ── Iteration 2: LLM (would pick a different mechanism here based
    # on the prompt). Simulate it picking `add_example_sql`; still
    # inert under the same RCA, so the gate routes again.
    state_iter2 = _state_at_evaluated(
        qid="gs_026",
        rca_kind="plural_top_n_collapse",
        mechanism="add_example_sql",
    )
    s2 = _run_gate(state_iter2)
    assert s2.accepted is not None
    assert s2.accepted.decision == "kit_forced_inert_reroute"
    iter2_rejected = s2.accepted.rejected_mechanism
    assert iter2_rejected != ""
    assert iter2_rejected != iter1_rejected

    # ── Harvest iter2 and extend cumulatively.
    history_iter2 = harvest_sm_inert_mechanism_history(
        [s2.accepted],
        qid_rca_pairs=[("gs_026", "plural_top_n_collapse")],
    )
    cumulative = extend_sm_inert_mechanism_history(history_iter1, history_iter2)
    assert len(cumulative) == 1
    assert set(cumulative[0].rejected_mechanisms) == {
        iter1_rejected,
        iter2_rejected,
    }

    # ── Persist iter2 diagnostic and verify file accumulates.
    diagnostic_iter2 = Trial29InertPatchDiagnostic(
        qid="gs_026",
        rca_kind="plural_top_n_collapse",
        rejected_mechanism=iter2_rejected,
        patch_json={"mechanism": iter2_rejected},
        pre_arbiter_score=0.0,
        post_arbiter_score=0.0,
        behavioral_diff="unchanged",
        signature=s2.accepted.insufficient_repair_signature,
        iteration=2,
        trial="trial29",
    )
    persist_inert_patch_diagnostic(diagnostic_iter2, bundle_dir=bundle_dir)

    loaded = load_inert_patch_diagnostics(bundle_dir)
    assert len(loaded) == 2
    rejected_mechanisms_loaded = {d.rejected_mechanism for d in loaded}
    assert rejected_mechanisms_loaded == {iter1_rejected, iter2_rejected}

    # ── Iteration 3 prompt would include BOTH rejected mechanisms.
    cumulative_prompt = render_inert_mechanism_history_section(cumulative)
    assert iter1_rejected in cumulative_prompt
    assert iter2_rejected in cumulative_prompt
