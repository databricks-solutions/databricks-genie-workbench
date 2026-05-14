"""Phase 1 + Phase 2 — cross-sub-phase end-to-end check.

Verifies the twelve invariants this plan establishes:

  1.1 — admission_trace filters candidate AGs
  1.2 — TerminalReason vocabulary closed
  1.3 — TerminalSignature retire memory fires on any non-accepted reason
  1.4 — prior_failure_count read from reflection_buffer
  1.5 — recovery priority list orders regressed → uncovered → original
  1.6 — reserved recovery iteration budget short-circuits when no work
  2.1 — CLUSTER_BLOCKED_NO_RCA filters strategist clusters
  2.2 — provisional RCA card synthesized at quorum >= 3 consistent hints
  2.3 — structural_repair_missing fires on metadata-only with structural intent
  2.4 — auto narrow-replacement triggers on collateral drop
  2.5 — protected_dependents threaded into ProposalContext + prompt
  2.6 — Best-of-N fires only for structural + prior_failure_count >= 1
"""
from __future__ import annotations

import pytest

from genie_space_optimizer.optimization.terminal_reason import TerminalReason
from genie_space_optimizer.optimization.terminal_signature import (
    EmittedPatchShape, build_terminal_signature,
)
from genie_space_optimizer.optimization.admission_trace_consumer import (
    apply_admission_trace,
)
from genie_space_optimizer.optimization.stages.action_groups import (
    AdmissionTrace, AdmissionVerdict, ForbiddenReason,
)
from genie_space_optimizer.optimization.forbidden_ag_set_v2 import (
    compute_retired_signatures,
)
from genie_space_optimizer.optimization.reflection_buffer_schema import (
    build_reflection_entry,
)
from genie_space_optimizer.optimization.prior_failure_count import (
    compute_prior_failure_count,
)
from genie_space_optimizer.optimization.recovery_priority import (
    build_recovery_priority_list,
)
from genie_space_optimizer.optimization.recovery_budget import (
    skip_or_proceed, RecoveryBudgetAction,
)
from genie_space_optimizer.optimization.blocked_cluster_filter import (
    filter_clusters_blocked_no_rca,
)
from genie_space_optimizer.optimization.rca_provisional_card import (
    build_provisional_card,
)
from genie_space_optimizer.optimization.structural_repair_gate import (
    enforce_structural_repair_shape,
)
from genie_space_optimizer.optimization.auto_narrow_replacement import (
    try_narrow_replacement,
)
from genie_space_optimizer.optimization.best_of_n_proposal import (
    should_run_best_of_n, rank_proposal_candidates,
)


def test_phase_1_1_admission_trace_filters():
    traces = (
        AdmissionTrace(ag_id="ag-1", verdict=AdmissionVerdict.DENIED,
                       denial_reason=ForbiddenReason.AG_RETIRED.value),
        AdmissionTrace(ag_id="ag-2", verdict=AdmissionVerdict.ADMITTED),
    )
    result = apply_admission_trace(
        slate_traces=traces,
        candidate_ags=[{"id": "ag-1"}, {"id": "ag-2"}],
    )
    assert [a["id"] for a in result.admitted_ags] == ["ag-2"]
    assert result.pivot_signal is True


def test_phase_1_2_terminal_reason_vocabulary_closed():
    # Contract spec Section 3.2 — 17 closed values.
    assert len({r.value for r in TerminalReason}) == 17


def _csig(cluster_id: str, qids: list[str]) -> tuple:
    """Cluster grouping key (independent of TerminalSignature)."""
    return ((str(cluster_id), tuple(sorted(qids))),)


def test_phase_1_3_retire_memory_fires_on_any_non_accepted():
    sig = build_terminal_signature(
        root_cause="r", blame_set=(), lever_set=[5],
        target_qids=["gs_001"],
        terminal_reason=TerminalReason.NO_APPLIED_PATCHES,
    )
    entry = build_reflection_entry(
        iteration=1, ag_id="ag-1", rollback_class="no_action",
        accepted=False, terminal_signature=sig,
        cluster_signature=_csig("c1", ["gs_001"]),
        emitted_patch_shape=EmittedPatchShape.STRUCTURAL,
        legacy_fields={},
    )
    retired = compute_retired_signatures(reflection_buffer=[entry])
    assert sig in retired


def test_phase_1_4_prior_failure_count_from_buffer():
    sig = build_terminal_signature(
        root_cause="r", blame_set=(), lever_set=[5],
        target_qids=["gs_001"],
        terminal_reason=TerminalReason.NO_APPLIED_PATCHES,
    )
    entry = build_reflection_entry(
        iteration=1, ag_id="ag-1", rollback_class="no_action",
        accepted=False, terminal_signature=sig,
        cluster_signature=_csig("c1", ["gs_001"]),
        emitted_patch_shape=EmittedPatchShape.STRUCTURAL,
        legacy_fields={},
    )
    assert compute_prior_failure_count(
        cluster_signature=_csig("c1", ["gs_001"]),
        reflection_buffer=[entry],
    ) == 1


def test_phase_1_5_recovery_priority_orders_regressed_first():
    assert build_recovery_priority_list(
        regressed_qids_to_cluster_id={"gs_003": "c_reg"},
        uncovered_cluster_ids=["c_unc"],
        original_target_cluster_id="c_orig",
    ) == ("c_reg", "c_unc", "c_orig")


def test_phase_1_6_recovery_budget_skips_when_no_work():
    assert skip_or_proceed(
        iteration=5, max_iterations=5,
        regressed_qids_count=0, uncovered_cluster_ids_count=0,
    ) == RecoveryBudgetAction.SKIP_EARLY_TERMINATE


def test_phase_2_1_cluster_blocked_no_rca_filters():
    clusters = filter_clusters_blocked_no_rca(
        clusters=[{"cluster_id": "c1"}, {"cluster_id": "c2"}],
        decision_records_this_iter=[
            {"record_type": "no_rca_ground", "cluster_id": "c1"},
        ],
    )
    assert [c["cluster_id"] for c in clusters] == ["c2"]


def test_phase_2_2_provisional_card_at_quorum():
    signals = [
        {"qid": f"gs_00{i}", "cluster_id": "c1",
         "root_cause_hint": "missing_metric_view", "confidence": 0.6}
        for i in (1, 2, 3)
    ]
    card = build_provisional_card(
        cluster_id="c1", soft_signals_for_cluster=signals,
    )
    assert card is not None
    assert card["provisional"] is True


def test_phase_2_3_structural_repair_missing_fires():
    verdict = enforce_structural_repair_shape(
        intended_patch_shape="structural",
        emitted_patch_shape=EmittedPatchShape.METADATA,
    )
    assert verdict.outcome == "rejected"
    assert verdict.terminal_reason == TerminalReason.STRUCTURAL_GATE_DROPPED_INSTRUCTION_ONLY.value


def test_phase_2_4_auto_narrow_replacement_invokes():
    calls = []
    result = try_narrow_replacement(
        dropped_patches=[{
            "patch_id": "p1", "patch_type": "sql_snippet",
            "drop_reason": "high_collateral_risk_flagged",
        }],
        outside_target_qids=("gs_003",),
        cluster={"cluster_id": "c1"},
        rca_card={"root_cause": "r"},
        synthesis_callable_l6=lambda **kw: (
            calls.append(kw) or
            {"patch_id": "narrow", "patch_type": "narrow_l6_sql"}
        ),
        synthesis_callable_l5=lambda **_: None,
    )
    assert result.attempted is True
    assert result.replacement_patch is not None
    assert calls[0]["protected_dependents"] == ("gs_003",)


def test_phase_2_5_protected_dependents_in_proposal_context():
    from genie_space_optimizer.optimization.cluster_driven_synthesis import (
        ProposalContext,
    )
    ctx = ProposalContext(
        cluster_id="c1", target_qids=("gs_001",),
        rca_card={"root_cause": "r"},
        protected_dependents=("gs_003",),
    )
    assert ctx.protected_dependents == ("gs_003",)


def test_phase_2_6_best_of_n_only_for_structural_with_prior_failure():
    assert should_run_best_of_n(
        intended_patch_shape="structural", prior_failure_count=1,
    ) is True
    assert should_run_best_of_n(
        intended_patch_shape="metadata", prior_failure_count=2,
    ) is False
    ranking = rank_proposal_candidates(
        candidates=[
            {"patch_id": "p1", "patch_type": "uc_table_description",
             "covers_target_qids": ["gs_001"],
             "blast_radius_dependents": 1, "preserves_protected": True,
             "patch_count": 1},
            {"patch_id": "p2", "patch_type": "narrow_l6_sql",
             "covers_target_qids": ["gs_001"],
             "blast_radius_dependents": 1, "preserves_protected": True,
             "patch_count": 1},
        ],
        target_qids=("gs_001",),
        protected_dependents=(),
    )
    assert ranking.top_candidate["patch_id"] == "p2"
