"""Pin the replay-fixture patch-snapshot target_qids defaulting chain.

Cycle 8 lesson: standard L1-L4 proposal-construction sites do not stamp
``target_qids``, but ``_backfill_patch_causal_metadata`` later defaults
them to the AG's ``affected_questions`` before the applier runs. The
patch snapshot captured at fixture-emit time used to read the
pre-backfill proposal value, producing fixture entries with empty
``target_qids`` that misled cycle 8 triage. ``_patch_snapshot_target_qids``
applies the same defaulting chain so the fixture captures the patch's
effective causal scope at apply time.

See `docs/2026-05-02-cycle8-bug1-target-qids-diagnosis-and-plan.md`
Phase 1 for the diagnosis and the defaulting-chain rule.
"""
from __future__ import annotations


def test_patch_snapshot_falls_back_to_ag_affected_when_proposal_lacks_target_qids() -> None:
    """A proposal lacking both ``target_qids`` and ``_grounding_target_qids``
    must fall back to the AG's ``affected_questions``. This is the cycle 8
    standard-lever path."""
    from genie_space_optimizer.optimization.harness import (
        _patch_snapshot_target_qids,
    )

    proposal = {
        "proposal_id": "P001",
        "patch_type": "add_sql_snippet_filter",
        # No target_qids, no _grounding_target_qids.
    }
    ag_affected = ["airline_ticketing_and_fare_analysis_gs_024"]

    out = _patch_snapshot_target_qids(proposal, ag_affected)

    assert out == ["airline_ticketing_and_fare_analysis_gs_024"]


def test_patch_snapshot_keeps_explicit_target_qids_over_ag_default() -> None:
    """When a proposal carries an explicit narrower ``target_qids`` (e.g.
    via the RCA-bridge path or cluster-driven synthesis), the snapshot
    must preserve that narrower scope — the AG-scoped default is only a
    fallback, not an overwrite."""
    from genie_space_optimizer.optimization.harness import (
        _patch_snapshot_target_qids,
    )

    proposal = {
        "proposal_id": "P002",
        "patch_type": "add_sql_snippet_filter",
        "target_qids": ["q_narrow"],
    }
    ag_affected = ["q_narrow", "q_other_in_ag", "q_yet_another"]

    out = _patch_snapshot_target_qids(proposal, ag_affected)

    assert out == ["q_narrow"]


def test_patch_snapshot_prefers_grounding_target_qids_over_target_qids() -> None:
    """``_grounding_target_qids`` is the RCA-grounding artefact that wins
    over the proposal's own ``target_qids`` (matches the chain used in
    every other reader inside the harness)."""
    from genie_space_optimizer.optimization.harness import (
        _patch_snapshot_target_qids,
    )

    proposal = {
        "proposal_id": "P003",
        "_grounding_target_qids": ["q_grounding"],
        "target_qids": ["q_other"],
    }
    ag_affected = ["q_should_not_appear"]

    out = _patch_snapshot_target_qids(proposal, ag_affected)

    assert out == ["q_grounding"]


def test_patch_snapshot_returns_empty_when_all_sources_empty() -> None:
    """Empty inputs across the chain produce an empty list (no synthetic
    fallback). This is the correct behavior for AGs that genuinely have
    no targets — the journey contract handles them via the AG-outcome
    branches, not via fictional target_qids."""
    from genie_space_optimizer.optimization.harness import (
        _patch_snapshot_target_qids,
    )

    out = _patch_snapshot_target_qids({}, [])

    assert out == []


def test_patch_snapshot_filters_falsy_qids_and_coerces_to_str() -> None:
    """``None``/empty-string entries must be filtered; non-string qids
    coerced to str."""
    from genie_space_optimizer.optimization.harness import (
        _patch_snapshot_target_qids,
    )

    proposal = {
        "target_qids": ["q_a", "", None, "q_b"],
    }

    out = _patch_snapshot_target_qids(proposal, ["fallback_should_not_apply"])

    assert out == ["q_a", "q_b"]
