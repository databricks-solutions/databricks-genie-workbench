"""C15 Phase 1 Task 1.10 — D-6 closure test.

Phase H acceptance writer must consume AgOutcome directly, not
re-derive from raw eval rows. The old parallel-derivation path
produced ``outcome=rolled_back / reason_code=missing_pre_rows``
even when the canonical gate said ACCEPTED — that's D-6.

Anchor: airline run 1105451933925748 iter 1 (accepted_with_attribution_drift
but Phase H said missing_pre_rows).
"""

from __future__ import annotations

import pytest

from genie_space_optimizer.optimization.stages.acceptance import (
    AgOutcome,
    AgOutcomeRecord,
)


# ── write_phase_h_acceptance_decision_output ──────────────────────────────────


def test_accepted_ag_outcome_produces_accepted_output() -> None:
    """When AgOutcome.outcomes_by_ag says 'accepted', Phase H output
    must say 'accepted' — no parallel derivation from pre_rows count."""
    from genie_space_optimizer.optimization.harness import (
        write_phase_h_acceptance_decision_output,
    )

    ag_outcome = AgOutcome(
        outcomes_by_ag={
            "AG1": AgOutcomeRecord(
                ag_id="AG1",
                outcome="accepted",
                reason_code="accepted_with_attribution_drift",
                target_qids=("gs_024",),
                affected_qids=("gs_024",),
            )
        },
    )
    out = write_phase_h_acceptance_decision_output(ag_outcome=ag_outcome, iteration=1)

    assert out["outcome"] == "accepted", (
        f"Phase H acceptance output must agree with AgOutcome. "
        f"Got outcome={out['outcome']!r} but AgOutcome says 'accepted'."
    )
    assert out["reason_code"] == "accepted_with_attribution_drift", (
        f"reason_code must match AgOutcomeRecord.reason_code. "
        f"Got {out['reason_code']!r}."
    )
    assert out["iteration"] == 1


def test_rolled_back_ag_outcome_produces_rolled_back_output() -> None:
    """When AgOutcome says rolled_back/missing_pre_rows, Phase H must agree."""
    from genie_space_optimizer.optimization.harness import (
        write_phase_h_acceptance_decision_output,
    )

    ag_outcome = AgOutcome(
        outcomes_by_ag={
            "AG_DECOMPOSED_H004": AgOutcomeRecord(
                ag_id="AG_DECOMPOSED_H004",
                outcome="rolled_back",
                reason_code="missing_pre_rows",
                target_qids=("gs_024",),
                affected_qids=("gs_024",),
            )
        },
    )
    out = write_phase_h_acceptance_decision_output(ag_outcome=ag_outcome, iteration=1)

    assert out["outcome"] == "rolled_back"
    assert out["reason_code"] == "missing_pre_rows"


def test_empty_outcomes_by_ag_returns_no_ags_sentinel() -> None:
    """Defensive: empty AgOutcome returns a no-ags sentinel, not a crash."""
    from genie_space_optimizer.optimization.harness import (
        write_phase_h_acceptance_decision_output,
    )

    ag_outcome = AgOutcome(outcomes_by_ag={})
    out = write_phase_h_acceptance_decision_output(ag_outcome=ag_outcome, iteration=2)
    assert out["outcome"] == "no_ags"
    assert out["iteration"] == 2


def test_ags_payload_carries_per_ag_records() -> None:
    """The ags field must contain one JSON dict per AgOutcomeRecord."""
    from genie_space_optimizer.optimization.harness import (
        write_phase_h_acceptance_decision_output,
    )

    ag_outcome = AgOutcome(
        outcomes_by_ag={
            "AG1": AgOutcomeRecord(
                ag_id="AG1",
                outcome="accepted_with_attribution_drift",
                reason_code="accepted_with_attribution_drift",
                target_qids=("gs_024",),
                affected_qids=("gs_024",),
            ),
        },
    )
    out = write_phase_h_acceptance_decision_output(ag_outcome=ag_outcome, iteration=3)
    assert len(out["ags"]) == 1
    assert out["ags"][0]["ag_id"] == "AG1"
    assert out["ags"][0]["outcome"] == "accepted_with_attribution_drift"
