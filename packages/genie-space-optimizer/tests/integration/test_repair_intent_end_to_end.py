"""Plan 1 Task 13 — End-to-end RepairIntent threading.

Verifies the typed RepairIntent contract holds across the F5→F7→F8
pipeline: a synthesizer stamps an intent; the proposal slate carrier
captures it; application carries intent_id; acceptance produces an
IntentOutcome carrying the SAME intent_id and the per-AG outcome.

This is the load-bearing test for Plan 1.
"""

from __future__ import annotations

from types import SimpleNamespace

from genie_space_optimizer.optimization.archetypes import ARCHETYPES
from genie_space_optimizer.optimization.cluster_driven_synthesis import (
    stamp_proposals_from_archetype,
)
from genie_space_optimizer.optimization.failure_cluster import FailureCluster
from genie_space_optimizer.optimization.repair_intent import RepairShape
from genie_space_optimizer.optimization.stages.acceptance import (
    AcceptanceInput,
    decide,
)
from genie_space_optimizer.optimization.stages.application import (
    ApplicationInput,
    apply,
)
from genie_space_optimizer.optimization.stages.proposals import (
    ProposalsInput,
    generate as generate_proposals,
)


def _cluster() -> FailureCluster:
    return FailureCluster(
        cluster_id="H001",
        target_qids=("gs_009",),
        root_cause="plural_top_n_collapse",
        asi_failure_type="plural_top_n_collapse",
        failure_keys=("plural_top_n_collapse",),
        blame_set_raw=("flights.carrier",),
        blame_set_normalized=("flights.carrier",),
        rca_card_id="rca_v1",
        rca_card_summary="needs top-n shape",
        is_grounded=True,
    )


def _ctx():
    emitted: list = []
    return SimpleNamespace(
        run_id="run",
        iteration=2,
        decision_emit=lambda r: emitted.append(r),
        _emitted=emitted,
    )


def test_intent_threads_synthesis_to_acceptance() -> None:
    arch = next(a for a in ARCHETYPES if a.name == "top_n_by_metric")
    proposals: list[dict] = [
        {
            "proposal_id": "p1",
            "patch_type": "add_example_sql",
            "target_qids": ["gs_009"],
            "example_sql": (
                "SELECT carrier, SUM(delay_minutes) FROM flights "
                "GROUP BY carrier ORDER BY 2 DESC LIMIT 5"
            ),
        }
    ]
    stamp_proposals_from_archetype(
        proposals=proposals,
        archetype=arch,
        cluster=_cluster(),
        ag_id="AG_H001_L5",
    )
    intent_id = proposals[0]["intent_id"]

    proposal_slate = generate_proposals(
        _ctx(),
        ProposalsInput(proposals_by_ag={"AG_H001_L5": tuple(proposals)}),
    )
    assert intent_id in proposal_slate.repair_intents_by_id
    captured_intent = proposal_slate.repair_intents_by_id[intent_id]
    assert captured_intent.repair_shape is RepairShape.TOP_N_BY_METRIC

    applied = apply(
        _ctx(),
        ApplicationInput(
            applied_entries_by_ag={
                "AG_H001_L5": (
                    {"patch": proposal_slate.proposals_by_ag["AG_H001_L5"][0]},
                )
            },
            ags=({"ag_id": "AG_H001_L5"},),
        ),
    )
    assert intent_id in applied.applied_by_intent_id
    assert applied.applied_by_intent_id[intent_id].patch_type == "add_example_sql"

    outcome = decide(
        _ctx(),
        AcceptanceInput(
            applied_entries_by_ag={
                "AG_H001_L5": (
                    {"patch": proposal_slate.proposals_by_ag["AG_H001_L5"][0]},
                )
            },
            ags=({"id": "AG_H001_L5", "target_qids": ("gs_009",)},),
            baseline_accuracy=0.5,
            candidate_accuracy=0.6,
            pre_rows=({"question_id": "gs_009", "result_correctness": "no"},),
            post_rows=({"question_id": "gs_009", "result_correctness": "yes"},),
            min_gain_pp=0.0,
        ),
    )
    assert intent_id in outcome.intent_outcomes_by_id
    intent_outcome = outcome.intent_outcomes_by_id[intent_id]
    assert intent_outcome.ag_id == "AG_H001_L5"
    assert intent_outcome.outcome.startswith("accepted")
    assert intent_outcome.applied_at_iter == 2

    assert captured_intent.cluster_id == "H001"
    assert captured_intent.target_qids == ("gs_009",)


def test_legacy_unstamped_proposal_does_not_appear_in_carriers() -> None:
    """A proposal without ``intent_id`` flows through every stage but
    appears in zero typed carriers. Plan 1 tolerance invariant."""
    proposals: list[dict] = [
        {
            "proposal_id": "p_legacy",
            "patch_type": "add_example_sql",
            "target_qids": ["gs_009"],
            "example_sql": "SELECT 1",
        }
    ]
    proposal_slate = generate_proposals(
        _ctx(),
        ProposalsInput(proposals_by_ag={"AG_X": tuple(proposals)}),
    )
    assert proposal_slate.repair_intents_by_id == {}

    applied = apply(
        _ctx(),
        ApplicationInput(
            applied_entries_by_ag={
                "AG_X": (
                    {"patch": proposal_slate.proposals_by_ag["AG_X"][0]},
                )
            },
            ags=({"ag_id": "AG_X"},),
        ),
    )
    assert applied.applied_by_intent_id == {}

    outcome = decide(
        _ctx(),
        AcceptanceInput(
            applied_entries_by_ag={
                "AG_X": (
                    {"patch": proposal_slate.proposals_by_ag["AG_X"][0]},
                )
            },
            ags=({"id": "AG_X", "target_qids": ("gs_009",)},),
            baseline_accuracy=0.5,
            candidate_accuracy=0.6,
            pre_rows=({"question_id": "gs_009", "result_correctness": "no"},),
            post_rows=({"question_id": "gs_009", "result_correctness": "yes"},),
            min_gain_pp=0.0,
        ),
    )
    assert outcome.intent_outcomes_by_id == {}
