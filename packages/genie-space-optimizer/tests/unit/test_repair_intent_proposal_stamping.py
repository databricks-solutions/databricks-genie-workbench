"""Plan 1 Task 4 — proposal-dict stamping helper.

``stamp_repair_intent_on_proposal`` mutates a proposal dict (the
legacy shape every synthesizer produces today) to carry the typed
intent:

  * ``proposal["intent_id"] = intent.intent_id``
  * ``proposal["repair_intent"] = intent.to_json()``

Downstream stages read ``proposal["intent_id"]`` as the lookup key
into ``ProposalSlate.repair_intents_by_id``. The serialized form is
what Phase H capture writes to MLflow.

Stamping is idempotent. Stamping a different intent raises
``RepairIntentCollisionError``.
"""

from __future__ import annotations

import pytest

from genie_space_optimizer.optimization.repair_intent import (
    PatchType,
    RepairIntent,
    RepairIntentCollisionError,
    RepairShape,
    stamp_repair_intent_on_proposal,
)


def _intent(intent_id: str = "i1") -> RepairIntent:
    return RepairIntent(
        intent_id=intent_id,
        intent_name="top_n_by_metric",
        intent_description="Top-N shape.",
        repair_shape=RepairShape.TOP_N_BY_METRIC,
        patch_type=PatchType.ADD_EXAMPLE_SQL,
        rationale="r",
        confidence="medium",
        source="test",
        cluster_id="H001",
        target_qids=("gs_009",),
        blame_set=("t.c",),
        rca_card_id="rca",
        ag_id="AG_X",
    )


def test_stamping_adds_intent_id_and_serialized_payload() -> None:
    proposal: dict = {"proposal_id": "p1", "patch_type": "add_example_sql"}
    intent = _intent("i1")
    stamp_repair_intent_on_proposal(proposal, intent)
    assert proposal["intent_id"] == "i1"
    assert proposal["repair_intent"]["intent_id"] == "i1"
    assert proposal["repair_intent"]["repair_shape"] == "top_n_by_metric"


def test_stamping_preserves_existing_proposal_fields() -> None:
    proposal = {
        "proposal_id": "p1",
        "patch_type": "add_example_sql",
        "example_sql": "SELECT 1",
        "rca_id": "rca_v1",
    }
    intent = _intent("i1")
    stamp_repair_intent_on_proposal(proposal, intent)
    assert proposal["example_sql"] == "SELECT 1"
    assert proposal["rca_id"] == "rca_v1"


def test_stamping_is_idempotent_for_same_intent() -> None:
    proposal = {"proposal_id": "p1", "patch_type": "add_example_sql"}
    intent = _intent("i1")
    stamp_repair_intent_on_proposal(proposal, intent)
    stamp_repair_intent_on_proposal(proposal, intent)
    assert proposal["intent_id"] == "i1"


def test_stamping_different_intent_raises_collision() -> None:
    proposal = {"proposal_id": "p1", "patch_type": "add_example_sql"}
    a = _intent("i1")
    b = _intent("i2")
    stamp_repair_intent_on_proposal(proposal, a)
    with pytest.raises(RepairIntentCollisionError) as excinfo:
        stamp_repair_intent_on_proposal(proposal, b)
    msg = str(excinfo.value)
    assert "p1" in msg and "i1" in msg and "i2" in msg


def test_extract_intent_from_stamped_proposal_round_trips() -> None:
    from genie_space_optimizer.optimization.repair_intent import (
        extract_repair_intent_from_proposal,
    )
    proposal = {"proposal_id": "p1", "patch_type": "add_example_sql"}
    intent = _intent("i1")
    stamp_repair_intent_on_proposal(proposal, intent)
    extracted = extract_repair_intent_from_proposal(proposal)
    assert extracted == intent


def test_extract_intent_returns_none_for_unstamped_proposal() -> None:
    from genie_space_optimizer.optimization.repair_intent import (
        extract_repair_intent_from_proposal,
    )
    assert extract_repair_intent_from_proposal(
        {"proposal_id": "p1", "patch_type": "add_example_sql"}
    ) is None


def test_stamping_patch_type_must_match_intent_patch_type() -> None:
    """If proposal.patch_type and intent.patch_type disagree, raise."""
    from genie_space_optimizer.optimization.repair_intent import (
        RepairIntentPatchTypeMismatchError,
    )
    proposal = {"proposal_id": "p1", "patch_type": "add_sql_snippet_filter"}
    intent = _intent("i1")
    with pytest.raises(RepairIntentPatchTypeMismatchError):
        stamp_repair_intent_on_proposal(proposal, intent)
