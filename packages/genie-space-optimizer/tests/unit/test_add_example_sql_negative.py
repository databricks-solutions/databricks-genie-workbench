"""Phase 2 P2.4 — negative-example_sql patch type.

The new ``add_example_sql_negative`` patch type rides the same Genie
``example_question_sqls`` slot as the positive variant, so the applier
dispatches both to the same command shape; the differentiation lives
in:

  * the optimizer's audit ledger (``patch_type`` field on the
    ProposalAttempt) — postmortems can count positive vs negative
    independently;
  * the proposal-dict projection — :func:`to_proposal_dict` stamps a
    ``negative=True`` flag on the projected body for negative patches;
  * the applier command — the dispatched JSON command also carries
    ``negative=True`` so downstream audit readers know which slot was
    targeted.

The lever-contract maps both to lever-5b (the example-SQL family).
"""
from __future__ import annotations

import json

from genie_space_optimizer.optimization.levers_contract import (
    LEVER_TO_PATCH_TYPES,
    infer_lever_from_patch_type,
)
from genie_space_optimizer.optimization.repair_intent import (
    PatchType,
    RepairShape,
)
from genie_space_optimizer.optimization.repair_proposal_typed import (
    RepairProposal,
    required_patch_body_fields,
)


def test_patch_type_enum_value() -> None:
    assert PatchType.ADD_EXAMPLE_SQL_NEGATIVE.value == "add_example_sql_negative"
    # Coerce from raw string works through the closed enum constructor.
    assert PatchType("add_example_sql_negative") is PatchType.ADD_EXAMPLE_SQL_NEGATIVE


def test_negative_patch_shares_required_body_fields_with_positive() -> None:
    pos = required_patch_body_fields(PatchType.ADD_EXAMPLE_SQL)
    neg = required_patch_body_fields(PatchType.ADD_EXAMPLE_SQL_NEGATIVE)
    assert pos == neg
    assert "example_question" in neg and "example_sql" in neg


def test_negative_patch_projection_stamps_negative_flag() -> None:
    p = RepairProposal(
        intent_id="i",
        intent_name="n",
        intent_description="d",
        repair_shape=RepairShape.OTHER,
        patch_type=PatchType.ADD_EXAMPLE_SQL_NEGATIVE,
        rationale="r",
        confidence="medium",
        patch_body={
            "example_question": "what was the top-1 revenue region?",
            "example_sql": "SELECT region FROM sales LIMIT 1",
            "parameters": [],
            "usage_guidance": "DO NOT match top-N grammar with LIMIT 1",
        },
        blame_set=(),
    )
    body = p.to_proposal_dict()
    assert body["negative"] is True
    assert body["example_sql"] == "SELECT region FROM sales LIMIT 1"


def test_positive_patch_projection_does_not_stamp_negative_flag() -> None:
    p = RepairProposal(
        intent_id="i",
        intent_name="n",
        intent_description="d",
        repair_shape=RepairShape.OTHER,
        patch_type=PatchType.ADD_EXAMPLE_SQL,
        rationale="r",
        confidence="medium",
        patch_body={
            "example_question": "what was the top-3 revenue?",
            "example_sql": "SELECT region, SUM(r) FROM s GROUP BY region ORDER BY 2 DESC LIMIT 3",
            "parameters": [],
        },
        blame_set=(),
    )
    body = p.to_proposal_dict()
    assert "negative" not in body


def test_applier_dispatches_negative_to_same_slot_with_flag() -> None:
    # Mirror the applier's inline dispatcher: when patch_type is the
    # negative variant the forward command JSON must (a) target the
    # example_question_sqls section (same Genie slot as positive
    # examples) AND (b) carry the polarity flag so downstream audit
    # readers can route the outcome to the negative-example tracker.
    # We exercise the dispatch by reading the source-level branch and
    # asserting the contract is wired — a full applier integration
    # test belongs in the integration suite.
    import inspect

    from genie_space_optimizer.optimization import applier

    src = inspect.getsource(applier)
    # The dispatch branch matches both names AND stamps negative=True
    # for the negative variant.
    assert (
        'patch_type in ("add_example_sql", "add_example_sql_negative")' in src
    )
    assert "cmd_dict[\"negative\"] = True" in src
    # Sanity: example_question_sqls is the routed slot.
    assert '"section": "example_question_sqls"' in src
    # JSON import keeps the module-level import quiet.
    _ = json.dumps({"ok": True})


def test_levers_contract_maps_negative_to_lever_5() -> None:
    # The lever-5 family covers both positive and negative example
    # SQLs because they share the Genie slot.
    assert PatchType.ADD_EXAMPLE_SQL_NEGATIVE in LEVER_TO_PATCH_TYPES["lever-5"]
    assert infer_lever_from_patch_type("add_example_sql_negative") == "lever-5"
