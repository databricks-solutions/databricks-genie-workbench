"""Plan 12 — _l6_dict_to_repair_proposal: convert the dict shape returned
by _proposal_from_structural_sql_candidate into a typed RepairProposal
that satisfies the survival contract."""


def _candidate_dict():
    return {
        "patch_type": "add_sql_snippet_filter",
        "lever": 6,
        "snippet_type": "filter",
        "display_name": "MTD filter",
        "alias": "mtd",
        "sql": "order_date >= DATE_TRUNC('month', CURRENT_DATE)",
        "synonyms": [],
        "instruction": "Filter to month-to-date",
        "target_table": "catalog.schema.orders",
        "rationale": "RCA structural SQL learning from gs_021",
        "affected_questions": ["gs_021"],
        "target_qids": ["gs_021"],
        "confidence": 0.85,
        "questions_fixed": 1,
        "validation_passed": True,
        "source": "rca_failed_question_sql",
        "source_question_id": "gs_021",
        "cluster_id": "H001",
    }


def test_adapter_carries_required_survival_fields():
    from genie_space_optimizer.optimization.optimizer import (
        _l6_dict_to_repair_proposal,
    )
    from genie_space_optimizer.optimization.repair_proposal_typed import (
        validate_survival_contract,
    )

    proposal = _l6_dict_to_repair_proposal(
        _candidate_dict(),
        intent_id="intent_l6_001",
        rca_card_id="rca_001",
        causal_target="catalog.schema.orders.order_date",
        repair_hypothesis="Replace trailing-30 with MTD",
    )

    assert proposal is not None
    assert proposal.intent_id == "intent_l6_001"
    assert proposal.target_qids == ("gs_021",)
    assert proposal.blame_set == ("catalog.schema.orders.order_date",)
    assert proposal.target_objects, "target_objects must be populated"
    result = validate_survival_contract(proposal)
    assert result.is_valid, (
        f"L6 adapter must produce survival-contract-valid proposals; "
        f"missing={result.missing_fields}"
    )


def test_adapter_returns_none_when_target_table_missing():
    from genie_space_optimizer.optimization.optimizer import (
        _l6_dict_to_repair_proposal,
    )
    cand = _candidate_dict()
    cand["target_table"] = ""
    cand["affected_questions"] = []
    cand["target_qids"] = []
    proposal = _l6_dict_to_repair_proposal(
        cand,
        intent_id="intent_l6_002",
        rca_card_id="",
        causal_target="",
        repair_hypothesis="",
    )
    assert proposal is None


def test_adapter_uses_target_table_as_target_object_identifier():
    from genie_space_optimizer.optimization.optimizer import (
        _l6_dict_to_repair_proposal,
    )
    proposal = _l6_dict_to_repair_proposal(
        _candidate_dict(),
        intent_id="intent_l6_003",
        rca_card_id="rca_001",
        causal_target="catalog.schema.orders.order_date",
        repair_hypothesis="MTD",
    )
    assert proposal.target_objects[0].identifier == "catalog.schema.orders"
