"""I23 — every blast-radius-dropped patch that has the Plan 12 required
fields MUST have a narrow-replacement attempt marker."""
from genie_space_optimizer.optimization.invariants import (
    check_i23_narrow_attempt_coverage,
)


def test_violation_when_drop_has_no_narrow_attempt():
    evidence = {
        "blast_radius_drop_records": [
            {
                "intent_id": "intent_001",
                "original_patch_type": "add_sql_snippet_filter",
                "causal_target": "catalog.schema.orders.col",
                "target_qids": ["gs_021"],
            },
        ],
        "plan11_narrow_replacement_markers": [],
    }
    violations = check_i23_narrow_attempt_coverage(evidence)
    assert len(violations) == 1
    assert violations[0]["invariant"] == "I23"
    assert violations[0]["missing_intent_id"] == "intent_001"


def test_green_when_drop_has_narrow_attempt():
    evidence = {
        "blast_radius_drop_records": [
            {
                "intent_id": "intent_001",
                "original_patch_type": "add_sql_snippet_filter",
                "causal_target": "catalog.schema.orders.col",
                "target_qids": ["gs_021"],
            },
        ],
        "plan11_narrow_replacement_markers": [
            {"patch_id": "intent_001", "outcome": "exhausted"},
        ],
    }
    assert check_i23_narrow_attempt_coverage(evidence) == []


def test_silent_when_no_drop_records():
    assert check_i23_narrow_attempt_coverage(
        {
            "blast_radius_drop_records": [],
            "plan11_narrow_replacement_markers": [],
        }
    ) == []


def test_silent_when_drop_record_lacks_required_fields():
    """Plan 12 only enforces narrow-attempt coverage on records that
    HAVE the required fields. Pre-Plan-12 records (without
    original_patch_type) are silently ignored."""
    evidence = {
        "blast_radius_drop_records": [
            {"intent_id": "old_001", "original_patch_type": ""},
        ],
        "plan11_narrow_replacement_markers": [],
    }
    assert check_i23_narrow_attempt_coverage(evidence) == []


def test_i23_in_high_tier():
    from genie_space_optimizer.optimization.contract_health import (
        HIGH_TIER_INVARIANT_IDS,
    )
    assert "I23" in HIGH_TIER_INVARIANT_IDS
